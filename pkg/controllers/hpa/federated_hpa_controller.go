package hpa

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	workv1alpha1 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/fedinformer"
	"github.com/karmada-io/karmada/pkg/util/fedinformer/genericmanager"
	"github.com/karmada-io/karmada/pkg/util/fedinformer/keys"
	"github.com/karmada-io/karmada/pkg/util/helper"
	"github.com/karmada-io/karmada/pkg/util/names"
	"github.com/karmada-io/karmada/pkg/util/restmapper"
)

// FederatedControllerName is the controller name that will be used when reporting events.
const FederatedControllerName = "federated-hpa-controller"

// defaultFieldPath Default path of deployment
const defaultFieldPath = "/" + util.SpecField + "/" + util.ReplicasField

// FederatedHorizontalPodAutoscalerController is to sync HorizontalPodAutoscaler.
type FederatedHorizontalPodAutoscalerController struct {
	client.Client                                                       // used to operate HorizontalPodAutoscaler resources.
	DynamicClient           dynamic.Interface                           // used to fetch arbitrary resources from api server.
	InformerManager         genericmanager.SingleClusterInformerManager // used to fetch arbitrary resources from cache.
	EventRecorder           record.EventRecorder
	RESTMapper              meta.RESTMapper
	ClusterCacheSyncTimeout metav1.Duration
	worker                  util.AsyncWorker // worker process resources periodic from rateLimitingQueue.
	StopChan                <-chan struct{}
	SpecReplicasPathMap     map[schema.GroupVersionKind]string
}

// Reconcile performs a full reconciliation for the object referred to by the Request.
// The Controller will requeue the Request to be processed again if an error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (c *FederatedHorizontalPodAutoscalerController) Reconcile(ctx context.Context, req controllerruntime.Request) (controllerruntime.Result, error) {
	klog.V(4).Infof("Reconciling FederatedHorizontalPodAutoscalerController %s.", req.NamespacedName.String())

	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	if err := c.Client.Get(context.TODO(), req.NamespacedName, hpa); err != nil {
		// The resource may no longer exist, in which case we delete related works.
		if apierrors.IsNotFound(err) {
			if err := c.deleteWorks(names.GenerateWorkName(util.HorizontalPodAutoscalerKind, req.Name, req.Namespace)); err != nil {
				return controllerruntime.Result{Requeue: true}, err
			}
			return controllerruntime.Result{}, nil
		}
		return controllerruntime.Result{Requeue: true}, err
	}

	if !hpa.DeletionTimestamp.IsZero() {
		// Do nothing, just return.
		return controllerruntime.Result{}, nil
	}

	return c.syncHPA(hpa)
}

// syncHPA gets placement from propagationBinding according to targetRef in hpa, then builds works in target execution namespaces.
func (c *FederatedHorizontalPodAutoscalerController) syncHPA(hpa *autoscalingv2.HorizontalPodAutoscaler) (controllerruntime.Result, error) {
	unstructuredWorkLoad, bindingSpec, err := c.getResourceBinding(hpa.Spec.ScaleTargetRef, hpa.GetNamespace())
	if err != nil {
		klog.Errorf("Failed to get target placement by hpa %s/%s. Error: %v.", hpa.GetNamespace(), hpa.GetName(), err)
		return controllerruntime.Result{Requeue: true}, err
	}
	err = c.buildWorks(hpa, bindingSpec)
	if err != nil {
		klog.Errorf("Failed to build work for hpa %s/%s. Error: %v.", hpa.GetNamespace(), hpa.GetName(), err)
		return controllerruntime.Result{Requeue: true}, err
	}

	err = c.buildOverridePolicies(hpa, unstructuredWorkLoad, bindingSpec)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			klog.Errorf("Failed to build OverridePolicy for hpa %s/%s. Error: %v.", hpa.GetNamespace(), hpa.GetName(), err)
		}
		return controllerruntime.Result{Requeue: true}, err
	}
	err = c.registerInformersAndStart(hpa)
	if err != nil {
		klog.Errorf("Failed to register informer for WorkLoad %s/%s. Error: %v.", unstructuredWorkLoad.GetNamespace(), unstructuredWorkLoad.GetName(), err)
		return controllerruntime.Result{Requeue: true}, err
	}
	return controllerruntime.Result{}, nil
}

// getResourceBinding gets target clusters by CrossVersionObjectReference in hpa object. We can find
// the propagationBinding by resource with special naming rule, then get target clusters from propagationBinding.
func (c *FederatedHorizontalPodAutoscalerController) getResourceBinding(objRef autoscalingv2.CrossVersionObjectReference, namespace string) (*unstructured.Unstructured, *workv1alpha2.ResourceBindingSpec, error) {
	// according to targetRef, find the resource.
	dynamicResource, err := restmapper.GetGroupVersionResource(c.RESTMapper,
		schema.FromAPIVersionAndKind(objRef.APIVersion, objRef.Kind))
	if err != nil {
		klog.Errorf("Failed to get GVR from GVK %s %s. Error: %v", objRef.APIVersion, objRef.Kind, err)
		return nil, nil, err
	}

	// Kind in CrossVersionObjectReference is not equal to the kind in bindingName, need to get obj from cache.
	workload, err := c.InformerManager.Lister(dynamicResource).ByNamespace(namespace).Get(objRef.Name)
	if err != nil {
		// fall back to call api server in case the cache has not been synchronized yet
		klog.Warningf("Failed to get workload from cache, kind: %s, namespace: %s, name: %s. Error: %v. Fall back to call api server",
			objRef.Kind, namespace, objRef.Name, err)
		workload, err = c.DynamicClient.Resource(dynamicResource).Namespace(namespace).Get(context.TODO(),
			objRef.Name, metav1.GetOptions{})
		if err != nil {
			klog.Errorf("Failed to get workload from api server, kind: %s, namespace: %s, name: %s. Error: %v",
				objRef.Kind, namespace, objRef.Name, err)
			return nil, nil, err
		}
	}
	unstructuredWorkLoad, err := helper.ToUnstructured(workload)
	if err != nil {
		klog.Errorf("Failed to transform object(%s/%s): %v", namespace, objRef.Name, err)
		return nil, nil, err
	}
	bindingName := names.GenerateBindingName(unstructuredWorkLoad.GetKind(), unstructuredWorkLoad.GetName())
	binding := &workv1alpha2.ResourceBinding{}
	namespacedName := types.NamespacedName{
		Namespace: namespace,
		Name:      bindingName,
	}
	if err := c.Client.Get(context.TODO(), namespacedName, binding); err != nil {
		return nil, nil, err
	}
	bindingSpec := &binding.Spec
	sort.SliceStable(bindingSpec.Clusters, func(i, j int) bool {
		return bindingSpec.Clusters[i].Replicas > bindingSpec.Clusters[j].Replicas
	})
	return unstructuredWorkLoad, bindingSpec, nil
}

// buildWorks transforms hpa obj to unstructured, creates or updates Works in the target execution namespaces.
func (c *FederatedHorizontalPodAutoscalerController) buildWorks(hpa *autoscalingv2.HorizontalPodAutoscaler, bindingSpec *workv1alpha2.ResourceBindingSpec) error {
	hpaObj, err := helper.ToUnstructured(hpa)
	if err != nil {
		klog.Errorf("Failed to transform hpa %s/%s. Error: %v", hpa.GetNamespace(), hpa.GetName(), err)
		return err
	}

	for _, clusterName := range util.GetBindingClusterNames(bindingSpec) {
		workNamespace, err := names.GenerateExecutionSpaceName(clusterName)
		if err != nil {
			klog.Errorf("Failed to ensure Work for cluster: %s. Error: %v.", clusterName, err)
			return err
		}
		workName := names.GenerateWorkName(hpaObj.GetKind(), hpaObj.GetName(), hpa.GetNamespace())
		objectMeta := metav1.ObjectMeta{
			Name:       workName,
			Namespace:  workNamespace,
			Finalizers: []string{util.ExecutionControllerFinalizer},
			Annotations: map[string]string{
				util.FederatedHPANamespaceLabel: hpa.GetNamespace(),
				util.FederatedHPANameLabel:      hpa.GetName(),
			},
		}
		util.MergeLabel(hpaObj, workv1alpha1.WorkNamespaceLabel, workNamespace)
		util.MergeLabel(hpaObj, workv1alpha1.WorkNameLabel, workName)

		minReplicas := c.calculateMinReplicas(clusterName, hpa, bindingSpec)
		maxReplicas := c.calculateMaxReplicas(clusterName, hpa, bindingSpec)

		if err = helper.ApplyReplica(hpaObj, int64(minReplicas), util.MinReplicasField); err != nil {
			return err
		}
		if err = helper.ApplyReplica(hpaObj, int64(maxReplicas), util.MaxReplicasField); err != nil {
			return err
		}
		if err = helper.CreateOrUpdateWork(c.Client, objectMeta, hpaObj); err != nil {
			return err
		}
	}
	return nil
}

// buildOverridePolicies transforms hpa obj to unstructured, creates or updates overridePolicy in the target execution namespaces.
func (c *FederatedHorizontalPodAutoscalerController) buildOverridePolicies(hpa *autoscalingv2.HorizontalPodAutoscaler, unstructuredWorkLoad *unstructured.Unstructured, bindingSpec *workv1alpha2.ResourceBindingSpec) error {
	specReplicasPath, err := c.getWorkloadSpecReplicasPath(hpa, unstructuredWorkLoad)
	if err != nil {
		return err
	}
	hpaObj, err := helper.ToUnstructured(hpa)
	if err != nil {
		klog.Errorf("Failed to transform hpa %s/%s. Error: %v", hpa.GetNamespace(), hpa.GetName(), err)
		return nil
	}
	policyName := names.GeneratePolicyName(hpaObj.GetNamespace(), hpaObj.GetName(), hpaObj.GetObjectKind().GroupVersionKind().String())
	objectMeta := metav1.ObjectMeta{
		OwnerReferences: []metav1.OwnerReference{
			*metav1.NewControllerRef(hpa, hpa.GroupVersionKind()),
		},
		Name:      policyName,
		Namespace: hpa.GetNamespace(),
		Annotations: map[string]string{
			util.FederatedHPANamespaceLabel: hpa.GetNamespace(),
			util.FederatedHPANameLabel:      hpa.GetName(),
		},
	}

	var overrideRules []policyv1alpha1.RuleWithCluster
	var skipErr []error
	for _, clusterName := range util.GetBindingClusterNames(bindingSpec) {
		hpaStatus, err := c.reflectHPAStatus(hpa, clusterName)
		if err != nil {
			return err
		}
		ok, workLoadReplicas := c.calculateWorkLoadReplicas(hpaStatus)
		if !ok {
			skipErr = append(skipErr, fmt.Errorf("the number of scale replicas cannot be correctly extracted from the HPA status. Skip cluster %s", clusterName))
			klog.Infof(skipErr[len(skipErr)-1].Error())
			continue
		}

		marshal, err := json.Marshal(workLoadReplicas)
		if err != nil {
			skipErr = append(skipErr, fmt.Errorf("failed to marshal workLoadReplicas %v into JSON: %v", workLoadReplicas, err))
			klog.Infof(skipErr[len(skipErr)-1].Error())
			continue
		}
		overrideRules = append(overrideRules, policyv1alpha1.RuleWithCluster{
			TargetCluster: &policyv1alpha1.ClusterAffinity{ClusterNames: []string{clusterName}},
			Overriders: policyv1alpha1.Overriders{
				Plaintext: []policyv1alpha1.PlaintextOverrider{
					{
						Path:     specReplicasPath,
						Operator: policyv1alpha1.OverriderOpReplace,
						Value:    apiextensionsv1.JSON{Raw: marshal},
					},
				},
			},
		})
	}
	if len(overrideRules) > 0 {
		oldOverridePolicy := &policyv1alpha1.OverridePolicy{}
		if err = c.Client.Get(context.TODO(), types.NamespacedName{Namespace: hpa.GetNamespace(), Name: policyName}, oldOverridePolicy); err != nil {
			if apierrors.IsNotFound(err) {
				return helper.CreateOrUpdateOverridePolicy(c.Client, objectMeta, unstructuredWorkLoad, overrideRules)
			}
			return err
		}
		if !reflect.DeepEqual(oldOverridePolicy.Spec.OverrideRules, overrideRules) &&
			oldOverridePolicy.GetAnnotations()[util.FederatedHPANamespaceLabel] == hpa.GetNamespace() &&
			oldOverridePolicy.GetAnnotations()[util.FederatedHPANameLabel] == hpa.GetName() {
			return helper.CreateOrUpdateOverridePolicy(c.Client, objectMeta, unstructuredWorkLoad, overrideRules)
		}
		klog.Infof("Ignore updating OverridePolicy(%s/%s) because the OverRideRules of the OverridePolicy are the same", hpa.GetNamespace(), policyName)
		return nil
	}
	return utilerrors.NewAggregate(skipErr)
}

func (c *FederatedHorizontalPodAutoscalerController) getWorkloadSpecReplicasPath(hpa *autoscalingv2.HorizontalPodAutoscaler, unstructuredWorkLoad *unstructured.Unstructured) (string, error) {
	if c.SpecReplicasPathMap == nil {
		c.SpecReplicasPathMap = map[schema.GroupVersionKind]string{
			appsv1.SchemeGroupVersion.WithKind(util.DeploymentKind):  defaultFieldPath,
			appsv1.SchemeGroupVersion.WithKind(util.StatefulSetKind): defaultFieldPath,
			appsv1.SchemeGroupVersion.WithKind(util.ReplicaSetKind):  defaultFieldPath,
		}
	}
	if c.SpecReplicasPathMap[unstructuredWorkLoad.GroupVersionKind()] == "" {
		targetGV, err := schema.ParseGroupVersion(hpa.Spec.ScaleTargetRef.APIVersion)
		if err != nil {
			return "", err
		}
		crdList := &apiextensionsv1.CustomResourceDefinitionList{}
		if err := c.Client.List(context.TODO(), crdList); err != nil {
			return "", err
		}
		for _, crd := range crdList.Items {
			if crd.Spec.Group != targetGV.Group && crd.Spec.Names.Kind != hpa.Spec.ScaleTargetRef.Kind {
				continue
			}
			for _, version := range crd.Spec.Versions {
				if !version.Deprecated &&
					version.Name == targetGV.Version &&
					version.Subresources != nil && version.Subresources.Scale != nil {
					c.SpecReplicasPathMap[unstructuredWorkLoad.GroupVersionKind()] = version.Subresources.Scale.SpecReplicasPath
				}
			}
		}
		return "", fmt.Errorf("unrecognized resource")
	}
	return c.SpecReplicasPathMap[unstructuredWorkLoad.GroupVersionKind()], nil
}

func (c *FederatedHorizontalPodAutoscalerController) calculateMaxReplicas(clusterName string, hpa *autoscalingv2.HorizontalPodAutoscaler, bindingSpec *workv1alpha2.ResourceBindingSpec) int32 {
	if hpa.Spec.MaxReplicas <= int32(len(bindingSpec.Clusters)) {
		return 1
	}
	return c.calculateReplicas(clusterName, bindingSpec, hpa.Spec.MaxReplicas)
}

func (c *FederatedHorizontalPodAutoscalerController) calculateMinReplicas(clusterName string, hpa *autoscalingv2.HorizontalPodAutoscaler, bindingSpec *workv1alpha2.ResourceBindingSpec) int32 {
	if *hpa.Spec.MinReplicas <= int32(len(bindingSpec.Clusters)) {
		return 1
	}
	return c.calculateReplicas(clusterName, bindingSpec, *hpa.Spec.MinReplicas)
}

// calculateReplicas calculate HPA Min/MaxReplicas according to RB weight
func (c *FederatedHorizontalPodAutoscalerController) calculateReplicas(clusterName string, bindingSpec *workv1alpha2.ResourceBindingSpec, sum int32) int32 {
	targetClusters := util.DivideReplicasByTargetCluster(bindingSpec.Clusters, sum)
	for i := range targetClusters {
		if targetClusters[i].Name == clusterName && targetClusters[i].Replicas > 0 {
			return targetClusters[i].Replicas
		}
	}
	return 1
}

//calculateWorkLoadReplicas  calculate replicas of workload
func (c *FederatedHorizontalPodAutoscalerController) calculateWorkLoadReplicas(hpaStatus *autoscalingv2.HorizontalPodAutoscalerStatus) (bool, int32) {
	for _, condition := range hpaStatus.Conditions {
		if condition.Type == autoscalingv2.AbleToScale && condition.Status == corev1.ConditionTrue {
			switch condition.Reason {
			//https://github.com/kubernetes/kubernetes/blob/HEAD/pkg/controller/podautoscaler/horizontal.go#L694
			case "SucceededRescale", "ScaleDownStabilized", "ScaleUpStabilized", "ReadyForNewScale":
				//https://github.com/kubernetes/kubernetes/blob/HEAD/pkg/controller/podautoscaler/horizontal.go#L638-L630
				if hpaStatus.DesiredReplicas == 0 {
					return false, 0
				}
				return true, hpaStatus.DesiredReplicas
			default:
				return false, 0
			}
		}
	}
	return false, 0
}

// reflectHPAStatus collect HPA status from work status
func (c *FederatedHorizontalPodAutoscalerController) reflectHPAStatus(hpa *autoscalingv2.HorizontalPodAutoscaler, clusterName string) (*autoscalingv2.HorizontalPodAutoscalerStatus, error) {
	hpaObj, err := helper.ToUnstructured(hpa)
	if err != nil {
		klog.Errorf("Failed to transform hpa %s/%s. Error: %v", hpa.GetNamespace(), hpa.GetName(), err)
		return nil, err
	}
	workNamespace, err := names.GenerateExecutionSpaceName(clusterName)
	if err != nil {
		klog.Errorf("Failed to ensure Work for cluster: %s. Error: %v.", clusterName, err)
		return nil, err
	}
	work := &workv1alpha1.Work{}
	if err := c.Client.Get(context.TODO(), types.NamespacedName{
		Namespace: workNamespace,
		Name:      names.GenerateWorkName(hpaObj.GetKind(), hpaObj.GetName(), hpaObj.GetNamespace())}, work); err != nil {
		return nil, err
	}
	identifierIndex, err := helper.GetManifestIndex(work.Spec.Workload.Manifests, hpaObj)
	if err != nil {
		klog.Errorf("no such manifest exist")
		return nil, err
	}
	hpaStatus := &autoscalingv2.HorizontalPodAutoscalerStatus{}
	for _, manifestStatus := range work.Status.ManifestStatuses {
		equal, err := helper.EqualIdentifier(&manifestStatus.Identifier, identifierIndex, hpaObj)
		if err != nil {
			return nil, err
		}
		if equal {
			err = json.Unmarshal(manifestStatus.Status.Raw, hpaStatus)
			if err != nil {
				klog.Errorf("failed to unmarshal work manifests index %d, error is: %v", identifierIndex, err)
				return nil, err
			}
			break
		} else {
			klog.Infof("no such manifest exist")
		}
	}
	return hpaStatus, nil
}

func (c *FederatedHorizontalPodAutoscalerController) deleteWorks(workName string) error {
	workList := &workv1alpha1.WorkList{}
	var errs []error
	if err := c.List(context.TODO(), workList); err != nil {
		klog.Errorf("Failed to list works: %v.", err)
		return err
	}

	for i := range workList.Items {
		work := &workList.Items[i]
		if workName == work.Name &&
			work.GetAnnotations()[util.FederatedHPANameLabel] != "" &&
			work.GetAnnotations()[util.FederatedHPANamespaceLabel] != "" {
			if err := c.Client.Delete(context.TODO(), work); err != nil && !apierrors.IsNotFound(err) {
				klog.Errorf("Failed to delete work %s/%s: %v.", work.Namespace, work.Name, err)
				errs = append(errs, err)
			}
		}
	}
	return utilerrors.NewAggregate(errs)
}

func (c *FederatedHorizontalPodAutoscalerController) registerInformersAndStart(hpa *autoscalingv2.HorizontalPodAutoscaler) error {
	dynamicResource, err := restmapper.GetGroupVersionResource(c.RESTMapper,
		schema.FromAPIVersionAndKind(hpa.Spec.ScaleTargetRef.APIVersion, hpa.Spec.ScaleTargetRef.Kind))
	if err != nil {
		klog.Errorf("Failed to get GVR from GVK %s %s. Error: %v", hpa.Spec.ScaleTargetRef.APIVersion, hpa.Spec.ScaleTargetRef.Kind, err)
		return err
	}
	if c.InformerManager.IsInformerSynced(dynamicResource) {
		return nil
	}
	c.InformerManager.ForResource(dynamicResource, fedinformer.NewHandlerOnEvents(c.OnAdd, c.OnUpdate, c.OnDelete))

	c.InformerManager.Start()

	if err = func() error {
		synced := c.InformerManager.WaitForCacheSyncWithTimeout(c.ClusterCacheSyncTimeout.Duration)
		if synced == nil || !synced[dynamicResource] {
			return fmt.Errorf("informer for %s hasn't synced", dynamicResource)
		}
		return nil
	}(); err != nil {
		klog.Errorf("Failed to sync cache for control panel, error: %v", err)
		c.InformerManager.Stop()
		return err
	}
	return nil
}

// RunWorkQueue initializes worker and run it, worker will process resource asynchronously.
func (c *FederatedHorizontalPodAutoscalerController) RunWorkQueue() {
	workerOptions := util.Options{
		Name:          "federated-hpa",
		KeyFunc:       nil,
		ReconcileFunc: c.deleteOverRidePolicy,
	}
	c.worker = util.NewAsyncWorker(workerOptions)
	c.worker.Run(1, c.StopChan)
}

// OnAdd handles object add event and push the object to queue.
func (c *FederatedHorizontalPodAutoscalerController) OnAdd(obj interface{}) {
}

// OnUpdate handles object update event and push the object to queue.
func (c *FederatedHorizontalPodAutoscalerController) OnUpdate(oldObj, newObj interface{}) {
	klog.Errorf("===== Update event ========\n")
	unstructuredOldObj, err := helper.ToUnstructured(oldObj)
	if err != nil {
		klog.Errorf("Failed to transform oldObj, error: %v", err)
		return
	}

	unstructuredNewObj, err := helper.ToUnstructured(newObj)
	if err != nil {
		klog.Errorf("Failed to transform newObj, error: %v", err)
		return
	}
	fieldPath, exist := c.SpecReplicasPathMap[unstructuredNewObj.GroupVersionKind()]
	if !exist {
		return
	}

	klog.Errorf("=====oldObjSpec  %v ====\n", unstructuredOldObj.Object)
	klog.Errorf("=====newObjSpec  %v ====\n", unstructuredNewObj.Object)

	oldReplicas, ok, err := unstructured.NestedInt64(unstructuredOldObj.Object, strings.Split(strings.Trim(fieldPath, "/"), "/")...)
	if !ok || err != nil {
		return
	}
	newReplicas, ok, err := unstructured.NestedInt64(unstructuredNewObj.Object, strings.Split(strings.Trim(fieldPath, "/"), "/")...)
	if !ok || err != nil {
		return
	}
	klog.Errorf("==== %d =========%d ==========\n", oldReplicas, newReplicas)
	if oldReplicas == newReplicas {
		klog.V(4).Infof("Ignore update event of object (kind=%s, %s/%s) as specification no change", unstructuredOldObj.GetKind(), unstructuredOldObj.GetNamespace(), unstructuredOldObj.GetName())
		return
	}
	hpaList := &autoscalingv2.HorizontalPodAutoscalerList{}
	err = c.List(context.TODO(), hpaList, &client.ListOptions{Namespace: unstructuredNewObj.GetNamespace()})
	if err != nil {
		klog.Errorf("Failed to list works: %v.", err)
		return
	}
	for _, hpa := range hpaList.Items {
		if hpa.Spec.ScaleTargetRef.APIVersion == unstructuredNewObj.GetAPIVersion() &&
			hpa.Spec.ScaleTargetRef.Name == unstructuredNewObj.GetName() &&
			hpa.Spec.ScaleTargetRef.Kind == unstructuredNewObj.GetKind() {
			key, err := keys.ClusterWideKeyFunc(&hpa)
			if err != nil {
				return
			}
			c.worker.Add(key)
		}
	}
}

// OnDelete handles object delete event and push the object to queue.
func (c *FederatedHorizontalPodAutoscalerController) OnDelete(obj interface{}) {
}

func (c *FederatedHorizontalPodAutoscalerController) deleteOverRidePolicy(key util.QueueKey) error {
	ckey, ok := key.(keys.ClusterWideKey)
	if !ok { // should not happen
		klog.Error("Found invalid key when reconciling override policy.")
		return fmt.Errorf("invalid key")
	}

	policyName := names.GeneratePolicyName(ckey.Namespace, ckey.Name, schema.GroupVersionKind{
		Group:   ckey.Group,
		Version: ckey.Version,
		Kind:    ckey.Kind,
	}.String())
	overRidePolicy := &policyv1alpha1.OverridePolicy{}
	if err := c.Client.Get(context.TODO(), types.NamespacedName{Namespace: ckey.Namespace, Name: policyName}, overRidePolicy); err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("Failed to get overridePolicy(%s): %v", ckey.NamespaceKey(), err)
		return err
	}
	if err := c.Client.Delete(context.TODO(), overRidePolicy); err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("Failed to delete overridePolicy(%s): %v.", ckey.NamespaceKey(), err)
		return err
	}
	return nil
}

// SetupWithManager creates a controller and register to controller manager.
func (c *FederatedHorizontalPodAutoscalerController) SetupWithManager(mgr controllerruntime.Manager) error {
	var workPredicateFn = builder.WithPredicates(predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			var statusesOld, statusesNew workv1alpha1.WorkStatus

			switch oldWork := e.ObjectOld.(type) {
			case *workv1alpha1.Work:
				statusesOld = oldWork.Status
			default:
				return false
			}

			switch newWork := e.ObjectNew.(type) {
			case *workv1alpha1.Work:
				statusesNew = newWork.Status
			default:
				return false
			}
			var oldDesiredReplicas, newDesiredReplicas int32
			hpaStatus := &autoscalingv2.HorizontalPodAutoscalerStatus{}
			for _, manifestStatus := range statusesOld.ManifestStatuses {
				if err := json.Unmarshal(manifestStatus.Status.Raw, hpaStatus); err != nil {
					return true
				}
				oldDesiredReplicas = hpaStatus.DesiredReplicas
			}
			for _, manifestStatus := range statusesNew.ManifestStatuses {
				if err := json.Unmarshal(manifestStatus.Status.Raw, hpaStatus); err != nil {
					return true
				}
				newDesiredReplicas = hpaStatus.DesiredReplicas
			}
			return oldDesiredReplicas != newDesiredReplicas || newDesiredReplicas == 0
		},
		DeleteFunc: func(event.DeleteEvent) bool {
			return false
		},
		GenericFunc: func(event.GenericEvent) bool {
			return false
		},
	})
	var hpaPredicateFn = builder.WithPredicates(predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return e.Object.GetLabels()[util.FederatedHPAEnabledLabel] != ""
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectNew.GetLabels()[util.FederatedHPAEnabledLabel] != ""
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return e.Object.GetLabels()[util.FederatedHPAEnabledLabel] != ""
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return e.Object.GetLabels()[util.FederatedHPAEnabledLabel] != ""
		},
	})
	workFn := handler.MapFunc(
		func(a client.Object) []reconcile.Request {
			var requests []reconcile.Request
			annotations := a.GetAnnotations()
			crNamespace, namespaceExist := annotations[util.FederatedHPANamespaceLabel]
			crName, nameExist := annotations[util.FederatedHPANameLabel]
			if namespaceExist && nameExist {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Namespace: crNamespace,
						Name:      crName,
					},
				})
			}

			return requests
		})

	return controllerruntime.NewControllerManagedBy(mgr).
		For(&autoscalingv2.HorizontalPodAutoscaler{}, hpaPredicateFn).
		Watches(&source.Kind{Type: &workv1alpha1.Work{}}, handler.EnqueueRequestsFromMapFunc(workFn), workPredicateFn).
		Complete(c)
}
