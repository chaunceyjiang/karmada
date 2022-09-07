package helper

import (
	"context"
	"encoding/json"
	"fmt"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/names"
)

// DenyReasonResourceSelectorsModify constructs a reason indicating that modify ResourceSelectors is not allowed.
const DenyReasonResourceSelectorsModify = "modify ResourceSelectors is forbidden"

// SetDefaultSpreadConstraints set default spread constraints if both 'SpreadByField' and 'SpreadByLabel' not set.
func SetDefaultSpreadConstraints(spreadConstraints []policyv1alpha1.SpreadConstraint) {
	for i := range spreadConstraints {
		if len(spreadConstraints[i].SpreadByLabel) == 0 && len(spreadConstraints[i].SpreadByField) == 0 {
			klog.Infof("Setting default SpreadByField with %s", policyv1alpha1.SpreadByFieldCluster)
			spreadConstraints[i].SpreadByField = policyv1alpha1.SpreadByFieldCluster
		}

		if spreadConstraints[i].MinGroups == 0 {
			klog.Infof("Setting default MinGroups to 1")
			spreadConstraints[i].MinGroups = 1
		}
	}
}

// ValidateSpreadConstraint tests if the constraints is valid.
func ValidateSpreadConstraint(spreadConstraints []policyv1alpha1.SpreadConstraint) error {
	spreadByFields := sets.NewString()

	for _, constraint := range spreadConstraints {
		// SpreadByField and SpreadByLabel should not co-exist
		if len(constraint.SpreadByField) > 0 && len(constraint.SpreadByLabel) > 0 {
			return fmt.Errorf("invalid constraints: SpreadByLabel(%s) should not co-exist with spreadByField(%s)", constraint.SpreadByLabel, constraint.SpreadByField)
		}

		// If MaxGroups provided, it should greater or equal than MinGroups.
		if constraint.MaxGroups > 0 && constraint.MaxGroups < constraint.MinGroups {
			return fmt.Errorf("maxGroups(%d) lower than minGroups(%d) is not allowed", constraint.MaxGroups, constraint.MinGroups)
		}

		if len(constraint.SpreadByField) > 0 {
			spreadByFields.Insert(string(constraint.SpreadByField))
		}
	}

	if spreadByFields.Len() > 0 {
		// If one of spread constraints are using 'SpreadByField', the 'SpreadByFieldCluster' must be included.
		// For example, when using 'SpreadByFieldRegion' to specify region groups, at the meantime, you must use
		// 'SpreadByFieldCluster' to specify how many clusters should be selected.
		if !spreadByFields.Has(string(policyv1alpha1.SpreadByFieldCluster)) {
			return fmt.Errorf("the cluster spread constraint must be enabled in one of the constraints in case of SpreadByField is enabled")
		}
	}

	return nil
}

// IsDependentOverridesPresent checks if a PropagationPolicy's dependent OverridePolicy all exist.
func IsDependentOverridesPresent(c client.Client, policy *policyv1alpha1.PropagationPolicy) (bool, error) {
	ns := policy.Namespace
	for _, override := range policy.Spec.DependentOverrides {
		exist, err := IsOverridePolicyExist(c, ns, override)
		if err != nil {
			return false, err
		}
		if !exist {
			return false, nil
		}
	}

	return true, nil
}

// IsDependentClusterOverridesPresent checks if a ClusterPropagationPolicy's dependent ClusterOverridePolicy all exist.
func IsDependentClusterOverridesPresent(c client.Client, policy *policyv1alpha1.ClusterPropagationPolicy) (bool, error) {
	for _, override := range policy.Spec.DependentOverrides {
		exist, err := IsClusterOverridePolicyExist(c, override)
		if err != nil {
			return false, err
		}
		if !exist {
			return false, nil
		}
	}

	return true, nil
}

// GetFollowedResourceSelectorsWhenMatchServiceImport get followed derived-service and endpointSlices resource selectors
// when policy's ResourceSelectors contains ResourceSelector, whose kind is ServiceImport.
func GetFollowedResourceSelectorsWhenMatchServiceImport(resourceSelectors []policyv1alpha1.ResourceSelector) []policyv1alpha1.ResourceSelector {
	var addedResourceSelectors []policyv1alpha1.ResourceSelector

	for _, resourceSelector := range resourceSelectors {
		if resourceSelector.Kind != util.ServiceImportKind {
			continue
		}

		if resourceSelector.Namespace == "" || resourceSelector.Name == "" {
			continue
		}

		addedResourceSelectors = append(addedResourceSelectors, GenerateResourceSelectorForServiceImport(resourceSelector)...)
	}

	return addedResourceSelectors
}

// GenerateResourceSelectorForServiceImport generates resource selectors for ServiceImport.
func GenerateResourceSelectorForServiceImport(svcImport policyv1alpha1.ResourceSelector) []policyv1alpha1.ResourceSelector {
	derivedServiceName := names.GenerateDerivedServiceName(svcImport.Name)
	return []policyv1alpha1.ResourceSelector{
		{
			APIVersion: "v1",
			Kind:       util.ServiceKind,
			Namespace:  svcImport.Namespace,
			Name:       derivedServiceName,
		},
		{
			APIVersion: "discovery.k8s.io/v1",
			Kind:       util.EndpointSliceKind,
			Namespace:  svcImport.Namespace,
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					discoveryv1.LabelServiceName: derivedServiceName,
				},
			},
		},
	}
}

// IsReplicaDynamicDivided checks if a PropagationPolicy schedules replicas as dynamic.
func IsReplicaDynamicDivided(strategy *policyv1alpha1.ReplicaSchedulingStrategy) bool {
	if strategy == nil || strategy.ReplicaSchedulingType != policyv1alpha1.ReplicaSchedulingTypeDivided {
		return false
	}

	switch strategy.ReplicaDivisionPreference {
	case policyv1alpha1.ReplicaDivisionPreferenceWeighted:
		return strategy.WeightPreference != nil && len(strategy.WeightPreference.DynamicWeight) != 0
	case policyv1alpha1.ReplicaDivisionPreferenceAggregated:
		return true
	default:
		return false
	}
}

// GetAppliedPlacement will get applied placement from annotations.
func GetAppliedPlacement(annotations map[string]string) (*policyv1alpha1.Placement, error) {
	appliedPlacement := util.GetLabelValue(annotations, util.PolicyPlacementAnnotation)
	if len(appliedPlacement) == 0 {
		return nil, nil
	}
	placement := &policyv1alpha1.Placement{}
	if err := json.Unmarshal([]byte(appliedPlacement), placement); err != nil {
		return nil, err
	}
	return placement, nil
}

// CreateOrUpdateOverridePolicy creates a overridePolicy object if not exist, or updates if it already exist.
func CreateOrUpdateOverridePolicy(client client.Client, policyMeta metav1.ObjectMeta, resource *unstructured.Unstructured, overrideRules []policyv1alpha1.RuleWithCluster) error {
	var err error
	overridePolicy := &policyv1alpha1.OverridePolicy{
		ObjectMeta: policyMeta,
		Spec: policyv1alpha1.OverrideSpec{
			ResourceSelectors: []policyv1alpha1.ResourceSelector{
				{
					APIVersion: resource.GetAPIVersion(),
					Kind:       resource.GetKind(),
					Namespace:  resource.GetNamespace(),
					Name:       resource.GetName(),
				},
			},
			OverrideRules: overrideRules,
		},
	}
	runtimeObject := overridePolicy.DeepCopy()
	var operationResult controllerutil.OperationResult
	err = retry.RetryOnConflict(retry.DefaultRetry, func() (err error) {
		operationResult, err = controllerutil.CreateOrUpdate(context.TODO(), client, runtimeObject, func() error {
			runtimeObject.Spec = overridePolicy.Spec
			runtimeObject.Labels = overridePolicy.Labels
			runtimeObject.Annotations = overridePolicy.Annotations
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		klog.Errorf("Failed to create/update overridePolicy %s/%s. Error: %v", overridePolicy.GetNamespace(), overridePolicy.GetName(), err)
		return err
	}

	if operationResult == controllerutil.OperationResultCreated {
		klog.V(2).Infof("Create overridePolicy %s/%s successfully.", overridePolicy.GetNamespace(), overridePolicy.GetName())
	} else if operationResult == controllerutil.OperationResultUpdated {
		klog.V(2).Infof("Update overridePolicy %s/%s successfully.", overridePolicy.GetNamespace(), overridePolicy.GetName())
	} else {
		klog.V(2).Infof("overridePolicy %s/%s is up to date.", overridePolicy.GetNamespace(), overridePolicy.GetName())
	}

	return nil
}
