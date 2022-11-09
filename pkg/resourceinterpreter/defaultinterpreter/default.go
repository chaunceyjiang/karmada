package defaultinterpreter

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"

	configv1alpha1 "github.com/karmada-io/karmada/pkg/apis/config/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/resourceinterpreter/framework"
	"github.com/karmada-io/karmada/pkg/util/fedinformer/genericmanager"
)

// DefaultInterpreter contains all default operation interpreter factory
// for interpreting common resource.
type DefaultInterpreter struct {
	replicaHandlers         map[schema.GroupVersionKind]replicaInterpreter
	reviseReplicaHandlers   map[schema.GroupVersionKind]reviseReplicaInterpreter
	retentionHandlers       map[schema.GroupVersionKind]retentionInterpreter
	aggregateStatusHandlers map[schema.GroupVersionKind]aggregateStatusInterpreter
	dependenciesHandlers    map[schema.GroupVersionKind]dependenciesInterpreter
	reflectStatusHandlers   map[schema.GroupVersionKind]reflectStatusInterpreter
	healthHandlers          map[schema.GroupVersionKind]healthInterpreter
}

// NewDefaultInterpreter return a new DefaultInterpreter.
func NewDefaultInterpreter(_ genericmanager.SingleClusterInformerManager) (framework.Interpreter, error) {
	return &DefaultInterpreter{
		replicaHandlers:         getAllDefaultReplicaInterpreter(),
		reviseReplicaHandlers:   getAllDefaultReviseReplicaInterpreter(),
		retentionHandlers:       getAllDefaultRetentionInterpreter(),
		aggregateStatusHandlers: getAllDefaultAggregateStatusInterpreter(),
		dependenciesHandlers:    getAllDefaultDependenciesInterpreter(),
		reflectStatusHandlers:   getAllDefaultReflectStatusInterpreter(),
		healthHandlers:          getAllDefaultHealthInterpreter(),
	}, nil
}

// HookEnabled tells if any hook exist for specific resource type and operation type.
func (e *DefaultInterpreter) HookEnabled(kind schema.GroupVersionKind, operationType configv1alpha1.InterpreterOperation) bool {
	switch operationType {
	case configv1alpha1.InterpreterOperationInterpretReplica:
		if _, exist := e.replicaHandlers[kind]; exist {
			return true
		}
	case configv1alpha1.InterpreterOperationReviseReplica:
		if _, exist := e.reviseReplicaHandlers[kind]; exist {
			return true
		}
	case configv1alpha1.InterpreterOperationRetain:
		if _, exist := e.retentionHandlers[kind]; exist {
			return true
		}
	case configv1alpha1.InterpreterOperationAggregateStatus:
		if _, exist := e.aggregateStatusHandlers[kind]; exist {
			return true
		}
	case configv1alpha1.InterpreterOperationInterpretDependency:
		if _, exist := e.dependenciesHandlers[kind]; exist {
			return true
		}
	case configv1alpha1.InterpreterOperationInterpretStatus:
		return true
	case configv1alpha1.InterpreterOperationInterpretHealth:
		if _, exist := e.healthHandlers[kind]; exist {
			return true
		}
		// TODO(RainbowMango): more cases should be added here
	}

	klog.V(4).Infof("Default interpreter is not enabled for kind %q with operation %q.", kind, operationType)
	return false
}

// GetReplicas returns the desired replicas of the object as well as the requirements of each replica.
func (e *DefaultInterpreter) GetReplicas(object *unstructured.Unstructured) (int32, *workv1alpha2.ReplicaRequirements, bool, error) {
	klog.V(4).Infof("Get replicas for object: %v %s/%s with build-in interpreter.", object.GroupVersionKind(), object.GetNamespace(), object.GetName())
	handler, exist := e.replicaHandlers[object.GroupVersionKind()]
	if !exist {
		return 0, nil, exist, nil
	}
	replicas, requirements, err := handler(object)

	return replicas, requirements, true, err
}

// ReviseReplica revises the replica of the given object.
func (e *DefaultInterpreter) ReviseReplica(object *unstructured.Unstructured, replica int64) (*unstructured.Unstructured, bool, error) {
	klog.V(4).Infof("Revise replicas for object: %v %s/%s with build-in interpreter.", object.GroupVersionKind(), object.GetNamespace(), object.GetName())
	handler, exist := e.reviseReplicaHandlers[object.GroupVersionKind()]
	if !exist {
		return nil, exist, nil
	}
	obj, err := handler(object, replica)

	return obj, true, err
}

// Retain returns the objects that based on the "desired" object but with values retained from the "observed" object.
func (e *DefaultInterpreter) Retain(desired *unstructured.Unstructured, observed *unstructured.Unstructured) (retained *unstructured.Unstructured, bool bool, err error) {
	klog.V(4).Infof("Retain object: %v %s/%s with build-in interpreter.", desired.GroupVersionKind(), desired.GetNamespace(), desired.GetName())
	handler, exist := e.retentionHandlers[desired.GroupVersionKind()]
	if !exist {
		return nil, exist, nil
	}
	retained, err = handler(desired, observed)

	return retained, true, err
}

// AggregateStatus returns the objects that based on the 'object' but with status aggregated.
func (e *DefaultInterpreter) AggregateStatus(object *unstructured.Unstructured, aggregatedStatusItems []workv1alpha2.AggregatedStatusItem) (*unstructured.Unstructured, bool, error) {
	klog.V(4).Infof("Aggregate status of object: %v %s/%s with build-in interpreter.", object.GroupVersionKind(), object.GetNamespace(), object.GetName())
	handler, exist := e.aggregateStatusHandlers[object.GroupVersionKind()]
	if !exist {
		return nil, exist, nil
	}
	obj, err := handler(object, aggregatedStatusItems)

	return obj, true, err
}

// GetDependencies returns the dependent resources of the given object.
func (e *DefaultInterpreter) GetDependencies(object *unstructured.Unstructured) (dependencies []configv1alpha1.DependentObjectReference, bool bool, err error) {
	klog.V(4).Infof("Get dependencies of object: %v %s/%s with build-in interpreter.", object.GroupVersionKind(), object.GetNamespace(), object.GetName())
	handler, exist := e.dependenciesHandlers[object.GroupVersionKind()]
	if !exist {
		return nil, exist, nil
	}
	references, err := handler(object)

	return references, true, err
}

// ReflectStatus returns the status of the object.
func (e *DefaultInterpreter) ReflectStatus(object *unstructured.Unstructured) (status *runtime.RawExtension, enabled bool, err error) {
	klog.V(4).Infof("Reflect status of object: %v %s/%s with build-in interpreter.", object.GroupVersionKind(), object.GetNamespace(), object.GetName())
	handler, exist := e.reflectStatusHandlers[object.GroupVersionKind()]
	if !exist {
		// for resource types that don't have a build-in handler, try to collect the whole status from '.status' filed.
		wholeStatus, err := reflectWholeStatus(object)
		return wholeStatus, true, err
	}
	status, err = handler(object)
	return status, true, err
}

// InterpretHealth returns the health state of the object.
func (e *DefaultInterpreter) InterpretHealth(object *unstructured.Unstructured) (bool, bool, error) {
	klog.V(4).Infof("Get health status of object: %v %s/%s with build-in interpreter.", object.GroupVersionKind(), object.GetNamespace(), object.GetName())
	handler, exist := e.healthHandlers[object.GroupVersionKind()]
	if !exist {
		return false, false, nil
	}
	healthy, err := handler(object)
	return healthy, true, err
}
