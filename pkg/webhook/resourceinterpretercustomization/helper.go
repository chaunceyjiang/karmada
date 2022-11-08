package resourceinterpretercustomization

import (
	configv1alpha1 "github.com/karmada-io/karmada/pkg/apis/config/v1alpha1"
)

//nolint:gocyclo
func validateCustomizationRule(oldRules, newRules configv1alpha1.ResourceInterpreterCustomization) bool {
	if oldRules.Spec.Target.APIVersion != newRules.APIVersion ||
		oldRules.Spec.Target.Kind != newRules.Kind {
		return true
	}
	if oldRules.Name == newRules.Name {
		return true
	}
	if oldRules.Spec.Customizations.Retention != nil && newRules.Spec.Customizations.Retention != nil {
		return false
	}
	if oldRules.Spec.Customizations.ReplicaResource != nil && newRules.Spec.Customizations.ReplicaResource != nil {
		return false
	}
	if oldRules.Spec.Customizations.ReplicaRevision != nil && newRules.Spec.Customizations.ReplicaRevision != nil {
		return false
	}
	if oldRules.Spec.Customizations.StatusReflection != nil && newRules.Spec.Customizations.StatusReflection != nil {
		return false
	}
	if oldRules.Spec.Customizations.StatusAggregation != nil && newRules.Spec.Customizations.StatusAggregation != nil {
		return false
	}
	if oldRules.Spec.Customizations.HealthInterpretation != nil && newRules.Spec.Customizations.HealthInterpretation != nil {
		return false
	}
	if oldRules.Spec.Customizations.DependencyInterpretation != nil && newRules.Spec.Customizations.DependencyInterpretation != nil {
		return false
	}
	return true
}
