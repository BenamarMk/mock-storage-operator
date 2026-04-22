package volsync

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// ensurePVCLabels ensures the PVC has required labels for replication
func (v *VSHandler) ensurePVCLabels(pvcName, pvcNamespace string) error {
	l := v.log.WithValues("pvcName", pvcName)

	pvc := &corev1.PersistentVolumeClaim{}
	err := v.client.Get(v.ctx, types.NamespacedName{
		Name:      pvcName,
		Namespace: pvcNamespace,
	}, pvc)
	if err != nil {
		if kerrors.IsNotFound(err) {
			l.V(1).Info("PVC not found, cannot add labels")
			return nil
		}
		return fmt.Errorf("failed to get PVC: %w", err)
	}

	// Check if labels and annotations need to be added
	needsUpdate := false

	// Ensure labels map exists
	if pvc.Labels == nil {
		pvc.Labels = make(map[string]string)
	}

	// Check and add VRG owner label
	if pvc.Labels[VRGOwnerLabel] != v.owner.GetName() {
		pvc.Labels[VRGOwnerLabel] = v.owner.GetName()
		needsUpdate = true
	}

	if needsUpdate {
		if err := v.client.Update(v.ctx, pvc); err != nil {
			return fmt.Errorf("failed to update PVC labels/annotations: %w", err)
		}
		l.Info("Added required labels and annotations to PVC")
	} else {
		l.V(1).Info("PVC already has required labels and annotations")
	}

	return nil
}

// DeletePVCsByLabel deletes all PVCs with the volumegroupreplication-owner label
func (v *VSHandler) DeletePVCsByLabel() error {
	pvcList := &corev1.PersistentVolumeClaimList{}

	// List PVCs with the owner label
	labelSelector := client.MatchingLabels{VRGOwnerLabel: v.owner.GetName()}
	if err := v.client.List(v.ctx, pvcList, labelSelector); err != nil {
		return fmt.Errorf("failed to list PVCs by label: %w", err)
	}

	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]

		if pvc.Spec.VolumeName != "" {
			pv := &corev1.PersistentVolume{}
			err := v.client.Get(v.ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, pv)
			if err != nil && !kerrors.IsNotFound(err) {
				v.log.Error(err, "Failed to get PV for PVC", "pvName", pvc.Spec.VolumeName, "pvcName", pvc.Name, "namespace", pvc.Namespace)
				return fmt.Errorf("failed to get PV %s for PVC %s/%s: %w", pvc.Spec.VolumeName, pvc.Namespace, pvc.Name, err)
			}

			if err == nil && pv.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimRetain {
				pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete
				if err := v.client.Update(v.ctx, pv); err != nil {
					v.log.Error(err, "Failed to update PV reclaim policy", "pvName", pv.Name, "pvcName", pvc.Name, "namespace", pvc.Namespace)
					return fmt.Errorf("failed to update PV %s reclaim policy for PVC %s/%s: %w", pv.Name, pvc.Namespace, pvc.Name, err)
				}
				v.log.Info("Updated PV reclaim policy from Retain to Delete", "pvName", pv.Name, "pvcName", pvc.Name, "namespace", pvc.Namespace)
			}
		}

		// Remove finalizer first to allow deletion
		if err := v.removeFinalizerFromPVC(pvc.Name, pvc.Namespace); err != nil {
			v.log.Error(err, "Failed to remove finalizer from PVC", "name", pvc.Name, "namespace", pvc.Namespace)
			// Continue with deletion attempt even if finalizer removal fails
		}

		v.log.Info("Deleting PVC", "name", pvc.Name, "namespace", pvc.Namespace)
		if err := v.client.Delete(v.ctx, pvc); err != nil && !kerrors.IsNotFound(err) {
			v.log.Error(err, "Failed to delete PVC", "name", pvc.Name, "namespace", pvc.Namespace)
			return fmt.Errorf("failed to delete PVC %s/%s: %w", pvc.Namespace, pvc.Name, err)
		}
	}

	v.log.Info("Deleted PVCs", "count", len(pvcList.Items))
	return nil
}

// RemoveFinalizersFromPVCsByLabel removes finalizers from all PVCs with the volumegroupreplication-owner label
// This is useful when transitioning from primary to secondary or vice versa
func (v *VSHandler) RemoveFinalizersFromPVCsByLabel() error {
	pvcList := &corev1.PersistentVolumeClaimList{}

	// List PVCs with the owner label
	labelSelector := client.MatchingLabels{VRGOwnerLabel: v.owner.GetName()}
	if err := v.client.List(v.ctx, pvcList, labelSelector); err != nil {
		return fmt.Errorf("failed to list PVCs by label: %w", err)
	}

	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		if err := v.removeFinalizerFromPVC(pvc.Name, pvc.Namespace); err != nil {
			v.log.Error(err, "Failed to remove finalizer from PVC", "name", pvc.Name, "namespace", pvc.Namespace)
			return fmt.Errorf("failed to remove finalizer from PVC %s/%s: %w", pvc.Namespace, pvc.Name, err)
		}
	}

	v.log.Info("Removed finalizers from PVCs", "count", len(pvcList.Items))
	return nil
}

// addFinalizerToPVC adds the PVC protection finalizer to a PVC
func (v *VSHandler) addFinalizerToPVC(pvcName, pvcNamespace string) error {
	l := v.log.WithValues("pvcName", pvcName, "namespace", pvcNamespace)

	pvc := &corev1.PersistentVolumeClaim{}
	err := v.client.Get(v.ctx, types.NamespacedName{
		Name:      pvcName,
		Namespace: pvcNamespace,
	}, pvc)
	if err != nil {
		if kerrors.IsNotFound(err) {
			l.V(1).Info("PVC not found, cannot add finalizer")
			return nil
		}
		return fmt.Errorf("failed to get PVC: %w", err)
	}

	// Check if finalizer already exists
	if ctrlutil.ContainsFinalizer(pvc, PVCFinalizerName) {
		l.V(1).Info("PVC already has finalizer")
		return nil
	}

	// Add finalizer
	ctrlutil.AddFinalizer(pvc, PVCFinalizerName)
	if err := v.client.Update(v.ctx, pvc); err != nil {
		return fmt.Errorf("failed to add finalizer to PVC: %w", err)
	}

	l.Info("Added finalizer to PVC", "finalizer", PVCFinalizerName)
	return nil
}

// removeFinalizerFromPVC removes the PVC protection finalizer from a PVC
func (v *VSHandler) removeFinalizerFromPVC(pvcName, pvcNamespace string) error {
	l := v.log.WithValues("pvcName", pvcName, "namespace", pvcNamespace)

	pvc := &corev1.PersistentVolumeClaim{}
	err := v.client.Get(v.ctx, types.NamespacedName{
		Name:      pvcName,
		Namespace: pvcNamespace,
	}, pvc)
	if err != nil {
		if kerrors.IsNotFound(err) {
			l.V(1).Info("PVC not found, cannot remove finalizer")
			return nil
		}
		return fmt.Errorf("failed to get PVC: %w", err)
	}

	// Check if finalizer exists
	if !ctrlutil.ContainsFinalizer(pvc, PVCFinalizerName) {
		l.V(1).Info("PVC does not have finalizer")
		return nil
	}

	// Remove finalizer
	ctrlutil.RemoveFinalizer(pvc, PVCFinalizerName)
	if err := v.client.Update(v.ctx, pvc); err != nil {
		return fmt.Errorf("failed to remove finalizer from PVC: %w", err)
	}

	l.Info("Removed finalizer from PVC", "finalizer", PVCFinalizerName)
	return nil
}

// isPVCTerminating checks if a PVC is in Terminating status (has DeletionTimestamp set)
func (v *VSHandler) isPVCTerminating(pvcName, pvcNamespace string) (bool, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	err := v.client.Get(v.ctx, types.NamespacedName{
		Name:      pvcName,
		Namespace: pvcNamespace,
	}, pvc)
	if err != nil {
		if kerrors.IsNotFound(err) {
			// PVC not found, consider it as not terminating
			return false, nil
		}
		return false, fmt.Errorf("failed to get PVC: %w", err)
	}

	// A PVC is terminating if it has a DeletionTimestamp set
	return !pvc.DeletionTimestamp.IsZero(), nil
}

// getPVFromPVC gets the PersistentVolume bound to a PVC
func (v *VSHandler) getPVFromPVC(pvcName, pvcNamespace string) (*corev1.PersistentVolume, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	err := v.client.Get(v.ctx, types.NamespacedName{
		Name:      pvcName,
		Namespace: pvcNamespace,
	}, pvc)
	if err != nil {
		return nil, fmt.Errorf("failed to get PVC: %w", err)
	}

	if pvc.Spec.VolumeName == "" {
		return nil, fmt.Errorf("PVC %s/%s is not bound to a PV", pvcNamespace, pvcName)
	}

	pv := &corev1.PersistentVolume{}
	err = v.client.Get(v.ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, pv)
	if err != nil {
		return nil, fmt.Errorf("failed to get PV %s: %w", pvc.Spec.VolumeName, err)
	}

	return pv, nil
}

// createTemporaryPVCFromTerminating creates a temporary PVC from a terminating PVC
// and updates the PV claimRef to point to the temporary PVC
func (v *VSHandler) createTemporaryPVCFromTerminating(pvcName, pvcNamespace string) error {
	l := v.log.WithValues("pvcName", pvcName, "namespace", pvcNamespace)

	// Get the terminating PVC
	pvc := &corev1.PersistentVolumeClaim{}
	err := v.client.Get(v.ctx, types.NamespacedName{
		Name:      pvcName,
		Namespace: pvcNamespace,
	}, pvc)
	if err != nil {
		return fmt.Errorf("failed to get terminating PVC: %w", err)
	}

	// Get the PV bound to this PVC
	pv, err := v.getPVFromPVC(pvcName, pvcNamespace)
	if err != nil {
		return fmt.Errorf("failed to get PV for terminating PVC: %w", err)
	}

	// Create temporary PVC name
	tmpPVCName := pvcName + "-tmp"

	// Filter annotations - keep only those starting with "apps.open-cluster-management.io"
	// and "volumereplicationgroups.ramendr.openshift.io/ramen-restore"
	tmpAnnotations := make(map[string]string)
	for key, value := range pvc.Annotations {
		if key == "volumereplicationgroups.ramendr.openshift.io/ramen-restore" {
			tmpAnnotations[key] = value
		} else if strings.HasPrefix(key, "apps.open-cluster-management.io") {
			tmpAnnotations[key] = value
		}
	}
	tmpAnnotations[TemporaryPVCAnnotation] = "not reconcilable; used only to hold the main PVC info for restore"

	// Filter labels - keep only "volumegroupreplication-owner" and "ramendr.openshift.io/consistency-group"
	tmpLabels := make(map[string]string)
	if val, ok := pvc.Labels[VRGOwnerLabel]; ok {
		tmpLabels[VRGOwnerLabel] = val
	}
	if val, ok := pvc.Labels["ramendr.openshift.io/consistency-group"]; ok {
		tmpLabels["ramendr.openshift.io/consistency-group"] = val
	}

	// Create the temporary PVC
	tmpPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        tmpPVCName,
			Namespace:   pvcNamespace,
			Annotations: tmpAnnotations,
			Labels:      tmpLabels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      pvc.Spec.AccessModes,
			Resources:        pvc.Spec.Resources,
			StorageClassName: pvc.Spec.StorageClassName,
			VolumeMode:       pvc.Spec.VolumeMode,
			VolumeName:       pv.Name, // Bind to the same PV
		},
	}

	// Create the temporary PVC
	err = v.client.Create(v.ctx, tmpPVC)
	if err != nil && !kerrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create temporary PVC: %w", err)
	}

	l.Info("Created temporary PVC", "tmpPVCName", tmpPVCName)

	// Update PV claimRef to point to the temporary PVC
	// Remove resourceVersion and UID from claimRef
	pv.Spec.ClaimRef = &corev1.ObjectReference{
		Kind:       "PersistentVolumeClaim",
		Namespace:  pvcNamespace,
		Name:       tmpPVCName,
		APIVersion: "v1",
	}

	err = v.client.Update(v.ctx, pv)
	if err != nil {
		return fmt.Errorf("failed to update PV claimRef: %w", err)
	}

	l.Info("Updated PV claimRef to point to temporary PVC", "pvName", pv.Name, "tmpPVCName", tmpPVCName)

	// Remove finalizer from the original terminating PVC
	err = v.removeFinalizerFromPVC(pvcName, pvcNamespace)
	if err != nil {
		return fmt.Errorf("failed to remove finalizer from terminating PVC: %w", err)
	}

	l.Info("Removed finalizer from terminating PVC", "pvcName", pvcName)

	return nil
}

// HasTemporaryPVC checks if a temporary PVC exists for the given PVC name
func (v *VSHandler) HasTemporaryPVC(pvcName, pvcNamespace string) (bool, error) {
	if strings.HasSuffix(pvcName, "-tmp") {
		return true, nil
	}

	tmpPVCName := pvcName + "-tmp"
	tmpPVC := &corev1.PersistentVolumeClaim{}
	err := v.client.Get(v.ctx, types.NamespacedName{
		Name:      tmpPVCName,
		Namespace: pvcNamespace,
	}, tmpPVC)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check for temporary PVC: %w", err)
	}
	return true, nil
}

// RestorePVCFromTemporary creates a new PVC from the temporary PVC and updates PV claimRef
// This is used when transitioning to secondary state
func (v *VSHandler) RestorePVCFromTemporary(pvcName, pvcNamespace string) error {
	l := v.log.WithValues("pvcName", pvcName, "namespace", pvcNamespace)

	l.V(1).Info("Temporary PVC restore...")

	tmpPVCName := pvcName + "-tmp"
	if strings.HasSuffix(pvcName, "-tmp") {
		tmpPVCName = pvcName
		pvcName = strings.TrimSuffix(tmpPVCName, "-tmp")
	}

	// Get the temporary PVC
	tmpPVC := &corev1.PersistentVolumeClaim{}
	err := v.client.Get(v.ctx, types.NamespacedName{
		Name:      tmpPVCName,
		Namespace: pvcNamespace,
	}, tmpPVC)
	if err != nil {
		if kerrors.IsNotFound(err) {
			l.V(1).Info("Temporary PVC not found, nothing to restore")
			return nil
		}
		return fmt.Errorf("failed to get temporary PVC: %w", err)
	}

	// Get the PV bound to the temporary PVC
	pv, err := v.getPVFromPVC(tmpPVCName, pvcNamespace)
	if err != nil {
		return fmt.Errorf("failed to get PV for temporary PVC: %w", err)
	}

	// If the main PVC still exists and is terminating, remove its finalizers and return.
	// Wait for deletion to complete before attempting to recreate it.
	existingPVC := &corev1.PersistentVolumeClaim{}
	err = v.client.Get(v.ctx, types.NamespacedName{
		Name:      pvcName,
		Namespace: pvcNamespace,
	}, existingPVC)
	if err != nil && !kerrors.IsNotFound(err) {
		return fmt.Errorf("failed to check existing PVC before restore: %w", err)
	}
	if err == nil && existingPVC.DeletionTimestamp != nil {
		if len(existingPVC.Finalizers) > 0 {
			existingPVC.Finalizers = nil
			if err := v.client.Update(v.ctx, existingPVC); err != nil {
				return fmt.Errorf("failed to remove finalizers from terminating PVC: %w", err)
			}
			l.Info("Removed finalizers from terminating PVC before restore", "pvcName", pvcName)
		}

		l.Info("Main PVC is still terminating, skipping restore create until deletion completes", "pvcName", pvcName)
		return nil
	}

	// Create the new PVC with the original name from the temporary PVC
	restoreAnnotations := make(map[string]string)
	for key, value := range tmpPVC.Annotations {
		if key == TemporaryPVCAnnotation || key == "pv.kubernetes.io/bind-completed" {
			continue
		}
		restoreAnnotations[key] = value
	}

	newPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        pvcName,
			Namespace:   pvcNamespace,
			Annotations: restoreAnnotations,
			Labels:      tmpPVC.Labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      tmpPVC.Spec.AccessModes,
			Resources:        tmpPVC.Spec.Resources,
			StorageClassName: tmpPVC.Spec.StorageClassName,
			VolumeMode:       tmpPVC.Spec.VolumeMode,
			VolumeName:       pv.Name, // Bind to the same PV
		},
	}

	// Create the new PVC
	err = v.client.Create(v.ctx, newPVC)
	if err != nil && !kerrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create restored PVC: %w", err)
	}

	l.Info("Created restored PVC from temporary PVC", "pvcName", pvcName, "tmpPVCName", tmpPVCName)

	// Update PV claimRef to point to the new PVC
	// Remove resourceVersion and UID from claimRef
	pv.Spec.ClaimRef = &corev1.ObjectReference{
		Kind:       "PersistentVolumeClaim",
		Namespace:  pvcNamespace,
		Name:       pvcName,
		APIVersion: "v1",
	}

	err = v.client.Update(v.ctx, pv)
	if err != nil {
		return fmt.Errorf("failed to update PV claimRef to restored PVC: %w", err)
	}

	l.Info("Updated PV claimRef to point to restored PVC", "pvName", pv.Name, "pvcName", pvcName)

	// Delete the temporary PVC
	err = v.client.Delete(v.ctx, tmpPVC)
	if err != nil && !kerrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete temporary PVC: %w", err)
	}

	err = v.removeFinalizerFromPVC(tmpPVC.Name, tmpPVC.Namespace)
	if err != nil {
		return fmt.Errorf("failed to remove finalizer from terminating PVC: %w", err)
	}

	l.Info("Deleted temporary PVC", "tmpPVCName", tmpPVCName)

	return nil
}
