package volsync

import (
	"fmt"
	"strings"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// ReconcileRS reconciles a ReplicationSource for primary cluster
func (v *VSHandler) ReconcileRS(
	pvcName, pvcNamespace string,
	remoteAddress string,
	pskSecretName string,
	storageClassName *string,
	accessModes []corev1.PersistentVolumeAccessMode,
	volumeSnapshotClassName *string,
) (*volsyncv1alpha1.ReplicationSource, error) {
	l := v.log.WithValues("pvcName", pvcName)

	if strings.HasSuffix(pvcName, "-tmp") {
		l.Info("Skipping ReplicationSource reconcile for temporary PVC")
		return nil, nil
	}

	// Check if PVC is terminating - if so, create temporary PVC and delete the RS
	isTerminating, err := v.isPVCTerminating(pvcName, pvcNamespace)
	if err != nil {
		l.Error(err, "Failed to check if PVC is terminating")
		return nil, err
	}
	if isTerminating {
		l.Info("PVC is terminating, creating temporary PVC and deleting ReplicationSource")

		// Create temporary PVC from terminating PVC
		if err := v.createTemporaryPVCFromTerminating(pvcName, pvcNamespace); err != nil {
			l.Error(err, "Failed to create temporary PVC for terminating PVC")
			return nil, err
		}

		// Delete the RS
		if err := v.DeleteRS(pvcName); err != nil {
			l.Error(err, "Failed to delete ReplicationSource for terminating PVC")
			return nil, err
		}

		return nil, nil
	}

	// Validate that the PSK secret exists
	secretExists, err := v.validateSecretAndAddOwnerRef(pskSecretName, pvcNamespace)
	if err != nil || !secretExists {
		return nil, err
	}

	// Check if a ReplicationDestination is still here (Can happen if transitioning from secondary to primary)
	// Before creating a new RS for this PVC, make sure any ReplicationDestination for this PVC is cleaned up first
	err = v.DeleteRD(pvcName)
	if err != nil {
		return nil, err
	}

	// Ensure PVC has required labels before creating RS
	if err := v.ensurePVCLabels(pvcName, pvcNamespace); err != nil {
		return nil, err
	}

	replicationSource, err := v.createOrUpdateRS(pvcName, pvcNamespace, remoteAddress, pskSecretName, storageClassName, accessModes, volumeSnapshotClassName)
	if err != nil {
		return nil, err
	}

	// Add finalizer to PVC for protection
	if err := v.addFinalizerToPVC(pvcName, pvcNamespace); err != nil {
		l.Error(err, "Failed to add finalizer to PVC")
		return nil, err
	}

	l.V(1).Info("ReplicationSource Reconcile Complete")

	return replicationSource, nil
}

// createOrUpdateRS creates or updates a ReplicationSource
func (v *VSHandler) createOrUpdateRS(
	pvcName, pvcNamespace string,
	remoteAddress string,
	pskSecretName string,
	storageClassName *string,
	accessModes []corev1.PersistentVolumeAccessMode,
	volumeSnapshotClassName *string,
) (*volsyncv1alpha1.ReplicationSource, error) {
	l := v.log.WithValues("pvcName", pvcName)

	rs := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getReplicationSourceName(pvcName),
			Namespace: pvcNamespace,
		},
	}

	op, err := ctrlutil.CreateOrUpdate(v.ctx, v.client, rs, func() error {
		addVRGOwnerLabel(v.owner, rs)

		rs.Spec.SourcePVC = pvcName

		// Set schedule
		scheduleCronSpec, err := v.getScheduleCronSpec()
		if err != nil {
			l.Error(err, "unable to parse schedulingInterval")
			return err
		}
		rs.Spec.Trigger = &volsyncv1alpha1.ReplicationSourceTriggerSpec{
			Schedule: scheduleCronSpec,
		}

		rs.Spec.RsyncTLS = &volsyncv1alpha1.ReplicationSourceRsyncTLSSpec{
			KeySecret: &pskSecretName,
			Address:   &remoteAddress,
			ReplicationSourceVolumeOptions: volsyncv1alpha1.ReplicationSourceVolumeOptions{
				CopyMethod:       volsyncv1alpha1.CopyMethodDirect,
				StorageClassName: storageClassName,
				AccessModes:      accessModes,
			},
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}

	l.V(1).Info("ReplicationSource createOrUpdate Complete", "op", op)

	return rs, nil
}

// DeleteRS deletes a ReplicationSource by PVC name
func (v *VSHandler) DeleteRS(pvcName string) error {
	currentRSListByOwner, err := v.listRSByOwner()
	if err != nil {
		return err
	}

	for i := range currentRSListByOwner.Items {
		rs := currentRSListByOwner.Items[i]

		if rs.GetName() == getReplicationSourceName(pvcName) {
			if err := v.client.Delete(v.ctx, &rs); err != nil {
				v.log.Error(err, "Error cleaning up ReplicationSource", "name", rs.GetName())
			} else {
				v.log.Info("Deleted ReplicationSource", "name", rs.GetName())
			}
		}
	}

	return nil
}

// listRSByOwner lists ReplicationSources owned by this VGR
func (v *VSHandler) listRSByOwner() (volsyncv1alpha1.ReplicationSourceList, error) {
	rsList := volsyncv1alpha1.ReplicationSourceList{}
	if err := v.listByOwner(&rsList); err != nil {
		v.log.Error(err, "Failed to list ReplicationSources", "owner", v.owner.GetName())
		return rsList, err
	}

	return rsList, nil
}

// DeleteRSByLabel deletes all ReplicationSources with the owner label
func (v *VSHandler) DeleteRSByLabel() error {
	rsList := &volsyncv1alpha1.ReplicationSourceList{}
	if err := v.listByOwner(rsList); err != nil {
		return err
	}

	for i := range rsList.Items {
		rs := &rsList.Items[i]
		v.log.Info("Deleting ReplicationSource", "name", rs.Name, "namespace", rs.Namespace)
		if err := v.client.Delete(v.ctx, rs); err != nil && !kerrors.IsNotFound(err) {
			v.log.Error(err, "Failed to delete ReplicationSource", "name", rs.Name, "namespace", rs.Namespace)
			return fmt.Errorf("failed to delete ReplicationSource %s/%s: %w", rs.Namespace, rs.Name, err)
		}
	}

	v.log.Info("Deleted ReplicationSources", "count", len(rsList.Items))
	return nil
}

// getReplicationSourceName returns the name for a ReplicationSource
func getReplicationSourceName(pvcName string) string {
	return pvcName // Use PVC name as name of ReplicationSource
}
