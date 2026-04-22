package volsync

import (
	"fmt"
	"strings"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// ReconcileRD reconciles a ReplicationDestination for secondary cluster
func (v *VSHandler) ReconcileRD(
	pvcName, pvcNamespace string,
	capacity *resource.Quantity,
	storageClassName *string,
	accessModes []corev1.PersistentVolumeAccessMode,
	pskSecretName string,
	serviceType *corev1.ServiceType,
	consistencyGroup string,
) (*volsyncv1alpha1.ReplicationDestination, error) {
	l := v.log.WithValues("pvcName", pvcName)

	if strings.HasSuffix(pvcName, "-tmp") {
		l.Info("Skipping ReplicationDestination reconcile for temporary PVC by name")
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

	pvc := &corev1.PersistentVolumeClaim{}
	err = v.client.Get(v.ctx, types.NamespacedName{
		Name:      pvcName,
		Namespace: pvcNamespace,
	}, pvc)
	if err != nil {
		return nil, fmt.Errorf("failed to get PVC for ReplicationDestination reconcile: %w", err)
	}

	if _, isTemporaryPVC := pvc.Annotations[TemporaryPVCAnnotation]; isTemporaryPVC {
		l.Info("Skipping ReplicationDestination reconcile for temporary PVC")
		return nil, nil
	}

	// Validate that the PSK secret exists
	secretExists, err := v.validateSecretAndAddOwnerRef(pskSecretName, pvcNamespace)
	if err != nil || !secretExists {
		return nil, err
	}

	// Check if a ReplicationSource is still here (Can happen if transitioning from primary to secondary)
	// Before creating a new RD for this PVC, make sure any ReplicationSource for this PVC is cleaned up first
	err = v.DeleteRS(pvcName)
	if err != nil {
		return nil, err
	}

	// Create RD first (without PVC initially)
	rd, err := v.createOrUpdateRD(pvcName, pvcNamespace, capacity, storageClassName, accessModes, pskSecretName, serviceType)
	if err != nil {
		return nil, err
	}

	// Now create destination PVC (like Ramen's EnsurePVCforDirectCopy)
	// err = v.ensureDestinationPVC(pvcName, pvcNamespace, capacity, storageClassName, accessModes, consistencyGroup)
	// if err != nil {
	// 	return nil, err
	// }

	// Add finalizer to PVC for protection
	if err := v.addFinalizerToPVC(pvcName, pvcNamespace); err != nil {
		l.Error(err, "Failed to add finalizer to PVC")
		return nil, err
	}

	// Create ServiceExport for Submariner
	err = v.reconcileServiceExportForRD(rd)
	if err != nil {
		return nil, err
	}

	if !rdStatusReady(rd, l) {
		return nil, nil
	}

	l.V(1).Info("ReplicationDestination Reconcile Complete")

	return rd, nil
}

// rdStatusReady checks if ReplicationDestination is ready
func rdStatusReady(rd *volsyncv1alpha1.ReplicationDestination, log logr.Logger) bool {
	if rd.Status == nil {
		return false
	}

	if rd.Status.RsyncTLS == nil || rd.Status.RsyncTLS.Address == nil {
		log.V(1).Info("ReplicationDestination waiting for Address ...")
		return false
	}

	return true
}

// ensureDestinationPVC creates the destination PVC if it doesn't exist
// This is similar to Ramen's EnsurePVCforDirectCopy function
// Note: Ownership is set separately via setPVCOwnerIfNeeded
func (v *VSHandler) ensureDestinationPVC(
	pvcName, pvcNamespace string,
	capacity *resource.Quantity,
	storageClassName *string,
	accessModes []corev1.PersistentVolumeAccessMode,
	consistencyGroup string,
) error {
	l := v.log.WithValues("pvcName", pvcName)

	if len(accessModes) == 0 {
		return fmt.Errorf("accessModes must be provided for PVC %s", pvcName)
	}

	if capacity == nil {
		return fmt.Errorf("capacity must be provided for PVC %s", pvcName)
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: pvcNamespace,
		},
	}

	op, err := ctrlutil.CreateOrUpdate(v.ctx, v.client, pvc, func() error {
		// Set consistency group label if provided
		if consistencyGroup != "" {
			if pvc.Labels == nil {
				pvc.Labels = make(map[string]string)
			}
			pvc.Labels["ramendr.openshift.io/consistency-group"] = consistencyGroup
			pvc.Labels[VRGOwnerLabel] = v.owner.GetName()
		}

		// Only set spec fields if PVC is being created (not already exists)
		if pvc.CreationTimestamp.IsZero() {
			pvc.Spec.AccessModes = accessModes
			pvc.Spec.StorageClassName = storageClassName
			volumeMode := corev1.PersistentVolumeFilesystem
			pvc.Spec.VolumeMode = &volumeMode
		}

		// Always update capacity (can be expanded)
		pvc.Spec.Resources.Requests = corev1.ResourceList{
			corev1.ResourceStorage: *capacity,
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create/update destination PVC: %w", err)
	}

	l.V(1).Info("Destination PVC ensured", "operation", op, "pvc", pvcName)

	return nil
}

// createOrUpdateRD creates or updates a ReplicationDestination
func (v *VSHandler) createOrUpdateRD(
	pvcName, pvcNamespace string,
	capacity *resource.Quantity,
	storageClassName *string,
	accessModes []corev1.PersistentVolumeAccessMode,
	pskSecretName string,
	serviceType *corev1.ServiceType,
) (*volsyncv1alpha1.ReplicationDestination, error) {
	l := v.log.WithValues("pvcName", pvcName)

	if len(accessModes) == 0 {
		accessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}

	rd := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getReplicationDestinationName(pvcName),
			Namespace: pvcNamespace,
		},
	}

	op, err := ctrlutil.CreateOrUpdate(v.ctx, v.client, rd, func() error {
		addVRGOwnerLabel(v.owner, rd)

		rd.Spec.RsyncTLS = &volsyncv1alpha1.ReplicationDestinationRsyncTLSSpec{
			ServiceType: serviceType,
			KeySecret:   &pskSecretName,
			ReplicationDestinationVolumeOptions: volsyncv1alpha1.ReplicationDestinationVolumeOptions{
				CopyMethod:       volsyncv1alpha1.CopyMethodDirect,
				Capacity:         capacity,
				StorageClassName: storageClassName,
				AccessModes:      accessModes,
				DestinationPVC:   &pvcName, // Use the pre-created PVC as destination
			},
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}

	l.V(1).Info("ReplicationDestination createOrUpdate Complete", "op", op)

	return rd, nil
}

// DeleteRD deletes a ReplicationDestination by PVC name
func (v *VSHandler) DeleteRD(pvcName string) error {
	currentRDListByOwner, err := v.listRDByOwner()
	if err != nil {
		return err
	}

	for i := range currentRDListByOwner.Items {
		rd := currentRDListByOwner.Items[i]

		if rd.GetName() == getReplicationDestinationName(pvcName) {
			if err := v.client.Delete(v.ctx, &rd); err != nil {
				v.log.Error(err, "Error cleaning up ReplicationDestination", "name", rd.GetName())
			} else {
				v.log.Info("Deleted ReplicationDestination", "name", rd.GetName())
			}
		}
	}

	return nil
}

// reconcileServiceExportForRD creates a ServiceExport for the ReplicationDestination service
// This allows Submariner to export the service for cross-cluster access
func (v *VSHandler) reconcileServiceExportForRD(rd *volsyncv1alpha1.ReplicationDestination) error {
	// Using unstructured to avoid needing to require serviceexport in client scheme
	svcExport := &unstructured.Unstructured{}
	svcExport.Object = map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":      getLocalServiceNameForRD(rd.GetName()), // Get name of the local service (this needs to be exported)
			"namespace": rd.GetNamespace(),
		},
	}
	svcExport.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   ServiceExportGroup,
		Kind:    ServiceExportKind,
		Version: ServiceExportVersion,
	})

	op, err := ctrlutil.CreateOrUpdate(v.ctx, v.client, svcExport, func() error {
		// Make this ServiceExport owned by the replication destination itself rather than the VRG
		// This way on relocate scenarios or failover/failback, when the RD is cleaned up the associated
		// ServiceExport will get cleaned up with it.
		if err := ctrlutil.SetOwnerReference(rd, svcExport, v.client.Scheme()); err != nil {
			v.log.Error(err, "unable to set controller reference", "resource", svcExport)
			return fmt.Errorf("%w", err)
		}

		return nil
	})

	v.log.V(1).Info("ServiceExport createOrUpdate Complete", "op", op)

	if err != nil {
		v.log.Error(err, "error creating or updating ServiceExport", "replication destination name", rd.GetName(),
			"namespace", rd.GetNamespace())
		return fmt.Errorf("error creating or updating ServiceExport (%w)", err)
	}

	v.log.V(1).Info("ServiceExport Reconcile Complete")

	return nil
}

// listRDByOwner lists ReplicationDestinations owned by this VGR
func (v *VSHandler) listRDByOwner() (volsyncv1alpha1.ReplicationDestinationList, error) {
	rdList := volsyncv1alpha1.ReplicationDestinationList{}
	if err := v.listByOwner(&rdList); err != nil {
		v.log.Error(err, "Failed to list ReplicationDestinations", "owner", v.owner.GetName())
		return rdList, err
	}

	return rdList, nil
}

// DeleteRDByLabel deletes all ReplicationDestinations with the owner label
func (v *VSHandler) DeleteRDByLabel() error {
	rdList := &volsyncv1alpha1.ReplicationDestinationList{}
	if err := v.listByOwner(rdList); err != nil {
		return err
	}

	for i := range rdList.Items {
		rd := &rdList.Items[i]
		v.log.Info("Deleting ReplicationDestination", "name", rd.Name, "namespace", rd.Namespace)
		if err := v.client.Delete(v.ctx, rd); err != nil && !kerrors.IsNotFound(err) {
			v.log.Error(err, "Failed to delete ReplicationDestination", "name", rd.Name, "namespace", rd.Namespace)
			return fmt.Errorf("failed to delete ReplicationDestination %s/%s: %w", rd.Namespace, rd.Name, err)
		}
	}

	v.log.Info("Deleted ReplicationDestinations", "count", len(rdList.Items))
	return nil
}

// getReplicationDestinationName returns the name for a ReplicationDestination
func getReplicationDestinationName(pvcName string) string {
	return pvcName // Use PVC name as name of ReplicationDestination
}

// getLocalServiceNameForRD returns the local service name for a ReplicationDestination
// This is the name VolSync will use for the service
func getLocalServiceNameForRD(rdName string) string {
	return fmt.Sprintf("volsync-rsync-tls-dst-%s", rdName)
}

// GetRemoteServiceNameForRDFromPVCName returns the remote service name for cross-cluster access
// This assumes Submariner and that a ServiceExport is created for the service
func GetRemoteServiceNameForRDFromPVCName(pvcName, rdNamespace string) string {
	rdName := getReplicationDestinationName(pvcName)
	return fmt.Sprintf("%s.%s.svc.clusterset.local", getLocalServiceNameForRD(rdName), rdNamespace)
}
