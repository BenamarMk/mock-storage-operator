package volsync

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	// VRGOwnerLabel is used to label VolSync resources with their owner
	VRGOwnerLabel = "volumegroupreplication-owner"

	// PVCFinalizerName is the finalizer added to PVCs protected by replication
	PVCFinalizerName = "mock.storage.io/pvc-protection"

	// TemporaryPVCAnnotation marks a temporary PVC as non-reconcilable and restore-only
	TemporaryPVCAnnotation = "volumegroupreplication.ramendr.openshift.io/temporary-pvc"

	// SchedulingIntervalMinLength is the minimum length for scheduling interval
	SchedulingIntervalMinLength = 2

	// CronSpecMaxDayOfMonth is the maximum day of month for cron spec
	CronSpecMaxDayOfMonth = 28

	// tlsPSKDataSize is the size of the TLS pre-shared key data
	tlsPSKDataSize = 64

	// ServiceExport constants for Submariner
	ServiceExportKind    = "ServiceExport"
	ServiceExportGroup   = "multicluster.x-k8s.io"
	ServiceExportVersion = "v1alpha1"
)

var (
	// DefaultScheduleCronSpec is the default schedule for replication
	DefaultScheduleCronSpec = "*/5 * * * *" // Every 5 mins

	// DefaultRsyncServiceType is ClusterIP for use with Submariner
	DefaultRsyncServiceType corev1.ServiceType = corev1.ServiceTypeClusterIP
)

// VSHandler handles VolSync ReplicationSource and ReplicationDestination resources
type VSHandler struct {
	ctx                context.Context
	client             client.Client
	log                logr.Logger
	owner              metav1.Object
	schedulingInterval string
}

// NewVSHandler creates a new VolSync handler
func NewVSHandler(
	ctx context.Context,
	client client.Client,
	log logr.Logger,
	owner metav1.Object,
	schedulingInterval string,
) *VSHandler {
	return &VSHandler{
		ctx:                ctx,
		client:             client,
		log:                log,
		owner:              owner,
		schedulingInterval: schedulingInterval,
	}
}

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
		l.Info("Skipping ReplicationDestination reconcile for temporary PVC and ensure RS is deleted")
		// Delete the RS
		if err := v.DeleteRS(pvcName); err != nil {
			l.Error(err, "Failed to delete ReplicationSource for terminating PVC")
			return nil, err
		}

		return nil, nil
	}

	// Check if PVC is terminating - if so, create temporary PVC and delete the RS
	isTerminating, err := IsPVCTerminating(v.ctx, v.client, pvcName, pvcNamespace)
	if err != nil {
		l.Error(err, "Failed to check if PVC is terminating")
		return nil, err
	}
	if isTerminating {
		l.Info("App PVC is terminating, creating temporary PVC", "pvcName", pvcName)

		// Create temporary PVC from terminating PVC
		if err := v.CreateTemporaryPVCFromTerminating(pvcName, pvcNamespace, true); err != nil {
			l.Error(err, "Failed to create temporary PVC for terminating PVC")
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
	isTerminating, err := IsPVCTerminating(v.ctx, v.client, pvcName, pvcNamespace)
	if err != nil {
		l.Error(err, "Failed to check if PVC is terminating")
		return nil, err
	}
	if isTerminating {
		l.Info("PVC is terminating, creating temporary PVC and deleting ReplicationSource")
		return nil, nil

		// Create temporary PVC from terminating PVC
		// if err := v.CreateTemporaryPVCFromTerminating(pvcName, pvcNamespace); err != nil {
		// 	l.Error(err, "Failed to create temporary PVC for terminating PVC")
		// 	return nil, err
		// }

		// // Delete the RS
		// if err := v.DeleteRS(pvcName); err != nil {
		// 	l.Error(err, "Failed to delete ReplicationSource for terminating PVC")
		// 	return nil, err
		// }

		// return nil, nil
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
			Name:      GetReplicationSourceName(pvcName),
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

// validateSecretAndAddOwnerRef validates that a secret exists and adds owner reference
// The secret must be pre-created by the user
func (v *VSHandler) validateSecretAndAddOwnerRef(secretName, secretNamespace string) (bool, error) {
	secret := &corev1.Secret{}

	err := v.client.Get(v.ctx,
		types.NamespacedName{
			Name:      secretName,
			Namespace: secretNamespace,
		}, secret)
	if err != nil {
		if kerrors.IsNotFound(err) {
			v.log.Error(err, "Secret not found - must be pre-created", "secretName", secretName)
			return false, fmt.Errorf("secret %s not found in namespace %s - must be created before replication",
				secretName, secretNamespace)
		}

		v.log.Error(err, "Failed to get secret", "secretName", secretName)
		return false, fmt.Errorf("error getting secret (%w)", err)
	}

	v.log.Info("Secret exists", "secretName", secretName)

	// Add owner reference
	// if err := v.addOwnerReferenceAndUpdate(secret, v.owner); err != nil {
	// 	v.log.Error(err, "Unable to update secret", "secretName", secretName)
	// 	return true, err
	// }

	v.log.V(1).Info("VolSync secret validated", "secret name", secretName)

	return true, nil
}

// DeleteRS deletes a ReplicationSource by PVC name
func (v *VSHandler) DeleteRS(pvcName string) error {
	currentRSListByOwner, err := v.listRSByOwner()
	if err != nil {
		return err
	}

	for i := range currentRSListByOwner.Items {
		rs := currentRSListByOwner.Items[i]

		if rs.GetName() == GetReplicationSourceName(pvcName) {
			if err := v.client.Delete(v.ctx, &rs); err != nil {
				v.log.Error(err, "Error cleaning up ReplicationSource", "name", rs.GetName())
			} else {
				v.log.Info("Deleted ReplicationSource", "name", rs.GetName())
			}
		}
	}

	return nil
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

// listRSByOwner lists ReplicationSources owned by this VGR
func (v *VSHandler) listRSByOwner() (volsyncv1alpha1.ReplicationSourceList, error) {
	rsList := volsyncv1alpha1.ReplicationSourceList{}
	if err := v.listByOwner(&rsList); err != nil {
		v.log.Error(err, "Failed to list ReplicationSources", "owner", v.owner.GetName())
		return rsList, err
	}

	return rsList, nil
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

// listByOwner lists resources by owner label
func (v *VSHandler) listByOwner(list client.ObjectList) error {
	matchLabels := map[string]string{
		VRGOwnerLabel: v.owner.GetName(),
	}
	listOptions := []client.ListOption{
		client.MatchingLabels(matchLabels),
	}

	if err := v.client.List(v.ctx, list, listOptions...); err != nil {
		v.log.Error(err, "Failed to list by label", "matchLabels", matchLabels)
		return fmt.Errorf("error listing by label (%w)", err)
	}

	v.log.Info("Found resources by label", "matchLabels", matchLabels, "items", list)
	return nil
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

// addOwnerReferenceAndUpdate adds owner reference and updates the object
func (v *VSHandler) addOwnerReferenceAndUpdate(obj client.Object, owner metav1.Object) error {
	needsUpdate, err := v.addOwnerReference(obj, owner)
	if err != nil {
		return err
	}

	if needsUpdate {
		if err := v.client.Update(v.ctx, obj); err != nil {
			v.log.Error(err, "Failed to add owner reference to obj", "obj", obj.GetName())
			return fmt.Errorf("failed to add owner reference to %s (%w)", obj.GetName(), err)
		}

		v.log.Info("ownerRef added to object", "obj", obj.GetName())
	}

	return nil
}

// addOwnerReference adds an owner reference to an object
func (v *VSHandler) addOwnerReference(obj, owner metav1.Object) (bool, error) {
	currentOwnerRefs := obj.GetOwnerReferences()

	err := ctrlutil.SetOwnerReference(owner, obj, v.client.Scheme())
	if err != nil {
		return false, fmt.Errorf("%w", err)
	}

	// Check if owner references changed
	needsUpdate := len(obj.GetOwnerReferences()) != len(currentOwnerRefs)
	if !needsUpdate {
		for i := range obj.GetOwnerReferences() {
			if i >= len(currentOwnerRefs) || obj.GetOwnerReferences()[i] != currentOwnerRefs[i] {
				needsUpdate = true
				break
			}
		}
	}

	return needsUpdate, nil
}

// getScheduleCronSpec returns the schedule in cron format
func (v *VSHandler) getScheduleCronSpec() (*string, error) {
	if v.schedulingInterval != "" && v.schedulingInterval != "0m" {
		return ConvertSchedulingIntervalToCronSpec(v.schedulingInterval)
	}

	// Use default value if not specified
	v.log.Info("Warning - scheduling interval is empty/0, using default Schedule for volsync",
		"DefaultScheduleCronSpec", DefaultScheduleCronSpec)

	return &DefaultScheduleCronSpec, nil
}

// Helper functions

// addVRGOwnerLabel adds owner label to an object
func addVRGOwnerLabel(owner, obj metav1.Object) {
	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}

	labels[VRGOwnerLabel] = owner.GetName()
	obj.SetLabels(labels)
}

// getReplicationDestinationName returns the name for a ReplicationDestination
func getReplicationDestinationName(pvcName string) string {
	return pvcName // Use PVC name as name of ReplicationDestination
}

// GetReplicationSourceName returns the name for a ReplicationSource
func GetReplicationSourceName(pvcName string) string {
	return pvcName // Use PVC name as name of ReplicationSource
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
func IsPVCTerminating(ctx context.Context, c client.Client, pvcName, pvcNamespace string) (bool, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	err := c.Get(ctx, types.NamespacedName{
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

// CreateTemporaryPVCFromTerminating creates a temporary PVC from a terminating PVC
// and updates the PV claimRef to point to the temporary PVC
func (v *VSHandler) CreateTemporaryPVCFromTerminating(pvcName, pvcNamespace string, rmFinalizer bool) error {
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
	// tmpLabels := make(map[string]string)
	// if val, ok := pvc.Labels[VRGOwnerLabel]; ok {
	// 	tmpLabels[VRGOwnerLabel] = val
	// }
	// if val, ok := pvc.Labels["ramendr.openshift.io/consistency-group"]; ok {
	// 	tmpLabels["ramendr.openshift.io/consistency-group"] = val
	// }
	tmpLabels := pvc.Labels

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

	// Check if temporary PVC already exists
	existingTmpPVC := &corev1.PersistentVolumeClaim{}
	err = v.client.Get(v.ctx, types.NamespacedName{Name: tmpPVCName, Namespace: pvcNamespace}, existingTmpPVC)
	if err != nil {
		if kerrors.IsNotFound(err) {
			// Create the temporary PVC only if it doesn't exist
			err = v.client.Create(v.ctx, tmpPVC)
			if err != nil {
				return fmt.Errorf("failed to create temporary PVC: %w", err)
			}
			l.Info("Created temporary PVC", "tmpPVCName", tmpPVCName)
		} else {
			return fmt.Errorf("failed to check for existing temporary PVC: %w", err)
		}
	} else {
		l.Info("Temporary PVC already exists", "tmpPVCName", tmpPVCName)
	}

	// Update PV claimRef to point to the temporary PVC only if it doesn't already
	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Name != tmpPVCName || pv.Spec.ClaimRef.Namespace != pvcNamespace {
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
	} else {
		l.Info("PV claimRef already points to temporary PVC", "pvName", pv.Name, "tmpPVCName", tmpPVCName)
	}

	if !rmFinalizer {
		l.Info("Finalizer removal not required", "pvcName", pvcName)
		return nil
	}
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
			return fmt.Errorf("waiting for the PVC to go away: %s", pvcName)
		}

		l.Info("Main PVC is still terminating, skipping restore create until deletion completes", "pvcName", pvcName)
		return fmt.Errorf("no Finalizers. But PVC still exists. Waiting for the PVC to go away: %s", pvcName)
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

// Made with Bob

// ConvertSchedulingIntervalToCronSpec converts scheduling interval to cron spec
// Format: <num><m,h,d> where m=minutes, h=hours, d=days
// Example: "5m" -> "*/5 * * * *", "2h" -> "0 */2 * * *", "1d" -> "0 0 */1 * *"
func ConvertSchedulingIntervalToCronSpec(schedulingInterval string) (*string, error) {
	// format needs to have at least 1 number and end with m or h or d
	if len(schedulingInterval) < SchedulingIntervalMinLength {
		return nil, fmt.Errorf("scheduling interval %s is invalid", schedulingInterval)
	}

	mhd := schedulingInterval[len(schedulingInterval)-1:]
	mhd = string([]rune(mhd)[0]) // Get first character

	// Convert to lowercase
	if mhd == "M" {
		mhd = "m"
	} else if mhd == "H" {
		mhd = "h"
	} else if mhd == "D" {
		mhd = "d"
	}

	num := schedulingInterval[:len(schedulingInterval)-1]

	numInt := 0
	_, err := fmt.Sscanf(num, "%d", &numInt)
	if err != nil {
		return nil, fmt.Errorf("scheduling interval prefix %s cannot be converted to an int value", num)
	}

	var cronSpec string

	switch mhd {
	case "m":
		cronSpec = fmt.Sprintf("*/%s * * * *", num)
	case "h":
		// TODO: cronspec has a max here of 23 hours - do we try to convert into days?
		cronSpec = fmt.Sprintf("0 */%s * * *", num)
	case "d":
		if numInt > CronSpecMaxDayOfMonth {
			// Max # of days in interval we'll allow is 28 - otherwise there are issues converting to a cronspec
			// which is expected to be a day of the month (1-31).  I.e. if we tried to set to */31 we'd get
			// every 31st day of the month
			num = "28"
		}

		cronSpec = fmt.Sprintf("0 0 */%s * *", num)
	}

	if cronSpec == "" {
		return nil, fmt.Errorf("scheduling interval %s is invalid. Unable to parse m/h/d", schedulingInterval)
	}

	return &cronSpec, nil
}

// generateVolSyncReplicationSecret generates a new VolSync replication secret with PSK
func (v *VSHandler) generateVolSyncReplicationSecret(secretName string) (*corev1.Secret, error) {
	tlsKey, err := genTLSPreSharedKey(v.log)
	if err != nil {
		v.log.Error(err, "Unable to generate new tls secret for VolSync replication")
		return nil, err
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: v.owner.GetNamespace(),
		},
		StringData: map[string]string{
			"psk.txt": "volsyncmock:" + tlsKey,
		},
	}

	return secret, nil
}

// genTLSPreSharedKey generates a TLS pre-shared key
func genTLSPreSharedKey(log logr.Logger) (string, error) {
	pskData := make([]byte, tlsPSKDataSize)
	if _, err := rand.Read(pskData); err != nil {
		log.Error(err, "error generating tls key")
		return "", err
	}

	return hex.EncodeToString(pskData), nil
}
