package controller

import (
	"context"
	"fmt"
	"time"

	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	"github.com/go-logr/logr"
	"github.com/ramendr/mock-storage-operator/internal/volsync"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	requeueInterval    = 30 * time.Second
	vgrFinalizer       = "mock.storage.io/volumegroupreplication"
	remoteAddressKey   = "mock.storage.io/remote-address"
	remoteKeySecretKey = "mock.storage.io/remote-key-secret"
)

// VolumeGroupReplicationReconciler reconciles VolumeGroupReplication objects
type VolumeGroupReplicationReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	ProvisionerName string
}

// +kubebuilder:rbac:groups=replication.storage.openshift.io,resources=volumegroupreplications,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=replication.storage.openshift.io,resources=volumegroupreplications/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=replication.storage.openshift.io,resources=volumegroupreplications/finalizers,verbs=update
// +kubebuilder:rbac:groups=replication.storage.openshift.io,resources=volumegroupreplicationclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=volsync.backube,resources=replicationsources,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=volsync.backube,resources=replicationdestinations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=multicluster.x-k8s.io,resources=serviceexports,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *VolumeGroupReplicationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	logger.V(1).Info("Reconciling VolumeGroupReplication", "volumeGroupReplication", req.NamespacedName) // controller/volumegroupreplication_controller.go"
	vgr := &volrep.VolumeGroupReplication{}
	if err := r.Get(ctx, req.NamespacedName, vgr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Check if this VGR is for our provisioner
	vgrClass := &volrep.VolumeGroupReplicationClass{}
	if err := r.Get(ctx, types.NamespacedName{Name: vgr.Spec.VolumeGroupReplicationClassName}, vgrClass); err != nil {
		logger.Error(err, "Failed to get VolumeGroupReplicationClass")
		return ctrl.Result{}, err
	}

	// Check if this VGR is for our provisioner
	if vgrClass.Spec.Provisioner != r.ProvisionerName {
		logger.V(1).Info("VGR not for this provisioner, skipping",
			"provisioner", vgrClass.Spec.Provisioner,
			"expected", r.ProvisionerName)
		return ctrl.Result{}, nil
	}

	// Log whether this is a global or non-global VGR
	isGlobal := vgrClass.GetLabels()["ramendr.openshift.io/global"] == "true"
	logger.V(1).Info("Processing VGR", "provisioner", vgrClass.Spec.Provisioner, "global", isGlobal)

	// Handle deletion
	if !vgr.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, logger, vgr)
	}

	// Ensure finalizer is present
	if !controllerutil.ContainsFinalizer(vgr, vgrFinalizer) {
		controllerutil.AddFinalizer(vgr, vgrFinalizer)
		if err := r.Update(ctx, vgr); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	logger.V(1).Info("Reconciling", "as", vgr.Spec.ReplicationState)
	// Reconcile based on replication state
	switch vgr.Spec.ReplicationState {
	case volrep.Primary:
		return r.reconcilePrimary(ctx, logger, vgr, vgrClass)
	case volrep.Secondary:
		return r.reconcileSecondary(ctx, logger, vgr, vgrClass)
	default:
		logger.Error(fmt.Errorf("unknown replication state %q", vgr.Spec.ReplicationState),
			"spec.replicationState must be primary, secondary, or resync")
		return ctrl.Result{}, nil
	}
}

// ── PRIMARY ──────────────────────────────────────────────────────────────────

func (r *VolumeGroupReplicationReconciler) reconcilePrimary(
	ctx context.Context,
	logger logr.Logger,
	vgr *volrep.VolumeGroupReplication,
	vgrClass *volrep.VolumeGroupReplicationClass,
) (ctrl.Result, error) {
	logger.Info("Reconciling VolumeGroupReplication as primary")

	// Get PVCs based on selector
	if vgr.Spec.Source.Selector == nil {
		logger.Info("No PVC selector specified")
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}

	sel, err := metav1.LabelSelectorAsSelector(vgr.Spec.Source.Selector)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("invalid pvcSelector: %w", err)
	}

	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcList,
		client.MatchingLabelsSelector{Selector: sel},
	); err != nil {
		return ctrl.Result{}, err
	}

	// Get default configuration from VGRClass
	defaultSchedulingInterval := vgrClass.Spec.Parameters["schedulingInterval"]
	if defaultSchedulingInterval == "" || defaultSchedulingInterval == "0m" {
		defaultSchedulingInterval = "5m" // Default to 5 minutes
	}

	defaultStorageClassName := vgrClass.Spec.Parameters["storageClassName"]
	if defaultStorageClassName == "" {
		defaultStorageClassName = "standard"
	}

	// Create VolSync handler
	vsHandler := volsync.NewVSHandler(ctx, r.Client, logger, vgr, defaultSchedulingInterval)

	protectedPVCs := []corev1.LocalObjectReference{}
	var latestSync *metav1.Time

	logger.V(1).Info("Protecting PVCs", "pvcCount", len(pvcList.Items))
	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]

		// Skip PVCs owned by VolSync to avoid self-replication loops
		if isVolSyncOwned(pvc) {
			continue
		}

		// Get PSK secret name from parameters or use default
		pskSecretName := vgrClass.Spec.Parameters["pskSecretName"]
		if pskSecretName == "" {
			pskSecretName = "volsync-rsync-tls-" + vgr.Name
		}

		// Use Submariner service name for remote address
		// The remote service name follows the pattern: <service-name>.<namespace>.svc.clusterset.local
		remoteAddress := volsync.GetRemoteServiceNameForRDFromPVCName(pvc.Name, pvc.Namespace)

		// Get VolumeSnapshotClassName from parameters (optional)
		var volumeSnapshotClassName *string
		if vscName := vgrClass.Spec.Parameters["volumeSnapshotClassName"]; vscName != "" {
			volumeSnapshotClassName = &vscName
		}

		logger.V(1).Info("Protecting SRC PVC", "pvc.metadata", pvc.ObjectMeta)

		// Use VolSync handler to reconcile ReplicationSource (like Ramen's ReconcileRS)
		rs, err := vsHandler.ReconcileRS(
			pvc.Name,
			pvc.Namespace,
			remoteAddress,
			pskSecretName,
			pvc.Spec.StorageClassName,
			pvc.Spec.AccessModes,
			volumeSnapshotClassName,
		)
		if err != nil {
			return ctrl.Result{}, err
		}

		// Only add to protectedPVCs if RS was created (not nil)
		// RS will be nil if PVC is terminating
		if rs != nil {
			protectedPVCs = append(protectedPVCs, corev1.LocalObjectReference{Name: pvc.Name})

			// Get last sync time from ReplicationSource status
			if rs.Status != nil {
				latestSync = rs.Status.LastSyncTime
			}

			if latestSync != nil {
				// Initialize annotations map if it doesn't exist
				if pvc.Annotations == nil {
					pvc.Annotations = make(map[string]string)
				}
				pvc.Annotations["mock.storage.io/lastSyncTime"] = latestSync.String()
			}
		}
	}

	// Update status
	vgr.Status.State = volrep.PrimaryState
	vgr.Status.PersistentVolumeClaimsRefList = protectedPVCs
	vgr.Status.LastSyncTime = latestSync
	vgr.Status.ObservedGeneration = vgr.Generation

	// Set Completed condition
	setCondition(&vgr.Status.Conditions, "Completed", true,
		"Promoted",
		"volume group is promoted to primary and replicating to secondary",
		vgr.Generation)

	// Set Validated condition
	setCondition(&vgr.Status.Conditions, "Validated", true,
		"PrerequisiteMet",
		"volume group is validated and met all prerequisites",
		vgr.Generation)

	setCondition(&vgr.Status.Conditions, "Ready", true,
		"ReplicationDestinationsReady",
		"RS is created ad ready",
		vgr.Generation)

	if err := r.Status().Update(ctx, vgr); err != nil {
		return ctrl.Result{}, err
	}

	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]

		if err := r.Client.Update(ctx, pvc); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update lastSyncTime annotation: %w", err)
		}
	}

	logger.Info("Primary reconcile complete", "protectedPVCs", len(protectedPVCs))
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// ── SECONDARY ────────────────────────────────────────────────────────────────

func (r *VolumeGroupReplicationReconciler) reconcileSecondary(
	ctx context.Context,
	logger logr.Logger,
	vgr *volrep.VolumeGroupReplication,
	vgrClass *volrep.VolumeGroupReplicationClass,
) (ctrl.Result, error) {
	logger = logger.WithValues("vgr", vgr.Name, "vgrClass", vgrClass.Name)

	logger.V(1).Info("Reconciling VolumeGroupReplication as secondary")

	// Get PVCs based on selector (same as primary)
	if vgr.Spec.Source.Selector == nil {
		logger.Info("No PVC selector specified")
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}

	sel, err := metav1.LabelSelectorAsSelector(vgr.Spec.Source.Selector)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("invalid pvcSelector: %w", err)
	}

	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcList,
		client.MatchingLabelsSelector{Selector: sel},
	); err != nil {
		return ctrl.Result{}, err
	}

	if len(pvcList.Items) == 0 {
		logger.Info("No PVCs found matching selector")
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}

	logger.Info("Found PVCs matching selector", "count", len(pvcList.Items))

	// Get default configuration from VGRClass
	defaultSchedulingInterval := vgrClass.Spec.Parameters["schedulingInterval"]
	if defaultSchedulingInterval == "" {
		defaultSchedulingInterval = vgrClass.Spec.Parameters["schedule"]
	}
	if defaultSchedulingInterval == "" || defaultSchedulingInterval == "0m" {
		defaultSchedulingInterval = "5m" // Default to 5 minutes
	}

	defaultStorageClassName := vgrClass.Spec.Parameters["storageClassName"]
	if defaultStorageClassName == "" {
		defaultStorageClassName = "standard"
	}

	// Get default capacity from VGRClass parameters
	defaultCapacity := vgrClass.Spec.Parameters["capacity"]
	if defaultCapacity == "" {
		defaultCapacity = "1Gi"
	}

	// Get PSK secret name from parameters or use default
	pskSecretName := vgrClass.Spec.Parameters["pskSecretName"]
	if pskSecretName == "" {
		pskSecretName = "volsync-rsync-tls-secret"
	}

	serviceType := volsync.DefaultRsyncServiceType
	protectedPVCs := []corev1.LocalObjectReference{}
	allReady := true

	// Create VolSync handler for checking temporary PVCs
	vsHandler := volsync.NewVSHandler(ctx, r.Client, logger, vgr, defaultSchedulingInterval)

	// Check for temporary PVCs and restore them if VGR is in secondary state
	// This handles the case where a PVC was deleted on primary and we need to restore it from temp
	for _, pvc := range pvcList.Items {
		// Check if a temporary PVC exists for this PVC
		hasTempPVC, err := vsHandler.HasTemporaryPVC(pvc.Name, pvc.Namespace)
		if err != nil {
			logger.Error(err, "Failed to check for temporary PVC", "pvcName", pvc.Name)
			return ctrl.Result{}, err
		}

		logger.Info("Checking if a temporary PVC exists for this PVC", "pvcName", pvc.Name)
		if hasTempPVC {
			logger.Info("Found temporary PVC, restoring original PVC", "pvcName", pvc.Name)
			if err := vsHandler.RestorePVCFromTemporary(pvc.Name, pvc.Namespace); err != nil {
				logger.Error(err, "Failed to restore PVC from temporary", "pvcName", pvc.Name)
				return ctrl.Result{}, err
			}
			logger.Info("Successfully restored PVC from temporary", "pvcName", pvc.Name)
		}
	}

	for _, pvc := range pvcList.Items {
		if pvc.Status.Phase == corev1.ClaimLost {
			if pvc.Annotations != nil {
				if _, exists := pvc.Annotations["pv.kubernetes.io/bind-completed"]; exists {
					delete(pvc.Annotations, "pv.kubernetes.io/bind-completed")
					if err := r.Client.Update(ctx, &pvc); err != nil {
						return ctrl.Result{}, fmt.Errorf("failed to update lost PVC after removing bind-completed annotation: %w", err)
					}
					logger.Info("Removed bind-completed annotation from lost PVC; waiting for next reconcile")
				}
			}

			return ctrl.Result{}, fmt.Errorf("PVC in lost phase. Remove annotation. Reconcile again")
		}
		// Extract scheduling interval from annotation (default to 5m if not set)
		schedulingInterval := "5m"
		if interval, ok := pvc.Annotations["replication.storage.openshift.io/scheduling-interval"]; ok && interval != "" {
			schedulingInterval = interval
		}

		// Extract consistency group from label
		consistencyGroup := pvc.Labels["ramendr.openshift.io/consistency-group"]

		// Create VolSync handler with per-PVC scheduling interval
		vsHandler := volsync.NewVSHandler(ctx, r.Client, logger, vgr, schedulingInterval)

		// Parse capacity from PVC spec
		capacityQuantity := pvc.Spec.Resources.Requests[corev1.ResourceStorage]

		// Get storage class name from PVC spec
		storageClassName := ""
		if pvc.Spec.StorageClassName != nil {
			storageClassName = *pvc.Spec.StorageClassName
		}

		logger.V(1).Info("Protecting DST PVC", "pvc.metadata", pvc.ObjectMeta)

		// Use VolSync handler to reconcile ReplicationDestination
		rd, err := vsHandler.ReconcileRD(
			pvc.Name,
			pvc.Namespace,
			&capacityQuantity,
			&storageClassName,
			pvc.Spec.AccessModes,
			pskSecretName,
			&serviceType,
			consistencyGroup,
		)
		if err != nil {
			return ctrl.Result{}, err
		}

		if rd == nil {
			// RD not ready yet
			allReady = false
			continue
		}

		protectedPVCs = append(protectedPVCs, corev1.LocalObjectReference{Name: pvc.Name})

		// Log the address and key secret for user to copy to primary
		if rd.Status != nil && rd.Status.RsyncTLS != nil {
			if rd.Status.RsyncTLS.Address != nil && rd.Status.RsyncTLS.KeySecret != nil {
				logger.Info("ReplicationDestination ready",
					"pvc", pvc.Name,
					"address", *rd.Status.RsyncTLS.Address,
					"keySecret", *rd.Status.RsyncTLS.KeySecret)
			}
		}
	}

	// Update status
	vgr.Status.State = volrep.SecondaryState
	vgr.Status.PersistentVolumeClaimsRefList = protectedPVCs
	vgr.Status.ObservedGeneration = vgr.Generation

	msg := fmt.Sprintf("%d destination(s) ready", len(protectedPVCs))
	if !allReady {
		msg = "waiting for service addresses to be assigned"
	}
	setCondition(&vgr.Status.Conditions, "Ready", allReady, "ReplicationDestinationsReady", msg, vgr.Generation)

	if err := r.Status().Update(ctx, vgr); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("Secondary reconcile complete", "destinations", len(protectedPVCs), "allReady", allReady)

	if !allReady {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// ── DELETION ─────────────────────────────────────────────────────────────────

func (r *VolumeGroupReplicationReconciler) reconcileDelete(
	ctx context.Context,
	logger logr.Logger,
	vgr *volrep.VolumeGroupReplication,
) (ctrl.Result, error) {
	logger.Info("VolumeGroupReplication being deleted — cleaning up RS/RD/PVC resources by label")

	// Create VSHandler to delete resources by label
	vsHandler := volsync.NewVSHandler(ctx, r.Client, logger, vgr, "")

	// Delete all ReplicationSources with the owner label
	if err := vsHandler.DeleteRSByLabel(); err != nil {
		logger.Error(err, "Failed to delete ReplicationSources by label")
		return ctrl.Result{}, err
	}

	// Delete all ReplicationDestinations with the owner label
	if err := vsHandler.DeleteRDByLabel(); err != nil {
		logger.Error(err, "Failed to delete ReplicationDestinations by label")
		return ctrl.Result{}, err
	}

	// Delete PVCs only for secondary VGRs.
	// For primary, allow Ramen to handle PVC lifecycle after removing our PVC finalizers.
	if vgr.Spec.ReplicationState == volrep.Secondary {
		if err := vsHandler.DeletePVCsByLabel(); err != nil {
			logger.Error(err, "Failed to delete PVCs by label")
			return ctrl.Result{}, err
		}
	} else {
		logger.Info("Skipping PVC deletion during VGR delete because replication state is not secondary; removing PVC finalizers instead",
			"replicationState", vgr.Spec.ReplicationState)
		if err := vsHandler.RemoveFinalizersFromPVCsByLabel(); err != nil {
			logger.Error(err, "Failed to remove PVC finalizers by label")
			return ctrl.Result{}, err
		}
	}

	// Remove finalizer after cleanup
	controllerutil.RemoveFinalizer(vgr, vgrFinalizer)
	if err := r.Update(ctx, vgr); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.Info("VolumeGroupReplication deletion complete")
	return ctrl.Result{}, nil
}

// ── HELPERS ──────────────────────────────────────────────────────────────────

func isVolSyncOwned(pvc *corev1.PersistentVolumeClaim) bool {
	// Check if PVC was created by VolSync using the standard Kubernetes label
	if createdBy, ok := pvc.Labels["app.kubernetes.io/created-by"]; ok && createdBy == "volsync" {
		return true
	}
	return false
}

func setCondition(conditions *[]metav1.Condition, condType string, status bool, reason, message string, observedGeneration int64) {
	s := metav1.ConditionFalse
	if status {
		s = metav1.ConditionTrue
	}
	now := metav1.Now()
	for i, c := range *conditions {
		if c.Type == condType {
			(*conditions)[i].Status = s
			(*conditions)[i].Reason = reason
			(*conditions)[i].Message = message
			(*conditions)[i].LastTransitionTime = now
			(*conditions)[i].ObservedGeneration = observedGeneration
			return
		}
	}
	*conditions = append(*conditions, metav1.Condition{
		Type:               condType,
		Status:             s,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
		ObservedGeneration: observedGeneration,
	})
}

func (r *VolumeGroupReplicationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&volrep.VolumeGroupReplication{}).
		Complete(r)
}

// Made with Bob
