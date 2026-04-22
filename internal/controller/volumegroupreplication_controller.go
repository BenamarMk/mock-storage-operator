package controller

import (
	"context"
	"fmt"
	"time"

	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	"github.com/go-logr/logr"
	"github.com/ramendr/mock-storage-operator/internal/util"
	"github.com/ramendr/mock-storage-operator/internal/volsync"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	requeueInterval = 30 * time.Second

	vgrFinalizer = "mock.storage.io/volumegroupreplication"

	pvcAnnotationBindCompleted      = "pv.kubernetes.io/bind-completed"
	pvcAnnotationSchedulingInterval = "replication.storage.openshift.io/scheduling-interval"

	pvcLabelConsistencyGroup = "ramendr.openshift.io/consistency-group"

	CreatedByLabelKey          = "app.kubernetes.io/created-by"
	CreatedByLabelValueVolSync = "volsync"

	VGRGlobalLabelKey = "ramendr.openshift.io/global"

	mockProvisionerName = "mock.storage.io"
	remoteAddressKey    = "mock.storage.io/remote-address"
	remoteKeySecretKey  = "mock.storage.io/remote-key-secret"
	PVCConfigMapName    = "pvc-configmap"

	// Defaults
	defaultSchedulingInterval = "5m"
	defaultStorageClassName   = "standard"
	defaultCapacity           = "1Gi"

	vgrParamSchedulingInterval  = "schedulingInterval"
	vgrParamSchedule            = "schedule"
	vgrParamStorageClass        = "storageClassName"
	vgrParamCapacity            = "capacity"
	vgrParamPSKSecret           = "pskSecretName"
	vgrParamVolumeSnapshotClass = "volumeSnapshotClassName"
)

// VolumeGroupReplicationReconciler reconciles VolumeGroupReplication objects
type VolumeGroupReplicationReconciler struct {
	client.Client
	log    logr.Logger
	Scheme *runtime.Scheme
}

type VRGInstance struct {
	reconciler *VolumeGroupReplicationReconciler
	ctx        context.Context
	log        logr.Logger
	vgr        *volrep.VolumeGroupReplication
	vgrClass   *volrep.VolumeGroupReplicationClass
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
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=multicluster.x-k8s.io,resources=serviceexports,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *VolumeGroupReplicationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {

	logger := r.log.WithValues("vgr", req.NamespacedName, "rid", util.GetRID())

	logger.V(1).Info("Reconciling VolumeGroupReplication", "volumeGroupReplication", req.NamespacedName)

	vgr := &volrep.VolumeGroupReplication{}
	if err := r.Get(ctx, req.NamespacedName, vgr); err != nil {

		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	vgrClass := &volrep.VolumeGroupReplicationClass{}
	if err := r.Get(ctx, types.NamespacedName{Name: vgr.Spec.VolumeGroupReplicationClassName}, vgrClass); err != nil {
		logger.Error(err, "Failed to get VolumeGroupReplicationClass")

		return ctrl.Result{}, err
	}

	// Check if this VGR is for our provisioner
	if vgrClass.GetLabels()[VGRGlobalLabelKey] != "true" || vgr.Spec.External != true {
		logger.V(1).Info("VGR not for this provisioner, skipping", "provisioner", vgrClass.Spec.Provisioner)

		return ctrl.Result{}, nil
	}

	inst := &VRGInstance{
		reconciler: r,
		ctx:        ctx,
		log:        logger,
		vgr:        vgr,
		vgrClass:   vgrClass,
	}

	// Handle deletion
	if !vgr.DeletionTimestamp.IsZero() {

		return inst.reconcileDelete()
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
		return inst.reconcilePrimary()
	case volrep.Secondary:
		return inst.reconcileSecondary()
	default:
		logger.Error(fmt.Errorf("unknown replication state %q", vgr.Spec.ReplicationState),
			"spec.replicationState must be primary, secondary, or resync")

		return ctrl.Result{}, nil
	}
}

// reconcilePrimary handles VolumeGroupReplication in Primary state.
// It selects PVCs using the provided selector, skips VolSync-owned PVCs,
// and creates/updates ReplicationSources for each eligible PVC.
// It also tracks protected PVCs and updates VGR status with last sync time.
func (v *VRGInstance) reconcilePrimary() (ctrl.Result, error) {
	v.log.Info("Reconciling VolumeGroupReplication as primary")

	// Get PVCs based on selector
	pvcList, res, err := v.getPVCsFromSelector()
	if err != nil || res.RequeueAfter > 0 {
		return res, err
	}

	// Get default configuration from VGRClass
	schedulingInterval := v.getSchedulingInterval()

	// Get default storage class from VGRClass parameters
	// defaultStorageClassName := v.getStorageClassName()

	// Create VolSync handler
	vsHandler := volsync.NewVSHandler(v.ctx, v.reconciler.Client, v.log, v.vgr, schedulingInterval)

	protectedPVCs := []corev1.LocalObjectReference{}
	var latestSync *metav1.Time

	v.log.V(1).Info("Protecting PVCs", "pvcCount", len(pvcList.Items))

	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]

		// Skip PVCs owned by VolSync to avoid self-replication loops
		if isVolSyncOwned(pvc) {
			continue
		}

		// Get PSK secret name from parameters or use default
		pskSecretName := v.getPSKSecretName()

		// Use Submariner service name for remote address
		// The remote service name follows the pattern: <service-name>.<namespace>.svc.clusterset.local
		remoteAddress := volsync.GetRemoteServiceNameForRDFromPVCName(pvc.Name, pvc.Namespace)

		// Get VolumeSnapshotClassName from parameters (optional)
		volumeSnapshotClassName := v.getVolumeSnapshotClass()

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
			if rs.Status != nil && rs.Status.LastSyncTime != nil {
				if latestSync == nil || rs.Status.LastSyncTime.After(latestSync.Time) {
					latestSync = rs.Status.LastSyncTime
				}

			}
		}
	}

	// Update status
	v.vgr.Status.State = volrep.PrimaryState
	v.vgr.Status.PersistentVolumeClaimsRefList = protectedPVCs
	v.vgr.Status.LastSyncTime = latestSync
	v.vgr.Status.ObservedGeneration = v.vgr.Generation
	setCondition(&v.vgr.Status.Conditions, "Ready", len(protectedPVCs) > 0,
		"ReplicationSourcesCreated",
		fmt.Sprintf("%d ReplicationSource(s) active", len(protectedPVCs)))

	if err := v.reconciler.Status().Update(v.ctx, v.vgr); err != nil {

		return ctrl.Result{}, err
	}

	v.log.Info("Primary reconcile complete", "protectedPVCs", len(protectedPVCs))

	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// reconcileSecondary handles VolumeGroupReplication in Secondary state.
// It discovers PVCs via selector, restores from temporary PVCs if needed,
// and reconciles ReplicationDestinations. It tracks which destinations are
// ready and updates VGR status to indicate readiness for primary to connect.
func (v *VRGInstance) reconcileSecondary() (ctrl.Result, error) {
	v.log.V(1).Info("Reconciling as secondary")

	// Get PVCs based on selector (same as primary)
	pvcList, res, err := v.getPVCsFromSelector()
	if err != nil || res.RequeueAfter > 0 {
		return res, err
	}

	if len(pvcList.Items) == 0 {
		v.log.Info("No PVCs found matching selector")
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}

	v.log.Info("Found PVCs matching selector", "count", len(pvcList.Items))

	// Get default configuration from VGRClass
	schedulingInterval := v.getSchedulingInterval()

	// Get default storage class from VGRClass parameters
	// defaultStorageClassName := v.getStorageClassName()

	// Get default capacity from VGRClass parameters
	// defaultCapacity := v.getCapacity()

	// Get PSK secret name from parameters or use default
	pskSecretName := v.getPSKSecretName()

	serviceType := volsync.DefaultRsyncServiceType
	protectedPVCs := []corev1.LocalObjectReference{}
	allReady := true

	// Create VolSync handler for checking temporary PVCs
	vsHandler := volsync.NewVSHandler(v.ctx, v.reconciler.Client, v.log, v.vgr, schedulingInterval)

	// Check for temporary PVCs and restore them if VGR is in secondary state
	// This handles the case where a PVC was deleted on primary and we need to restore it from temp
	for _, pvc := range pvcList.Items {
		// Check if a temporary PVC exists for this PVC
		hasTempPVC, err := vsHandler.HasTemporaryPVC(pvc.Name, pvc.Namespace)
		if err != nil {
			v.log.Error(err, "Failed to check for temporary PVC", "pvcName", pvc.Name)

			return ctrl.Result{}, err
		}

		v.log.Info("Checking if a temporary PVC exists for this PVC", "pvcName", pvc.Name)
		if hasTempPVC {
			v.log.Info("Found temporary PVC, restoring original PVC", "pvcName", pvc.Name)
			if err := vsHandler.RestorePVCFromTemporary(pvc.Name, pvc.Namespace); err != nil {
				v.log.Error(err, "Failed to restore PVC from temporary", "pvcName", pvc.Name)

				return ctrl.Result{}, err
			}

			v.log.Info("Successfully restored PVC from temporary", "pvcName", pvc.Name)
		}
	}

	for _, pvc := range pvcList.Items {
		if pvc.Status.Phase == corev1.ClaimLost {
			if pvc.Annotations != nil {
				if _, exists := pvc.Annotations[pvcAnnotationBindCompleted]; exists {
					delete(pvc.Annotations, pvcAnnotationBindCompleted)
					if err := v.reconciler.Client.Update(v.ctx, &pvc); err != nil {

						return ctrl.Result{}, fmt.Errorf("failed to update lost PVC after removing bind-completed annotation: %w", err)
					}

					v.log.Info("Removed bind-completed annotation from lost PVC; waiting for next reconcile")
				}
			}

			return ctrl.Result{}, fmt.Errorf("PVC in lost phase. Remove annotation. Reconcile again")
		}

		// Extract scheduling interval from annotation (default to 5m if not set)
		schedulingInterval := defaultSchedulingInterval

		if interval, ok := pvc.Annotations[pvcAnnotationSchedulingInterval]; ok && interval != "" {
			schedulingInterval = interval
		}

		// Extract consistency group from label
		consistencyGroup := pvc.Labels[pvcLabelConsistencyGroup]

		// Create VolSync handler with per-PVC scheduling interval
		vsHandler := volsync.NewVSHandler(v.ctx, v.reconciler.Client, v.log, v.vgr, schedulingInterval)

		// Parse capacity from PVC spec
		capacityQuantity := pvc.Spec.Resources.Requests[corev1.ResourceStorage]

		// Get storage class name from PVC spec
		storageClassName := ""
		if pvc.Spec.StorageClassName != nil {
			storageClassName = *pvc.Spec.StorageClassName
		}

		v.log.V(1).Info("Protecting DST PVC", "pvc.metadata", pvc.ObjectMeta)

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
				v.log.Info("ReplicationDestination ready",
					"pvc", pvc.Name,
					"address", *rd.Status.RsyncTLS.Address,
					"keySecret", *rd.Status.RsyncTLS.KeySecret)
			}
		}
	}

	// Update status
	v.vgr.Status.State = volrep.SecondaryState
	v.vgr.Status.PersistentVolumeClaimsRefList = protectedPVCs
	v.vgr.Status.ObservedGeneration = v.vgr.Generation

	msg := fmt.Sprintf("%d destination(s) ready", len(protectedPVCs))

	if !allReady {
		msg = "waiting for service addresses to be assigned"
	}

	setCondition(&v.vgr.Status.Conditions, "Ready", allReady, "ReplicationDestinationsReady", msg)

	if err := v.reconciler.Status().Update(v.ctx, v.vgr); err != nil {

		return ctrl.Result{}, err
	}

	v.log.Info("Secondary reconcile complete", "destinations", len(protectedPVCs), "allReady", allReady)

	if !allReady {

		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// reconcileDelete handles VGR deletion by removing all associated
// ReplicationSources, ReplicationDestinations, and PVCs using labels,
// and finally clears the finalizer to allow object deletion
func (v *VRGInstance) reconcileDelete() (ctrl.Result, error) {
	v.log.Info("VolumeGroupReplication being deleted — cleaning up RS/RD/PVC resources by label")

	// Create VSHandler to delete resources by label
	vsHandler := volsync.NewVSHandler(v.ctx, v.reconciler.Client, v.log, v.vgr, "")

	// Delete all ReplicationSources with the owner label
	if err := vsHandler.DeleteRSByLabel(); err != nil {
		v.log.Error(err, "Failed to delete ReplicationSources by label")

		return ctrl.Result{}, err
	}

	// Delete all ReplicationDestinations with the owner label
	if err := vsHandler.DeleteRDByLabel(); err != nil {
		v.log.Error(err, "Failed to delete ReplicationDestinations by label")

		return ctrl.Result{}, err
	}

	// Delete PVCs only for secondary VGRs.
	// For primary, allow Ramen to handle PVC lifecycle after removing our PVC finalizers.
	if v.vgr.Spec.ReplicationState == volrep.Secondary {
		if err := vsHandler.DeletePVCsByLabel(); err != nil {
			v.log.Error(err, "Failed to delete PVCs by label")

			return ctrl.Result{}, err
		}
	} else {
		v.log.Info("Skipping PVC deletion during VGR delete because replication state is not secondary; removing PVC finalizers instead",
			"replicationState", v.vgr.Spec.ReplicationState)
		if err := vsHandler.RemoveFinalizersFromPVCsByLabel(); err != nil {
			v.log.Error(err, "Failed to remove PVC finalizers by label")

			return ctrl.Result{}, err
		}
	}

	// Remove finalizer after successful cleanup
	controllerutil.RemoveFinalizer(v.vgr, vgrFinalizer)
	if err := v.reconciler.Update(v.ctx, v.vgr); err != nil {
		if errors.IsNotFound(err) {

			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	v.log.Info("VolumeGroupReplication deletion complete")

	return ctrl.Result{}, nil
}

// getPVCsFromSelector resolves the PVC selector from VGR spec and returns matching PVCs.
// It handles selector validation, conversion, and listing PVCs from the cluster.
func (v *VRGInstance) getPVCsFromSelector() (*corev1.PersistentVolumeClaimList, ctrl.Result, error) {
	if v.vgr.Spec.Source.Selector == nil {
		v.log.Info("No PVC selector specified")

		return nil, ctrl.Result{RequeueAfter: requeueInterval}, nil
	}

	sel, err := metav1.LabelSelectorAsSelector(v.vgr.Spec.Source.Selector)
	if err != nil {

		return nil, ctrl.Result{}, fmt.Errorf("invalid pvcSelector: %w", err)
	}

	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := v.reconciler.List(
		v.ctx,
		pvcList,
		client.MatchingLabelsSelector{Selector: sel},
	); err != nil {

		return nil, ctrl.Result{}, err
	}

	return pvcList, ctrl.Result{}, nil
}

// getSchedulingInterval resolves the scheduling interval from VGRClass parameters.
// Falls back to default if neither schedulingInterval nor schedule is provided.
func (v *VRGInstance) getSchedulingInterval() string {
	interval := v.vgrClass.Spec.Parameters[vgrParamSchedulingInterval]

	if interval == "" {
		interval = v.vgrClass.Spec.Parameters[vgrParamSchedule]
	}

	if interval == "" || interval == "0m" {
		interval = defaultSchedulingInterval // Default to 5 minutes
	}

	return interval
}

// getStorageClassName resolves the storage class from VGRClass parameters.
// Falls back to default storage class if not specified.
func (v *VRGInstance) getStorageClassName() string {
	storageClass := v.vgrClass.Spec.Parameters[vgrParamStorageClass]

	if storageClass == "" {
		storageClass = defaultStorageClassName
	}

	return storageClass
}

// getCapacity resolves the default capacity from VGRClass parameters.
// Falls back to default capacity if not specified.
func (v *VRGInstance) getCapacity() string {
	capacity := v.vgrClass.Spec.Parameters[vgrParamCapacity]

	if capacity == "" {
		capacity = defaultCapacity
	}

	return capacity
}

// getPSKSecretName resolves the PSK secret name from VGRClass parameters.
// Falls back to a generated name based on VGR if not specified.
func (v *VRGInstance) getPSKSecretName() string {
	psk := v.vgrClass.Spec.Parameters[vgrParamPSKSecret]

	if psk == "" {
		psk = "volsync-rsync-tls-" + v.vgr.Name
	}

	return psk
}

// getVolumeSnapshotClass resolves the VolumeSnapshotClass from VGRClass parameters.
// Returns nil if not specified (optional field).
func (v *VRGInstance) getVolumeSnapshotClass() *string {
	if vsc := v.vgrClass.Spec.Parameters[vgrParamVolumeSnapshotClass]; vsc != "" {

		return &vsc
	}

	return nil
}

// isVolSyncOwned checks whether the given PVC is managed by VolSync
func isVolSyncOwned(pvc *corev1.PersistentVolumeClaim) bool {
	// Check if PVC was created by VolSync using the standard Kubernetes label
	if pvc.Labels == nil {

		return false
	}

	return pvc.Labels[CreatedByLabelKey] == CreatedByLabelValueVolSync
}

func setCondition(conditions *[]metav1.Condition, condType string, status bool, reason, message string) {
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
			(*conditions)[i].ObservedGeneration = 0 // VGR doesn't track this in conditions
			return
		}
	}
	*conditions = append(*conditions, metav1.Condition{
		Type:               condType,
		Status:             s,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
		ObservedGeneration: 0,
	})
}

func (r *VolumeGroupReplicationReconciler) SetupWithManager(mgr ctrl.Manager, log logr.Logger) error {
	r.log = log
	return ctrl.NewControllerManagedBy(mgr).
		For(&volrep.VolumeGroupReplication{}).
		Complete(r)
}

// Made with Bob
