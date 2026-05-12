package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	"github.com/go-logr/logr"
	"github.com/ramendr/mock-storage-operator/internal/volsync"
	ramendrv1alpha1 "github.com/ramendr/ramen/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
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
// +kubebuilder:rbac:groups=ramendr.openshift.io,resources=volumereplicationgroups,verbs=get;list;watch
// +kubebuilder:rbac:groups=volsync.backube,resources=replicationsources,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=volsync.backube,resources=replicationdestinations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=multicluster.x-k8s.io,resources=serviceexports,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// isPVCInUse checks if a PVC is currently being used by any pod
func (r *VolumeGroupReplicationReconciler) isPVCInUse(ctx context.Context, namespace, pvcName string) (bool, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(namespace)); err != nil {
		return false, fmt.Errorf("listing pods: %w", err)
	}

	for _, pod := range podList.Items {
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil &&
				volume.PersistentVolumeClaim.ClaimName == pvcName {
				return true, nil
			}
		}
	}
	return false, nil
}

func (r *VolumeGroupReplicationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	logger.V(1).Info("Reconciling VolumeGroupReplication", "volumeGroupReplication", req.NamespacedName)
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
	var result ctrl.Result
	var err error
	switch vgr.Spec.ReplicationState {
	case volrep.Primary:
		result, err = r.reconcilePrimary(ctx, logger, vgr, vgrClass)
	case volrep.Secondary:
		result, err = r.reconcileSecondary(ctx, logger, vgr, vgrClass)
	default:
		logger.Error(fmt.Errorf("unknown replication state %q", vgr.Spec.ReplicationState),
			"spec.replicationState must be primary, secondary, or resync")
		return ctrl.Result{}, nil
	}

	if err := r.Status().Update(ctx, vgr); err != nil {
		return ctrl.Result{}, err
	}

	return result, err
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

	// Check if there's a VRG in the same namespace
	vrgList := &ramendrv1alpha1.VolumeReplicationGroupList{}
	if err := r.List(ctx, vrgList, client.InNamespace(vgr.Namespace)); err != nil {
		logger.V(1).Info("Failed to list VRGs, continuing without VRG check", "error", err)
	} else if len(vrgList.Items) > 0 {
		// If we're reconciling as Primary but VRG replicationState is secondary, pause all RS and skip reconciliation
		for _, vrg := range vrgList.Items {
			if vrg.Spec.ReplicationState == ramendrv1alpha1.Secondary {
				logger.Info("VGR is Primary but VRG is Secondary, pausing all ReplicationSources",
					"vgrName", vgr.Name,
					"vrgName", vrg.Name,
					"vrgReplicationState", vrg.Spec.ReplicationState)

				// Pause all existing ReplicationSources
				rsList := &volsyncv1alpha1.ReplicationSourceList{}
				if err := r.List(ctx, rsList, client.InNamespace(vgr.Namespace)); err != nil {
					logger.Error(err, "Failed to list ReplicationSources")
					return ctrl.Result{}, err
				}

				for i := range rsList.Items {
					rs := &rsList.Items[i]
					if !rs.Spec.Paused {
						rs.Spec.Paused = true
						if err := r.Update(ctx, rs); err != nil {
							logger.Error(err, "Failed to pause ReplicationSource", "rsName", rs.Name)
							return ctrl.Result{}, err
						}
						logger.Info("Paused ReplicationSource", "rsName", rs.Name)
					}
				}

				return ctrl.Result{}, nil
			}
		}
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
			pskSecretName = "volsync-rsync-tls-secret"
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
				pvc.Annotations["mock.storage.io/lastSyncTime"] = latestSync.Format(time.RFC3339)
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

	// Set Degraded condition (False = healthy)
	setCondition(&vgr.Status.Conditions, "Degraded", false,
		"Healthy",
		"volume group is healthy",
		vgr.Generation)

	// Set Resyncing condition (False = not resyncing)
	setCondition(&vgr.Status.Conditions, "Resyncing", false,
		"NotResyncing",
		"volume group is not resyncing",
		vgr.Generation)

	// Set Replicating condition
	setCondition(&vgr.Status.Conditions, "Replicating", true,
		"Replicating",
		"volume group is replicating: local group is primary",
		vgr.Generation)

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
	completedFinalsyncCount := 0

	// Create VolSync handler for checking temporary PVCs
	vsHandler := volsync.NewVSHandler(ctx, r.Client, logger, vgr, defaultSchedulingInterval)

	// Check if VGR has annotation to run final sync
	runFinalSync := false
	if vgr.Annotations != nil && vgr.Annotations["ramendr.openshift.io/run-final-sync"] == "true" {
		runFinalSync = true
		logger.Info("VGR has run-final-sync annotation set to true")
	}

	// Track final sync completion for all PVCs
	finalSyncPVCs := []string{}
	checkResult := false

	if runFinalSync && vgr.Status.State != volrep.SecondaryState {
		setCondition(&vgr.Status.Conditions, "Completed", false,
			"FinalSync",
			"RunningFinalSync",
			vgr.Generation)

		// Check each PVC for final sync completion
		pvcsInTerminating := []string{}
		pvcsToProtect := 0
		for _, pvc := range pvcList.Items {
			// skip tmp PVCs
			if strings.HasSuffix(pvc.Name, "-tmp") {
				logger.Info("Found temporary PVC. Skipping", "pvcName", pvc.Name)
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
				}
				continue
			}

			pvcsToProtect++
			// Check if PVC is terminating - if so, create temporary PVC
			isTerminating, err := volsync.IsPVCTerminating(ctx, r.Client, pvc.Name, pvc.Namespace)
			if err != nil {
				logger.Error(err, "Failed to check if PVC is terminating")
				return ctrl.Result{}, err
			}
			if isTerminating {
				logger.Info("PVC is terminating, creating temporary PVC")

				// Create temporary PVC from terminating PVC
				if err := vsHandler.CreateTemporaryPVCFromTerminating(pvc.Name, pvc.Namespace, false); err != nil {
					logger.Error(err, "Failed to create temporary PVC for terminating PVC")
					return ctrl.Result{}, err
				}

				// Pause the ReplicationSource for the main PVC
				rsName := pvc.Name
				rs := &volsyncv1alpha1.ReplicationSource{}
				if err := r.Get(ctx, types.NamespacedName{Name: rsName, Namespace: pvc.Namespace}, rs); err != nil {
					logger.Error(err, "Failed to get ReplicationSource to pause", "rsName", rsName)
					return ctrl.Result{}, err
				}

				// Only update if not already paused
				if !rs.Spec.Paused {
					rs.Spec.Paused = true
					if err := r.Update(ctx, rs); err != nil {
						logger.Error(err, "Failed to pause ReplicationSource for main PVC", "rsName", rsName)
						return ctrl.Result{}, err
					}
					logger.Info("Paused ReplicationSource for main PVC", "rsName", rsName)
				} else {
					logger.Info("ReplicationSource already paused for main PVC", "rsName", rsName)
				}

				pvcsInTerminating = append(pvcsInTerminating, pvc.Name)
			} else {
				logger.Info("PVC not in terminating state while in final sync", "pvcName", pvc.Name)
			}

			if len(pvcsInTerminating) != pvcsToProtect {
				logger.Info("Waiting for PVCs to be terminated or final sync be canceled", "PVCsInTermining", pvcsInTerminating)

				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}

			// Check if there's a temporary PVC (indicates terminating PVC with final sync)
			hasTempPVC, err := vsHandler.HasTemporaryPVC(pvc.Name, pvc.Namespace)
			if err != nil {
				logger.Error(err, "Failed to check for temporary PVC", "pvcName", pvc.Name)
				return ctrl.Result{}, err
			}

			logger.Info("Has Temporary PVC?", "hasTempPVC", hasTempPVC)

			if hasTempPVC {
				checkResult = true

				// Check if the main PVC is still in use before proceeding
				inUse, err := r.isPVCInUse(ctx, pvc.Namespace, pvc.Name)
				if err != nil {
					logger.Error(err, "Failed to check if PVC is in use", "pvcName", pvc.Name)
					return ctrl.Result{}, err
				}

				if inUse {
					logger.Info("PVC is still in use, waiting before updating RS", "pvcName", pvc.Name)
					return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
				}

				// Get the ReplicationSource for this PVC (RS name is same as PVC name)
				rsName := pvc.Name
				rs := &volsyncv1alpha1.ReplicationSource{}
				err = r.Get(ctx, types.NamespacedName{Name: rsName, Namespace: pvc.Namespace}, rs)
				if err != nil {
					logger.Error(err, "Failed to get ReplicationSource for final sync", "rsName", rsName)
					return ctrl.Result{}, err
				}

				// Update the existing RS to point to temporary PVC for final sync
				tmpPVCName := pvc.Name + "-tmp"

				// Update RS to use temporary PVC and trigger final sync
				rs.Spec.Paused = false
				rs.Spec.SourcePVC = tmpPVCName
				rs.Spec.Trigger = &volsyncv1alpha1.ReplicationSourceTriggerSpec{
					Manual: "vgr-final-sync",
				}

				if err := r.Update(ctx, rs); err != nil {
					logger.Error(err, "Failed to update ReplicationSource for final sync")
					return ctrl.Result{}, err
				}

				logger.Info("Unpaused ReplicationSource for final sync", "tmpPVC", tmpPVCName, "rsName", rsName)

				// Check if final sync is complete
				if !isFinalSyncComplete(rs, logger.WithValues("pvcName", pvc.Name)) {
					logger.Info("Final sync is NOT complete for PVC", "pvcName", pvc.Name)
					finalSyncPVCs = append(finalSyncPVCs, pvc.Name)
				} else {
					logger.Info("Final sync complete for PVC", "pvcName", pvc.Name)
					completedFinalsyncCount++
				}
			}
		}

		if checkResult {
			// Only set status to secondary if all final syncs are complete (or not running final sync)
			statusReady := (completedFinalsyncCount == pvcsToProtect)
			if !statusReady {
				msg := fmt.Sprintf("Waiting for final sync to complete. PVCs still running final sync %v", finalSyncPVCs)
				logger.Info(msg)

				// Keep Resyncing true while final sync is in progress
				setCondition(&vgr.Status.Conditions, "Resyncing", true, "FinalSync", msg, vgr.Generation)

				if err := r.Status().Update(ctx, vgr); err != nil {
					return ctrl.Result{}, err
				}

				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}

			// Final sync complete, set Resyncing to false
			setCondition(&vgr.Status.Conditions, "Resyncing", false, "NotResyncing", "volume group is not resyncing", vgr.Generation)
			logger.Info("FinalSync completed for all PVCs. Proceeding to setting up ReplicationDestination", "pvcs", pvcsToProtect)
		}
	}
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

	allReady := true

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

	msg := fmt.Sprintf("%d destination(s) ready", len(protectedPVCs))
	if allReady {
		vgr.Status.State = volrep.SecondaryState
	} else {
		msg = "waiting for all RDs to be created"
	}
	vgr.Status.PersistentVolumeClaimsRefList = protectedPVCs
	vgr.Status.ObservedGeneration = vgr.Generation

	// Set Completed condition for secondary
	setCondition(&vgr.Status.Conditions, "Completed", allReady,
		"Demoted",
		"volume group is demoted to secondary and ready to be promoted",
		vgr.Generation)

	// Set Validated condition
	setCondition(&vgr.Status.Conditions, "Validated", true,
		"PrerequisiteMet",
		"volume group is validated and met all prerequisites",
		vgr.Generation)

	// Set Degraded condition (False = healthy)
	setCondition(&vgr.Status.Conditions, "Degraded", false,
		"Healthy",
		"volume group is healthy",
		vgr.Generation)

	// Set Resyncing condition (False = not resyncing on secondary)
	setCondition(&vgr.Status.Conditions, "Resyncing", false,
		"NotResyncing",
		"volume group is not resyncing",
		vgr.Generation)

	// Set Replicating condition for secondary
	if allReady {
		setCondition(&vgr.Status.Conditions, "Replicating", true,
			"Replicating",
			"volume group is replicating: local group is secondary",
			vgr.Generation)
	} else {
		setCondition(&vgr.Status.Conditions, "Replicating", false,
			"NotReplicating",
			msg,
			vgr.Generation)
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

// isFinalSyncComplete checks if the ReplicationSource has completed final sync
func isFinalSyncComplete(replicationSource *volsyncv1alpha1.ReplicationSource, log logr.Logger) bool {
	if replicationSource.Status == nil || replicationSource.Status.LastManualSync != "vgr-final-sync" {
		log.V(1).Info("ReplicationSource running final sync - waiting for status ...")
		return false
	}
	log.V(1).Info("ReplicationSource final sync complete")
	return true
}

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
	// Create a predicate that only triggers on VRG replicationState changes
	vrgPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return true // Trigger on create
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldVRG, oldOk := e.ObjectOld.(*ramendrv1alpha1.VolumeReplicationGroup)
			newVRG, newOk := e.ObjectNew.(*ramendrv1alpha1.VolumeReplicationGroup)

			if !oldOk || !newOk {
				return false
			}

			// Only trigger if replicationState changed
			return oldVRG.Spec.ReplicationState != newVRG.Spec.ReplicationState
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return false // Don't trigger on delete
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&volrep.VolumeGroupReplication{}).
		Watches(&ramendrv1alpha1.VolumeReplicationGroup{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				return r.vrgToVGRRequests(ctx, obj)
			}),
			builder.WithPredicates(vrgPredicate)).
		Complete(r)
}

// vrgToVGRRequests maps VRG events to VGR reconcile requests
func (r *VolumeGroupReplicationReconciler) vrgToVGRRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	vgrList := &volrep.VolumeGroupReplicationList{}
	if err := r.List(ctx, vgrList, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0, len(vgrList.Items))
	for _, vgr := range vgrList.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      vgr.Name,
				Namespace: vgr.Namespace,
			},
		})
	}
	return requests
}

// Made with Bob
