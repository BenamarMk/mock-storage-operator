package controller

import (
	"context"
	"fmt"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	"github.com/go-logr/logr"
	"github.com/ramendr/mock-storage-operator/internal/volsync"
	ramendrv1alpha1 "github.com/ramendr/ramen/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PrimaryReconciler handles VolumeGroupReplication primary reconciliation
type PrimaryReconciler struct {
	client    client.Client
	ctx       context.Context
	logger    logr.Logger
	vgr       *volrep.VolumeGroupReplication
	vgrClass  *volrep.VolumeGroupReplicationClass
	vsHandler *volsync.VSHandler
}

// PrimaryConfig holds configuration for primary reconciliation
type PrimaryConfig struct {
	SchedulingInterval      string
	StorageClassName        string
	PSKSecretName           string
	VolumeSnapshotClassName *string
}

// NewPrimaryReconciler creates a new PrimaryReconciler
func NewPrimaryReconciler(
	ctx context.Context,
	client client.Client,
	logger logr.Logger,
	vgr *volrep.VolumeGroupReplication,
	vgrClass *volrep.VolumeGroupReplicationClass,
) *PrimaryReconciler {
	return &PrimaryReconciler{
		client:   client,
		ctx:      ctx,
		logger:   logger.WithValues("reconciler", "Primary"),
		vgr:      vgr,
		vgrClass: vgrClass,
	}
}

// Reconcile orchestrates the primary reconciliation process
func (r *PrimaryReconciler) Reconcile() (ctrl.Result, error) {
	r.logger.Info("Reconciling VolumeGroupReplication as primary")

	// Phase 1: Validate PVC selector
	if r.vgr.Spec.Source.Selector == nil {
		r.logger.Info("No PVC selector specified")
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}

	sel, err := metav1.LabelSelectorAsSelector(r.vgr.Spec.Source.Selector)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("invalid pvcSelector: %w", err)
	}

	// Phase 2: Check for VRG conflicts
	if result, err := r.checkVRGConflict(); err != nil || result != nil {
		return *result, err
	}

	// Phase 3: Get PVC list
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.client.List(r.ctx, pvcList, client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return ctrl.Result{}, err
	}

	// Phase 4: Get configuration
	config := r.getConfiguration()

	// Create VolSync handler
	r.vsHandler = volsync.NewVSHandler(r.ctx, r.client, r.logger, r.vgr, config.SchedulingInterval)

	// Phase 5: Reconcile ReplicationSources for all PVCs
	protectedPVCs, latestSync, err := r.reconcileReplicationSources(pvcList, config)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Phase 6: Update PVC annotations
	if err := r.updatePVCAnnotations(pvcList, latestSync); err != nil {
		return ctrl.Result{}, err
	}

	// Phase 7: Update status
	if err := r.updateStatus(protectedPVCs, latestSync); err != nil {
		return ctrl.Result{}, err
	}

	r.logger.Info("Primary reconcile complete", "protectedPVCs", len(protectedPVCs))
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// checkVRGConflict checks if VRG is in Secondary state and pauses all RS if needed
func (r *PrimaryReconciler) checkVRGConflict() (*ctrl.Result, error) {
	vrgList := &ramendrv1alpha1.VolumeReplicationGroupList{}
	if err := r.client.List(r.ctx, vrgList, client.InNamespace(r.vgr.Namespace)); err != nil {
		r.logger.V(1).Info("Failed to list VRGs, continuing without VRG check", "error", err)
		return nil, nil
	}

	if len(vrgList.Items) == 0 {
		return nil, nil
	}

	// Check if any VRG is in Secondary state
	for _, vrg := range vrgList.Items {
		if vrg.Spec.ReplicationState == ramendrv1alpha1.Secondary {
			r.logger.Info("VGR is Primary but VRG is Secondary, pausing all ReplicationSources",
				"vgrName", r.vgr.Name,
				"vrgName", vrg.Name,
				"vrgReplicationState", vrg.Spec.ReplicationState)

			if err := r.pauseAllReplicationSources(); err != nil {
				return nil, err
			}

			result := ctrl.Result{}
			return &result, nil
		}
	}

	return nil, nil
}

// pauseAllReplicationSources pauses all ReplicationSources in the namespace
func (r *PrimaryReconciler) pauseAllReplicationSources() error {
	rsList := &volsyncv1alpha1.ReplicationSourceList{}
	if err := r.client.List(r.ctx, rsList, client.InNamespace(r.vgr.Namespace)); err != nil {
		r.logger.Error(err, "Failed to list ReplicationSources")
		return err
	}

	for i := range rsList.Items {
		rs := &rsList.Items[i]
		if !rs.Spec.Paused {
			rs.Spec.Paused = true
			if err := r.client.Update(r.ctx, rs); err != nil {
				r.logger.Error(err, "Failed to pause ReplicationSource", "rsName", rs.Name)
				return err
			}
			r.logger.Info("Paused ReplicationSource", "rsName", rs.Name)

			// Delete the associated VolSync job
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "volsync-rsync-tls-src-" + rs.Name,
					Namespace: r.vgr.Namespace,
				},
			}

			err := r.client.Delete(r.ctx, job)
			if client.IgnoreNotFound(err) != nil {
				r.logger.Error(err, "Failed to delete job after pausing RS", "jobName", job.Name)
				return err
			}
			if err == nil {
				r.logger.Info("Deleted job after pausing RS", "jobName", job.Name)
			} else {
				r.logger.V(1).Info("Job not found, skipping deletion", "jobName", job.Name)
			}
		}
	}

	podList := &corev1.PodList{}
	if err := r.client.List(r.ctx, podList, client.InNamespace(r.vgr.Namespace)); err != nil {
		r.logger.Error(err, "Failed to list pods for cleanup")
		return err
	}

	for _, pod := range podList.Items {
		if pod.Labels["app.kubernetes.io/created-by"] == "volsync" {
			if err := r.client.Delete(r.ctx, &pod); err != nil {
				r.logger.Error(err, "Failed to delete VolSync pod", "podName", pod.Name)
				return fmt.Errorf("deleting pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
			r.logger.Info("Deleted VolSync pod", "podName", pod.Name)
		}
	}

	return nil
}

// getConfiguration extracts configuration from VGRClass
func (r *PrimaryReconciler) getConfiguration() *PrimaryConfig {
	config := &PrimaryConfig{
		SchedulingInterval: "5m",
		StorageClassName:   "standard",
		PSKSecretName:      "volsync-rsync-tls-secret",
	}

	// Get scheduling interval
	if interval := r.vgrClass.Spec.Parameters["schedulingInterval"]; interval != "" && interval != "0m" {
		config.SchedulingInterval = interval
	}

	// Get storage class name
	if scName := r.vgrClass.Spec.Parameters["storageClassName"]; scName != "" {
		config.StorageClassName = scName
	}

	// Get PSK secret name
	if pskSecret := r.vgrClass.Spec.Parameters["pskSecretName"]; pskSecret != "" {
		config.PSKSecretName = pskSecret
	}

	// Get VolumeSnapshotClassName (optional)
	if vscName := r.vgrClass.Spec.Parameters["volumeSnapshotClassName"]; vscName != "" {
		config.VolumeSnapshotClassName = &vscName
	}

	return config
}

// reconcileReplicationSources creates/updates ReplicationSources for all PVCs
func (r *PrimaryReconciler) reconcileReplicationSources(
	pvcList *corev1.PersistentVolumeClaimList,
	config *PrimaryConfig,
) ([]corev1.LocalObjectReference, *metav1.Time, error) {
	protectedPVCs := []corev1.LocalObjectReference{}
	var latestSync *metav1.Time

	r.logger.V(1).Info("Protecting PVCs", "pvcCount", len(pvcList.Items))

	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]

		// Skip PVCs owned by VolSync to avoid self-replication loops
		if isVolSyncOwned(pvc) {
			continue
		}

		// Reconcile ReplicationSource for this PVC
		rs, err := r.reconcilePVCReplicationSource(pvc, config)
		if err != nil {
			return nil, nil, err
		}

		// Only add to protectedPVCs if RS was created (not nil)
		// RS will be nil if PVC is terminating
		if rs != nil {
			protectedPVCs = append(protectedPVCs, corev1.LocalObjectReference{Name: pvc.Name})

			// Get last sync time from ReplicationSource status
			if rs.Status != nil && rs.Status.LastSyncTime != nil {
				latestSync = rs.Status.LastSyncTime
			}
		}
	}

	return protectedPVCs, latestSync, nil
}

// reconcilePVCReplicationSource creates/updates RS for a single PVC
func (r *PrimaryReconciler) reconcilePVCReplicationSource(
	pvc *corev1.PersistentVolumeClaim,
	config *PrimaryConfig,
) (*volsyncv1alpha1.ReplicationSource, error) {
	r.logger.V(1).Info("Protecting SRC PVC", "pvc.metadata", pvc.ObjectMeta)

	// Use Submariner service name for remote address
	// The remote service name follows the pattern: <service-name>.<namespace>.svc.clusterset.local
	remoteAddress := volsync.GetRemoteServiceNameForRDFromPVCName(pvc.Name, pvc.Namespace)

	// Use VolSync handler to reconcile ReplicationSource
	rs, err := r.vsHandler.ReconcileRS(
		pvc.Name,
		pvc.Namespace,
		remoteAddress,
		config.PSKSecretName,
		pvc.Spec.StorageClassName,
		pvc.Spec.AccessModes,
		config.VolumeSnapshotClassName,
	)
	if err != nil {
		return nil, err
	}

	return rs, nil
}

// updatePVCAnnotations updates lastSyncTime annotations on PVCs
func (r *PrimaryReconciler) updatePVCAnnotations(
	pvcList *corev1.PersistentVolumeClaimList,
	latestSync *metav1.Time,
) error {
	if latestSync == nil {
		return nil
	}

	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]

		// Initialize annotations map if it doesn't exist
		if pvc.Annotations == nil {
			pvc.Annotations = make(map[string]string)
		}

		pvc.Annotations["mock.storage.io/lastSyncTime"] = latestSync.Format(time.RFC3339)

		if err := r.client.Update(r.ctx, pvc); err != nil {
			return fmt.Errorf("failed to update lastSyncTime annotation: %w", err)
		}
	}

	return nil
}

// updateStatus updates VGR status and conditions
func (r *PrimaryReconciler) updateStatus(
	protectedPVCs []corev1.LocalObjectReference,
	latestSync *metav1.Time,
) error {
	// Update status
	r.vgr.Status.State = volrep.PrimaryState
	r.vgr.Status.PersistentVolumeClaimsRefList = protectedPVCs
	r.vgr.Status.LastSyncTime = latestSync
	r.vgr.Status.ObservedGeneration = r.vgr.Generation

	// Set Completed condition
	setCondition(&r.vgr.Status.Conditions, "Completed", true,
		"Promoted",
		"volume group is promoted to primary and replicating to secondary",
		r.vgr.Generation)

	// Set Validated condition
	setCondition(&r.vgr.Status.Conditions, "Validated", true,
		"PrerequisiteMet",
		"volume group is validated and met all prerequisites",
		r.vgr.Generation)

	// Set Degraded condition (False = healthy)
	setCondition(&r.vgr.Status.Conditions, "Degraded", false,
		"Healthy",
		"volume group is healthy",
		r.vgr.Generation)

	// Set Resyncing condition (False = not resyncing)
	setCondition(&r.vgr.Status.Conditions, "Resyncing", false,
		"NotResyncing",
		"volume group is not resyncing",
		r.vgr.Generation)

	// Set Replicating condition
	setCondition(&r.vgr.Status.Conditions, "Replicating", true,
		"Replicating",
		"volume group is replicating: local group is primary",
		r.vgr.Generation)

	return r.client.Status().Update(r.ctx, r.vgr)
}

// Made with Bob
