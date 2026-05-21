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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// FinalSyncHandler handles final sync operations for VolumeGroupReplication
type FinalSyncHandler struct {
	client    client.Client
	ctx       context.Context
	logger    logr.Logger
	vgr       *volrep.VolumeGroupReplication
	vsHandler *volsync.VSHandler
}

// NewFinalSyncHandler creates a new FinalSyncHandler
func NewFinalSyncHandler(
	ctx context.Context,
	client client.Client,
	logger logr.Logger,
	vgr *volrep.VolumeGroupReplication,
	vsHandler *volsync.VSHandler,
) *FinalSyncHandler {
	return &FinalSyncHandler{
		client:    client,
		ctx:       ctx,
		logger:    logger,
		vgr:       vgr,
		vsHandler: vsHandler,
	}
}

// ShouldRunFinalSync checks if final sync should be executed by checking VRG state
func (h *FinalSyncHandler) ShouldRunFinalSync() bool {
	// List VRGs in the namespace
	vrgList := &ramendrv1alpha1.VolumeReplicationGroupList{}
	if err := h.client.List(h.ctx, vrgList, client.InNamespace(h.vgr.Namespace)); err != nil {
		h.logger.V(1).Info("Warning: Failed to list VRGs, skipping final sync check", "error", err)
		return false
	}

	// Check if no VRGs exist
	if len(vrgList.Items) == 0 {
		h.logger.V(1).Info("VRG list is empty, skipping final sync check")
		return false
	}

	// Check if any VRG meets the final sync criteria
	for _, vrg := range vrgList.Items {
		h.logger.V(1).Info("Checking VRG", "vrgName", vrg.Name)
		if vrg.Spec.Action == ramendrv1alpha1.VRGActionRelocate &&
			vrg.Spec.ReplicationState == ramendrv1alpha1.Secondary &&
			h.vgr.Status.State != volrep.SecondaryState {
			h.logger.Info("Final sync required based on VRG state/status",
				"vrgName", vrg.Name,
				"action", vrg.Spec.Action,
				"replicationState", vrg.Spec.ReplicationState,
				"statusState", vrg.Status.State)
			return true
		}
	}

	return false
}

// areAllRSFinalSyncsComplete checks if all ReplicationSources in the namespace have completed final sync
func (h *FinalSyncHandler) AreAllRSFinalSyncsComplete() (bool, error) {
	// List all ReplicationSources in the namespace
	rsList := &volsyncv1alpha1.ReplicationSourceList{}
	if err := h.client.List(h.ctx, rsList, client.InNamespace(h.vgr.Namespace)); err != nil {
		h.logger.Error(err, "Failed to list ReplicationSources for final sync check")
		return false, err
	}

	// Check if there are any ReplicationSources
	if len(rsList.Items) == 0 {
		h.logger.V(1).Info("No ReplicationSources found in namespace")
		return true, nil
	}

	// Check each RS to see if final sync is complete
	allComplete := true
	for i := range rsList.Items {
		rs := &rsList.Items[i]
		if !isFinalSyncComplete(rs, h.logger) {
			h.logger.V(1).Info("ReplicationSource final sync not complete", "rsName", rs.Name)
			allComplete = false
		}
	}

	if allComplete {
		h.logger.Info("All ReplicationSources have completed final sync", "count", len(rsList.Items))
	}

	return allComplete, nil
}

// FinalSyncResult contains the result of final sync processing
type FinalSyncResult struct {
	AllComplete bool
	PVCsInSync  []string
	TotalPVCs   int
}

// ProcessFinalSync orchestrates the final sync process for all PVCs
func (h *FinalSyncHandler) ProcessFinalSync(pvcList *corev1.PersistentVolumeClaimList) (*FinalSyncResult, error) {
	if !h.ShouldRunFinalSync() || h.vgr.Status.State == volrep.SecondaryState {
		return &FinalSyncResult{AllComplete: true}, nil
	}

	h.logger.Info("Processing final sync for VGR")

	// Set initial condition
	setCondition(&h.vgr.Status.Conditions, "Completed", false,
		"FinalSync",
		"RunningFinalSync",
		h.vgr.Generation)

	pvcsInTerminating := []string{}
	finalSyncPVCs := []string{}
	completedCount := 0
	totalPVCs := 0

	// Phase 1: Process terminating PVCs and create temporary PVCs
	for _, pvc := range pvcList.Items {
		// Skip temporary PVCs
		if h.isTemporaryPVC(&pvc) {
			h.logger.Info("Skipping temporary pvcs", "pvcName", pvc.Name)
			if err := h.handleLostTemporaryPVC(&pvc); err != nil {
				return nil, err
			}
			continue
		}

		h.logger.Info("Processing PVC", "pvcName", pvc.Name, "pvcNamespace", pvc.Namespace)
		totalPVCs++

		// Check if PVC is terminating
		isTerminating, err := volsync.IsPVCTerminating(h.ctx, h.client, pvc.Name, pvc.Namespace)
		if err != nil {
			h.logger.Error(err, "Failed to check if PVC is terminating", "pvcName", pvc.Name)
			return nil, err
		}

		if isTerminating {
			if err := h.handleTerminatingPVC(&pvc); err != nil {
				return nil, err
			}
			pvcsInTerminating = append(pvcsInTerminating, pvc.Name)
		} else {
			h.logger.Info("PVC not in terminating state while in final sync", "pvcName", pvc.Name)
		}
	}

	// Wait for all PVCs to be in terminating state
	if len(pvcsInTerminating) != totalPVCs {
		h.logger.Info("Waiting for PVCs to be terminated or final sync be canceled",
			"pvcsInTerminating", pvcsInTerminating,
			"totalPVCs", totalPVCs)
		return nil, fmt.Errorf("waiting for PVCs to terminate")
	}

	// Phase 2: Trigger final sync for PVCs with temporary PVCs
	for _, pvc := range pvcList.Items {
		if h.isTemporaryPVC(&pvc) {
			continue
		}

		hasTempPVC, err := h.vsHandler.HasTemporaryPVC(pvc.Name, pvc.Namespace)
		if err != nil {
			h.logger.Error(err, "Failed to check for temporary PVC", "pvcName", pvc.Name)
			return nil, err
		}

		if !hasTempPVC {
			continue
		}

		// Check if main PVC is still in use
		inUse, err := h.isPVCInUse(pvc.Namespace, pvc.Name)
		if err != nil {
			h.logger.Error(err, "Failed to check if PVC is in use", "pvcName", pvc.Name)
			return nil, err
		}

		if inUse {
			h.logger.Info("PVC is still in use, waiting before updating RS", "pvcName", pvc.Name)

			// Pause the ReplicationSource for the main PVC
			if err := h.pauseReplicationSource(pvc.Name, pvc.Namespace); err != nil {
				return nil, err
			}

			return nil, fmt.Errorf("PVC %s still in use", pvc.Name)
		}

		// Trigger final sync
		complete, err := h.triggerFinalSyncForPVC(&pvc)
		if err != nil {
			return nil, err
		}

		if complete {
			completedCount++
		} else {
			finalSyncPVCs = append(finalSyncPVCs, pvc.Name)
		}
	}

	return &FinalSyncResult{
		AllComplete: completedCount == totalPVCs,
		PVCsInSync:  finalSyncPVCs,
		TotalPVCs:   totalPVCs,
	}, nil
}

// isTemporaryPVC checks if a PVC is a temporary PVC
func (h *FinalSyncHandler) isTemporaryPVC(pvc *corev1.PersistentVolumeClaim) bool {
	return strings.HasSuffix(pvc.Name, "-tmp")
}

// handleLostTemporaryPVC handles temporary PVCs in Lost phase
func (h *FinalSyncHandler) handleLostTemporaryPVC(pvc *corev1.PersistentVolumeClaim) error {
	if pvc.Status.Phase != corev1.ClaimLost {
		return nil
	}

	h.logger.Info("Found temporary PVC in Lost phase", "pvcName", pvc.Name)

	if pvc.Annotations != nil {
		if _, exists := pvc.Annotations["pv.kubernetes.io/bind-completed"]; exists {
			delete(pvc.Annotations, "pv.kubernetes.io/bind-completed")
			if err := h.client.Update(h.ctx, pvc); err != nil {
				return fmt.Errorf("failed to update lost temporary PVC: %w", err)
			}
			h.logger.Info("Removed bind-completed annotation from lost temporary PVC", "pvcName", pvc.Name)
		}
	}

	return nil
}

// handleTerminatingPVC creates temporary PVC and pauses RS for terminating PVC
func (h *FinalSyncHandler) handleTerminatingPVC(pvc *corev1.PersistentVolumeClaim) error {
	h.logger.Info("PVC is terminating, creating temporary PVC", "pvcName", pvc.Name)

	// Create temporary PVC from terminating PVC
	if err := h.vsHandler.CreateTemporaryPVCFromTerminating(pvc.Name, pvc.Namespace, false); err != nil {
		h.logger.Error(err, "Failed to create temporary PVC for terminating PVC", "pvcName", pvc.Name)
		return err
	}

	return nil
}

// pauseReplicationSource pauses the RS for a given PVC
func (h *FinalSyncHandler) pauseReplicationSource(pvcName, namespace string) error {
	rsName := pvcName
	rs := &volsyncv1alpha1.ReplicationSource{}

	if err := h.client.Get(h.ctx, types.NamespacedName{Name: rsName, Namespace: namespace}, rs); err != nil {
		h.logger.Error(err, "Failed to get ReplicationSource to pause", "rsName", rsName)
		return err
	}

	// Only update if not already paused
	if !rs.Spec.Paused {
		rs.Spec.Paused = true
		if err := h.client.Update(h.ctx, rs); err != nil {
			h.logger.Error(err, "Failed to pause ReplicationSource for main PVC", "rsName", rsName)
			return err
		}

		h.logger.Info("Paused ReplicationSource for main PVC", "rsName", rsName)

		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "volsync-rsync-tls-src-" + rsName,
				Namespace: namespace,
			},
		}

		err := h.client.Delete(h.ctx, job)
		if client.IgnoreNotFound(err) != nil {
			h.logger.Error(err, "Failed to delete job for final sync", "jobName", job.Name)
			return err
		}
		if err == nil {
			h.logger.Info("Deleted job for final sync", "jobName", job.Name)
		} else {
			h.logger.V(1).Info("Job not found, skipping deletion", "jobName", job.Name)
		}
	} else {
		h.logger.Info("ReplicationSource already paused for main PVC", "rsName", rsName)
	}

	return nil
}

// triggerFinalSyncForPVC updates RS to use temporary PVC and triggers final sync
func (h *FinalSyncHandler) triggerFinalSyncForPVC(pvc *corev1.PersistentVolumeClaim) (bool, error) {
	rsName := pvc.Name
	rs := &volsyncv1alpha1.ReplicationSource{}

	if err := h.client.Get(h.ctx, types.NamespacedName{Name: rsName, Namespace: pvc.Namespace}, rs); err != nil {
		h.logger.Error(err, "Failed to get ReplicationSource for final sync", "rsName", rsName)
		return false, err
	}

	tmpPVCName := pvc.Name + "-tmp"

	// Update RS to use temporary PVC and trigger final sync
	rs.Spec.Paused = false
	rs.Spec.SourcePVC = tmpPVCName
	rs.Spec.Trigger = &volsyncv1alpha1.ReplicationSourceTriggerSpec{
		Manual: "vgr-final-sync",
	}

	if err := h.client.Update(h.ctx, rs); err != nil {
		h.logger.Error(err, "Failed to update ReplicationSource for final sync", "rsName", rsName)
		return false, err
	}

	h.logger.Info("Unpaused ReplicationSource for final sync", "tmpPVC", tmpPVCName, "rsName", rsName)

	// Check if final sync is complete
	complete := isFinalSyncComplete(rs, h.logger)
	if complete {
		h.logger.Info("Final sync complete for PVC", "pvcName", pvc.Name)
	} else {
		h.logger.Info("Final sync is NOT complete for PVC", "pvcName", pvc.Name)
	}

	return complete, nil
}

// isPVCInUse checks if a PVC is currently being used by any pod
func (h *FinalSyncHandler) isPVCInUse(namespace, pvcName string) (bool, error) {
	podList := &corev1.PodList{}
	if err := h.client.List(h.ctx, podList, client.InNamespace(namespace)); err != nil {
		return false, fmt.Errorf("listing pods: %w", err)
	}

	for _, pod := range podList.Items {
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil &&
				volume.PersistentVolumeClaim.ClaimName == pvcName {
				if pod.Labels["app.kubernetes.io/created-by"] == "volsync" {
					if err := h.client.Delete(h.ctx, &pod); err != nil {
						return false, fmt.Errorf("deleting pod %s/%s: %w", pod.Namespace, pod.Name, err)
					}
				}
				return true, nil
			}
		}
	}
	return false, nil
}

// UpdateFinalSyncStatus updates VGR status based on final sync result
func (h *FinalSyncHandler) UpdateFinalSyncStatus(result *FinalSyncResult) error {
	if result.AllComplete {
		// Final sync complete, set Resyncing to false
		setCondition(&h.vgr.Status.Conditions, "Resyncing", false,
			"NotResyncing",
			"volume group is not resyncing",
			h.vgr.Generation)
		h.logger.Info("Final sync completed for all PVCs", "totalPVCs", result.TotalPVCs)
		return nil
	}

	// Still in progress
	msg := fmt.Sprintf("Waiting for final sync to complete. PVCs still running final sync %v", result.PVCsInSync)
	setCondition(&h.vgr.Status.Conditions, "Resyncing", true,
		"FinalSync",
		msg,
		h.vgr.Generation)

	return fmt.Errorf("final sync in progress")
}

// GetRequeueDuration returns the requeue duration for final sync
func (h *FinalSyncHandler) GetRequeueDuration() time.Duration {
	return 10 * time.Second
}

// Made with Bob
