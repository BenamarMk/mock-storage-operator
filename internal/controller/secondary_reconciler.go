package controller

import (
	"context"
	"fmt"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	"github.com/go-logr/logr"
	"github.com/ramendr/mock-storage-operator/internal/volsync"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SecondaryReconciler handles VolumeGroupReplication secondary reconciliation
type SecondaryReconciler struct {
	client    client.Client
	ctx       context.Context
	logger    logr.Logger
	vgr       *volrep.VolumeGroupReplication
	vgrClass  *volrep.VolumeGroupReplicationClass
	vsHandler *volsync.VSHandler
}

// SecondaryConfig holds configuration for secondary reconciliation
type SecondaryConfig struct {
	SchedulingInterval string
	StorageClassName   string
	Capacity           string
	PSKSecretName      string
	ServiceType        corev1.ServiceType
}

// NewSecondaryReconciler creates a new SecondaryReconciler
func NewSecondaryReconciler(
	ctx context.Context,
	client client.Client,
	logger logr.Logger,
	vgr *volrep.VolumeGroupReplication,
	vgrClass *volrep.VolumeGroupReplicationClass,
) *SecondaryReconciler {
	return &SecondaryReconciler{
		client:   client,
		ctx:      ctx,
		logger:   logger.WithValues("reconciler", "Secondary", "vgr", vgr.Name, "vgrClass", vgrClass.Name),
		vgr:      vgr,
		vgrClass: vgrClass,
	}
}

// Reconcile orchestrates the secondary reconciliation process
func (r *SecondaryReconciler) Reconcile() (ctrl.Result, error) {
	r.logger.V(1).Info("Reconciling as secondary")

	// Phase 1: Validate and get PVCs
	pvcList, err := r.getPVCList()
	if err != nil {
		return ctrl.Result{}, err
	}

	if len(pvcList.Items) == 0 {
		r.logger.Info("No PVCs found matching selector")
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}

	r.logger.Info("Found PVCs matching selector", "count", len(pvcList.Items))

	// Phase 2: Get configuration
	config := r.getConfiguration()

	// Create VolSync handler
	r.vsHandler = volsync.NewVSHandler(r.ctx, r.client, r.logger, r.vgr, config.SchedulingInterval)

	// Phase 3: Handle final sync if needed
	if result, err := r.handleFinalSync(pvcList); err != nil || result != nil {
		if result != nil {
			return *result, err
		}
		return ctrl.Result{}, err
	}

	// Phase 4: Restore temporary PVCs
	if err := r.restoreTemporaryPVCs(pvcList); err != nil {
		return ctrl.Result{}, err
	}

	allComplete, err := r.isFinalSyncComplete()
	if err != nil {
		r.logger.V(1).Info("Failed to check final sync status", "error", err)
		return ctrl.Result{}, err
	}

	if !allComplete {
		r.logger.V(1).Info("Not all final syncs are complete")
		return ctrl.Result{}, err
	}
	// Phase 5: Reconcile ReplicationDestinations
	protectedPVCs, allReady, err := r.reconcileReplicationDestinations(pvcList, config)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Phase 6: Update status
	if err := r.updateStatus(protectedPVCs, allReady); err != nil {
		return ctrl.Result{}, err
	}

	r.logger.Info("Secondary reconcile complete", "destinations", len(protectedPVCs), "allReady", allReady)

	if !allReady {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// getPVCList retrieves and validates the PVC list
func (r *SecondaryReconciler) getPVCList() (*corev1.PersistentVolumeClaimList, error) {
	r.logger.V(1).Info("Getting PVC list")
	
	if r.vgr.Spec.Source.Selector == nil {
		r.logger.Info("No PVC selector specified")
		return nil, fmt.Errorf("no PVC selector specified")
	}

	sel, err := metav1.LabelSelectorAsSelector(r.vgr.Spec.Source.Selector)
	if err != nil {
		return nil, fmt.Errorf("invalid pvcSelector: %w", err)
	}

	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.client.List(r.ctx, pvcList, client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return nil, err
	}

	return pvcList, nil
}

// getConfiguration extracts configuration from VGRClass
func (r *SecondaryReconciler) getConfiguration() *SecondaryConfig {
	r.logger.V(1).Info("Extracting configuration from VGRClass")
	
	config := &SecondaryConfig{
		SchedulingInterval: "5m",
		StorageClassName:   "standard",
		Capacity:           "1Gi",
		PSKSecretName:      "volsync-rsync-tls-secret",
		ServiceType:        volsync.DefaultRsyncServiceType,
	}

	// Get scheduling interval
	if interval := r.vgrClass.Spec.Parameters["schedulingInterval"]; interval != "" {
		config.SchedulingInterval = interval
	} else if schedule := r.vgrClass.Spec.Parameters["schedule"]; schedule != "" {
		config.SchedulingInterval = schedule
	}
	if config.SchedulingInterval == "0m" {
		config.SchedulingInterval = "5m"
	}

	// Get storage class name
	if scName := r.vgrClass.Spec.Parameters["storageClassName"]; scName != "" {
		config.StorageClassName = scName
	}

	// Get capacity
	if capacity := r.vgrClass.Spec.Parameters["capacity"]; capacity != "" {
		config.Capacity = capacity
	}

	// Get PSK secret name
	if pskSecret := r.vgrClass.Spec.Parameters["pskSecretName"]; pskSecret != "" {
		config.PSKSecretName = pskSecret
	}

	return config
}

// handleFinalSync processes final sync if annotation is present
func (r *SecondaryReconciler) handleFinalSync(pvcList *corev1.PersistentVolumeClaimList) (*ctrl.Result, error) {
	r.logger.V(1).Info("Checking if final sync should be run")
	
	finalSyncHandler := NewFinalSyncHandler(r.ctx, r.client, r.logger, r.vgr, r.vsHandler)

	if !finalSyncHandler.ShouldRunFinalSync() {
		return nil, nil
	}

	r.logger.Info("VGR requires final sync. Check if it is already complete")
	if allComplete, err := finalSyncHandler.AreAllRSFinalSyncsComplete(); err != nil || allComplete {
		if err != nil {
			return nil, err
		}
		
		if allComplete {
			return nil, nil
		}
	}
	
	result, err := finalSyncHandler.ProcessFinalSync(pvcList)
	if err != nil {
		// Check if it's a "waiting" error
		if err.Error() == "waiting for PVCs to terminate" || 
		   err.Error() == fmt.Sprintf("PVC %s still in use", "") {
			requeue := ctrl.Result{RequeueAfter: finalSyncHandler.GetRequeueDuration()}
			return &requeue, nil
		}
		return nil, err
	}

	// Update status based on result
	if err := finalSyncHandler.UpdateFinalSyncStatus(result); err != nil {
		if err.Error() == "final sync in progress" {
			if updateErr := r.client.Status().Update(r.ctx, r.vgr); updateErr != nil {
				return nil, updateErr
			}
			requeue := ctrl.Result{RequeueAfter: finalSyncHandler.GetRequeueDuration()}
			return &requeue, nil
		}
		return nil, err
	}

	r.logger.Info("Final sync completed for all PVCs. Proceeding to setting up ReplicationDestination")
	return nil, nil
}

func (r *SecondaryReconciler) isFinalSyncComplete() (bool, error) {
	finalSyncHandler := NewFinalSyncHandler(r.ctx, r.client, r.logger, r.vgr, r.vsHandler)
	return finalSyncHandler.AreAllRSFinalSyncsComplete()
}

// restoreTemporaryPVCs restores PVCs from temporary PVCs if they exist
func (r *SecondaryReconciler) restoreTemporaryPVCs(pvcList *corev1.PersistentVolumeClaimList) error {
	r.logger.V(1).Info("Checking for temporary PVCs to restore")

	for _, pvc := range pvcList.Items {
		hasTempPVC, err := r.vsHandler.HasTemporaryPVC(pvc.Name, pvc.Namespace)
		if err != nil {
			r.logger.Error(err, "Failed to check for temporary PVC", "pvcName", pvc.Name)
			return err
		}

		if hasTempPVC {
			r.logger.Info("Found temporary PVC, restoring original PVC", "pvcName", pvc.Name)
			if err := r.vsHandler.RestorePVCFromTemporary(pvc.Name, pvc.Namespace); err != nil {
				r.logger.Error(err, "Failed to restore PVC from temporary", "pvcName", pvc.Name)
				return err
			}
			r.logger.Info("Successfully restored PVC from temporary", "pvcName", pvc.Name)
		}
	}

	return nil
}

// reconcileReplicationDestinations creates/updates ReplicationDestinations for all PVCs
func (r *SecondaryReconciler) reconcileReplicationDestinations(
	pvcList *corev1.PersistentVolumeClaimList,
	config *SecondaryConfig,
) ([]corev1.LocalObjectReference, bool, error) {
	r.logger.V(1).Info("Reconciling ReplicationDestinations for all PVCs", "pvcCount", len(pvcList.Items))
	
	protectedPVCs := []corev1.LocalObjectReference{}
	allReady := true

	for _, pvc := range pvcList.Items {
		// Handle PVC in Lost phase
		if pvc.Status.Phase == corev1.ClaimLost {
			if err := r.handleLostPVC(&pvc); err != nil {
				return nil, false, err
			}
			return nil, false, fmt.Errorf("PVC in lost phase. Remove annotation. Reconcile again")
		}

		// Get per-PVC configuration
		pvcConfig := r.getPVCConfiguration(&pvc, config)

		// Reconcile ReplicationDestination for this PVC
		rd, err := r.reconcilePVCReplicationDestination(&pvc, pvcConfig)
		if err != nil {
			return nil, false, err
		}

		if rd == nil {
			// RD not ready yet
			allReady = false
			continue
		}

		protectedPVCs = append(protectedPVCs, corev1.LocalObjectReference{Name: pvc.Name})

		// Log RD status
		r.logReplicationDestinationStatus(&pvc, rd)
	}

	return protectedPVCs, allReady, nil
}

// handleLostPVC handles PVCs in Lost phase
func (r *SecondaryReconciler) handleLostPVC(pvc *corev1.PersistentVolumeClaim) error {
	r.logger.V(1).Info("Handling PVC in Lost phase", "pvcName", pvc.Name)
	
	if pvc.Annotations == nil {
		return nil
	}

	if _, exists := pvc.Annotations["pv.kubernetes.io/bind-completed"]; !exists {
		return nil
	}

	delete(pvc.Annotations, "pv.kubernetes.io/bind-completed")
	if err := r.client.Update(r.ctx, pvc); err != nil {
		return fmt.Errorf("failed to update lost PVC after removing bind-completed annotation: %w", err)
	}

	r.logger.Info("Removed bind-completed annotation from lost PVC; waiting for next reconcile", "pvcName", pvc.Name)
	return nil
}

// getPVCConfiguration extracts per-PVC configuration
func (r *SecondaryReconciler) getPVCConfiguration(pvc *corev1.PersistentVolumeClaim, defaultConfig *SecondaryConfig) *PVCConfig {
	r.logger.V(2).Info("Extracting per-PVC configuration", "pvcName", pvc.Name)
	
	// Extract scheduling interval from annotation (default to config value if not set)
	schedulingInterval := defaultConfig.SchedulingInterval
	if interval, ok := pvc.Annotations["replication.storage.openshift.io/scheduling-interval"]; ok && interval != "" {
		schedulingInterval = interval
	}

	// Extract consistency group from label
	consistencyGroup := pvc.Labels["ramendr.openshift.io/consistency-group"]

	// Parse capacity from PVC spec
	capacityQuantity := pvc.Spec.Resources.Requests[corev1.ResourceStorage]

	// Get storage class name from PVC spec
	storageClassName := ""
	if pvc.Spec.StorageClassName != nil {
		storageClassName = *pvc.Spec.StorageClassName
	}

	return &PVCConfig{
		SchedulingInterval: schedulingInterval,
		ConsistencyGroup:   consistencyGroup,
		Capacity:           &capacityQuantity,
		StorageClassName:   &storageClassName,
		AccessModes:        pvc.Spec.AccessModes,
		PSKSecretName:      defaultConfig.PSKSecretName,
		ServiceType:        defaultConfig.ServiceType,
	}
}

// PVCConfig holds per-PVC configuration
type PVCConfig struct {
	SchedulingInterval string
	ConsistencyGroup   string
	Capacity           *resource.Quantity
	StorageClassName   *string
	AccessModes        []corev1.PersistentVolumeAccessMode
	PSKSecretName      string
	ServiceType        corev1.ServiceType
}

// reconcilePVCReplicationDestination creates/updates RD for a single PVC
func (r *SecondaryReconciler) reconcilePVCReplicationDestination(
	pvc *corev1.PersistentVolumeClaim,
	config *PVCConfig,
) (*volsyncv1alpha1.ReplicationDestination, error) {
	r.logger.V(1).Info("Protecting DST PVC", "pvc.metadata", pvc.ObjectMeta)

	// Create VolSync handler with per-PVC scheduling interval
	vsHandler := volsync.NewVSHandler(r.ctx, r.client, r.logger, r.vgr, config.SchedulingInterval)

	// Use VolSync handler to reconcile ReplicationDestination
	rd, err := vsHandler.ReconcileRD(
		pvc.Name,
		pvc.Namespace,
		config.Capacity,
		config.StorageClassName,
		config.AccessModes,
		config.PSKSecretName,
		&config.ServiceType,
		config.ConsistencyGroup,
	)
	if err != nil {
		return nil, err
	}

	return rd, nil
}

// logReplicationDestinationStatus logs RD status information
func (r *SecondaryReconciler) logReplicationDestinationStatus(
	pvc *corev1.PersistentVolumeClaim,
	rd *volsyncv1alpha1.ReplicationDestination,
) {
	if rd.Status == nil || rd.Status.RsyncTLS == nil {
		return
	}

	if rd.Status.RsyncTLS.Address != nil && rd.Status.RsyncTLS.KeySecret != nil {
		r.logger.Info("ReplicationDestination ready",
			"pvc", pvc.Name,
			"address", *rd.Status.RsyncTLS.Address,
			"keySecret", *rd.Status.RsyncTLS.KeySecret)
	}
}

// updateStatus updates VGR status and conditions
func (r *SecondaryReconciler) updateStatus(protectedPVCs []corev1.LocalObjectReference, allReady bool) error {
	r.logger.V(1).Info("Updating VGR status", "protectedPVCs", len(protectedPVCs), "allReady", allReady)
	
	// If VGR is already in Secondary state, clear all conditions
	if r.vgr.Status.State == volrep.SecondaryState {
		r.logger.Info("VGR already in Secondary state, clearing conditions")
		r.vgr.Status.Conditions = []metav1.Condition{}
		r.vgr.Status.PersistentVolumeClaimsRefList = protectedPVCs
		r.vgr.Status.ObservedGeneration = r.vgr.Generation
		return r.client.Status().Update(r.ctx, r.vgr)
	}

	msg := fmt.Sprintf("%d destination(s) ready", len(protectedPVCs))
	if allReady {
		r.vgr.Status.State = volrep.SecondaryState
	} else {
		msg = "waiting for all RDs to be created"
	}

	r.vgr.Status.PersistentVolumeClaimsRefList = protectedPVCs
	r.vgr.Status.ObservedGeneration = r.vgr.Generation

	// Set Completed condition for secondary
	setCondition(&r.vgr.Status.Conditions, "Completed", allReady,
		"Demoted",
		"volume group is demoted to secondary and ready to be promoted",
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

	// Set Resyncing condition (False = not resyncing on secondary)
	setCondition(&r.vgr.Status.Conditions, "Resyncing", false,
		"NotResyncing",
		"volume group is not resyncing",
		r.vgr.Generation)

	// Set Replicating condition for secondary
	if allReady {
		setCondition(&r.vgr.Status.Conditions, "Replicating", true,
			"Replicating",
			"volume group is replicating: local group is secondary",
			r.vgr.Generation)
	} else {
		setCondition(&r.vgr.Status.Conditions, "Replicating", false,
			"NotReplicating",
			msg,
			r.vgr.Generation)
	}

	return r.client.Status().Update(r.ctx, r.vgr)
}

// Made with Bob
