package controller

import (
	"context"

	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	"github.com/go-logr/logr"
	"github.com/ramendr/mock-storage-operator/internal/volsync"
	"k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// DeleteReconciler handles VolumeGroupReplication deletion
type DeleteReconciler struct {
	client client.Client
	ctx    context.Context
	logger logr.Logger
	vgr    *volrep.VolumeGroupReplication
}

// NewDeleteReconciler creates a new DeleteReconciler
func NewDeleteReconciler(
	ctx context.Context,
	client client.Client,
	logger logr.Logger,
	vgr *volrep.VolumeGroupReplication,
) *DeleteReconciler {
	return &DeleteReconciler{
		client: client,
		ctx:    ctx,
		logger: logger,
		vgr:    vgr,
	}
}

// Reconcile handles the deletion of VolumeGroupReplication resources
func (r *DeleteReconciler) Reconcile() (ctrl.Result, error) {
	r.logger.Info("VolumeGroupReplication being deleted — cleaning up RS/RD/PVC resources by label")

	// Create VSHandler to delete resources by label
	vsHandler := volsync.NewVSHandler(r.ctx, r.client, r.logger, r.vgr, "")

	// Delete all ReplicationSources with the owner label
	if err := r.deleteReplicationSources(vsHandler); err != nil {
		return ctrl.Result{}, err
	}

	// Delete all ReplicationDestinations with the owner label
	if err := r.deleteReplicationDestinations(vsHandler); err != nil {
		return ctrl.Result{}, err
	}

	// Handle PVC deletion based on replication state
	if err := r.handlePVCDeletion(vsHandler); err != nil {
		return ctrl.Result{}, err
	}

	// Remove finalizer after cleanup
	if err := r.removeFinalizer(); err != nil {
		return ctrl.Result{}, err
	}

	r.logger.Info("VolumeGroupReplication deletion complete")
	return ctrl.Result{}, nil
}

// deleteReplicationSources deletes all ReplicationSources with the owner label
func (r *DeleteReconciler) deleteReplicationSources(vsHandler *volsync.VSHandler) error {
	if err := vsHandler.DeleteRSByLabel(); err != nil {
		r.logger.Error(err, "Failed to delete ReplicationSources by label")
		return err
	}
	r.logger.Info("Successfully deleted ReplicationSources")
	return nil
}

// deleteReplicationDestinations deletes all ReplicationDestinations with the owner label
func (r *DeleteReconciler) deleteReplicationDestinations(vsHandler *volsync.VSHandler) error {
	if err := vsHandler.DeleteRDByLabel(); err != nil {
		r.logger.Error(err, "Failed to delete ReplicationDestinations by label")
		return err
	}
	r.logger.Info("Successfully deleted ReplicationDestinations")
	return nil
}

// handlePVCDeletion handles PVC deletion based on replication state
// For secondary VGRs: delete PVCs
// For primary VGRs: only remove finalizers (let Ramen handle PVC lifecycle)
func (r *DeleteReconciler) handlePVCDeletion(vsHandler *volsync.VSHandler) error {
	if r.vgr.Spec.ReplicationState == volrep.Secondary {
		return r.deletePVCs(vsHandler)
	}
	return r.removePVCFinalizers(vsHandler)
}

// deletePVCs deletes all PVCs with the owner label (secondary only)
func (r *DeleteReconciler) deletePVCs(vsHandler *volsync.VSHandler) error {
	r.logger.Info("Deleting PVCs for secondary VGR")
	if err := vsHandler.DeletePVCsByLabel(); err != nil {
		r.logger.Error(err, "Failed to delete PVCs by label")
		return err
	}
	r.logger.Info("Successfully deleted PVCs")
	return nil
}

// removePVCFinalizers removes finalizers from PVCs (primary only)
func (r *DeleteReconciler) removePVCFinalizers(vsHandler *volsync.VSHandler) error {
	r.logger.Info("Skipping PVC deletion during VGR delete because replication state is not secondary; removing PVC finalizers instead",
		"replicationState", r.vgr.Spec.ReplicationState)

	if err := vsHandler.RemoveFinalizersFromPVCsByLabel(); err != nil {
		r.logger.Error(err, "Failed to remove PVC finalizers by label")
		return err
	}
	r.logger.Info("Successfully removed PVC finalizers")
	return nil
}

// removeFinalizer removes the VGR finalizer after cleanup
func (r *DeleteReconciler) removeFinalizer() error {
	controllerutil.RemoveFinalizer(r.vgr, vgrFinalizer)
	if err := r.client.Update(r.ctx, r.vgr); err != nil {
		if errors.IsNotFound(err) {
			// VGR already deleted, this is fine
			return nil
		}
		r.logger.Error(err, "Failed to remove finalizer")
		return err
	}
	r.logger.Info("Successfully removed finalizer")
	return nil
}

// Made with Bob
