package controller

import (
	"context"
	"fmt"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	"github.com/go-logr/logr"
	ramendrv1alpha1 "github.com/ramendr/ramen/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
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
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;delete

// Reconcile is the main reconciliation loop
func (r *VolumeGroupReplicationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("------------------------------------------------------------")
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

	logger.V(1).Info("Reconciled", "as", vgr.Spec.ReplicationState, "result", result, "error", err)
	
	if err := r.Status().Update(ctx, vgr); err != nil {
		return ctrl.Result{}, err
	}

	return result, err
}

// reconcilePrimary delegates to PrimaryReconciler
func (r *VolumeGroupReplicationReconciler) reconcilePrimary(
	ctx context.Context,
	logger logr.Logger,
	vgr *volrep.VolumeGroupReplication,
	vgrClass *volrep.VolumeGroupReplicationClass,
) (ctrl.Result, error) {
	reconciler := NewPrimaryReconciler(ctx, r.Client, logger, vgr, vgrClass)
	return reconciler.Reconcile()
}

// reconcileSecondary delegates to SecondaryReconciler
func (r *VolumeGroupReplicationReconciler) reconcileSecondary(
	ctx context.Context,
	logger logr.Logger,
	vgr *volrep.VolumeGroupReplication,
	vgrClass *volrep.VolumeGroupReplicationClass,
) (ctrl.Result, error) {
	reconciler := NewSecondaryReconciler(ctx, r.Client, logger, vgr, vgrClass)
	return reconciler.Reconcile()
}

// reconcileDelete delegates to DeleteReconciler
func (r *VolumeGroupReplicationReconciler) reconcileDelete(
	ctx context.Context,
	logger logr.Logger,
	vgr *volrep.VolumeGroupReplication,
) (ctrl.Result, error) {
	reconciler := NewDeleteReconciler(ctx, r.Client, logger, vgr)
	return reconciler.Reconcile()
}

// ── COMMON HELPERS ───────────────────────────────────────────────────────────

// isFinalSyncComplete checks if the ReplicationSource has completed final sync
func isFinalSyncComplete(replicationSource *volsyncv1alpha1.ReplicationSource, log logr.Logger) bool {
	if replicationSource.Status == nil || replicationSource.Status.LastManualSync != "vgr-final-sync" {
		log.V(1).Info("ReplicationSource running final sync - waiting for status ...")
		return false
	}
	log.V(1).Info("ReplicationSource final sync complete")
	return true
}

// isVolSyncOwned checks if a PVC was created by VolSync
func isVolSyncOwned(pvc *corev1.PersistentVolumeClaim) bool {
	// Check if PVC was created by VolSync using the standard Kubernetes label
	if createdBy, ok := pvc.Labels["app.kubernetes.io/created-by"]; ok && createdBy == "volsync" {
		return true
	}
	return false
}

// setCondition sets or updates a condition in the conditions list
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

// ── CONTROLLER SETUP ─────────────────────────────────────────────────────────

// SetupWithManager sets up the controller with the Manager
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
