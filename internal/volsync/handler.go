package volsync

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// addVRGOwnerLabel adds owner label to an object
func addVRGOwnerLabel(owner, obj metav1.Object) {
	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}

	labels[VRGOwnerLabel] = owner.GetName()
	obj.SetLabels(labels)
}

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
