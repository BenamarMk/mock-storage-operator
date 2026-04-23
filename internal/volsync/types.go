package volsync

import (
	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Constants for VolSync operations
const (
	// DefaultCopyMethod is the default copy method for VolSync replication
	DefaultCopyMethod = volsyncv1alpha1.CopyMethodDirect

	// DefaultServiceType is the default service type for ReplicationDestination
	DefaultServiceType = corev1.ServiceTypeLoadBalancer

	// DefaultAccessMode is the default access mode for PVCs
	DefaultAccessMode = corev1.ReadWriteOnce

	// RSPrefix is the prefix for ReplicationSource and ReplicationDestination names
	RSPrefix = "mockdr-"
)

// ReplicationConfig holds configuration for VolSync replication
type ReplicationConfig struct {
	// PVCName is the name of the PVC to replicate
	PVCName string

	// PVCNamespace is the namespace of the PVC
	PVCNamespace string

	// Capacity is the storage capacity for the destination PVC
	Capacity string

	// StorageClassName is the storage class for the destination PVC
	StorageClassName string

	// Schedule is the cron schedule for replication (for ReplicationSource)
	Schedule string

	// ServiceType is the service type for ReplicationDestination
	ServiceType corev1.ServiceType

	// CopyMethod is the copy method for replication
	CopyMethod volsyncv1alpha1.CopyMethodType

	// AccessModes are the access modes for the destination PVC
	AccessModes []corev1.PersistentVolumeAccessMode
}

// RemoteInfo holds information about the remote replication endpoint
type RemoteInfo struct {
	// Address is the remote ReplicationDestination address
	Address string

	// KeySecret is the name of the secret containing the PSK
	KeySecret string
}

// ReplicationStatus represents the status of a replication operation
type ReplicationStatus struct {
	// Ready indicates if the replication is ready
	Ready bool

	// Address is the service address (for ReplicationDestination)
	Address string

	// KeySecret is the PSK secret name (for ReplicationDestination)
	KeySecret string

	// LastSyncTime is the time of the last successful sync (for ReplicationSource)
	LastSyncTime *metav1.Time
}

// Made with Bob
