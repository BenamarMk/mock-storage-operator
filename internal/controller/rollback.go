package controller

import (
	"context"

	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RollbackHandler handles rollback operations during failover scenarios
type RollbackHandler struct {
	client client.Client
	ctx    context.Context
	logger logr.Logger
	vgr    *volrep.VolumeGroupReplication
}

// NewRollbackHandler creates a new RollbackHandler
func NewRollbackHandler(
	ctx context.Context,
	client client.Client,
	logger logr.Logger,
	vgr *volrep.VolumeGroupReplication,
) *RollbackHandler {
	return &RollbackHandler{
		client: client,
		ctx:    ctx,
		logger: logger,
		vgr:    vgr,
	}
}
