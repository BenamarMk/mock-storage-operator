# Mock Storage Operator - Complete Deployment Steps

This document provides step-by-step instructions for deploying the Mock Storage Operator and setting up VolumeGroupReplication for disaster recovery testing using ConfigMap-based PVC configuration.

## Table of Contents

1. [Environment Setup](#environment-setup)
2. [Prerequisites Installation](#prerequisites-installation)
3. [Operator Deployment](#operator-deployment)
4. [Configuration](#configuration)
5. [Testing Replication](#testing-replication)
6. [Verification](#verification)

---

## Environment Setup

### Required Infrastructure

You need two Kubernetes clusters:

- **Primary Cluster**: Source cluster where applications run
- **Secondary Cluster**: Destination cluster for DR

### Cluster Requirements

- Kubernetes 1.25+
- kubectl configured with contexts for both clusters
- Network connectivity between clusters (or Submariner for multi-cluster networking)
- Storage provisioner with snapshot support

### Verify Cluster Access

```bash
# List available contexts
kubectl config get-contexts

# Test primary cluster access
kubectl get nodes --context primary

# Test secondary cluster access
kubectl get nodes --context secondary
```

---

## Prerequisites Installation

### Step 1: Install VolumeGroupReplication CRDs

Install on **both clusters**:

```bash
# Method 1: Install all kubernetes-csi-addons CRDs (recommended)
kubectl apply -k "github.com/csi-addons/kubernetes-csi-addons/config/crd?ref=v0.14.0" --context primary
kubectl apply -k "github.com/csi-addons/kubernetes-csi-addons/config/crd?ref=v0.14.0" --context secondary

# Method 2: Install only VolumeGroupReplication CRDs
kubectl apply -f https://raw.githubusercontent.com/csi-addons/kubernetes-csi-addons/v0.14.0/config/crd/bases/replication.storage.openshift.io_volumegroupreplicationclasses.yaml --context primary
kubectl apply -f https://raw.githubusercontent.com/csi-addons/kubernetes-csi-addons/v0.14.0/config/crd/bases/replication.storage.openshift.io_volumegroupreplicationcontents.yaml --context primary
kubectl apply -f https://raw.githubusercontent.com/csi-addons/kubernetes-csi-addons/v0.14.0/config/crd/bases/replication.storage.openshift.io_volumegroupreplications.yaml --context primary

kubectl apply -f https://raw.githubusercontent.com/csi-addons/kubernetes-csi-addons/v0.14.0/config/crd/bases/replication.storage.openshift.io_volumegroupreplicationclasses.yaml --context secondary
kubectl apply -f https://raw.githubusercontent.com/csi-addons/kubernetes-csi-addons/v0.14.0/config/crd/bases/replication.storage.openshift.io_volumegroupreplicationcontents.yaml --context secondary
kubectl apply -f https://raw.githubusercontent.com/csi-addons/kubernetes-csi-addons/v0.14.0/config/crd/bases/replication.storage.openshift.io_volumegroupreplications.yaml --context secondary
```

**Verify installation:**

```bash
# Check CRDs on primary
kubectl get crd --context primary | grep replication.storage.openshift.io

# Check CRDs on secondary
kubectl get crd --context secondary | grep replication.storage.openshift.io
```

**Expected output:**

```
volumegroupreplicationclasses.replication.storage.openshift.io    2026-04-05T10:00:00Z
volumegroupreplicationcontents.replication.storage.openshift.io   2026-04-05T10:00:00Z
volumegroupreplications.replication.storage.openshift.io          2026-04-05T10:00:00Z
```

### Step 2: Install VolSync

Install on **both clusters** using Helm:

```bash
# Add VolSync Helm repository
helm repo add backube https://backube.github.io/helm-charts/
helm repo update

# Install VolSync on primary cluster
helm install volsync backube/volsync \
  -n volsync-system \
  --create-namespace \
  --kube-context primary

# Install VolSync on secondary cluster
helm install volsync backube/volsync \
  -n volsync-system \
  --create-namespace \
  --kube-context secondary
```

**Verify installation:**

```bash
# Check VolSync on primary
kubectl get pods -n volsync-system --context primary

# Check VolSync on secondary
kubectl get pods -n volsync-system --context secondary
```

**Expected output:**

```
NAME                       READY   STATUS    RESTARTS   AGE
volsync-7b8c9d5f4d-xxxxx   1/1     Running   0          1m
```

### Step 3: Install Submariner (Optional)

If using Submariner for multi-cluster networking, follow the [Submariner installation guide](https://submariner.io/getting-started/).

**Quick Submariner setup:**

```bash
# Install subctl CLI
curl -Ls https://get.submariner.io | bash

# Join clusters to broker
subctl join broker-info.subm --kubeconfig primary-kubeconfig
subctl join broker-info.subm --kubeconfig secondary-kubeconfig

# Verify connectivity
subctl show connections
```

---

## Operator Deployment

### Step 4: Deploy Mock Storage Operator

Deploy on **both clusters**:

```bash
# Deploy on primary cluster
kubectl apply -k https://github.com/BenamarMk/mock-storage-operator/config/default?ref=main --context primary

# Deploy on secondary cluster
kubectl apply -k https://github.com/BenamarMk/mock-storage-operator/config/default?ref=main --context secondary
```

**What this deploys:**

- Namespace: `mock-storage-operator-system`
- ServiceAccount: `mock-storage-operator-controller-manager`
- ClusterRole: `mock-storage-operator-manager-role`
- ClusterRoleBinding: `mock-storage-operator-manager-rolebinding`
- Deployment: `mock-storage-operator-controller-manager`

**Verify deployment:**

```bash
# Check operator on primary
kubectl get pods -n mock-storage-operator-system --context primary

# Check operator on secondary
kubectl get pods -n mock-storage-operator-system --context secondary
```

**Expected output:**

```
NAME                                                    READY   STATUS    RESTARTS   AGE
mock-storage-operator-controller-manager-xxxxxxxxxx-xxxxx   1/1     Running   0          30s
```

**Check operator logs:**

```bash
# Primary cluster logs
kubectl logs -n mock-storage-operator-system -l app=mock-storage-operator --context primary -f

# Secondary cluster logs
kubectl logs -n mock-storage-operator-system -l app=mock-storage-operator --context secondary -f
```

---

## Configuration

### Step 5: Create Application Namespace

Create namespace on **both clusters**:

```bash
# Create namespace on primary
kubectl create namespace myapp --context primary

# Create namespace on secondary
kubectl create namespace myapp --context secondary
```

### Step 6: Create PSK Secrets

Create Pre-Shared Key secrets for rsync-tls authentication on **both clusters**:

```bash
# Generate a random PSK (run once)
PSK=$(openssl rand -base64 48)

# Create secret on primary cluster
kubectl create secret generic volsync-rsync-tls-secret \
  --from-literal=psk.txt="$PSK" \
  -n myapp \
  --context primary

# Create the SAME secret on secondary cluster
kubectl create secret generic volsync-rsync-tls-secret \
  --from-literal=psk.txt="$PSK" \
  -n myapp \
  --context secondary
```

**Verify secrets:**

```bash
# Check secret on primary
kubectl get secret volsync-rsync-tls-secret -n myapp --context primary

# Check secret on secondary
kubectl get secret volsync-rsync-tls-secret -n myapp --context secondary

# Verify PSK content matches (optional)
kubectl get secret volsync-rsync-tls-secret -n myapp --context primary -o jsonpath='{.data.psk\.txt}' | base64 -d
kubectl get secret volsync-rsync-tls-secret -n myapp --context secondary -o jsonpath='{.data.psk\.txt}' | base64 -d
```

### Step 7: Create Application PVCs on Primary

Create PVCs on **primary cluster** with appropriate labels:

```bash
cat <<EOF | kubectl apply -f - --context primary
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: mysql-data
  namespace: myapp
  labels:
    app: myapp  # This label is used by VGR selector
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
  storageClassName: standard
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: postgres-data
  namespace: myapp
  labels:
    app: myapp
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 5Gi
  storageClassName: standard
EOF
```

**Verify PVCs:**

```bash
# Check PVC status
kubectl get pvc -n myapp --context primary

# Verify labels
kubectl get pvc -n myapp --context primary --show-labels
```

**Expected output:**

```
NAME            STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   AGE   LABELS
mysql-data      Bound    pvc-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx   10Gi       RWO            standard       1m    app=myapp
postgres-data   Bound    pvc-yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy   5Gi        RWO            standard       1m    app=myapp
```

### Step 8: Create PVC ConfigMap on Secondary

**IMPORTANT**: This ConfigMap must be created on the **secondary cluster** and should match the PVCs from the primary cluster.

Create the ConfigMap based on your primary cluster PVCs:

```bash
cat <<EOF | kubectl apply -f - --context secondary
apiVersion: v1
kind: ConfigMap
metadata:
  name: pvc-config
  namespace: myapp
data:
  # Format: "pvc=<pvc-name>/<namespace>": "schedulingInterval=<value>:storageClassName=<value>:volumeSnapshotClassName=<value>"

  # MySQL data - sync every 5 minutes
  "pvc=mysql-data/myapp": "schedulingInterval=5m:storageClassName=standard:volumeSnapshotClassName=csi-snapclass"

  # PostgreSQL data - sync every 10 minutes
  "pvc=postgres-data/myapp": "schedulingInterval=10m:storageClassName=standard:volumeSnapshotClassName=csi-snapclass"
EOF
```

**ConfigMap Format Explained:**

- **Key**: `pvc=<pvc-name>/<namespace>`
  - `pvc-name`: Name of the PVC on primary cluster
  - `namespace`: Namespace where the PVC exists (must match VGR namespace)

- **Value**: `schedulingInterval=<value>:storageClassName=<value>:volumeSnapshotClassName=<value>`
  - `schedulingInterval`: How often to sync (e.g., `3m`, `5m`, `1h`, or cron format `*/5 * * * *`)
  - `storageClassName`: Storage class to use for ReplicationDestination PVC
  - `volumeSnapshotClassName`: Volume snapshot class for snapshots

**Verify ConfigMap:**

```bash
# Check ConfigMap
kubectl get configmap pvc-config -n myapp --context secondary

# View ConfigMap content
kubectl get configmap pvc-config -n myapp --context secondary -o yaml
```

### Step 9: Create VolumeGroupReplicationClass

Create VGRClass on **both clusters**:

```bash
cat <<EOF | kubectl apply -f - --context primary
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeGroupReplicationClass
metadata:
  name: mock-vgr-class
  annotations:
    replication.storage.openshift.io/is-default-class: "true"
  labels:
    ramendr.openshift.io/groupreplicationid: mock-storage-group-id
    ramendr.openshift.io/storageid: mock-storage-id
    ramendr.openshift.io/global: "true"
spec:
  provisioner: mock.storage.io
  parameters:
    # Default capacity for ReplicationDestinations
    capacity: "10Gi"

    # PSK secret name (optional, defaults to volsync-rsync-tls-<vgr-name>)
    pskSecretName: "volsync-rsync-tls-secret"

    # ConfigMap name containing PVC configurations (required for secondary)
    pvcConfigMap: "pvc-config"
EOF

# Apply the same VGRClass on secondary
cat <<EOF | kubectl apply -f - --context secondary
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeGroupReplicationClass
metadata:
  name: mock-vgr-class
  annotations:
    replication.storage.openshift.io/is-default-class: "true"
  labels:
    ramendr.openshift.io/groupreplicationid: mock-storage-group-id
    ramendr.openshift.io/storageid: mock-storage-id
    ramendr.openshift.io/global: "true"
spec:
  provisioner: mock.storage.io
  parameters:
    capacity: "10Gi"
    pskSecretName: "volsync-rsync-tls-secret"
    pvcConfigMap: "pvc-config"
EOF
```

**Verify VGRClass:**

```bash
# Check on primary
kubectl get volumegroupreplicationclass mock-vgr-class --context primary

# Check on secondary
kubectl get volumegroupreplicationclass mock-vgr-class --context secondary
```

---

## Testing Replication

### Step 10: Deploy Secondary VGR

Deploy VGR on **secondary cluster first**:

```bash
cat <<EOF | kubectl apply -f - --context secondary
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeGroupReplication
metadata:
  name: myapp-vgr
  namespace: myapp
spec:
  replicationState: secondary
  volumeGroupReplicationClassName: mock-vgr-class
  source:
    selector:
      matchLabels:
        app: myapp
  autoResync: true
EOF
```

**Monitor secondary VGR:**

```bash
# Watch VGR status
kubectl get vgr myapp-vgr -n myapp --context secondary -w

# Check detailed status
kubectl describe vgr myapp-vgr -n myapp --context secondary

# Check ReplicationDestinations
kubectl get replicationdestinations -n myapp --context secondary

# Check operator logs
kubectl logs -n mock-storage-operator-system -l app=mock-storage-operator --context secondary --tail=50
```

**Wait for Ready condition:**

```bash
kubectl wait --for=condition=Ready vgr/myapp-vgr -n myapp --context secondary --timeout=5m
```

**Expected log output:**

```
Found PVC configurations count=2 configMap=pvc-config
ReplicationDestination created for PVC mysql-data
ReplicationDestination created for PVC postgres-data
ReplicationDestination ready pvc=mysql-data address=mysql-data-rd.myapp.svc.clusterset.local
ReplicationDestination ready pvc=postgres-data address=postgres-data-rd.myapp.svc.clusterset.local
ServiceExport created for ReplicationDestination mysql-data
ServiceExport created for ReplicationDestination postgres-data
```

### Step 11: Deploy Primary VGR

Deploy VGR on **primary cluster**:

```bash
cat <<EOF | kubectl apply -f - --context primary
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeGroupReplication
metadata:
  name: myapp-vgr
  namespace: myapp
spec:
  replicationState: primary
  volumeGroupReplicationClassName: mock-vgr-class
  source:
    selector:
      matchLabels:
        app: myapp
EOF
```

**Monitor primary VGR:**

```bash
# Watch VGR status
kubectl get vgr myapp-vgr -n myapp --context primary -w

# Check detailed status
kubectl describe vgr myapp-vgr -n myapp --context primary

# Check ReplicationSources
kubectl get replicationsources -n myapp --context primary

# Check operator logs
kubectl logs -n mock-storage-operator-system -l app=mock-storage-operator --context primary --tail=50
```

**Wait for Ready condition:**

```bash
kubectl wait --for=condition=Ready vgr/myapp-vgr -n myapp --context primary --timeout=5m
```

**Expected log output:**

```
ReplicationSource created for PVC mysql-data
ReplicationSource created for PVC postgres-data
ReplicationSource ready pvc=mysql-data
ReplicationSource ready pvc=postgres-data
Sync completed successfully for mysql-data
Sync completed successfully for postgres-data
```

---

## Verification

### Step 12: Verify Replication Status

**Check VGR status on both clusters:**

```bash
# Primary cluster
kubectl get vgr myapp-vgr -n myapp --context primary -o yaml

# Secondary cluster
kubectl get vgr myapp-vgr -n myapp --context secondary -o yaml
```

**Key status fields to check:**

```yaml
status:
  state: Primary # or Secondary
  conditions:
    - type: Ready
      status: "True"
      reason: ReconcileComplete
  lastSyncTime: "2026-04-05T10:30:00Z"
  persistentVolumeClaimsRefList:
    - name: mysql-data
    - name: postgres-data
```

### Step 13: Verify VolSync Resources

**On primary cluster:**

```bash
# List ReplicationSources
kubectl get replicationsources -n myapp --context primary

# Check ReplicationSource details
kubectl get replicationsource <name> -n myapp --context primary -o yaml
```

**On secondary cluster:**

```bash
# List ReplicationDestinations
kubectl get replicationdestinations -n myapp --context secondary

# Check ReplicationDestination details
kubectl get replicationdestination <name> -n myapp --context secondary -o yaml
```

### Step 14: Monitor Sync Progress

**Check last sync time:**

```bash
# On primary
kubectl get vgr myapp-vgr -n myapp --context primary -o jsonpath='{.status.lastSyncTime}'

# Watch for updates
watch kubectl get vgr myapp-vgr -n myapp --context primary -o jsonpath='{.status.lastSyncTime}'
```

**Check ReplicationSource sync status:**

```bash
kubectl get replicationsource <name> -n myapp --context primary -o jsonpath='{.status.lastSyncTime}'
```

### Step 15: Test Data Replication

**Write test data on primary:**

```bash
# Create a test pod that writes to the PVC
cat <<EOF | kubectl apply -f - --context primary
apiVersion: v1
kind: Pod
metadata:
  name: test-writer
  namespace: myapp
spec:
  containers:
  - name: writer
    image: busybox
    command: ["/bin/sh", "-c"]
    args:
      - |
        echo "Test data at \$(date)" > /data/test.txt
        cat /data/test.txt
        sleep 3600
    volumeMounts:
    - name: data
      mountPath: /data
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: mysql-data
EOF
```

**Wait for sync to complete:**

```bash
# Wait for next sync cycle (based on schedulingInterval in ConfigMap)
sleep 300  # Wait 5 minutes if schedulingInterval is 5m

# Check sync time updated
kubectl get vgr myapp-vgr -n myapp --context primary -o jsonpath='{.status.lastSyncTime}'
```

**Verify data on secondary:**

```bash
# Create a test pod on secondary to read the data
cat <<EOF | kubectl apply -f - --context secondary
apiVersion: v1
kind: Pod
metadata:
  name: test-reader
  namespace: myapp
spec:
  containers:
  - name: reader
    image: busybox
    command: ["/bin/sh", "-c"]
    args:
      - |
        if [ -f /data/test.txt ]; then
          echo "Data found:"
          cat /data/test.txt
        else
          echo "Data not found"
        fi
        sleep 3600
    volumeMounts:
    - name: data
      mountPath: /data
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: mysql-data
EOF

# Check the output
kubectl logs test-reader -n myapp --context secondary
```

**Expected output:**

```
Data found:
Test data at Fri Apr  5 10:30:00 UTC 2026
```

---

## Troubleshooting

### Common Issues and Solutions

#### Issue: ConfigMap Not Found

```bash
# Error: Failed to get ConfigMap
# Solution: Verify ConfigMap exists in the correct namespace

# Check if ConfigMap exists
kubectl get configmap pvc-config -n myapp --context secondary

# If missing, create it
kubectl apply -f examples/pvc-configmap.yaml --context secondary
```

#### Issue: No PVC Configurations Found

```bash
# Error: No PVC configurations found in ConfigMap
# Solution: Verify ConfigMap format

# Check ConfigMap content
kubectl get configmap pvc-config -n myapp --context secondary -o yaml

# Ensure keys follow format: "pvc=<name>/<namespace>"
# Ensure values follow format: "schedulingInterval=<value>:storageClassName=<value>:volumeSnapshotClassName=<value>"
```

#### Issue: VGR Not Becoming Ready

```bash
# Check VGR conditions
kubectl get vgr myapp-vgr -n myapp -o jsonpath='{.status.conditions[*]}' --context secondary

# Check operator logs
kubectl logs -n mock-storage-operator-system -l app=mock-storage-operator --context secondary --tail=100
```

#### Issue: PVC Namespace Mismatch

```bash
# Error: Skipping PVC from different namespace
# Solution: Ensure PVC namespace in ConfigMap matches VGR namespace

# Check VGR namespace
kubectl get vgr myapp-vgr -o jsonpath='{.metadata.namespace}' --context secondary

# Update ConfigMap to use correct namespace
# Key format: "pvc=<pvc-name>/<correct-namespace>"
```

#### Issue: Replication Not Syncing

```bash
# Check ReplicationSource status
kubectl get replicationsource -n myapp --context primary -o yaml

# Check ReplicationDestination status
kubectl get replicationdestination -n myapp --context secondary -o yaml

# Verify PSK secrets match
kubectl get secret volsync-rsync-tls-secret -n myapp --context primary -o jsonpath='{.data.psk\.txt}' | base64 -d
kubectl get secret volsync-rsync-tls-secret -n myapp --context secondary -o jsonpath='{.data.psk\.txt}' | base64 -d

# Check network connectivity (if using Submariner)
subctl show connections
```

---

## Creating ConfigMap from Primary PVCs

### Helper Script

Save this script to generate a ConfigMap from primary cluster PVCs:

```bash
#!/bin/bash
# generate-pvc-configmap.sh

NAMESPACE="${1:-myapp}"
CONFIGMAP_NAME="${2:-pvc-config}"
CONTEXT="${3:-primary}"
SCHEDULING_INTERVAL="${4:-5m}"
STORAGE_CLASS="${5:-standard}"
SNAPSHOT_CLASS="${6:-csi-snapclass}"

echo "Generating ConfigMap from PVCs in namespace: $NAMESPACE"
echo "---"
echo "apiVersion: v1"
echo "kind: ConfigMap"
echo "metadata:"
echo "  name: $CONFIGMAP_NAME"
echo "  namespace: $NAMESPACE"
echo "data:"

# Get all PVCs in the namespace
kubectl get pvc -n "$NAMESPACE" --context "$CONTEXT" -o json | \
  jq -r '.items[] | "  \"pvc=\(.metadata.name)/\(.metadata.namespace)\": \"schedulingInterval='$SCHEDULING_INTERVAL':storageClassName='$STORAGE_CLASS':volumeSnapshotClassName='$SNAPSHOT_CLASS'\""'
```

**Usage:**

```bash
# Make script executable
chmod +x generate-pvc-configmap.sh

# Generate ConfigMap for myapp namespace
./generate-pvc-configmap.sh myapp pvc-config primary 5m standard csi-snapclass > pvc-configmap.yaml

# Apply to secondary cluster
kubectl apply -f pvc-configmap.yaml --context secondary
```

---

## Cleanup

To remove the deployment:

```bash
# Delete VGRs
kubectl delete vgr myapp-vgr -n myapp --context primary
kubectl delete vgr myapp-vgr -n myapp --context secondary

# Delete ConfigMap
kubectl delete configmap pvc-config -n myapp --context secondary

# Delete VGRClass
kubectl delete volumegroupreplicationclass mock-vgr-class --context primary
kubectl delete volumegroupreplicationclass mock-vgr-class --context secondary

# Delete operator
kubectl delete -k https://github.com/BenamarMk/mock-storage-operator/config/default?ref=main --context primary
kubectl delete -k https://github.com/BenamarMk/mock-storage-operator/config/default?ref=main --context secondary

# Delete VolSync
helm uninstall volsync -n volsync-system --kube-context primary
helm uninstall volsync -n volsync-system --kube-context secondary

# Delete CRDs (optional)
kubectl delete -k "github.com/csi-addons/kubernetes-csi-addons/config/crd?ref=v0.14.0" --context primary
kubectl delete -k "github.com/csi-addons/kubernetes-csi-addons/config/crd?ref=v0.14.0" --context secondary
```

---

## Next Steps

- Review the [User Guide](USER_GUIDE.md) for detailed configuration options
- Check the [VGR Quick Reference](VGR_QUICK_REFERENCE.md) for VGR resource examples
- Explore [examples/](../examples/) for more YAML configurations
- Test failover scenarios by switching replication states

---

**Document Version:** 2.0  
**Last Updated:** 2026-04-05  
**Operator Version:** latest (ConfigMap-based configuration)
