# Mock Storage Operator - User Guide

## Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Installation Steps](#installation-steps)
4. [Creating VolumeGroupReplication Resources](#creating-volumegroupreplication-resources)
5. [Parameter Configuration](#parameter-configuration)
6. [Deployment Scenarios](#deployment-scenarios)
7. [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)
8. [Common Issues](#common-issues)

---

## Overview

The Mock Storage Operator simulates a storage vendor's VolumeGroupReplication implementation for DR testing with Ramen. It uses VolSync internally for actual data replication while presenting a storage-vendor-like interface.

**Key Features:**
- Implements VolumeGroupReplication API (`replication.storage.openshift.io/v1alpha1`)
- Per-PVC configuration for scheduling, storage classes, and snapshot classes
- Submariner support for multi-cluster service discovery
- Multi-architecture support (AMD64/ARM64)

---

## Prerequisites

Before deploying the Mock Storage Operator, ensure the following components are installed on **both clusters** (primary and secondary):

### 1. VolumeGroupReplication CRDs

Install the CRDs from kubernetes-csi-addons:

```bash
# Install all CRDs (recommended)
kubectl apply -k "github.com/csi-addons/kubernetes-csi-addons/config/crd?ref=v0.14.0"

# OR install only VolumeGroupReplication CRDs
kubectl apply -f https://raw.githubusercontent.com/csi-addons/kubernetes-csi-addons/v0.14.0/config/crd/bases/replication.storage.openshift.io_volumegroupreplicationclasses.yaml
kubectl apply -f https://raw.githubusercontent.com/csi-addons/kubernetes-csi-addons/v0.14.0/config/crd/bases/replication.storage.openshift.io_volumegroupreplications.yaml
```

**Verify installation:**
```bash
kubectl get crd | grep replication.storage.openshift.io
```

Expected output:
```
volumegroupreplicationclasses.replication.storage.openshift.io
volumegroupreplications.replication.storage.openshift.io
```

### 2. VolSync

Install VolSync using Helm:

```bash
# Add VolSync Helm repository
helm repo add backube https://backube.github.io/helm-charts/
helm repo update

# Install VolSync
helm install volsync backube/volsync \
  -n volsync-system \
  --create-namespace
```

**Verify installation:**
```bash
kubectl get pods -n volsync-system
```

Expected output:
```
NAME                       READY   STATUS    RESTARTS   AGE
volsync-7b8c9d5f4d-xxxxx   1/1     Running   0          1m
```

### 3. Submariner

For multi-cluster networking, install Submariner. Follow the [Submariner installation guide](https://submariner.io/getting-started/).

### 4. Storage Classes

Ensure appropriate storage classes are available on both clusters:

```bash
# List available storage classes
kubectl get storageclass
```

You'll need:
- **For drenv environment**: Use `standard` storage class
- **For non-drenv setup**: Use LSO/LVM-based storage classes (e.g., `lvm-vg1`)

---

## Installation Steps

### Step 1: Deploy Mock Storage Operator

Deploy the operator on **both clusters** (primary and secondary):

```bash
# Deploy using Kustomize from GitHub
kubectl apply -k 'https://github.com/BenamarMk/mock-storage-operator/config/default?ref=agnostic-storage'
```

**What this does:**
- Creates `mock-storage-operator-system` namespace
- Deploys ServiceAccount, ClusterRole, and ClusterRoleBinding
- Deploys the operator pod using `quay.io/bmekhiss/mock-storage-operator:latest`

#### Configuring the Provisioner (Optional)

By default, the operator watches for VGRs with the `kubernetes.io/no-provisioner` provisioner. To use a different provisioner:

**Option 1: Environment Variable (Recommended)**

Edit the deployment to set the `PROVISIONER_NAME` environment variable:

```yaml
env:
  - name: PROVISIONER_NAME
    value: "your.custom.provisioner"
```

**Option 2: Command-Line Flag**

Add the `--provisioner-name` flag to the container args:

```yaml
args:
  - --leader-elect
  - --provisioner-name=your.custom.provisioner
```

**Common Provisioner Values:**
- `kubernetes.io/no-provisioner` - Default, for static PVs and local storage
- `k8s.io/minikube-hostpath` - Minikube hostPath provisioner
- `topolvm.io` - TopoLVM provisioner
- `kubernetes.io/gce-pd` - Google Compute Engine Persistent Disk
- `kubernetes.io/aws-ebs` - AWS Elastic Block Store
- Custom LSO (Local Storage Operator) provisioner names

> [!NOTE]
> The provisioner configured in the operator must match the `spec.provisioner` field in your VolumeGroupReplicationClass resources.


**Verify deployment:**
```bash
# Check operator pod is running
kubectl get pods -n mock-storage-operator-system

# Expected output:
# NAME                                    READY   STATUS    RESTARTS   AGE
# mock-storage-operator-xxxxxxxxxx-xxxxx  1/1     Running   0          30s

# Check operator logs
kubectl logs -n mock-storage-operator-system -l app=mock-storage-operator -f
```

### Step 2: Create Pre-Shared Key (PSK) Secrets

Create PSK secrets for rsync-tls authentication on **both clusters**:

```bash
# Generate a random PSK
PSK=$(openssl rand -base64 48)

# Create secret on primary cluster
kubectl create secret generic volsync-rsync-tls-secret \
  --from-literal=psk.txt="$PSK" \
  -n <your-namespace>

# Create the same secret on secondary cluster
kubectl create secret generic volsync-rsync-tls-secret \
  --from-literal=psk.txt="$PSK" \
  -n <your-namespace>
```

> [!IMPORTANT]
> **The PSK secret must be created in both clusters and in every namespace where you want to protect workloads.**
> The secret must be identical across all clusters for replication to work. Create the same secret in each namespace that contains PVCs you want to replicate.

### Step 3: Create VolumeGroupReplicationClass

Create the VGRClass on **both clusters**. This defines how the operator should handle replication.

The operator supports two types of VGRClass:

#### Global VGRClass

Used for cluster-scoped replication managed by Ramen. Include the `ramendr.openshift.io/global: "true"` label:

```yaml
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeGroupReplicationClass
metadata:
  annotations:
    replication.storage.openshift.io/is-default-class: "true"
  labels:
    ramendr.openshift.io/groupreplicationid: 48cc84f712b8dcb1f9ea
    ramendr.openshift.io/storageid: e1a9e2831d450379ce51d30a418b2
    ramendr.openshift.io/global: "true"  # Marks this as a global VGRClass
  name: mock-vgr-class
spec:
  provisioner: kubernetes.io/no-provisioner  # Use LSO provisioner if using Red Hat Local Storage Operator
  parameters:
    schedulingInterval: "5m"  # Use "0m" for Metro, ">0m" for Regional DR
```

#### Non-Global VGRClass (Namespace-scoped)

Used for namespace-specific replication. Omit the `ramendr.openshift.io/global` label:

```yaml
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeGroupReplicationClass
metadata:
  annotations:
    replication.storage.openshift.io/is-default-class: "true"
  labels:
    ramendr.openshift.io/groupreplicationid: 48cc84f712b8dcb1f9ea
    ramendr.openshift.io/storageid: e1a9e2831d450379ce51d30a418b2
    # No global label - this is namespace-scoped
  name: mock-vgr-class-ns
spec:
  provisioner: kubernetes.io/no-provisioner  # Use LSO provisioner if using Red Hat Local Storage Operator
  parameters:
    schedulingInterval: "5m"  # Use "0m" for Metro, ">0m" for Regional DR
```

**Apply on both clusters:**
```bash
kubectl apply -f vgrclass.yaml
```

**Verify:**
```bash
kubectl get volumegroupreplicationclass
```

> [!NOTE]
> - The operator processes VGRs for both global and non-global VGRClasses as long as the provisioner matches the configured value.
> - **provisioner**: Must match the `PROVISIONER_NAME` environment variable in the operator deployment (default: `kubernetes.io/no-provisioner`). Common values include `kubernetes.io/no-provisioner` for static PVs, `k8s.io/minikube-hostpath` for Minikube, or custom LSO provisioner names.
> - **schedulingInterval**: Set to `"0m"` for Metro (synchronous replication), or a value greater than `"0m"` (e.g., `"5m"`) for Regional DR (asynchronous replication).

---

## Creating VolumeGroupReplication Resources

### Understanding VGR States

The VolumeGroupReplication resource has three possible states:

| State | Description | Cluster Role |
|-------|-------------|--------------|
| `primary` | Creates ReplicationSources that push data | Source cluster |
| `secondary` | Creates ReplicationDestinations that receive data | Destination cluster |

### Step 4: Create Application PVC

Before deploying the VGR, create an application PVC on the **primary cluster** with the consistency group label.

Save as `app-pvc.yaml`:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  labels:
    ramendr.openshift.io/consistency-group: test-group-1
  name: mock-pvc-test
  namespace: default
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: standard
  resources:
    requests:
      storage: 1Gi
```

Apply on primary cluster:
```bash
kubectl apply -f app-pvc.yaml --context primary
```

> [!NOTE]
> The `ramendr.openshift.io/consistency-group` label is critical - it groups PVCs for replication and will be propagated to the secondary cluster.

### Step 5: Enable DR Protection (When Using Ramen)

When using Ramen for disaster recovery management, you need to enable DR protection for your application. This is typically done through the ODF UI or by applying a DRPlacementControl resource.

**Enable DR Protection:**

Once DR is enabled for your application:
- Ramen will automatically create the VolumeGroupReplication (VGR) resources on both primary cluster
- The VGR will use the consistency group label to identify which PVCs to protect
- ReplicationSources will be created on the primary cluster
- In order for the ReplicationDestination to be created on the secondary cluster, you will need to run the migration script (describe in the next step)
- ReplicationDestinations will be created on the secondary cluster

> [!NOTE]
> If you are **not** using Ramen, you will need to manually create the VGR resources as shown in the following steps. When using Ramen, skip to Step 7 (Migration Script) after enabling DR protection.


### Step 6: Deploy Primary VGR (Manual - Without Ramen)

Deploy the VGR on the **primary cluster**. This creates ReplicationSources that connect to the secondary.

Save as `primary-vgr.yaml`:

```yaml
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeGroupReplication
metadata:
  labels:
    ramendr.openshift.io/created-by-ramen: "true"
  name: vgr-1
  namespace: default
spec:
  external: true
  replicationState: primary
  source:
    selector:
      matchLabels:
        ramendr.openshift.io/consistency-group: test-group-1
  volumeGroupReplicationClassName: vgrc-1
```

Apply on primary cluster:
```bash
kubectl apply -f primary-vgr.yaml --context primary
```

### Step 7: Verify Primary VGR Status

**Monitor replication:**
```bash
# Watch VGR status
kubectl get vgr myapp-vgr -n myapp --context primary -w

# Check ReplicationSources
kubectl get replicationsources -n myapp --context primary

# Check sync status
kubectl get vgr myapp-vgr -n myapp --context primary -o jsonpath='{.status.lastSyncTime}'
```

### Step 8: Migrate PVC/PV Resources (Required for Mock Operator)

For the mock operator to protect workloads, a migration script must be run from the source cluster (Primary) where the application is running.

**What it does:**
- Migrates PersistentVolumeClaims (PVCs) and PersistentVolumes (PVs) from primary to secondary cluster
- Filters resources by consistency group label
- Strips cluster-specific metadata (uid, resourceVersion, managedFields, ownerReferences, status)
- Prepares PVs for static binding by removing claimRef
- Adds required Ramen restore annotation
- Creates target namespaces automatically

Save as `migrate.sh`:

```bash
#!/bin/bash

# Usage check for 6 arguments
if [ "$#" -ne 6 ]; then
    echo "Usage: $0 <LABEL_QUERY> <CONTEXT_C1> <CONTEXT_C2> <VGR_NAME> <VGR_NAMESPACE> <VGR_CLASS>"
    echo "Example: $0 'ramendr.openshift.io/consistency-group=my-cg' c1 c2 vgr-1 ramen-system vgrc-1"
    exit 1
fi

# Assign arguments
LABEL_QUERY=$1
CONTEXT_C1=$2
CONTEXT_C2=$3
VGR_NAME=$4
VGR_NAMESPACE=$5
VGR_CLASS=$6

# Extract CG value
CG_VALUE=$(echo "$LABEL_QUERY" | cut -d'=' -f2)

RESTORE_ANN="volumereplicationgroups.ramendr.openshift.io/ramen-restore"
ACM_PREFIX="apps.open-cluster-management.io"
CG_LABEL="ramendr.openshift.io/consistency-group"

# Base cleaning logic (Notice: .metadata.annotations is removed from the wipe list)
BASE_CLEAN='del(
    .metadata.resourceVersion,
    .metadata.uid,
    .metadata.creationTimestamp,
    .metadata.managedFields,
    .metadata.ownerReferences,
    .status
)'

# PV Specific: Wipe all annotations, add Ramen restore, isolate CG label
JQ_FILTER_PV="$BASE_CLEAN | del(.spec.claimRef, .metadata.annotations)
  | .metadata.annotations = {(\$ann): \"True\"}
  | .metadata.labels = {(\$cg_key): .metadata.labels[\$cg_key]}"

# PVC Specific:
# 1. del(.metadata.finalizers) -> prevents deletion hangs
# 2. .metadata.annotations //= {} -> ensure object exists
# 3. Filter annotations: Keep only ACM keys + add Ramen key
# 4. .metadata.labels -> isolate CG label
JQ_FILTER_PVC="$BASE_CLEAN | del(.metadata.finalizers)
  | .metadata.annotations //= {}
  | .metadata.annotations |= (with_entries(select(.key | startswith(\"$ACM_PREFIX\"))) + {(\$ann): \"True\"})
  | .metadata.labels = {(\$cg_key): .metadata.labels[\$cg_key]}"

echo "Starting migration: $CONTEXT_C1 -> $CONTEXT_C2"

# 1. Process PVs and PVCs
PVCS=$(kubectl --context="$CONTEXT_C1" get pvc -A -l "$LABEL_QUERY" -o jsonpath='{range .items[*]}{.metadata.namespace}{":"}{.metadata.name}{" "}{end}')

if [ -z "$PVCS" ]; then
    echo "No PVCs found for $LABEL_QUERY"
else
    for entry in $PVCS; do
        NAMESPACE=$(echo "$entry" | cut -d':' -f1)
        PVC_NAME=$(echo "$entry" | cut -d':' -f2)
        
        PV_NAME=$(kubectl --context="$CONTEXT_C1" -n "$NAMESPACE" get pvc "$PVC_NAME" -o jsonpath='{.spec.volumeName}')
        
        if [ -n "$PV_NAME" ]; then
            echo "[PV]  Migrating: $PV_NAME"
            kubectl --context="$CONTEXT_C1" get pv "$PV_NAME" -o json | \
            jq --arg ann "$RESTORE_ANN" --arg cg_key "$CG_LABEL" "$JQ_FILTER_PV" | \
            kubectl --context="$CONTEXT_C2" apply -f -
        fi

        kubectl --context="$CONTEXT_C2" create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl --context="$CONTEXT_C2" apply -f -

        echo "[PVC] Migrating (Filtering ACM Annotations): $NAMESPACE/$PVC_NAME"
        kubectl --context="$CONTEXT_C1" -n "$NAMESPACE" get pvc "$PVC_NAME" -o json | \
        jq --arg ann "$RESTORE_ANN" --arg cg_key "$CG_LABEL" "$JQ_FILTER_PVC" | \
        kubectl --context="$CONTEXT_C2" apply -f -
    done
fi

echo "---------------------------------------------------"
echo "Creating VolumeGroupReplication (VGR) on $CONTEXT_C2..."

kubectl --context="$CONTEXT_C2" create namespace "$VGR_NAMESPACE" --dry-run=client -o yaml | kubectl --context="$CONTEXT_C2" apply -f -

cat <<EOF | kubectl --context="$CONTEXT_C2" apply -f -
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeGroupReplication
metadata:
  labels:
    ramendr.openshift.io/created-by-ramen: "true"
  name: $VGR_NAME
  namespace: $VGR_NAMESPACE
spec:
  external: true
  replicationState: secondary
  source:
    selector:
      matchLabels:
        $CG_LABEL: $CG_VALUE
  volumeGroupReplicationClassName: $VGR_CLASS
EOF

echo "Migration and VGR creation complete."
```

**Make the script executable:**
```bash
chmod +x migrate.sh
```

**Usage:**
```bash
./migrate.sh <LABEL_QUERY> <C1> <C2> <VGR_NAME> <VGR_NS> <VGR_CLASS>
```

**Arguments:**
- `LABEL_QUERY`: Label selector to identify PVCs to migrate. Found in the PVC's `metadata.labels`:
  ```yaml
  apiVersion: v1
  kind: PersistentVolumeClaim
  metadata:
    labels:
      ramendr.openshift.io/consistency-group: 48cc84f712b8dcb1f9ea
  ```
  Use format: `'ramendr.openshift.io/consistency-group=<cg-id>'`
- `C1`: Kubernetes context name for the primary (source) cluster
- `C2`: Kubernetes context name for the secondary (destination) cluster
- `VGR_NAME`: Name for the VolumeGroupReplication resource to create on secondary cluster (use the VGR name from the primary cluster)
- `VGR_NS`: Namespace where the VGR will be created (typically `ramen-system`)
- `VGR_CLASS`: Name of the VolumeGroupReplicationClass to use

**Example:**
```bash
./migrate.sh \
  'ramendr.openshift.io/consistency-group=48cc84f712b8dcb1f9ea' \
  dr1 \
  dr2 \
  global-48cc84f712b8dcb1f9ea \
  ramen-system \
  vgrc-1
```

> [!IMPORTANT]
> **This migration script must be run ONLY ONCE after deploying your application(s) and it should be run from the primary cluster.**
>
> Running it multiple times may cause conflicts or unexpected behavior. The script:
> - Migrates PVCs and PVs from primary to secondary cluster
> - Preserves ACM (Advanced Cluster Management) annotations on PVCs
> - Isolates only the consistency group label on both PVs and PVCs
> - Automatically creates the secondary VGR after migration
> - Removes finalizers to prevent deletion hangs

**Verify migration:**
```bash
# Check PVs on secondary
kubectl get pv --context secondary

# Check PVCs on secondary
kubectl get pvc -A --context secondary -l 'ramendr.openshift.io/consistency-group=my-cg'

# Check VGR on secondary
kubectl get vgr -n ramen-system --context secondary

# Verify Ramen restore annotation
kubectl get pvc -A --context secondary -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.annotations.volumereplicationgroups\.ramendr\.openshift\.io/ramen-restore}{"\n"}{end}'
```

---

# Advanced Configuration and Reference

The following sections provide additional configuration options, troubleshooting guidance, and reference information for advanced use cases.

---

## Parameter Configuration

### VGRClass Parameters Explained

#### VGRClass Parameters

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `schedulingInterval` | Default sync frequency | `"5m"`, `"1h"`, or cron format | No (default: `"5m"`) |
| `capacity` | Default capacity for ReplicationDestinations | `"10Gi"` | No (default: `"1Gi"`) |
| `storageClassName` | Default storage class | `"standard"` | No (default: `"standard"`) |
| `pskSecretName` | PSK secret name for rsync-tls | `"volsync-rsync-tls-secret"` | No |
| `volumeSnapshotClassName` | Volume snapshot class | `"csi-snapclass"` | No |

#### Per-PVC Configuration

PVC configuration is done through **PVC annotations and labels**:

| Annotation/Label | Description | Example | Default |
|------------------|-------------|---------|---------|
| `replication.storage.openshift.io/scheduling-interval` (annotation) | Override sync frequency | `"3m"`, `"10m"`, `"1h"` | Uses VGRClass default |
| `ramendr.openshift.io/consistency-group` (label) | Consistency group identifier | `"test-group-1"` | (empty) |
| `app` (label) | Application identifier for VGR selector | `"myapp"` | Required for selection |

**Note:** Storage class and capacity are taken directly from the PVC spec.

### Configuration Examples

#### Example 1: VGRClass with Default Settings

```yaml
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeGroupReplicationClass
metadata:
  name: mock-vgr-class
spec:
  provisioner: kubernetes.io/no-provisioner
  parameters:
    schedulingInterval: "5m"
    capacity: "10Gi"
    storageClassName: "standard"
    pskSecretName: "volsync-rsync-tls-secret"
```

#### Example 2: PVC with Custom Sync Interval

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: mysql-data
  namespace: myapp
  labels:
    app: myapp
    ramendr.openshift.io/consistency-group: test-group-1
  annotations:
    replication.storage.openshift.io/scheduling-interval: "3m"  # Override default
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
  storageClassName: fast-ssd
```

#### Example 3: Using cron expressions

```yaml
parameters:
  capacity: "10Gi"
  # Sync every 5 minutes
  pvc=data1/myapp: "schedulingInterval=*/5 * * * *:storageClassName=standard:consistencyGroup=test-group-1"
  # Sync every hour at minute 0
  pvc=data2/myapp: "schedulingInterval=0 * * * *:storageClassName=standard:consistencyGroup=test-group-1"
  # Sync daily at 2 AM
  pvc=data3/myapp: "schedulingInterval=0 2 * * *:storageClassName=standard:consistencyGroup=test-group-1"
```

---

## Deployment Scenarios

### Scenario 1: Simple Two-Cluster Setup

**Topology:**
- Primary cluster: `cluster1`
- Secondary cluster: `cluster2`
- Application namespace: `myapp`
- Single PVC: `mysql-data`

**Steps:**

1. **Install prerequisites on both clusters**
   ```bash
   # On both clusters
   kubectl apply -k "github.com/csi-addons/kubernetes-csi-addons/config/crd?ref=v0.14.0"
   helm install volsync backube/volsync -n volsync-system --create-namespace
   ```

2. **Deploy operator on both clusters**
   ```bash
   # On both clusters
   kubectl apply -k 'https://github.com/BenamarMk/mock-storage-operator/config/default?ref=agnostic-storage'
   ```

3. **Create namespace and PSK secret on both clusters**
   ```bash
   # On both clusters
   kubectl create namespace myapp
   PSK=$(openssl rand -base64 48)
   kubectl create secret generic volsync-rsync-tls-secret \
     --from-literal=psk.txt="$PSK" -n myapp
   ```

4. **Create VGRClass on both clusters**
   ```bash
   cat <<EOF | kubectl apply -f -
   apiVersion: replication.storage.openshift.io/v1alpha1
   kind: VolumeGroupReplicationClass
   metadata:
     name: mock-vgr-class
   spec:
     provisioner: kubernetes.io/no-provisioner
     parameters:
       capacity: "10Gi"
       pvc=mysql-data/myapp: "schedulingInterval=5m:storageClassName=standard:consistencyGroup=test-group-1"
   EOF
   ```

5. **Create PVC on primary cluster**
   ```bash
   cat <<EOF | kubectl apply -f - --context cluster1
   apiVersion: v1
   kind: PersistentVolumeClaim
   metadata:
     name: mysql-data
     namespace: myapp
     labels:
       app: myapp
   spec:
     accessModes:
       - ReadWriteOnce
     resources:
       requests:
         storage: 10Gi
     storageClassName: standard
   EOF
   ```

6. **Deploy secondary VGR**
   ```bash
   cat <<EOF | kubectl apply -f - --context cluster2
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

7. **Wait for secondary to be ready**
   ```bash
   kubectl wait --for=condition=Ready vgr/myapp-vgr -n myapp --context cluster2 --timeout=5m
   ```

8. **Deploy primary VGR**
   ```bash
   cat <<EOF | kubectl apply -f - --context cluster1
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

9. **Verify replication**
   ```bash
   # Check primary status
   kubectl get vgr myapp-vgr -n myapp --context cluster1 -o yaml
   
   # Check secondary status
   kubectl get vgr myapp-vgr -n myapp --context cluster2 -o yaml
   ```

### Scenario 2: Multi-PVC Application

**Topology:**
- Application with 3 PVCs: `mysql-data`, `app-config`, `logs`
- Different sync schedules for each PVC

**VGRClass configuration:**

```yaml
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeGroupReplicationClass
metadata:
  name: mock-vgr-class
spec:
  provisioner: kubernetes.io/no-provisioner
  parameters:
    capacity: "10Gi"
    # Database - critical, sync every 5 minutes
    pvc=mysql-data/myapp: "schedulingInterval=5m:storageClassName=fast-ssd:consistencyGroup=test-group-1"
    # Config - moderate, sync every 15 minutes
    pvc=app-config/myapp: "schedulingInterval=15m:storageClassName=standard:consistencyGroup=test-group-1"
    # Logs - low priority, sync hourly
    pvc=logs/myapp: "schedulingInterval=1h:storageClassName=slow-hdd:consistencyGroup=test-group-1"
```

Follow the same deployment steps as Scenario 1, but ensure all PVCs have the `app: myapp` label.

---

## Monitoring and Troubleshooting

### Checking VGR Status

```bash
# Get VGR status
kubectl get vgr <vgr-name> -n <namespace> -o yaml

# Check conditions
kubectl get vgr <vgr-name> -n <namespace> -o jsonpath='{.status.conditions[*]}'

# Check last sync time
kubectl get vgr <vgr-name> -n <namespace> -o jsonpath='{.status.lastSyncTime}'

# Check replicated PVCs
kubectl get vgr <vgr-name> -n <namespace> -o jsonpath='{.status.persistentVolumeClaimsRefList[*].name}'
```

### Checking VolSync Resources

```bash
# List ReplicationSources (primary)
kubectl get replicationsources -n <namespace>

# List ReplicationDestinations (secondary)
kubectl get replicationdestinations -n <namespace>

# Check ReplicationSource status
kubectl get replicationsource <name> -n <namespace> -o yaml

# Check ReplicationDestination status
kubectl get replicationdestination <name> -n <namespace> -o yaml
```

### Checking Operator Logs

```bash
# View operator logs
kubectl logs -n mock-storage-operator-system -l app=mock-storage-operator -f

# Search for specific PVC
kubectl logs -n mock-storage-operator-system -l app=mock-storage-operator | grep "mysql-data"

# Check for errors
kubectl logs -n mock-storage-operator-system -l app=mock-storage-operator | grep -i error
```

### Checking ServiceExport (Submariner)

```bash
# List ServiceExports
kubectl get serviceexports -n <namespace>

# Check ServiceExport details
kubectl get serviceexport <name> -n <namespace> -o yaml
```

---

## Common Issues

### Issue 1: VGR Not Becoming Ready

**Symptoms:**
- VGR status shows `Ready=False`
- Condition message: "VolumeGroupReplicationClass not found"

**Solution:**
```bash
# Verify VGRClass exists
kubectl get volumegroupreplicationclass

# Check VGRClass name matches VGR spec
kubectl get vgr <name> -n <namespace> -o jsonpath='{.spec.volumeGroupReplicationClassName}'
```

### Issue 2: No PVCs Found

**Symptoms:**
- VGR status shows empty `persistentVolumeClaimsRefList`
- Operator logs: "No PVCs found matching selector"

**Solution:**
```bash
# Check PVC labels
kubectl get pvc -n <namespace> --show-labels

# Verify selector matches PVC labels
kubectl get vgr <name> -n <namespace> -o jsonpath='{.spec.source.selector}'

# Add missing labels to PVCs
kubectl label pvc <pvc-name> -n <namespace> app=myapp
```

### Issue 3: ReplicationSource Not Created

**Symptoms:**
- No ReplicationSources on primary cluster
- Operator logs: "Failed to parse PVC parameters"

**Solution:**
```bash
# Check VGRClass parameters format
kubectl get volumegroupreplicationclass mock-vgr-class -o yaml

# Verify parameter format:
# pvc=<name>/<namespace>: "schedulingInterval=<value>:storageClassName=<value>:consistencyGroup=<value>"

# Fix parameter format if incorrect
kubectl edit volumegroupreplicationclass mock-vgr-class
```

### Issue 4: Replication Not Syncing

**Symptoms:**
- ReplicationSource shows `lastSyncTime` not updating
- Operator logs: "Failed to connect to remote service"

**Solution:**
```bash
# Check PSK secret exists on both clusters
kubectl get secret volsync-rsync-tls-secret -n <namespace>

# Verify PSK secret content matches
kubectl get secret volsync-rsync-tls-secret -n <namespace> -o jsonpath='{.data.psk\.txt}' | base64 -d

# Check Submariner connectivity (if using)
subctl show connections

# Check ReplicationDestination service
kubectl get svc -n <namespace> | grep rd
```

### Issue 5: Operator Pod CrashLooping

**Symptoms:**
- Operator pod status: `CrashLoopBackOff`
- Operator logs show errors

**Solution:**
```bash
# Check operator logs
kubectl logs -n mock-storage-operator-system -l app=mock-storage-operator --previous

# Verify CRDs are installed
kubectl get crd | grep replication.storage.openshift.io

# Reinstall CRDs if missing
kubectl apply -k "github.com/csi-addons/kubernetes-csi-addons/config/crd?ref=v0.14.0"

# Restart operator
kubectl rollout restart deployment -n mock-storage-operator-system
```

### Issue 6: Permission Denied Errors

**Symptoms:**
- Operator logs: "forbidden: User cannot create resource"

**Solution:**
```bash
# Check RBAC resources exist
kubectl get clusterrole mock-storage-operator-manager-role
kubectl get clusterrolebinding mock-storage-operator-manager-rolebinding

# Verify ServiceAccount
kubectl get sa -n mock-storage-operator-system

# Reapply RBAC if missing
kubectl apply -k 'https://github.com/BenamarMk/mock-storage-operator/config/rbac?ref=agnostic-storage'
```

---

## Best Practices

1. **Always deploy secondary before primary** - This ensures ReplicationDestinations are ready before ReplicationSources try to connect.

2. **Use consistent PSK secrets** - The same PSK must exist on both clusters for each VGR.

3. **Label PVCs appropriately** - Ensure all PVCs you want to replicate have matching labels for the VGR selector.

4. **Monitor sync times** - Regularly check `lastSyncTime` in VGR status to ensure replication is working.

5. **Use appropriate scheduling intervals** - Balance RPO requirements with network bandwidth and storage performance.

6. **Test failover procedures** - Regularly test switching from primary to secondary to ensure DR readiness.

7. **Keep operator updated** - Pull the latest image from Quay.io for bug fixes and improvements.

8. **Use Submariner for production** - Manual service address configuration is error-prone and not recommended for production.

---

## Support and Resources

- **GitHub Repository**: https://github.com/BenamarMk/mock-storage-operator
- **Container Registry**: https://quay.io/repository/bmekhiss/mock-storage-operator
- **VolSync Documentation**: https://volsync.readthedocs.io/
- **Submariner Documentation**: https://submariner.io/
- **kubernetes-csi-addons**: https://github.com/csi-addons/kubernetes-csi-addons

---

## Appendix: Complete Example

Here's a complete working example for a simple application:

### 1. Prerequisites Installation

```bash
# On both clusters
kubectl apply -k "github.com/csi-addons/kubernetes-csi-addons/config/crd?ref=v0.14.0"
helm install volsync backube/volsync -n volsync-system --create-namespace
kubectl apply -k 'https://github.com/BenamarMk/mock-storage-operator/config/default?ref=agnostic-storage'
```

### 2. Create Namespace and Secrets

```bash
# On both clusters
kubectl create namespace demo-app
PSK=$(openssl rand -base64 48)
kubectl create secret generic volsync-rsync-tls-secret \
  --from-literal=psk.txt="$PSK" -n demo-app
```

### 3. Create VGRClass

```bash
cat <<EOF | kubectl apply -f -
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeGroupReplicationClass
metadata:
  name: demo-vgr-class
spec:
  provisioner: kubernetes.io/no-provisioner
  parameters:
    capacity: "5Gi"
    pvc=demo-data/demo-app: "schedulingInterval=3m:storageClassName=standard:consistencyGroup=demo-group"
EOF
```

### 4. Create Application PVC (Primary)

```bash
cat <<EOF | kubectl apply -f - --context primary
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: demo-data
  namespace: demo-app
  labels:
    app: demo
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 5Gi
  storageClassName: standard
EOF
```

### 5. Deploy Secondary VGR

```bash
cat <<EOF | kubectl apply -f - --context secondary
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeGroupReplication
metadata:
  name: demo-vgr
  namespace: demo-app
spec:
  replicationState: secondary
  volumeGroupReplicationClassName: demo-vgr-class
  source:
    selector:
      matchLabels:
        app: demo
  autoResync: true
EOF
```

### 6. Deploy Primary VGR

```bash
cat <<EOF | kubectl apply -f - --context primary
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeGroupReplication
metadata:
  name: demo-vgr
  namespace: demo-app
spec:
  replicationState: primary
  volumeGroupReplicationClassName: demo-vgr-class
  source:
    selector:
      matchLabels:
        app: demo
EOF
```

### 7. Verify Replication

```bash
# Check primary
kubectl get vgr demo-vgr -n demo-app --context primary -o yaml

# Check secondary
kubectl get vgr demo-vgr -n demo-app --context secondary -o yaml

# Monitor sync
watch kubectl get vgr demo-vgr -n demo-app --context primary -o jsonpath='{.status.lastSyncTime}'
```

---

**Document Version:** 1.0  
**Last Updated:** 2026-04-05  
**Operator Version:** latest