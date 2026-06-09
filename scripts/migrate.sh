#!/bin/bash

# Default settings
MIGRATE_PV=true
MIGRATE_LOCAL_PV=false
LABEL_QUERY=""
CONTEXT_C1=""
CONTEXT_C2=""
VGR_NAME=""
VGR_NS=""
VGR_CLASS=""
SPECIFIED_NS=""
SECRET_NAME="volsync-rsync-tls-secret"

# Local PV Config
LOCAL_NODES=("compute-0" "compute-1" "compute-2")
LOCAL_SC="localblock"

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --no-pv              Skip PV migration (PVC only)"
    echo "  --local-pv           Provision Local PVs on target cluster"
    echo "  --namespace          Scope to this namespace (Optional)"
    echo "  --label              Label query"
    echo "  --from-context       Source context (C1)"
    echo "  --to-context         Target context (C2)"
    echo "  --vgr-name           VGR Name"
    echo "  --vgr-ns             VGR Namespace"
    echo "  --vgr-class          VGR StorageClass"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --no-pv) MIGRATE_PV=false; shift ;;
        --local-pv) MIGRATE_LOCAL_PV=true; MIGRATE_PV=false; shift ;;
        --namespace) SPECIFIED_NS="$2"; shift 2 ;;
        --namespace=*) SPECIFIED_NS="${1#*=}"; shift ;;
        --label) LABEL_QUERY="$2"; shift 2 ;;
        --label=*) LABEL_QUERY="${1#*=}"; shift ;;
        --from-context) CONTEXT_C1="$2"; shift 2 ;;
        --from-context=*) CONTEXT_C1="${1#*=}"; shift ;;
        --to-context) CONTEXT_C2="$2"; shift 2 ;;
        --to-context=*) CONTEXT_C2="${1#*=}"; shift ;;
        --vgr-name) VGR_NAME="$2"; shift 2 ;;
        --vgr-name=*) VGR_NAME="${1#*=}"; shift ;;
        --vgr-ns) VGR_NS="$2"; shift 2 ;;
        --vgr-ns=*) VGR_NS="${1#*=}"; shift ;;
        --vgr-class) VGR_CLASS="$2"; shift 2 ;;
        --vgr-class=*) VGR_CLASS="${1#*=}"; shift ;;
        -h|--help) usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

if [[ -z "$LABEL_QUERY" || -z "$CONTEXT_C1" || -z "$CONTEXT_C2" || -z "$VGR_NAME" || -z "$VGR_NS" || -z "$VGR_CLASS" ]]; then
    echo "Error: Missing required arguments."; usage
fi

provision_local_storage() {
    echo "--------------------------------------------------"
    echo "[Local-PV] Ensuring StorageClass '$LOCAL_SC' exists on $CONTEXT_C2..."
    cat <<EOF | kubectl --context="$CONTEXT_C2" apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: $LOCAL_SC
provisioner: kubernetes.io/no-provisioner
volumeBindingMode: WaitForFirstConsumer
EOF

    for NODE in "${LOCAL_NODES[@]}"; do
        echo "[Local-PV] Node: $NODE"
        TARGET_DEVICE=$(oc --context="$CONTEXT_C2" debug node/$NODE -- chroot /host /bin/bash -c \
            "lsblk -dno NAME,TYPE | grep 'disk' | grep -E '^sd[b-z]|^nvme[1-9]n1'" 2>/dev/null | awk '{print $1}' | head -n 1)

        if [[ -z "$TARGET_DEVICE" ]]; then
            echo "  [ERROR] No disk on $NODE."; continue
        fi

        ID=$(oc --context="$CONTEXT_C2" debug node/$NODE -- chroot /host /bin/bash -c \
            "ls -l /dev/disk/by-id/ | grep 'wwn-' | grep '../../$TARGET_DEVICE'" 2>/dev/null | awk '{print $9}')

        if [[ -z "$ID" ]]; then
            echo "  [ERROR] No WWN ID on $NODE."; continue
        fi

        PV_NAME="local-pv-disk-$NODE"
        cat <<EOF | kubectl --context="$CONTEXT_C2" apply -f -
apiVersion: v1
kind: PersistentVolume
metadata:
  name: $PV_NAME
  labels:
    kubernetes.io/hostname: $NODE
spec:
  capacity:
    storage: 250Gi
  accessModes: ["ReadWriteOnce"]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: $LOCAL_SC
  volumeMode: Filesystem
  local:
    path: /dev/disk/by-id/$ID
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions:
            - key: kubernetes.io/hostname
              operator: In
              values: ["$NODE"]
EOF
    done
}

sync_volsync_secret() {
    local target_ns=$1
    EXISTING_PSK=$(kubectl --context="$CONTEXT_C1" -n "$target_ns" get secret "$SECRET_NAME" -o jsonpath='{.data.psk\.txt}' 2>/dev/null | base64 -d)
    if [[ -z "$EXISTING_PSK" ]]; then
        PSK="volsync-mock:$(openssl rand -base64 64)"
        kubectl --context="$CONTEXT_C1" -n "$target_ns" create secret generic "$SECRET_NAME" --from-literal=psk.txt="$PSK" --dry-run=client -o yaml | kubectl --context="$CONTEXT_C1" apply -f -
    else
        PSK="$EXISTING_PSK"
    fi
    kubectl --context="$CONTEXT_C2" create namespace "$target_ns" --dry-run=client -o yaml | kubectl --context="$CONTEXT_C2" apply -f -
    kubectl --context="$CONTEXT_C2" -n "$target_ns" create secret generic "$SECRET_NAME" --from-literal=psk.txt="$PSK" --dry-run=client -o yaml | kubectl --context="$CONTEXT_C2" apply -f -
}

# DISCOVERY
NS_FLAG=${SPECIFIED_NS:+-n $SPECIFIED_NS}
[[ -z "$NS_FLAG" ]] && NS_FLAG="-A"

PVCS=$(kubectl --context="$CONTEXT_C1" get pvc $NS_FLAG -l "$LABEL_QUERY" -o jsonpath='{range .items[*]}{.metadata.namespace}{":"}{.metadata.name}{" "}{end}')

if [[ -z "$PVCS" ]]; then
    echo "Error: No PVCs found for '$LABEL_QUERY'. Aborting."; exit 1
fi

if [[ "$MIGRATE_LOCAL_PV" == "true" ]]; then provision_local_storage; fi

UNIQUE_NS=$(echo "$PVCS" | tr ' ' '\n' | cut -d':' -f1 | sort -u)
for ns in $UNIQUE_NS; do sync_volsync_secret "$ns"; done

# LOGIC SETTINGS
RESTORE_ANN="volumereplicationgroups.ramendr.openshift.io/ramen-restore"
CG_LABEL="ramendr.openshift.io/consistency-group"
PREFIX_ACM="apps.open-cluster-management.io"
PREFIX_VOLSYNC="volsync.backube"
PREFIX_ARGO="argocd.argoproj.io"

BASE_CLEAN='del(.metadata.resourceVersion, .metadata.uid, .metadata.creationTimestamp, .metadata.managedFields, .metadata.ownerReferences, .status)'
JQ_FILTER_PV="$BASE_CLEAN | del(.spec.claimRef, .metadata.annotations) | .metadata.annotations = {(\$ann): \"True\"} | .metadata.labels = {(\$cg_key): .metadata.labels[\$cg_key]}"

# PVC FILTER: Keeps ACM, VolSync, and Argo CD annotations
JQ_FILTER_PVC="$BASE_CLEAN | del(.metadata.finalizers) | .metadata.annotations //= {} | .metadata.annotations |= (with_entries(select(.key | (startswith(\"$PREFIX_ACM\") or startswith(\"$PREFIX_VOLSYNC\") or startswith(\"$PREFIX_ARGO\")))) + {(\$ann): \"True\"}) | .metadata.labels = {(\$cg_key): .metadata.labels[\$cg_key]}"

for entry in $PVCS; do
    NAMESPACE=$(echo "$entry" | cut -d':' -f1)
    PVC_NAME=$(echo "$entry" | cut -d':' -f2)
    
    if [[ "$MIGRATE_PV" == "true" ]]; then
        PV_NAME=$(kubectl --context="$CONTEXT_C1" -n "$NAMESPACE" get pvc "$PVC_NAME" -o jsonpath='{.spec.volumeName}')
        if [[ -n "$PV_NAME" ]]; then
            kubectl --context="$CONTEXT_C1" get pv "$PV_NAME" -o json | jq --arg ann "$RESTORE_ANN" --arg cg_key "$CG_LABEL" "$JQ_FILTER_PV" | kubectl --context="$CONTEXT_C2" apply -f -
        fi
    fi

    echo "[PVC] Migrating: $NAMESPACE/$PVC_NAME"
    kubectl --context="$CONTEXT_C1" -n "$NAMESPACE" get pvc "$PVC_NAME" -o json | jq --arg ann "$RESTORE_ANN" --arg cg_key "$CG_LABEL" "$JQ_FILTER_PVC" | kubectl --context="$CONTEXT_C2" apply -f -
done

# VGR
CG_VALUE=$(echo "$LABEL_QUERY" | cut -d'=' -f2)
kubectl --context="$CONTEXT_C2" create namespace "$VGR_NS" --dry-run=client -o yaml | kubectl --context="$CONTEXT_C2" apply -f -
cat <<EOF | kubectl --context="$CONTEXT_C2" apply -f -
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeGroupReplication
metadata:
  labels: { "ramendr.openshift.io/created-by-ramen": "true" }
  name: $VGR_NAME
  namespace: $VGR_NS
spec:
  external: true
  replicationState: secondary
  source:
    selector:
      matchLabels:
        $CG_LABEL: $CG_VALUE
  volumeGroupReplicationClassName: $VGR_CLASS
EOF

echo "Done."
