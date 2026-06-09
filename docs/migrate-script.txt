================================================================================
RAMENDR & VOLSYNC MIGRATION TOOL: DOCUMENTATION
================================================================================

DESCRIPTION:
A cross-cluster disaster recovery (DR) utility that automates the migration of 
storage identities and replication orchestration between Kubernetes environments. 
It handles hardware-aware local storage provisioning, synchronizes identical 
VolSync replication keys across namespaces, strips environment-specific metadata 
to prevent resource locks, and selectively filters metadata to maintain strict 
continuity with GitOps (Argo CD) and multi-cluster management (ACM) control planes.


================================================================================
SCRIPT EXECUTION FLOW: STEP-BY-STEP
================================================================================

Step 1: Argument Parsing & Validation
-------------------------------------
* Parses incoming command-line options, supporting both standard space-separated 
  ('--flag value') and assignment-based ('--flag=value') syntax.
* Evaluates if the '--local-pv' or '--no-pv' flags are present to toggle target 
  storage provisioning behavior.
* Validates that all required configuration parameters are non-empty. If any 
  mandatory argument is missing, the script prints the help usage menu and 
  aborts execution.

Step 2: Target Storage Discovery (Source Cluster)
-------------------------------------------------
* Authenticates against the source cluster using the context provided via 
  '--from-context'.
* Queries the cluster API to locate all PersistentVolumeClaims (PVCs) matching 
  the targeted '--label' selector query.
* Dynamically scopes the discovery process to a single namespace if the 
  '--namespace' flag is provided; otherwise, executes a cluster-wide scan ('-A').
* Fail-Fast Check: If zero matching PVCs are found, the script prints an error 
  message and immediately terminates with exit code 1 to prevent invalid or 
  hollow configurations down the line.

Step 3: Hardware Discovery & Local PV Provisioning (Target Cluster — Optional)
------------------------------------------------------------------------------
* Note: This step executes only if the '--local-pv' flag is explicitly passed.
* Authenticates against the target cluster using the context provided via 
  '--to-context' and applies the base 'localblock' StorageClass definition.
* Iterates through a predefined array of physical host nodes ('LOCAL_NODES') and 
  spins up an interactive debug container ('oc debug node/') on each host file 
  system.
* Scans host controllers for active, unpartitioned NVMe or SSD data disks 
  (safely filtering out the primary boot volume 'sda' and virtual loopback 
  interfaces).
* Extracts the drive's unique World Wide Name (WWN) disk identification serial.
* Generates a unique, node-affiliated 'PersistentVolume' (PV) object pointing 
  directly to the physical device path via a 'Filesystem' mount scheme and 
  registers it to the target cluster API.

Step 4: VolSync Crypto-Key Synchronization (Cross-Cluster)
----------------------------------------------------------
* Aggregates a deduplicated list of all unique namespaces hosting the discovered 
  source PVCs.
* Iterates through each unique namespace on the source cluster to locate the 
  active 'volsync-rsync-tls-secret'.
* Secret Recovery/Generation: If the secret already exists, it extracts the 
  original encoded Pre-Shared Key (PSK). If the secret is missing, it generates 
  a fresh 64-byte random cryptographic token and safely instantiates it in the 
  source namespace.
* Re-targets the destination cluster, programmatically provisions the mirror 
  namespaces, and applies an identical copy of the secret containing the matched 
  PSK token to ensure immediate synchronization compatibility.

Step 5: PersistentVolume Re-registration (Cross-Cluster — Optional)
-------------------------------------------------------------------
* Note: This step is skipped automatically if '--local-pv' or '--no-pv' is active.
* Maps every discovered PVC to its active backend volume identifier 
  ('spec.volumeName').
* Extracts the raw underlying PV manifest from the source cluster.
* Pipes the JSON string into a sanitization engine ('jq') which strips away 
  cluster-specific infrastructure footprints (including 'uid', 'resourceVersion', 
  'managedFields', 'ownerReferences', and 'status'). Wipes all original 
  annotations and injects the 'ramendr.openshift.io/ramen-restore: "True"' marker.
* Applies the fresh PV manifest directly onto the target cluster.

Step 6: PersistentVolumeClaim Cloning & Whitelist Filtering (Cross-Cluster)
----------------------------------------------------------------------------
* Extracts the raw runtime configuration of each source PVC.
* Pipes the definition into the sanitization engine ('jq') to execute three 
  vital changes:
    1. Strips all cluster-local system fields and completely removes 
       '.metadata.finalizers' to eliminate target-side resource deletion hangs.
    2. Runs a whitelisting filter across the '.metadata.annotations' array, 
       discarding local platform variables while preserving active tracks for 
       Open Cluster Management ('apps.open-cluster-management.io'), 
       VolSync replication state ('volsync.backube'), and Argo CD tracking 
       IDs ('argocd.argoproj.io').
    3. Appends the mandatory 'ramendr.openshift.io/ramen-restore: "True"' 
       recovery annotation.
* Registers the sanitized PVC directly to the target cluster API inside its 
  respective namespace.

Step 7: VolumeGroupReplication Framework Deployment (Target Cluster)
--------------------------------------------------------------------
* Extracts the core consistency group signature value directly out of the 
  provided '--label' query string.
* Creates the specified target VGR administrative namespace if it does not 
  already exist inside the target context.
* Dynamically constructs and applies the final 'VolumeGroupReplication' custom 
  resource template onto the target cluster, assigning its state to 'secondary' 
  to hand over active replication management.
================================================================================