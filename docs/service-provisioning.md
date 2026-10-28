# Configuration, service provisioning and scope

The application runs on OL9. Debezium and the history writer run as OCI Container Instances. Configuration & setup includes a service Tab View with inventory, create, refresh, selection, guarded delete and CSV download.

Resource lists exclude the `DELETED` state. Resources still in `DELETING` remain visible until deletion completes.

| Service | Resources and actions |
| --- | --- |
| OCI Streaming | Create pools and streams; select an existing pool in a queue connection; OCI principals handle messaging authentication. |
| OCI Container Instances | Select workload subnet, availability domain, private repository image release, authentication mode, shape and size; deploy a registered flow after readiness checks. |
| Container Registry | List/create private repositories; Build & publish Debezium and history writer images on the VM, track progress and download build logs. |
| Vault | List/create vaults and secrets using an existing symmetric encryption key. Secret and vault deletion is scheduled with an eight-day delay. |
| HeatWave | List/create DB systems; start/stop selected systems. Creation loads availability domains, available DB system shapes, matching configurations and MySQL versions from OCI. Database connections themselves use supplied endpoints and an actual MySQL connection test. |

Use [IAM and database access](iam-policy.md) for the consolidated policy templates and [OL9 setup and operations](operations.md) for installation, image replacement and recovery.

## Compartment boundary

The only selectable compartment is the VM's compartment. Every Cloud client verifies private configuration against VM instance metadata and uses instance-principal authentication. Inventory is scoped to that compartment and returned objects are verified. Creation and deletion repeat these checks. Existing subnets, their VCNs, private endpoint attachments, registry images, and secrets are checked before use. No IAM or shared-network modifications are performed.

An operator cannot extend scope by changing a form field or supplying a resource identifier. Missing service permissions remain an error; there is no credential fallback. The Database connection endpoint is not resolved to a DB-system OCID, in accordance with the connection design.

## Resolving stream-pool access errors

Stream and pool management require distinct OCI permissions. A denied request reports the scoped operation, safe code and request ID; it does not change principals or broaden the compartment.

An OCI administrator should verify that the VM belongs to the intended dynamic group. For the complete Streaming workflow, use the family policy below; replace both placeholders, including the identity-domain prefix where needed:

```text
Allow dynamic-group <VM_DYNAMIC_GROUP> to manage stream-family in compartment id <VM_COMPARTMENT_OCID>
```


These permissions are documented in Oracle's [Streaming policy reference](https://docs.oracle.com/en-us/iaas/Content/Identity/Reference/streamingpolicyreference.htm) and [stream-pool private endpoint requirements](https://docs.oracle.com/en-us/iaas/Content/Streaming/Tasks/managing-stream-pools.htm).



```text
Allow dynamic-group <VM_DYNAMIC_GROUP> to {KAFKA_CLUSTER_INSPECT, KAFKA_CLUSTER_CONFIG_INSPECT} in compartment id <VM_COMPARTMENT_OCID>
```

For the app's cluster/configuration create, read and delete workflow, the corresponding compartment-scoped management policy is:

```text
Allow dynamic-group <VM_DYNAMIC_GROUP> to {KAFKA_CLUSTER_INSPECT, KAFKA_CLUSTER_READ, KAFKA_CLUSTER_CREATE, KAFKA_CLUSTER_UPDATE, KAFKA_CLUSTER_DELETE, KAFKA_CLUSTER_CONFIG_INSPECT, KAFKA_CLUSTER_CONFIG_READ, KAFKA_CLUSTER_CONFIG_CREATE, KAFKA_CLUSTER_CONFIG_UPDATE, KAFKA_CLUSTER_CONFIG_DELETE} in compartment id <VM_COMPARTMENT_OCID>
```


## Mapping and deployment

1. Test and register the source and target MySQL connections using Vault credentials.
3. Register a mapping. Select source schema/table and target schema from live database metadata. Select a compatible audit table or choose automatic creation. Select the messaging connection and data topic.
4. Run source validation. Choose **Prepare schema topic** in Control Center or the mapping toolbar. The schema-change topic is named `audit_<flow_id>` and requires exactly one partition. Wait for both this topic and the selected data topic to become ACTIVE. The data topic carries row images; the schema-change topic carries public DDL/schema events. The preparation action checks existing topics without changing their settings or contents.
5. Build & publish the Debezium and history writer images in Container Registry, then select their releases and workload settings. Deploy from the service or Control Center workflow. The application creates OCI Container Instances only after replication, target layout, queue and scope checks pass.

Changes to workload defaults apply to subsequent deployment revisions; an existing instance retains its deployed settings. Stop and replace a flow through Control Center for changed images or configuration, preserving its durable checkpoint and schema-history stores. Start/stop/restart use the OCI Container Instances API and verify flow ownership.

For a Debezium image update, select the flow row and choose **Update Debezium image**.
Choose the exact completed release in its dialog and submit the update. This choice
applies only to the selected flow; Workload settings remain deployment defaults.
The action validates
the image, stops only Debezium, and waits for INACTIVE before replacement. The
writer continues draining the queue. If shutdown is still pending after 60 seconds,
wait for INACTIVE, reopen Update and choose the intended release again. The new instance
uses the same source identity, native offsets and JDBC schema history.
The previous instance stays stopped for rollback and can be explicitly deleted
from the OCI resource inventory after verification. Never start both copies.

Dashboard and Control Center refresh process status every 15 seconds while
visible. An ACTIVE OCI instance whose container process has exited is reported
as **attention**, including its exit code. Creation is **deploying**, missing
workloads are **pending**, and both running processes plus a fresh RUNNING connector-task report are **active**. An intentionally paused connector with a running writer is **capture paused**; an unexpected inactive workload is **attention**. Stop pauses capture; More actions → Stop both workloads stops both for maintenance. Read-only
polls do not modify the saved registration; **Refresh status** saves the current
observation. Process/task health does not prove completed source-to-target delivery. Check logs and captured target events as well.

Deployment records each created container before starting the next workload. If Debezium is created but history-writer creation fails, retry **Deploy** to retain the recorded Debezium instance and create the missing writer. The writer's target connection payload includes only its endpoint, username, Vault secret reference and runtime TLS settings; registration timestamps and other repository metadata are excluded.

Deployment checks the source configuration before queue and target preparation. `PARTIAL_JSON` must be disabled even when `binlog_row_image=FULL`; source validation reports it as a blocker. HeatWave accounts with `FLUSH_TABLES` and `LOCK TABLES` satisfy the snapshot-lock requirement without `RELOAD`, as described in [Oracle's HeatWave Debezium guidance](https://blogs.oracle.com/mysql/heatwave-mysql-solving-missing-updates-for-debezium-cdc). The remaining replication and snapshot grants are still required. The app advises these settings and does not change the source server during validation.

Runtime settings must reference a dedicated, same-compartment Vault secret in `fingerprint_secret_ocid`. It holds the stable row-fingerprint key; do not replace it for an existing history namespace. Missing configuration is reported before creating containers. A new draft mapping does not initialize target tables: history lookup is available after successful target preparation during deployment. MySQL failures report an operation and numeric code without exposing SQL, account details, passwords or row values.

Use **Runtime settings → Row-history fingerprint key → Generate and configure key** for first-time setup. Select an ACTIVE vault and an ENABLED AES encryption key from its dropdowns; both are verified in the VM compartment. The app generates 256 bits of randomness, stores a hexadecimal representation as a dedicated Vault secret and saves only its OCID. No secret value is displayed or written to deployment JSON, browser state or logs. The AES key encrypts the Vault secret; the secret's generated contents are the HMAC key used for row fingerprints.

Setup uses a stable, installation-specific secret name and ownership tags. If creation succeeds but the response or configuration save fails, retrying reconciles the existing secret without changing its contents. The configured reference cannot be removed or changed through workload or runtime settings. Preserve the secret value and do not enable automatic rotation: changing it would disconnect existing row-history lookups from their stored fingerprints. Restoring a deployment with existing history requires its original secret, not generating a replacement.

## Operational challenges and controls

Resource creation is asynchronous. Refresh the inventory until the resource becomes ACTIVE; submitted does not mean ready. Stored specification digests and stable OCI retry tokens protect retries where the API supports them. CreateStream does not accept an OCI retry token: the app checks the selected pool and stream name, records its request identifier as a resource tag, and reconciles a matching stream before any repeat create call. It disables automatic create retries and refuses to adopt an unrelated existing stream. Definitive rejected requests can be corrected and retried. Ambiguous requests older than the safe retry window require inventory review rather than automatic duplicate creation. Secret request digests use HMAC; plaintext secret values are not stored in the repository.

Repeating an accepted stream/topic request re-reads its stored OCI identifier and verifies the compartment, app ownership, pool, name, partition count, retention and lifecycle state. An ACTIVE or CREATING match is reused without creating another stream or changing its settings. Different settings and unavailable states are rejected. Topic creation returns to the selected connection's inventory; Refresh updates its state. Mapping dropdowns include only ACTIVE topics, and stream lists exclude DELETED entries.

Deletion is restricted to unused resources tagged as created by this app. Every selected resource is checked before any delete call. Referenced queues, credentials, active containers, nonempty pools/vaults and referenced configurations are refused. OCI deletion can still partially succeed if a later service call fails; refresh before retrying. Existing streams are never adopted as app-owned merely because a create request found the name already present.


Queue provisioning does not increase source binlog extraction parallelism: a MySQL source remains ordered. Scale independent source databases with separate connector identities, spread data topics across partitions and scale writer consumers within each consumer group. Keep all events for a row on the same partition. Large transactions, hot keys, source retention and target index/partition overhead remain capacity constraints; monitor lag and size retention for the longest recoverable interruption.


## Image build and publish

In Configuration & setup → OCI resources → Container Registry, create or select a private repository in the VM compartment. Choose **Build & publish**, select **History writer** or **Debezium**, and choose that repository. The OL9 VM builds the installed application source with rootless Podman and uploads the image to:

```text
<region-key>.ocir.io/<namespace>/<repository>:historywriter-<UTC timestamp>-<revision>
<region-key>.ocir.io/<namespace>/<repository>:debezium-<UTC timestamp>-<revision>
```

For example, a history writer release tag is `historywriter-20261023T181500Z-a1b2c3d4e5f6`. The twelve hexadecimal revision characters identify a SHA-256 manifest of the copied application build inputs. They do not require a Git commit. The timestamp uses UTC to the second. Existing release tags are refused. Dependency/base-image changes can produce a different binary for the same source revision; the registry digest identifies the exact published image.

The embedded capture image aligns Debezium, JDBC history, MySQL driver and OCI SDK dependencies in one build. Runtime checks validate provider loading and the native capture stack before publication.

The image releases table shows the destination, release, component, status, creation time and result. Select a row to download its build log. CSV download, search, paging, sorting, resizing and column reordering use the shared table controls. Completed releases appear by name in OCI Container Instances → Workload settings for initial deployment defaults, and in the matching Control Center Update image dialog for a specific flow. The dialog lists only available completed app-built releases from the configured repository; the current digest is excluded. Submitting resolves the release to a verified, same-compartment `@sha256:` address without changing other flows or global defaults. Creating a build does not replace a running workload.

`heatwave-audit-image-builder.service` processes one build at a time, with a maximum of four active/queued requests. Application source is copied into a private, checksummed context; runtime configuration, credentials, TLS files, logs and local databases are excluded. The worker rejects changed context files, checks image imports, verifies the repository again before upload, then checks the published digest in OCI inventory. Authenticated administrator routes and CSRF checks protect build submission and log access. Build states and logs are stored under Git-ignored `.data/image-builds/`.

The VM uses its instance principal to obtain a short-lived OCIR token. The token is stored in a temporary `0600` auth file in a `0700` directory, never passed as a command argument, and removed after push or worker recovery. There is no static registry login or API-key fallback. The web service retains its existing hardening; the separate non-root builder permits Podman's user-namespace helpers. The builder launcher loads the app-local `.runtime.env` and checks `AUDIT_CONFIG_FILE` before reporting itself ready, so systemd and manual launches use the same private configuration. OL9 requires Podman, configured subordinate UID/GID ranges and outbound access to the base-image registries, package repositories and regional OCIR endpoint. Rootless image storage is isolated under `.data/image-builder/`. The service keeps Podman runtime state in its private systemd-managed `/run/heatwave-audit-image-builder/` directory so reboot clears stale runtime files while retaining the image cache.

The VM dynamic group needs repository management in the VM compartment for the create/publish workflow:

```text
Allow dynamic-group <VM_DYNAMIC_GROUP> to manage repos in compartment id <VM_COMPARTMENT_OCID>
```

OCI Container Instances use **their own resource principal** for private OCIR pulls by default. They do not inherit the VM identity. An administrator can define a container dynamic group and grant it access to repositories in the same compartment:

```text
ALL {resource.type='computecontainerinstance', resource.compartment.id='<VM_COMPARTMENT_OCID>'}
Allow dynamic-group <CONTAINER_DYNAMIC_GROUP> to read repos in compartment id <VM_COMPARTMENT_OCID>
```


An interrupted upload can leave an image in Registry even when the build status is failed/interrupted. Review releases before retrying; a new request receives a new timestamp. The worker removes temporary contexts and its local release reference; shared build layers and logs remain for diagnostics and reuse. Monitor VM disk space and apply an operator-controlled cache/log retention policy. Container image retention is separate from target audit-table housekeeping; do not remove an image still needed for deployment or rollback.

References: [Oracle OCIR credential helper tutorial](https://docs.oracle.com/en/learn/cred-helper/index.html), [Container Instances private registry policy](https://docs.oracle.com/en-us/iaas/Content/container-instances/permissions/policy-reference.htm), [Podman build](https://docs.podman.io/en/latest/markdown/podman-build.1.html), [Podman push](https://docs.podman.io/en/latest/markdown/podman-push.1.html).

## HeatWave DB system controls

Database connection setup can create dedicated Debezium/history-writer accounts
using temporary administrator inputs. This is separate from creating or starting
an OCI DB system. See [dedicated database accounts](dedicated-database-accounts.md)
for selected-database grants, direct Vault password storage and handoff to runtime
credentials. Existing deployed registrations are not automatically migrated.

Select one or more DB systems in the HeatWave resource table and use **Start selected** or **Stop selected** on the left toolbar. Start requires all selected systems to be `INACTIVE`; Stop requires `ACTIVE`. Stop prompts for confirmation because database connections and any dependent capture or history writing will be interrupted. The application uses OCI's `FAST` shutdown mode, checks every selected system's compartment and state before any action, and omits ETag/If-Match headers. OCI operations are asynchronous; refresh to see their resulting state. If OCI rejects one of several submissions, the page reports each result. It does not imply an all-or-nothing cloud transaction.

Creation uses an availability-domain selector, a live DB-system shape selector and a live MySQL version selector. Configurations reload when the shape changes. The configuration list contains an OCI default choice for that shape and active custom configurations from the VM compartment with the exact same shape. Oracle owns its default templates globally; selecting the default omits `configuration_id` and lets OCI apply its default. It does not attach a foreign custom configuration. The backend validates the shape, version and custom configuration again before creation. The form creates a standalone DB system; adding or sizing a HeatWave analytical cluster is a separate operation.

## Deletion ownership

Delete is disabled when the selection is empty or contains any resource not managed by **this app installation**. Such resources remain selectable for download and applicable Start/Stop actions. Backend deletion applies the same ownership rule and existing in-use checks regardless of browser state.

New resources carry both `managed-by=heatwave-audit` and an `audit-app-id` matching the stable, private `.data/app-instance-id` of the installation. A resource carrying another installation's identity is never eligible. Older resources with only the application tag require a matching successful provisioning record in this installation's embedded MySQL repository; the generic tag alone is insufficient. Preserve the private identity and repository during backup/restore. Existing unmanaged resources are not retagged or adopted automatically.

## Native OCI Streaming configuration

Add a queue connection with only a name and the selected stream pool. The VM
validates the pool, streams and private attachments with its instance principal.
The capture and writer use their own resource principals for native Streaming
is required. Database passwords and the fingerprint key remain in Vault.

Select/prepare the data stream for the mapping. Deploy prepares its separate
single-partition public schema stream automatically. The persisted stream IDs,
not just names, bind checkpoints to the correct retained evidence. Recreating a
stream with the same name is not a recovery shortcut.

The Java capture publisher acknowledges Debezium batches only after every OCI
publish succeeds. The Python writer uses individual partition cursors; target
commit and durable checkpoints precede in-memory acknowledgment. Retention gaps,
partial publishes and oversized records fail without silently skipping evidence.
Message Explorer samples recent messages without changing the writer checkpoint.

An administrator grants workload Streaming access, scoped to the VM compartment:

```text
Allow dynamic-group <CONTAINER_DYNAMIC_GROUP> to {STREAM_INSPECT, STREAM_READ, STREAM_CONSUME, STREAM_PRODUCE} in compartment id <VM_COMPARTMENT_OCID>
```

The app does not modify IAM. See [IAM and database access](iam-policy.md).

DB system Start/Stop does not send ETag/If-Match headers. Errors identify the requested action, resource, last checked lifecycle state and OCI request ID. See the observed failure and concurrency considerations below.

For a history-writer image update, use **Update history writer image** on the selected flow. The action stops only that writer and waits for INACTIVE before replacement. It preserves the connector and writer checkpoints. Saving an image selection or pressing Start does not upgrade an existing instance. See the [replacement procedure](operations.md#update-an-image-without-losing-position).

## HeatWave lifecycle operations and ETags

HeatWave OCI operations in this application **do not send ETag/If-Match headers**.
This applies to Start/Stop; the existing HeatWave creation/deletion paths also do
not send them. Request identifiers, instance-principal authentication and the VM
compartment restriction remain in effect. Deletion still requires installation
ownership and unused dependencies. Start/Stop can operate on permitted selected
DB systems in the compartment, with a fresh supported-state check.

The observed failure was HTTP 412 `NoEtagMatch` on DB-system Start even after a
fresh GET/retry. Some GET responses carried representation ETags ending in
`--gzip`; requesting an uncompressed representation removed that suffix, but a
subsequent resource-version mismatch still occurred. It would be inaccurate to
attribute every 412 solely to compression or to tell the operator that refreshing
must resolve it. ETags are opaque; the app does not strip a suffix and reuse a
modified token.

Oracle documents `if_match` as optional for StartDbSystem/StopDbSystem. The revised
lifecycle path omits it, checks each selected resource's compartment and required
state before submitting, and uses one idempotency token per operation. Start
requires INACTIVE and Stop requires ACTIVE. The OCI service still validates its
own operation/state prerequisites. There is a possible race between the read and
submission; without If-Match this is not optimistic version locking. A concurrent
operation can still be rejected and must be reviewed, rather than retried blindly.
[Oracle Python DB system client](https://docs.oracle.com/en-us/iaas/tools/python/latest/api/mysql/client/oci.mysql.DbSystemClient.html).

A failed action reports the resource name, requested action, last checked DB
state, safe OCI code and request ID. A remaining 412 is not automatically an IAM
or password problem. Refresh the lifecycle state, inspect ongoing work requests
and retain the request ID for service investigation. The app does not switch
principals, broaden scope or disable ownership checks to work around failures.

A stopped target can make the history writer exit with a database connectivity
error while the Container Instance remains ACTIVE. Restore DB availability,
verify the configured login, then restart/resume the registered workload with its
existing image and checkpoints. Source reconnection and target checkpoint
progress must be verified separately. Prior stopped instances from image
replacement are not themselves evidence that the currently registered workload
is stopped; do not start every inactive instance or create duplicate writers.


See [the same-compartment limitation](compartment-scope.md) for Vault/secret
selection, other OCI resource boundaries and the host-only database identity
exception. Broader IAM access does not expand this application boundary.
