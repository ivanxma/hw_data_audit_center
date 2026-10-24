# Configuration, service provisioning and scope

The application runs on OL9. Debezium and the history writer run as OCI Container Instances. Configuration & setup includes a service Tab View with inventory, create, refresh, selection, guarded delete and CSV download.

Resource lists exclude the `DELETED` state. Resources still in `DELETING` remain visible until deletion completes.

| Service | Resources and actions |
| --- | --- |
| OCI Streaming | Create pools, streams and Kafka Connect configurations; select an existing pool in a queue connection; OCI principals handle messaging authentication. |
| OCI Streaming with Apache Kafka | Create broker configurations and clusters; inspect/create/delete application topics through the selected queue connection. |
| OCI Container Instances | Select workload subnet, availability domain, private repository image release, authentication mode, shape and size; deploy a registered flow after readiness checks. |
| Container Registry | List/create private repositories; Build & publish Debezium and history writer images on the VM, track progress and download build logs. |
| Vault | List/create vaults and secrets using an existing symmetric encryption key. Secret and vault deletion is scheduled with an eight-day delay. |
| HeatWave | List/create DB systems; start/stop selected systems. Creation loads availability domains, available DB system shapes, matching configurations and MySQL versions from OCI. Database connections themselves use supplied endpoints and an actual MySQL connection test. |

Use [IAM and database access](iam-policy.md) for the consolidated policy templates and [OL9 setup and operations](operations.md) for installation, image replacement and recovery.

## Compartment boundary

The only selectable compartment is the VM's compartment. Every Cloud client verifies private configuration against VM instance metadata and uses instance-principal authentication. Inventory is scoped to that compartment and returned objects are verified. Creation and deletion repeat these checks. Existing subnets, their VCNs, private endpoint attachments, registry images, secrets and Kafka configurations are checked before use. No IAM or shared-network modifications are performed.

An operator cannot extend scope by changing a form field or supplying a resource identifier. Missing service permissions remain an error; there is no credential fallback. The Database connection endpoint is not resolved to a DB-system OCID, in accordance with the connection design.

## Resolving stream-pool access errors

A successful stream inventory does not establish permission to manage stream pools or Connect configurations. OCI uses separate `streams`, `stream-pools` and `connect-harness` resource types. A denied list request in the verified VM compartment returns `NotAuthorizedOrNotFound`; the app now shows the operation, code, request ID and scoped guidance without exposing request credentials or raw OCI response bodies.

An OCI administrator should verify that the VM belongs to the intended dynamic group. For the complete Streaming workflow, use the family policy below; replace both placeholders, including the identity-domain prefix where needed:

```text
Allow dynamic-group <VM_DYNAMIC_GROUP> to manage stream-family in compartment id <VM_COMPARTMENT_OCID>
```

This family includes `streams`, `stream-pools` and `connect-harness`; it does not cover managed Apache Kafka. A private endpoint additionally requires `use vnics` and `use subnets` in the same compartment; an NSG requires its corresponding use permission when selected. The application does not apply IAM policies. Refresh inventory after the administrator resolves access, then retry the same rejected request.

These permissions are documented in Oracle's [Streaming policy reference](https://docs.oracle.com/en-us/iaas/Content/Identity/Reference/streamingpolicyreference.htm) and [stream-pool private endpoint requirements](https://docs.oracle.com/en-us/iaas/Content/Streaming/Tasks/managing-stream-pools.htm).

## Resolving managed Apache Kafka access errors

OCI Streaming with Apache Kafka uses separate permissions from OCI Streaming's `stream-family`. For cluster and configuration inventory, an administrator can grant the VM dynamic group these permissions within the VM compartment:

```text
Allow dynamic-group <VM_DYNAMIC_GROUP> to {KAFKA_CLUSTER_INSPECT, KAFKA_CLUSTER_CONFIG_INSPECT} in compartment id <VM_COMPARTMENT_OCID>
```

For the app's cluster/configuration create, read and delete workflow, the corresponding compartment-scoped management policy is:

```text
Allow dynamic-group <VM_DYNAMIC_GROUP> to {KAFKA_CLUSTER_INSPECT, KAFKA_CLUSTER_READ, KAFKA_CLUSTER_CREATE, KAFKA_CLUSTER_UPDATE, KAFKA_CLUSTER_DELETE, KAFKA_CLUSTER_CONFIG_INSPECT, KAFKA_CLUSTER_CONFIG_READ, KAFKA_CLUSTER_CONFIG_CREATE, KAFKA_CLUSTER_CONFIG_UPDATE, KAFKA_CLUSTER_CONFIG_DELETE} in compartment id <VM_COMPARTMENT_OCID>
```

Cluster creation also requires administrator-configured `rawfka` service access to VNICs, subnets and network security groups in that same compartment. SASL/SCRAM setup requires the service's secret metadata and update permissions described in [Oracle's Kafka setup requirements](https://docs.oracle.com/en-us/iaas/Content/kafka/gs.htm). The app does not apply these policies. If the correct permissions and dynamic-group membership are already present, check regional service availability and retain the OCI request ID for support. Failed inventories show an unavailable state and policy guidance; Refresh retries the request without leaving stale selectable resources.

## Mapping and deployment

1. Test and register the source and target MySQL connections using Vault credentials.
2. Create or select a stream pool or managed Kafka cluster. Register its messaging connection; only managed Kafka requires broker credentials. Create the data stream/topic.
3. Register a mapping. Select source schema/table and target schema from live database metadata. Select a compatible audit table or choose automatic creation. Select the messaging connection and data topic. OCI Streaming additionally requires an active Kafka Connect configuration.
4. Run source validation. Choose **Prepare schema topic** in Control Center or the mapping toolbar. The schema-change topic is named `audit_<flow_id>` and requires exactly one partition. Wait for both this topic and the selected data topic to become ACTIVE. The data topic carries row images; the schema-change topic carries public DDL/schema events. The preparation action checks existing topics without changing their settings or contents.
5. Build & publish the Debezium and history writer images in Container Registry, then select their releases and workload settings. Deploy from the service or Control Center workflow. The application creates OCI Container Instances only after replication, target layout, queue and scope checks pass.

Changes to workload defaults apply to subsequent deployment revisions; an existing instance retains its deployed settings. Stop and replace a flow through Control Center for changed images or configuration, preserving its durable checkpoint and schema-history stores. Start/stop/restart use the OCI Container Instances API and verify flow ownership.

For a Debezium image update, select the published release in workload settings,
select the flow row and choose **Update Debezium image**. The action validates
the image, stops only Debezium, and waits for INACTIVE before replacement. The
writer continues draining the queue. If shutdown is still pending after 60 seconds,
wait for INACTIVE and retry Update. The new instance
uses the same connector name, Connect state topics and JDBC schema history.
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

Deletion is restricted to unused resources tagged as created by this app. Every selected resource is checked before any delete call. Referenced queues, credentials, active containers, nonempty pools/vaults and referenced configurations are refused. OCI deletion can still partially succeed if a later service call fails; refresh before retrying. Existing Kafka topics are never adopted as app-owned merely because a create request found the name already present.

OCI Streaming supports the Kafka API with service-specific constraints. Debezium uses Kafka Connect with the exact Connect configuration topics and durable JDBC schema history. Managed Apache Kafka uses Kafka internal state topics and SCRAM-SHA-512. Both paths use a separate history writer with bounded batches and acknowledgement after durable target commit. Keep Connect state and schema history across restarts; do not treat a configuration switch as a new source snapshot.

Queue provisioning does not increase source binlog extraction parallelism: a MySQL source remains ordered. Scale independent source databases with separate connector identities, spread data topics across partitions and scale writer consumers within each consumer group. Keep all events for a row on the same partition. Large transactions, hot keys, source retention and target index/partition overhead remain capacity constraints; monitor lag and size retention for the longest recoverable interruption.

References: [OCI Streaming Kafka compatibility](https://docs.oracle.com/en-us/iaas/Content/Streaming/Tasks/kafkacompatibility.htm), [Kafka Connect with OCI Streaming](https://docs.oracle.com/en-us/iaas/Content/Streaming/Tasks/kafkacompatibility_topic-Kafka_Connect.htm), [OCI Streaming with Apache Kafka](https://docs.oracle.com/en-us/iaas/Content/kafka/home.htm).

## Image build and publish

In Configuration & setup → OCI resources → Container Registry, create or select a private repository in the VM compartment. Choose **Build & publish**, select **History writer** or **Debezium**, and choose that repository. The OL9 VM builds the installed application source with rootless Podman and uploads the image to:

```text
<region-key>.ocir.io/<namespace>/<repository>:historywriter-<UTC timestamp>-<revision>
<region-key>.ocir.io/<namespace>/<repository>:debezium-<UTC timestamp>-<revision>
```

For example, a history writer release tag is `historywriter-20261023T181500Z-a1b2c3d4e5f6`. The twelve hexadecimal revision characters identify a SHA-256 manifest of the copied application build inputs. They do not require a Git commit. The timestamp uses UTC to the second. Existing release tags are refused. Dependency/base-image changes can produce a different binary for the same source revision; the registry digest identifies the exact published image.

Debezium uses Kafka Connect's versions of shared runtime libraries. The image
removes duplicate OCI SDK dependency jars for artifacts already supplied by
Connect, preventing mixed Jersey/HK2 versions. The image check detects duplicate
Jersey classes and initializes the Jersey server runtime before publication;
loading the OCI SASL provider alone cannot detect this startup failure.

The image releases table shows the destination, release, component, status, creation time and result. Select a row to download its build log. CSV download, search, paging, sorting, resizing and column reordering use the shared table controls. Completed releases appear by name in OCI Container Instances → Workload settings. Saving the selection resolves it to a verified, same-compartment `@sha256:` address. Creating a build does not replace a running workload; use the normal flow replacement procedure to deploy it.

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

Vault image pull authentication remains optional for an explicitly configured workload. Existing configurations with a pull secret retain that mode until changed. Source and target passwords remain in Vault. New OCI Streaming connections do not use a messaging token secret; managed Kafka credentials remain separate. The application does not create or modify IAM policies.

An interrupted upload can leave an image in Registry even when the build status is failed/interrupted. Review releases before retrying; a new request receives a new timestamp. The worker removes temporary contexts and its local release reference; shared build layers and logs remain for diagnostics and reuse. Monitor VM disk space and apply an operator-controlled cache/log retention policy. Container image retention is separate from target audit-table housekeeping; do not remove an image still needed for deployment or rollback.

References: [Oracle OCIR credential helper tutorial](https://docs.oracle.com/en/learn/cred-helper/index.html), [Container Instances private registry policy](https://docs.oracle.com/en-us/iaas/Content/container-instances/permissions/policy-reference.htm), [Podman build](https://docs.podman.io/en/latest/markdown/podman-build.1.html), [Podman push](https://docs.podman.io/en/latest/markdown/podman-push.1.html).

## HeatWave DB system controls

Select one or more DB systems in the HeatWave resource table and use **Start selected** or **Stop selected** on the left toolbar. Start requires all selected systems to be `INACTIVE`; Stop requires `ACTIVE`. Stop prompts for confirmation because database connections and any dependent capture or history writing will be interrupted. The application uses OCI's `FAST` shutdown mode, checks every selected system's compartment and state before any action, and sends the fetched ETag for concurrency control. OCI operations are asynchronous; refresh to see their resulting state. If OCI rejects one of several submissions, the page reports each result. It does not imply an all-or-nothing cloud transaction.

Creation uses an availability-domain selector, a live DB-system shape selector and a live MySQL version selector. Configurations reload when the shape changes. The configuration list contains an OCI default choice for that shape and active custom configurations from the VM compartment with the exact same shape. Oracle owns its default templates globally; selecting the default omits `configuration_id` and lets OCI apply its default. It does not attach a foreign custom configuration. The backend validates the shape, version and custom configuration again before creation. The form creates a standalone DB system; adding or sizing a HeatWave analytical cluster is a separate operation.

## Deletion ownership

Delete is disabled when the selection is empty or contains any resource not managed by **this app installation**. Such resources remain selectable for download and applicable Start/Stop actions. Backend deletion applies the same ownership rule and existing in-use checks regardless of browser state.

New resources carry both `managed-by=heatwave-audit` and an `audit-app-id` matching the stable, private `.data/app-instance-id` of the installation. A resource carrying another installation's identity is never eligible. Older resources with only the application tag require a matching successful provisioning record in this installation's embedded MySQL repository; the generic tag alone is insufficient. Preserve the private identity and repository during backup/restore. Existing unmanaged resources are not retagged or adopted automatically.

## OCI Streaming without a queue token

New OCI Streaming connections require only a name and a selected stream pool. The application derives the pool endpoint and authenticates VM-side validation and Message Explorer with the VM instance principal. Debezium and the history writer authenticate with their OCI Container Instances resource principals using Oracle's `OCI-RSA-SHA256` Kafka SASL provider. No OCI user lookup, formatted Kafka username or auth-token secret is needed for this mode.

The Python history writer calls the Java Kafka consumer through JPype. It retains the existing Kafka protocol, topic/partition coordinates, group names, offsets and target-side checkpoints. Consumer rebalances seek to validated checkpoints without cancelling group subscriptions. Automatic topic creation, offset reset and commit remain disabled; target writes and checkpoint persistence precede synchronous Kafka acknowledgement. The change does not introduce an OCI REST consumer group or silently translate offsets.

`setup.sh` installs Java 21 and the pinned Java dependencies. The VM dependency jars live in private `.data/java/streaming-libs/`; the two workload images include their required provider dependencies. Build & publish and select new image releases before deploying a principal-based flow. Existing queue registrations containing SASL credentials continue to use their existing authentication, and their secrets are not deleted or overwritten automatically.

The container dynamic group must have Streaming access in the VM compartment, in addition to its existing repository/Vault permissions. An administrator can use the following scoped permission set:

```text
Allow dynamic-group <CONTAINER_DYNAMIC_GROUP> to {STREAM_INSPECT, STREAM_READ, STREAM_CONSUME, STREAM_PRODUCE} in compartment id <VM_COMPARTMENT_OCID>
```

The app does not create or change IAM policies. Source/target database passwords and the row-fingerprint key still use Vault. Apache Kafka broker authentication remains SASL/SCRAM with its configured username and password secret; OCI Streaming principal authentication is not applied to that service.

References: [Oracle resource-principal Kafka authentication](https://docs.oracle.com/en/learn/ocifn-kafka-resource-principal/), [Oracle Java resource-principal login module](https://github.com/oracle/oci-java-sdk/blob/master/bmc-addons/bmc-sasl/src/main/java/com/oracle/bmc/auth/sasl/ResourcePrincipalsLoginModule.java).

DB system Start/Stop keeps OCI ETag concurrency checks. On `NoEtagMatch` (HTTP 412), the app refreshes the resource and retries once only if the compartment and required state still match. Further conflicts show refresh guidance and the OCI request ID.

For a history-writer image update, use **Update history writer image** on the selected flow. The action stops only that writer and waits for INACTIVE before replacement. It preserves the connector and writer checkpoints. Saving an image selection or pressing Start does not upgrade an existing instance. See the [replacement procedure](operations.md#update-an-image-without-losing-position).
