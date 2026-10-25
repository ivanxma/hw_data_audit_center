# HeatWave Audit Center

## Background

HeatWave Audit Center is a prototype for exploring row-change auditing with OCI
MySQL HeatWave. It brings connection setup, table registration, capture controls
and audit-history exploration into one web application running on an Oracle
Linux 9 Compute VM.

The aim is to capture changes to registered source tables through Debezium and
OCI messaging, retain BEFORE and AFTER row images in a target HeatWave database,
and make row history, schema changes and activity charts easier to investigate.

**This is an independent prototype for evaluation, demonstration and rehearsal.
It is not an Oracle product and is not supported or endorsed by Oracle.**

The data flow is:

HeatWave binlog → Debezium on OCI Container Instances → OCI Streaming or managed Apache Kafka → history writer on OCI Container Instances → target HeatWave audit store.

The web application provides a dashboard, connection/table registration, source replication validation, tabbed OCI service provisioning, service controls, bounded queue exploration, row-image history, charts and monthly audit-partition inventory. Private deployment configuration is selected with `AUDIT_CONFIG_FILE`. OCI management calls use the VM's instance principal and reject resources outside its compartment.

Current guides: [setup and operations](docs/operations.md), [IAM and database access](docs/iam-policy.md), [capture positioning](docs/capture-start.md).

The [user manual](docs/user-manual/html/index.html) covers the prototype background,
architecture and network diagrams, OL9 installation, every app page and its
fields/actions, operations, scaling, updates and cleanup. A
[printable PDF](docs/user-manual/pdf/HeatWave-Audit-Center-User-Manual.pdf) is included.
A [Markdown edition](docs/user-manual/md/README.md) is also included.
Screenshots use clearly labelled illustrative data.

**Application limitation:** Vaults, secrets and other app-managed OCI resources
must stay in the VM compartment and region. See the
[supported resource boundary and host-only DB exception](docs/compartment-scope.md).

## Choose where Debezium starts reading

For a new flow, open **Configuration & setup → Table mappings → Register table →
Initial capture**. Choose **End** to capture schema metadata and future binlog
changes without copying existing rows. Choose **Start** to snapshot current rows
as SNAPSHOT events, then follow new changes. Before deployment, select the mapping
and choose **Edit** to change this initial choice.

For an existing flow, open **Control Center**, select its checkbox, choose
**More actions… → Capture start**, and click **Run**. The position page offers:

| Choice | Meaning for an existing flow |
| --- | --- |
| Start | Replay from the earliest retained schema baseline, without another initial row snapshot. |
| End | Capture after the source position recorded when the preview is created. |
| Specific GTID | Capture after the supplied complete executed GTID set. |
| Datetime | Resolve a UTC time to the first retained transaction at or after it. |

Choose **Preview position**, review the resolved boundary, acknowledge the effect
of replaying or skipping changes, then choose **Apply this position once**. The
app validates binlog retention and schema-history coverage; positions older than
the retained baseline or beyond confirmed schema coverage are rejected. Specific
GTID and datetime positioning require an established schema baseline and a
compatible Debezium capture-control image.

Normal **Start**, **Resume** and **Restart** continue from the saved checkpoint;
they do not apply a new capture position. See [Capture start](docs/capture-start.md)
for validation, recovery and GTID semantics.

## Five-minute demonstration

Open **Dashboard → Watch demo** in the running application, or visit `/demo/`
after signing in. The [demo playback page](demo/index.html) provides an embedded
player, English captions and chapter navigation; the video plays in the browser.
Its files are also available under `demo/` for local hosting. The video shows
the source on the left, the application/browser in the centre and target audit
evidence on the right, with the browser enlarged during app operations. It covers
the OCI Streaming architecture, registration, INSERT/UPDATE/DELETE images, row-key
fingerprint lookup, DDL structure comparisons and chart drill-down.

The video uses labelled illustrative data rather than a live delivery test. Its
user/privilege segment explains the separate database-audit collector required
for DCL history; that collector is not implemented in this application. English
captions, example SQL and the architecture diagram accompany the video in `demo/`.

## OL9 installation

Run as a non-root application owner with sudo. Python 3.12+, app-managed MySQL 9.x, Gunicorn and systemd are used. MySQL listens only on an application-owned Unix socket. OL9 is the only deployment platform.

Provide an official MySQL Linux generic tarball URL and its verified SHA256, plus a private deployment JSON created on this VM:

```bash
export AUDIT_CONFIG_FILE=/path/to/private/deployment.json
export AUDIT_MYSQL_TARBALL_URL='https://official-download-host/path/to/mysql-9.x-linux-generic.tar.xz'
export AUDIT_MYSQL_TARBALL_SHA256='<verified-sha256>'
./setup.sh
.venv/bin/python scripts/enroll_admin.py
```

Enrollment prompts privately for the explicit bootstrap password; it is not written to the configuration. The first `localadmin` login requires a password change, clears all sessions for that account and signs out. Re-running enrollment does not reset an existing administrator. Use the authenticated password-change screen for subsequent changes.

The installer creates `heatwave-audit-local-mysql.service` and `heatwave-audit-https.service`. HTTPS binds to `0.0.0.0:443`; browse the VM's routable address or DNS name. The initial certificate is self-signed; install a certificate matching the intended DNS name for trusted browser access. Generated runtime state, keys, data and private configuration stay out of Git.

## Capture readiness

Register source and target connections using host/port, database accounts and Vault password secret references. Test connection checks the actual MySQL login; no DB system OCID or IP-to-OCI lookup is required. TLS encryption is required without certificate verification or CA files by default. The global `mysql_tls` policy applies to tests, writers and Debezium. Configure transport and image settings through the authenticated Runtime Settings screen. Example fields are in `examples/deployment.example.json`; all deployment values are placeholders.

Deploy only after live checks pass: binary logging, ROW format, FULL row images, no PARTIAL_JSON, GTID consistency, retention greater than three days, primary key and connector permissions. The app advises source changes rather than modifying source DB settings automatically.

Managed OCI Kafka uses SCRAM-SHA-512 and durable Kafka internal topics. New OCI Streaming connections use OCI principal authentication, exact harness topics and durable JDBC schema history. Debezium and the history writer use Oracle’s Java SASL provider; existing SASL/PLAIN connections remain compatible. The selected connector image must include the appropriate storage module and JDBC driver. App-provisioned Kafka/Streaming resources, DB systems, secrets, subnets and registry repositories stay in the VM compartment. Database connections use the operator-supplied endpoint without OCI IP validation. Tenancy IAM changes are outside this application's scope.

Shared tables keep compact rows. Click a field value, or focus it and press Enter, to open its complete text in a dialog. Escape or Close returns to the table; selection, sorting, column resizing and column reordering remain available. The top-right notification bell keeps recent action messages and your completed or failed image-build results for the current session; Clear all dismisses the list.

## Audit storage

See [Capturing and retrieving BEFORE / AFTER history](docs/before-after-capture.md)
for source settings, event examples, target fields, fingerprint lookup and recovery boundaries.

New audit tables use monthly event-time RANGE partitions, a `(event_time,event_id)` primary key and a row-key history index. An unpartitioned identity ledger deduplicates events across partition boundaries. Target writes commit before queue acknowledgement. An explicit partition preparation action extends the future runway; there is no automatic audit-data deletion.

Row-key and image fingerprints use a Vault-referenced HMAC key. Primary-key lookup accepts the complete key as JSON. Public schema events are archived separately with available before/after definitions and semantic differences. Detailed metadata is labeled partial where the event does not establish every MySQL property. Privilege/security capture requires a separate configured database-audit collector; row CDC alone does not provide it.

**Coverage limitation:** partition exchanges, partition drops/truncates and other
DDL can change large row sets without individual row-image events, even with
ROW/FULL logging. Schema evidence does not reconstruct those missing row changes;
history searches and charts count only captured events. See
[audit coverage and limitations](docs/limitations.md) before using this prototype
for a table that requires complete row history.

## Validation

```bash
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m compileall -q app.py modules scripts worker.py
.venv/bin/pip check
bash -n setup.sh start_https.sh start_http.sh ol9/setup_platform.sh
```

Live source/target capture, pinned connector images, transport recovery, identity continuity and capacity must be validated against the actual deployment before production use.

## Design review and refactoring

[Design validation](docs/design-validation.md) records implemented corrections,
scaling limits, upgrade steps and the remaining engineering work. The processor
now uses bounded batches and target-side delivery checkpoints; it never silently
resets an expired queue offset. Each independent connector requires isolated
Connect state topics or its own OCI Streaming harness profile.

Target housekeeping is opt-in. An administrator enables a per-flow policy in
Runtime Settings, stops the flow, and chooses **Clean eligible history** under
Audit partitions. Cleanup retains the identity ledger, schema history, queue
checkpoints and guard partitions. A durable cutoff rejects late/replayed expired
events. Archive-required and held policies block expiry. See the design review
before enabling a policy or upgrading existing writers.


## OCI Container Instances deployment

The OL9 VM hosts the web control plane and its socket-only MySQL repository.
Debezium and history-writer workloads run on OCI Container Instances with private
VNICs, digest-pinned images from OCI Container Registry and resource-principal
access to permitted Vault secrets. The VM uses instance-principal APIs to create,
start, stop and terminate these instances within its compartment.

Set `AUDIT_CONFIG_FILE` to the private deployment JSON on the VM. The installer
persists this setting in `.runtime.env`. Runtime Settings manages the same file.
Configure the `debezium` and `processor` sections with subnet, availability
domain, image digest, registry, shape and secret references. Workload deployment
remains gated by source, target, topic and permission checks.

Configuration & setup provides tabs for OCI Streaming (pools, streams and Connect configurations), OCI Streaming with Apache Kafka (cluster configurations and clusters), OCI Container Instances, private Container Registry, Vault and HeatWave. Resource lists support selection, sorting, resizing, column reordering and CSV download. Creation uses structured forms; deletion is disabled for resources outside this installation’s ownership and rechecks ownership, scope and in-use references on the server. OCI authorization or availability failures are shown explicitly.

The compartment selector contains only the VM compartment, verified through instance metadata. Changing a submitted compartment or referencing a foreign resource is rejected server-side. Subnets and their VCNs, registry images, Vault secrets and Kafka/Streaming attachments must satisfy the same scope. The application does not create IAM policies or shared networks.

Register an OCI Streaming queue connection by selecting its existing pool; no queue username or token is needed. Managed Kafka connections use their broker username and Vault credential. Create a data stream/topic, then select that connection and topic in the table mapping. OCI Streaming mappings also select an active Connect configuration. Prepare the single-partition schema topic from the mappings toolbar before deployment. Source schemas/tables and compatible target audit tables are fetched from the selected MySQL connection; a new audit table name can be derived automatically. Container workload settings select the subnet, availability domain, private repository image release, resource-principal authentication (or an optional Vault pull secret) and compute size. OCI Container Instances deployment runs the existing flow readiness checks.

Select exactly one HeatWave connection, queue connection or table mapping and choose **Edit** in the table toolbar. The dialog loads its saved values. Changed MySQL settings are tested before saving; draft mapping edits use the same live object and compartment validation as registration. Entries with deployment history allow name changes only, preserving their capture path, offsets and row-fingerprint identity. Referenced connections retain their endpoint; unused connections can change it. Concurrent edits are rejected if the saved entry has changed since the dialog opened.

See [service provisioning and scope](docs/service-provisioning.md) for operation and failure handling.

Container Registry includes **Build & publish** for `debezium-<UTC timestamp>-<revision>` and `historywriter-<UTC timestamp>-<revision>` releases. The OL9 VM builds and uploads them to the selected same-compartment repository using its instance principal. Workload settings show release names and pin the selected digest internally. See [image build and publish](docs/service-provisioning.md#image-build-and-publish).

HeatWave resources support multi-select Start/Stop. DB system creation loads OCI availability domains, available shapes, shape-matched configurations and MySQL versions instead of requiring typed values.

HeatWave OCI operations do not send ETag/If-Match headers. Fresh compartment,
lifecycle, ownership and dependency checks still apply. The observed HTTP 412
issue and revised behavior are documented in
[HeatWave lifecycle operations](docs/service-provisioning.md#heatwave-lifecycle-operations-and-etags).

Control Center's **Update Debezium image** and **Update history writer image**
actions open a release-selection dialog for the selected flow. Review the current
image, choose an available completed component release and confirm its digest.
The update replaces only that workload using existing checkpoints; it does not
change other flows or Workload settings defaults. See the
[image update procedure](docs/operations.md#update-an-image-without-losing-position).

Add connection supports **Create Debezium user** for sources and **Create history
writer user** for targets. Use the administrator only for setup, select databases,
and store the new password directly in a same-compartment Vault. The dedicated
username and secret reference populate the connection after grant/login checks;
the administrator is not registered. Target partition maintenance is an explicit
additional grant option. See [dedicated database accounts](docs/dedicated-database-accounts.md)
for exact grants, scope limits and retry behavior.

Control Center shows each process's latest safe logged error, report time, failed
step and recovery guidance alongside process/exit status. New workload images
report structured diagnostics; older images can expose only the limited cause
they actually logged. Passwords, SQL text and row images are excluded.

Capture position controls and recovery semantics are described in [Capture start](docs/capture-start.md).

Control Center presents one row per flow with process status, selected-image update availability and next-step tips. Select one flow for lifecycle actions. Stop pauses Debezium while the history writer drains queued records and waits; Start resumes capture. Image updates stop and replace only their selected workload. Use More actions → Stop both workloads for maintenance. Audit Center opens on All databases, with Row history available for table-specific lookup. It provides custom UTC ranges, labelled hour/minute/day/week charts, line/bar/stacked-bar views and a global database/table pie chart with drill-down to bar charts.

Read-only VM setup/access check: `.venv/bin/python scripts/check_setup.py --oci --transport streaming` (or `kafka` / `both`). It verifies read access without applying IAM or changing workload state.
