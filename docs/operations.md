# OL9 setup and operating guide

This is the current procedure for the corrected application. OL9 is the deployment
platform; Debezium and history writers run on OCI Container Instances. Review
[IAM and MySQL access](iam-policy.md) before resource setup.

## Install or rerun setup

1. Use the non-root app owner with sudo. Create the private deployment JSON **on
   this VM** from `examples/deployment.example.json`; set its exact VM compartment
   and region. Keep its directory mode 0700 and file mode 0600. Do not copy another
   deployment's credentials, runtime registration or application identity.
2. Export `AUDIT_CONFIG_FILE`. On first install supply the official architecture-
   compatible MySQL 9.x tarball URL and its independently verified SHA256. Run
   `./setup.sh` from the checkout (the scripts resolve their supporting paths).
3. Run `.venv/bin/python scripts/enroll_admin.py` and enter the explicit bootstrap
   password privately. First login forces rotation and sign-out. Rerunning setup
   or enrollment does not reset an existing administrator.
4. Run `.venv/bin/python scripts/check_setup.py`, then its `--oci` form for the
   selected transport. Use actual connection-test buttons for database access.

The installer validates saved configuration before installing local MySQL. It
preserves the saved config path, HTTPS port and custom TLS paths unless explicitly
overridden, keeps MySQL socket-only, validates generated units with the OL9 systemd parser and restarts the web service after validation. Launchers read the private runtime file as the app owner; systemd does not need to read a home-directory EnvironmentFile. `SKIP_PRIVILEGED_SETUP=1` skips package
installation only; service/firewall installation still needs sudo. Do not run
setup while an image build is active. The builder installer refuses to interrupt
an active build. Back up before a setup rerun; the script is not an embedded-MySQL
major-version migration tool and does not replace a working server binary.

| VM service | Role |
| --- | --- |
| heatwave-audit-local-mysql | Socket-only authentication and control repository |
| heatwave-audit-https | HTTPS web app, bound to 0.0.0.0 on the configured port |
| heatwave-audit-image-builder | Separate rootless Podman build/publish worker |

Browse the VM's routable DNS/address, not `0.0.0.0`. Restrict ingress through the
host firewall and administrator-managed OCI network rules. The HTTP launcher is
for explicit loopback development and defaults to 127.0.0.1 even when HTTPS
runtime settings bind publicly. Provide both custom certificate/key paths together.
`verify_bootstrap.py` is for initial rotation-gate verification, not a check to
run with an already rotated password.

## Configure a working flow

1. Test and register source/target connections by host, port, schema, username and
   same-compartment Vault password reference. MySQL TLS defaults to REQUIRED;
   optional global verified trust is a Runtime Settings concern.
2. Generate/configure the dedicated fingerprint secret once. Retain the same key
   for existing history. It is independent of every database password.
3. Create/select an OCI Streaming pool and data stream. Register table selects
   the pool directly and lists its ACTIVE streams; authentication uses OCI principals.
   There is no separate Streaming connection registration. OCI resources manages
   pools and streams from the same pool selector.
4. Register the source table and audit destination. End is the default; Start
   scans existing rows, while Specific GTID uses a validated executed set without
   copying rows. Inspect executed/purged bounds before using GTID.
5. Run source checks: ROW/FULL, empty binlog_row_value_options, GTID mode, primary
   key, retention and effective grants. Size retention for outage plus catch-up.
6. Build and select current native capture/writer images. Deploy prepares the
   separate single-partition schema stream `audit_<flow_id>` automatically and
   persists immutable data/schema stream bindings before workload creation.
7. Verify the source task, writer, stream arrivals, target history and checkpoints.
   Running containers alone are insufficient.

## Edit configuration registrations

In Configuration & setup, select one row and choose **Edit** in the left table
toolbar. Edit is disabled with no selection or multiple selected rows. HeatWave
connections and registered table mappings open a prefilled
dialog; Cancel leaves the registration unchanged.

- Unused connections can change their endpoint and credentials. Database settings
  must pass a fresh MySQL connection test on Save; the Test connection button is
  also available. Password values remain in Vault.
- Connections used only by undeployed drafts can change their name and credentials.
  Their endpoint and role remain fixed. Stream pools are selected directly in
  table mappings; existing deployed stream bindings remain fixed.
- Undeployed mappings can change their source, target, topic and initial capture
  choice. Object dropdowns reload from the selected connections. Mapping validation
  checks accessible objects, audit-table compatibility and capture feedback loops.
- After a deployment attempt, the mapping and its connections allow name changes
  only. Stopping a container does not make its stored offsets or audit history
  disposable. Use a separate registration for a different capture path; use Control
  Center's Capture start workflow for supported position changes.

Save is protected by administrator authorization, CSRF and registration/flow locks.
A stale dialog must be reloaded before saving. Rename preserves connector names,
flow IDs, container registrations and row-fingerprint namespaces; it does not
restart workloads. OCI service resources retain their existing lifecycle controls.

## Find schema and row changes

Open Audit Center, choose **Event timeline**, then select the registered flow.
Set **Event type** to **All changes**, **Schema changes only**, or **Row changes
only**. For a wildcard flow, use Source database and Source table to narrow the
view. Choose **Oldest first** to follow table creation and subsequent row changes,
or **Newest first** to inspect recent activity. All event timestamps use UTC.

Schema and row evidence is combined by source timestamp, then binlog file,
position and row. Events without a usable source timestamp show recorded time;
a schema baseline is not the table's original creation. Schema details exposes
the captured DDL, definitions and source metadata. Row INSERT is distinct from
CREATE TABLE. CSV export includes schema fields as well as row images.

## Fixed container build contract

The capture image builds an embedded Debezium Engine, MySQL driver, JDBC history
module and OCI Java SDK together, with aligned dependencies. Runtime probes check
engine/provider loading, native offset serialization, the custom GTID snapshotter
and OCI SDK setup before publication. The writer image is Python with the OCI SDK.
Both publish/consume via native HTTPS APIs with resource principals.

Private schema history and full source offsets are stored on target HeatWave,
separately from public schema events. Each native flow has its own source state
and target writer lease; there are no shared connector state topics or harnesses.

Releases are named `debezium-<UTC timestamp>-<revision>` and
`historywriter-<UTC timestamp>-<revision>`. The VM publishes with its instance
principal; workloads pull with their resource principals. No static pull secret
is needed for this mode. The selected registry digest identifies the deployed
binary. Saving a selection does not alter existing instances.

## Update an image without losing position

1. Build and publish the corrected component image in Container Registry.
2. Select the flow row and choose **Update history writer image** or
   **Update Debezium image**. The button is available for a deployed, non-retired
   workload in ACTIVE or INACTIVE state. Its dialog shows the currently deployed
   image and a dropdown of completed, available app-built releases for the matching
   component and configured private repository. Choose the exact release and review
   its build time, repository and digest. The current digest is excluded; an earlier
   published release can be selected after compatibility review. This choice applies
   only to the selected flow and does not change Workload settings defaults.
   Preflight verifies the source,
   queue, private registry image/digest and same-compartment attachments before
   stopping the affected workload.
3. Click the dialog's **Update image** button. It stops only that workload and waits for OCI INACTIVE
   before creating its replacement. The other workload keeps its current state.
   Existing connector identities, offsets, stream bindings and history remain.
   A writer update can temporarily grow the queue while Debezium continues.
4. If shutdown takes more than 60 seconds, the app reports that it is still
   stopping. Wait for INACTIVE, reopen Update image and choose the intended release again. It never
   creates a second active instance before shutdown is confirmed. If creation
   fails, retry Update; the old instance remains stopped and registered until
   a replacement is returned. Verify task/process health and advancing target
   checkpoints after replacement; retire the old stopped instance explicitly.

The submitted release ID is resolved again under the flow lock. The backend checks
the completed build's component, repository, VM compartment/region and the live
registry image's availability/digest before stopping anything. A dialog opened for
an instance that has since been replaced is rejected; reopen it to see current
state. Start/Resume continues to use the instance's deployed digest. Workload
settings retain the defaults used for initial deployments; **Workload default
differs** describes a difference from those defaults, not proof that one release
is newer or that an update is required.

**Start/Resume runs the existing deployed image.** It does not install a selected
new release. An inactive writer means no queue consumption until started or
replaced. If its old image contains a known failure, use Update rather than Start.
The snapshot schema repair gives synthetic DROP/CREATE observations distinct
identities and labels (`SNAPSHOT_RESET` / `SNAPSHOT`); it does not erase evidence.

| State | Meaning / next step |
| --- | --- |
| Deploying / starting | OCI creation or process initialization is progressing; inspect if it persists. |
| Active | Both processes and a fresh Debezium task report are running; still verify delivery. |
| Attention | Exited/failed process, stopped task, or an unexpected inactive workload. Follow the process tip. |
| Capture paused | Stop intentionally paused Debezium; the writer remains running to drain the queue and waits when caught up. Start resumes capture. |
| Pending | Deployment incomplete or task health unavailable; inspect missing workload/log access. |
| Stopped | Both registered workloads are inactive; Start resumes their existing images. |
| Retired | Registration retired; history remains. Termination/deletion are separate guarded actions. |

**Stop** pauses only Debezium. The history writer continues processing already
queued records and stays running, waiting when caught up; the app does not infer
zero lag from a running process. **Start/Resume** restarts the existing connector
from its durable position. **More actions → Stop both workloads** is the explicit
maintenance action. Retire also stops both workloads. During a capture pause,
source changes can continue accumulating in the MySQL binlog, so resume before
its retention window expires. Queue data remains subject to messaging retention.

Capture start has an explicit preview/apply procedure. Start on an existing flow
replays from retained schema evidence; GTID means an executed set; datetime is a
UTC transaction boundary. Capture-control protocol 3 requires a matching current
image and checks the durable pre-change checkpoint so a forward jump cannot
skip uncaptured schema changes. Normal restart never re-applies a reset.
See [capture position and recovery](capture-start.md).

## Browsing and charts

Before source partition/table maintenance, review [audit coverage limitations](limitations.md).
EXCHANGE PARTITION and DROP/TRUNCATE PARTITION can change the source row set
without per-row BEFORE/AFTER events. Preserve separately coordinated evidence
where complete row history is required; this prototype does not automate that
capture or reconcile the gap. An empty chart interval is not proof of no
DDL-driven data change.

Control Center uses one row per flow with process blocks, status and next-step
tips. Select one checkbox row for lifecycle actions; multi-selection is available
for CSV export. Columns resize/reorder/sort and full values open in a dialog.
Enabled actions use the accent color; only disabled controls are grey.

Audit Center opens on **All databases**. Choose **Row history** for a table-specific
search; its All tables overview and Export page CSV actions sit together at the
right of the heading. Audit Center supports custom UTC From/To ranges, operation filters and typed-key
lookup. Changes over time offers Hour, Minute (up to one hour), Day and Monday-
aligned Week buckets, operation legends, line/bar/stacked-bar views and chart
export. Empty time buckets show zero; large ranges require a coarser interval
instead of silently truncating recent points. All tables compares counts by source
database or registered table in a pie/bar chart. A database slice opens table bars;
a table slice opens its time-series bars with the date range preserved. Unavailable
targets are marked explicitly and excluded from the known total. Global views are
bounded to 100 mappings; narrow by source for larger installations.

Process blocks include the latest safe logged error, its report time, failed step
and recovery guidance. MySQL connection error 2003 indicates that the configured
database is unreachable: check that its DB system is ACTIVE before retrying the
workload, then review endpoint/port, DNS, routing and security rules. This can
follow a source/target DB stop while OCI Container Instances still report ACTIVE.
Older images may log only an exception class; the UI does not invent missing
numeric codes. Deploy current images for structured stage-specific diagnostics.

HeatWave Start/Stop omit ETag/If-Match and retain fresh scope/state checks and
idempotency tokens. See [the observed 412 issue](service-provisioning.md#heatwave-lifecycle-operations-and-etags).

## Recovery, retention and backups

Preserve the socket-MySQL repository, private deployment JSON, `.runtime.env`,
`.data/app-instance-id`, server session/TLS keys, selected image digests, Vault
fingerprint secret, native source offsets, JDBC schema history and target audit/state
tables. Back up MySQL consistently; copying a running datadir is not a backup.
Do not change flow lineage, queue incarnation or delivery identities as a recovery shortcut.

Payload expiry is opt-in: set policy, use **Stop both workloads** and wait for
INACTIVE, then preview eligible complete
monthly partitions, then Clean eligible history. Holds and archive requirements
can block cleanup. Deduplication, schema and checkpoint state are retained;
late replay below the durable cutoff is rejected. Image/cache retention is a
separate operator procedure. Restore and backlog recovery still need a controlled
acceptance test for the actual deployment.

Global charts count at most 100 registered tables per view. Each count query has a five-second MySQL execution limit; after 15 seconds the request starts no further table counts. A call already in progress may finish after that budget. Unavailable or uncounted tables remain visibly unknown and the total is marked incomplete. Narrow the database or date filters before retrying. Chart download icons export PNG; table download icons export the loaded counts as CSV.


See [the same-compartment limitation](compartment-scope.md) for Vault/secret
selection, other OCI resource boundaries and the host-only database identity
exception. Broader IAM access does not expand this application boundary.

## Streaming settings and workload replacement

Configure message limits, PUT batching/byte pacing and GET polling under
`streaming` in Runtime settings. **Control Center → More actions → Apply stream
settings** validates both selected images, replaces the writer before Debezium,
and retains checkpoints/history. Repeat the action after an unfinished OCI stop;
unchanged effective image/policy pairs are retained. Limit reductions require a
separately reviewed drain and fragment reconciliation. See
[Streaming capacity and message limits](streaming-and-size-limits.md).

Control Center shares OCI status reads across web workers and tabs. The page polls
every 15 seconds; settled samples can be 45 seconds old, transitions 10 seconds.
The displayed Checked time is the cloud sample time. A default-image difference
is informational and does not imply that the source database is stopped.

[Source scopes and target layouts](table-scopes.md) explains wildcard inventory,
physical target routing and the grouped-retention/historical-position boundaries.

## Java MySQL integration tests

Offset-store tests use real MySQL through the same Connector/J driver as capture.
H2 is not used. Point the tests at a **disposable MySQL 8.4+ test service**, with
strict SQL mode enabled and an account permitted to create and drop test schemas.
Each test creates a unique `audit_offset_test_` schema and drops it afterward.
Never point this configuration at a production, source or target database service.

Set `AUDIT_TEST_MYSQL_JDBC_URL` to the test service's `jdbc:mysql://` URL without
credentials. Set `AUDIT_TEST_MYSQL_CREDENTIALS_FILE` to a private, mode-0600 Java
properties file containing `username` and `history_password`. Keep both values
outside source control. Run `mvn -B -f deploy/native-capture/pom.xml verify` with
Java 21 and Maven. Other Java tests run normally without the service; offset-store
integration tests are explicitly skipped when the JDBC URL is unset. A configured
but unreachable or misconfigured service fails the tests.
