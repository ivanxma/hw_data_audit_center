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
overridden, keeps MySQL socket-only, installs Java principal dependencies and
validates generated units with the OL9 systemd parser and restarts the web service after validation. Launchers read the private runtime file as the app owner; systemd does not need to read a home-directory EnvironmentFile. `SKIP_PRIVILEGED_SETUP=1` skips package
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
3. Create/select messaging resources. Streaming uses principal authentication,
   a data stream and a dedicated Connect harness for each independent connector.
   Managed Kafka uses broker SCRAM credentials and dedicated internal topics.
4. Register a source table and compatible target schema/table through metadata
   dropdowns. Choose Start (current-row snapshot) or End (schema-only baseline).
5. Validate source ROW/FULL settings, empty binlog_row_value_options, GTID mode,
   primary key, retention greater than three days and effective grants. Seven
   days is a starting retention recommendation; size for outage plus catch-up.
6. Prepare the single-partition public schema topic `audit_<flow_id>`. Both data
   and schema topics must be ready. Public schema evidence, internal schema
   history and Connect state are separate stores.
7. Build both images, select completed releases and save workload settings.
   Deploy, then verify connector task, writer process, queue arrivals and target
   history/checkpoints. Running containers alone are insufficient.

## Edit configuration registrations

In Configuration & setup, select one row and choose **Edit** in the left table
toolbar. Edit is disabled with no selection or multiple selected rows. HeatWave
connections, registered table mappings and queue connections open a prefilled
dialog; Cancel leaves the registration unchanged.

- Unused connections can change their endpoint and credentials. Database settings
  must pass a fresh MySQL connection test on Save; the Test connection button is
  also available. Password values remain in Vault.
- Connections used only by undeployed drafts can change their name and credentials.
  Their endpoint and role remain fixed. Queue connections with app-managed topics
  also retain their selected messaging service so topic ownership cannot be reassigned.
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

## Fixed container build contract

The Debezium image installs the JDBC history module matching the base connector
version and the MySQL driver. Shared Jersey/HK2 libraries are taken from Connect
to avoid incompatible duplicates. Runtime probes check imports, class loading
and Jersey startup before publication. Streaming JDBC history uses a qualified,
per-flow table with MySQL-safe LONGTEXT storage. Managed Kafka uses durable
single-partition internal schema history instead.

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
   Existing connector identities, offsets, consumer groups and history remain.
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
UTC transaction boundary. Capture-control protocol 2 requires a matching current
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
fingerprint secret, Connect state, internal schema history and target audit/state
tables. Back up MySQL consistently; copying a running datadir is not a backup.
Do not change flow lineage, queue incarnation or group names as a recovery shortcut.

Payload expiry is opt-in: set policy, use **Stop both workloads** and wait for
INACTIVE, then preview eligible complete
monthly partitions, then Clean eligible history. Holds and archive requirements
can block cleanup. Deduplication, schema and checkpoint state are retained;
late replay below the durable cutoff is rejected. Image/cache retention is a
separate operator procedure. Restore and backlog recovery still need a controlled
acceptance test for the actual deployment.

Global charts count at most 100 registered tables per view. Each count query has a five-second MySQL execution limit; after 15 seconds the request starts no further table counts. A call already in progress may finish after that budget. Unavailable or uncounted tables remain visibly unknown and the total is marked incomplete. Narrow the database or date filters before retrying. Chart download icons export PNG; table download icons export the loaded counts as CSV.
