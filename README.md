# HeatWave Audit Center

HeatWave Audit Center captures registered MySQL HeatWave table changes and makes
BEFORE/AFTER history searchable in a target HeatWave audit store. The web app runs
on Oracle Linux 9; capture and history writing run on private OCI Container Instances.

**Independent prototype for evaluation, demonstration and rehearsal. Not an Oracle
product; not supported or endorsed by Oracle.**

## License

Original project code and documentation are licensed under the [MIT License](LICENSE).
Third-party components retain their own licenses. See the
[dependency license review](docs/third-party-licenses.md) for the MySQL connector
exceptions and redistribution requirements, including source and notice obligations.

## Architecture

Source HeatWave binlog → embedded Debezium Engine → native OCI Streaming → Python
history writer → target HeatWave audit store.

The capture image publishes with the OCI Java SDK. The writer consumes with the
OCI Python SDK. Both use container resource principals over HTTPS. The VM manages
resources with its instance principal. Register table selects an OCI stream pool
and its data stream directly; no Streaming connection registration is needed.
Database passwords and the row-fingerprint key remain in Vault.

Each flow keeps its complete source offset and private JDBC schema history in the
target database. Native stream IDs bind delivery checkpoints to the actual streams.
The writer commits audit evidence, deduplication state and delivery checkpoints
before acknowledging messages in memory. A target database lease permits one
writer per flow. Replays are checked for identity conflicts rather than discarded
without verification.

![Deployment architecture](docs/user-manual/diagrams/deployment-architecture.png)

![Capture workflow](docs/user-manual/diagrams/capture-workflow.png)

## Source scope and target layout

Register one table, `database.*`, or `*.*`. Choose one audit table per flow,
per actual source database, or per actual source table. Grouped destinations stay
inside the selected target schema. Source checks require stable primary keys and
exclude system/audit-target schemas. Audit Center filters actual source names.
Deployed wildcard historical repositioning and automatic grouped retention
cleanup remain disabled. See [source scopes and target layouts](docs/table-scopes.md).

Audit Center combines schema and row evidence in an **Event timeline**. Choose
**All changes**, **Schema changes only**, or **Row changes only**. Source timestamps
and binlog positions determine order; CREATE TABLE is distinct from a row INSERT.
Schema details shows DDL and before/after definitions. See the
[history and schema guide](docs/before-after-capture.md).

## Streaming capacity and larger events

Streaming buffers changes while apply is delayed, within its retention window.
Capture offsets and JDBC history still depend on the target database, so a full
target outage can also pause extraction. Ordinary events remain one message;
oversized events are fragmented and durably reassembled, retaining the full
Debezium schema and row images. The service maximum remains 1 MiB per message
and request. Application logical events default to 16 MiB, with bounded incomplete
custody. Review abandoned fragment generations before cleanup.

Small events share PUT requests; byte pacing, larger GET batches and idle polling
reduce calls before bounded retries are needed. A validated 100-row burst plus
one schema event used two PUT requests with zero throttled entries. Save pacing
and size limits in Runtime settings, then use **Apply stream settings** in Control
Center. See [buffering, limits and request control](docs/streaming-and-size-limits.md).

## Choose the initial capture position

In **Configuration & setup → Table mappings → Register table**, choose:

| Initial capture | Behavior |
| --- | --- |
| **End** (default) | Capture schema metadata and subsequent changes; do not copy existing rows. |
| **Start** | Read current rows directly from the source in an initial snapshot, then stream changes. Plan capacity before using this for a large table. |
| **Specific GTID** | Establish schema without scanning rows, then stream transactions after a validated already-executed GTID set. |

A GTID set can contain multiple server UUIDs and multiple intervals per UUID.
Inspection displays source executed and purged sets and checks that
`purged ⊆ selected ⊆ executed`. All transactions still needed after the selection
must remain available. Arbitrary holes or transaction-order cuts and incompatible
schema boundaries are rejected. A GTID is an executed set, not an individual
transaction to include. Normal Start/Resume and image replacement preserve the
saved full offset; they do not repeat initialization.

With End or GTID, the fixed audit-history schema can store later changes to rows
that already existed. With ROW/FULL binlogging, an update supplies the BEFORE and
AFTER images. Rows that never change have no row-history baseline. See
[capture position and recovery](docs/capture-start.md) and
[BEFORE/AFTER design](docs/before-after-capture.md).

## Install and run

1. Prepare an Oracle Linux 9 VM, private worker and database connectivity, and
   administrator-managed IAM policies. App-managed resources and attachments must
   be in the VM compartment and region.
2. Copy `examples/deployment.example.json` to a private location on the VM. Set
   `AUDIT_CONFIG_FILE`; protect the file with mode 0600. Supply an official MySQL
   9.x archive URL and verified SHA256 as described in the setup guide.
3. Run `./setup.sh` as the application owner with sudo available. Enroll an
   administrator with `.venv/bin/python scripts/enroll_admin.py`; rotate the
   bootstrap password at first login.
4. Run `.venv/bin/python scripts/check_setup.py`, then `--oci`. Register and test
   source/target connections and the OCI Streaming pool connection.
5. Register a table, run source checks, prepare/select the data stream, build and
   select digest-pinned capture/writer images, and Deploy. Deployment prepares the
   separate single-partition schema stream automatically.
6. Verify source task health, stream arrival, newest target events and advancing
   checkpoints. A running container alone does not establish successful delivery.

Setup installs socket-only local MySQL for login and configuration. Java and Maven
are capture-image build/runtime dependencies; the VM app and history writer use
Python. Scripts resolve supporting files relative to their checkout. Keep private
configuration, registrations, backups and deployment runbooks out of source control.

## Guides and demonstration

- Third-party dependencies and licenses: [reference](docs/third-party-licenses.md), [Word inventory](docs/Third-Party-Libraries.docx)
- [Setup and operations](docs/operations.md)
- [IAM and database access](docs/iam-policy.md)
- [OCI service provisioning](docs/service-provisioning.md)
- [Resource boundary](docs/compartment-scope.md)
- [Coverage and limitations](docs/limitations.md)
- User manual: [HTML](docs/user-manual/html/index.html), [PDF](docs/user-manual/pdf/HeatWave-Audit-Center-User-Manual.pdf), [Markdown](docs/user-manual/md/README.md)
- [Watch the narrated demonstration](https://ivanxma.github.io/hw_data_audit_center/demo/heatwave-audit-oci-streaming-demo.mp4)

Screenshots and video use isolated illustrative data. The implementation captures
row changes and supported schema evidence; it does not implement a database
security-audit collector. Partition operations can change row sets without row
images. Retention, recovery and source-load limits need deployment-specific planning.
