# Audit coverage and limitations

HeatWave Audit Center is an independent prototype for evaluation and rehearsal,
not an Oracle product or an Oracle-supported service. Its history consists of
observed events and baselines; it is not proof that every change to the source
row set has a corresponding row-image record.

## DDL can change data without producing row-image events

**Yes: `ALTER TABLE ... EXCHANGE PARTITION`, `DROP PARTITION` and related
operations are a limitation of binlog row-image auditing.** MySQL logs DDL as
statements even with `binlog_format=ROW`. A DDL statement does not contain a
DELETE/INSERT/UPDATE image for each row it removes, exchanges or transforms.
`binlog_row_image=FULL` controls images in row events; it cannot manufacture
images for an operation logged as DDL. Debezium and the writer cannot recover
values that are absent from their input. [MySQL binary-log formats](https://dev.mysql.com/doc/refman/8.4/en/binary-log-setting.html).

| Source operation | Possible data effect | Row-audit coverage |
| --- | --- | --- |
| Ordinary INSERT / UPDATE / DELETE with ROW/FULL | Add, change or remove individual rows | Supported row envelopes contain the applicable AFTER, both images, or BEFORE image |
| EXCHANGE PARTITION ... WITH TABLE | Swap a partition's rows with an unpartitioned table, including bulk import/export | No per-row CREATE/DELETE images for the exchange; registering both tables does not create those missing events |
| DROP PARTITION | In the usual INPLACE RANGE/LIST operation, remove the partition and its data | No individual DELETE images for the removed row set |
| TRUNCATE PARTITION | Empty selected partition(s) while retaining their definitions | No individual DELETE images for the emptied row set |
| TRUNCATE TABLE / DROP TABLE | Remove a table's current data | A supported table-level/schema event does not supply all deleted-row images |
| ALTER column type / DROP COLUMN / ADD COLUMN DEFAULT | Change logical values, discard a column or change values returned for existing rows | Do not expect an UPDATE event for every existing row |
| REORGANIZE / other partition-layout maintenance | Often redistribute rows while preserving their logical contents | Physical relocation is not inherently a logical row change; classify the actual operation rather than inventing UPDATE events |

Review the precise MySQL version, engine and algorithm. In particular,
`DROP PARTITION` with COPY or `old_alter_table=ON` can redistribute some rows and
delete those that cannot be placed; do not equate every algorithm with deleting
the entire former partition. [Partition drop semantics](https://dev.mysql.com/doc/refman/8.4/en/partitioning-management-range-list.html),
[online partition DDL](https://dev.mysql.com/doc/refman/8.4/en/innodb-online-ddl-operations.html).
MySQL also documents that EXCHANGE PARTITION does not invoke either table's
triggers, and TRUNCATE TABLE is statement-logged DDL rather than per-row DML.
[Partition exchange](https://dev.mysql.com/doc/refman/8.4/en/partitioning-management-exchange.html),
[TRUNCATE TABLE](https://dev.mysql.com/doc/refman/8.4/en/truncate-table.html).

### What the application can and cannot show

The public schema stream can carry the DDL and available structural evidence.
When the supported decoded event identifies the registered table, the writer can
retain the statement, source position, before/after definitions and semantic
schema difference in `audit_schema_changes`. These definitions describe table
structure, not snapshots of every row in a partition.

**DDL present in the binlog is not a guarantee that this prototype archives that
statement.** The current writer processes Debezium `tableChanges` entries for the
registered object. An unsupported/ignored statement, parser/version limitation,
filter or event with no matching table-change entry can leave no schema-history
row. There is no raw-DDL catch-all archive, automatic partition-impact warning,
row-count reconciliation or DDL-to-row expansion in the current implementation.
Validate the selected connector version and exact statements on a test flow.

EXCHANGE PARTITION can leave the column/partition definition unchanged while
replacing millions of visible rows. An empty `semantic_delta` is therefore not
evidence that the row set was unchanged. A partition TRUNCATE can likewise leave
its structural definition intact. The affected-row count in a client response is
not a complete measure of this data impact.

Previously captured row history remains in the target until explicit target
retention removes it. A row removed by source partition DDL may have an old
CREATE/UPDATE with no later DELETE. A row introduced by an exchange can have no
CREATE at all. The most recent retained row event is its last observed version,
not proof of its current source presence. Event charts count captured events;
a large DDL-driven data change can have no corresponding row-volume spike.

Fingerprints identify and compare values that were actually observed. They do
not monitor source table membership or prove capture completeness. Extending
retention, restarting, moving a GTID position or changing ROW/FULL cannot create
missing row events. A post-operation snapshot supplies a new observation; it
cannot reconstruct unavailable old values or original transaction order.

## Operating policy for tables requiring complete row history

1. Identify partition/table/column DDL that can affect the registered data. Have
   the DBA govern those privileges and maintenance procedures; the prototype
   does not automatically block these operations.
2. Where appropriate and approved, use ordinary logged DML for removals/imports
   that require individual row evidence. Assess transaction size, locking,
   binlog growth, message limits, target load and retention before batching it.
3. When DDL is necessary, preserve the affected row sets **before** the operation,
   including both sides of an exchange. Record schemas, object/partition
   identities, full typed keys/values, exact scope and source coordinates.
4. Use DBA-approved consistency/coordination so concurrent writes cannot make
   before/after comparisons ambiguous. Preserve the DDL outcome and a consistent
   after-state; retain backups/exports with an appropriate evidence chain.
5. If comparing those states, label differences as separately derived
   reconciliation evidence, not original Debezium DML. Counts/checksums alone
   cannot reconstruct individual images or the sequence of intermediate changes.
6. Record the affected scope and coverage gap. Do not fabricate DELETE/CREATE
   events, interpret missing events as zero change, or claim a complete row
   history without additional evidence.

Pre/post materialization, coordinated snapshot comparison and derived evidence
storage are mitigation designs, **not implemented application features**.
An audit/security log may establish the statement/actor/outcome but does not by
itself provide every removed row. Surviving exchanged tables or earlier backups
can help recovery if preserved; a DDL-only log cannot restore missing values.

## Source changes versus target housekeeping

Source partition DDL is an input-coverage limitation. The application's target
**Clean eligible history** action is intentional expiry of selected audit
payload partitions. It records cleanup/cutoff state and retains recovery/identity
records, but it does not synthesize a DELETE audit event for each expired payload.
After expiry, those payload images are unavailable through normal history lookup.
Neither operation is a full archive/restore mechanism.

## Other prototype boundaries

| Boundary | Consequence |
| --- | --- |
| Initial snapshot | A current-row baseline, not reconstruction of the original INSERTs or prior changes |
| Initial End | No existing-row copy; normal restarts resume durable offsets rather than resetting End |
| Purged source/queue history | Coverage can be lost; the writer fails closed on unrecoverable delivery checkpoints |
| Incomplete old row images | Later FULL settings cannot repair values omitted from earlier logs |
| Schema fidelity | Missing defaults, indexes and partition metadata remain unknown; definitions are not full row sets |
| Row-key changes / secret rotation | Key aliases and fingerprint-key version routing are not implemented |
| Cross-topic/source order | Partition ordering does not establish one global transaction order or atomic cross-table reconstruction |
| DCL / actor attribution | Needs a separate database-audit source/collector; complete privilege/security history is not implemented |
| Scaling | One connector/writer per table flow; automatic fleet scaling/shared multi-table extraction are not implemented |
| Retention / archives | Payload cleanup is opt-in; metadata grows; verified archive export/restore is not implemented |

See the [user manual](user-manual/html/index.html#ddl-data-limitations),
[capture contract](before-after-capture.md) and
[design validation](design-validation.md). Service/connector versions and exact
DDL behavior require acceptance tests; a healthy process or successful DDL parse
is not a row-history completeness guarantee.


## Same-compartment deployment boundary

Vaults, keys, credential/fingerprint secrets and other app-managed OCI resources
must remain in the VM compartment and region. Cross-compartment sharing is not
supported by this prototype. This is an application restriction; host-only MySQL
connection testing does not attest DB-system compartment placement. See the
[resource boundary matrix](compartment-scope.md) and user manual Appendix K.

## Streaming, wildcard and storage boundaries

Streaming retention bounds how long apply can lag. Capture offsets and JDBC
schema history are target-backed, so an entire target outage can pause extraction.
Large events are fragmented within configured bounds, but fragmentation adds
bytes and cannot raise target packet/document limits. Interrupted source attempts
can leave incomplete generations requiring evidence-based review; completed
receipts are retained. Full schemas remain in the event envelope. See
[Streaming and size limits](streaming-and-size-limits.md).

Wildcard inventory is bounded to 1,000 eligible tables and requires stable primary
keys. Historical repositioning of deployed wildcard flows and automatic retention
cleanup of grouped targets are disabled. Existing deployed scope/layout identifiers
are locked. See [source scopes and target layouts](table-scopes.md).
