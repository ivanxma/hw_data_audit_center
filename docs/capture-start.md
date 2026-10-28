# Capture position and recovery

Initial capture is selected while registering an undeployed table mapping. New
mappings default to **End**. Existing flows use **Control Center → Capture start**
with a reviewed preview and explicit apply. Routine Start/Resume, Stop and image
updates preserve the checkpoint.

## Initial modes and source load

**Start** performs a Debezium initial snapshot: it queries source metadata and
reads the table's current rows directly over MySQL, then follows the binlog from
a coordinated boundary. Snapshot records are labelled SNAPSHOT; they are not
historical INSERT events. A large table can impose substantial source I/O, CPU,
network and locking costs. Rehearse and schedule it with the database operator.

**End** reads schema metadata without copying existing rows and starts at the
current binlog boundary. **Specific GTID** also establishes schema without a
row scan, but starts after the supplied already-executed set. The target uses a
fixed audit event/image structure and does not require a row-copy baseline.
Existing rows appear when they subsequently change; ROW/FULL gives full images
for supported row events. An unchanged row has no retained row snapshot.

## GTID syntax and retained range

Enter a MySQL GTID set, for example:

```text
11111111-1111-1111-1111-111111111111:1-32:40-45,
22222222-2222-2222-2222-222222222222:1-18
```

A UUID can have multiple intervals; a set can have multiple UUID groups. The
selection means **transactions already processed**. Capture starts after that
set, so listing a transaction excludes it from new delivery.

Inspection shows `gtid_executed`, `gtid_purged`, selected intervals and the retained
range. Necessary checks are:

- Every selected transaction has executed on this source: `selected ⊆ executed`.
- Every purged transaction is already covered: `purged ⊆ selected`. Otherwise
  capture needs transactions that are no longer available.
- The cut is a supported complete committed transaction-order prefix. Interval
  membership alone cannot prove this when UUID groups interleave.
- Schema evidence covers the boundary. A current table definition is not assumed
  to describe arbitrary older events across intervening DDL.

A selected set may include purged transactions that are already accounted for.
The unsafe case is **needed transactions that have been purged**. Source retention
can change after inspection, so runtime checks remain necessary. Unsupported,
incomplete, unexecuted or ambiguous boundaries fail closed. Inspection uses GTID
retention evidence rather than scanning every earlier binlog file merely to find
a baseline; large retained inventories do not require the old 128-file walk.

## Schema-only GTID initialization

The native capture bootstrap validates source schema and establishes metadata
under the supported lock/check procedure; it does not scan table rows. It writes
a seed containing the selected executed set, runs a schema-only Debezium phase,
and publishes schema evidence. It waits for both schema completion and a durable
source checkpoint proving queued schema publication completed before stopping
that phase. It removes initialization/snapshot flags once, then starts normal
streaming from the selected boundary. Restarts use the resulting durable offset.

The complete source offset retains file, position, event and row progress as well
as GTIDs. Reducing an established offset to its GTID string can lose progress
within a transaction and is not a migration procedure.

## Existing-flow preview and apply

Historical repositioning is available for deployed single-table flows with the
required retained baseline. It is disabled for deployed wildcard flows. Initial
End/GTID selection and ordinary restarts remain supported for wildcard flows.

Select the flow and open Capture start. Inspect the displayed source bounds,
current checkpoint, proposed boundary and retained schema evidence. Datetime
selection is a UTC transaction boundary and requires an established compatible
schema baseline; it is not a new-mapping historical snapshot option. Start for an
existing flow means a supported retained baseline, not an instruction to rescan
all current rows.

Apply is guarded by administrator authorization, CSRF, flow locking and the
reviewed checkpoint. A stale preview must be regenerated. The source workload
processes the request through its loopback native capture-control API. The writer
continues to honor target deduplication and durable delivery state. Position
changes can replay evidence or intentionally omit history; review the operation
record and verify expected target events after applying.

## State and recovery

Keep target native offsets, per-flow JDBC schema history, delivery checkpoints,
stream identities, source lineage, fragment custody/receipts and the same Vault
fingerprint key together.
A native consumer resumes after the last actually processed offset. Missing
retained data, recreated streams, schema gaps and identity conflicts stop capture
or delivery rather than silently resetting to latest.

Capture-control and writer runtime protocol 3 images are required. A legacy
workload must undergo an explicit state-preserving migration before a native
image can be selected. Ordinary image update is not a transport migration.

See [operations](operations.md), [design validation](design-validation.md) and
[MySQL GTID concepts](https://dev.mysql.com/doc/refman/8.4/en/replication-gtids-concepts.html).
