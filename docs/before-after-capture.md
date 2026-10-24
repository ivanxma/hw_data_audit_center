# Capturing and retrieving BEFORE / AFTER history

This document describes the application's row-change capture contract and the
current implementation. It uses illustrative records and contains no deployment
credentials. Oracle Linux 9 is the deployment platform.

## Capture path

Source HeatWave binlog → Debezium on OCI Container Instances → Kafka or OCI
Streaming → processor on OCI Container Instances → target HeatWave audit tables.

MySQL records the row images. Debezium decodes those records into change events.
The processor stores the images from the event; it does not issue a later SELECT
against the source to reconstruct the old value. A later query could observe a
different transaction's value or find that the row has already been deleted.

## Source readiness

The source must have binary logging enabled and expose the following effective
settings, subject to the selected HeatWave version's supported configuration:

```ini
binlog_format = ROW
binlog_row_image = FULL
binlog_row_value_options = ''
binlog_expire_logs_seconds = 604800
```

`ROW` produces row events for ordinary DML. DDL remains statement-logged and can
change row sets without per-row images; see [coverage limitations](limitations.md).
`FULL` requests all available columns in row-event images.
The empty value disables `PARTIAL_JSON`, so JSON changes carry complete values.
Seven-day retention is this application's starting recommendation, not a
guarantee: budget for the longest outage, catch-up time and a safety margin.
The application's readiness check requires more than three days.

Inspect the live source rather than relying on saved connection settings:

```sql
SHOW GLOBAL VARIABLES WHERE Variable_name IN (
  'log_bin', 'binlog_format', 'binlog_row_image',
  'binlog_row_value_options', 'binlog_expire_logs_seconds',
  'server_id', 'gtid_mode', 'enforce_gtid_consistency'
);
```

The implementation in [heatwave.py](../modules/heatwave.py) also checks a primary
key, connector permissions, nonzero server ID and GTID consistency. Source and
connector replication IDs must be unique; a nonzero value alone does not prove
uniqueness. Grant checking is conservative and may require review of scoped or
role-derived permissions. The app advises changes; it does not automatically
alter the source configuration.

Apply supported HeatWave configuration changes through the service's supported
mechanism. Global values for session-scoped variables do not prove that existing
writer sessions use them: reconnect application writers where required, inspect
effective session settings, and run a controlled capture test. New settings do
not repair images already recorded with incomplete logging.

See [MySQL binary logging options](https://dev.mysql.com/doc/refman/8.4/en/replication-options-binary-log.html)
and [Debezium MySQL setup and event semantics](https://debezium.io/documentation/reference/3.4/connectors/mysql.html).
Pin and validate the actual connector and database versions before rollout.

## Event contract

| Source operation | Debezium op | BEFORE | AFTER |
| --- | --- | --- | --- |
| INSERT | `c` | null | Inserted row |
| UPDATE | `u` | Previous row | Updated row |
| DELETE | `d` | Deleted row | null |
| Initial snapshot | `r` | null | Current baseline row |

A simplified illustrative UPDATE envelope is:

```json
{
  "op": "u",
  "before": {"order_id": 1042, "status": "pending"},
  "after": {"order_id": 1042, "status": "paid"},
  "source": {
    "db": "sales", "table": "orders",
    "file": "binlog.000123", "pos": 4567, "row": 0,
    "ts_ms": 1735689600000
  }
}
```

Production messages also have a separate record key and may contain converter
schema information and additional metadata. Preserve the envelope: an unwrap
transform that discards BEFORE values violates this processor's contract.
Converter choices must preserve decimals, binary and temporal values without
unintended type loss. Column exclusion or masking changes the available evidence.

The decoder in [events.py](../modules/events.py) accepts plain envelopes or JSON
converter payload wrappers. It validates the registered source table and image
objects required for each operation. It also checks image columns against converter metadata when supplied. Metadata
checks do not prove that every source column was included; full-image readiness
and end-to-end tests remain needed.
Null tombstones are ignored rather than stored as another DELETE.

## Target layout and write ordering

The generated audit payload table contains:

| Fields | Purpose |
| --- | --- |
| `flow_id`, `source_schema`, `source_table` | Registered table context |
| `record_key`, `key_hash` | Original message key and indexed row identity |
| `operation`, `event_time`, `recorded_at` | Change type, source/event time and ingestion time |
| `before_image`, `after_image` | JSON images supplied by the event |
| `before_fingerprint`, `after_fingerprint` | Content comparison fingerprints |
| `schema_document`, `schema_fingerprint` | Converter schema when supplied |
| `source_metadata` | Source coordinates and other capture evidence |
| `topic`, `partition_id`, `queue_offset` | Queue delivery coordinates |
| `event_id`, `capture_fingerprint` | Source-event identity and delivery identity |

`schema_document` describes converter metadata when present; it is not a
guaranteed complete MySQL DDL definition or a separately managed schema version.
The executable DDL is maintained in `audit_ddl()` in
[heatwave.py](../modules/heatwave.py).

The bounded batch writer inserts event identities into the unpartitioned `audit_event_dedup`
ledger and inserts the payload in one target transaction. A repeated identity
with matching content is accepted as replay; conflicting content stops the
write. [worker.py](../worker.py) acknowledges the queue only after successful
target persistence. A crash after target commit but before acknowledgement can
therefore replay safely for the supported event-identity contract.

For DML with complete coordinates, identity incorporates capture lineage, source
identity, binlog file/position/row, GTID when present, operation and table. Without
those coordinates the decoder rejects the DML event and does not acknowledge it.
Snapshot events use delivery coordinates; snapshot lineage, failover, source replacement and
binlog reset continuity require explicit acceptance testing.

New payload tables use monthly UTC event-time partitions with
`PRIMARY KEY (event_time,event_id)`. The separate ledger provides identity
uniqueness across partitions. The row lookup index is:

```sql
INDEX row_history(flow_id, key_hash, event_time, event_id)
```

Partitions support retention and bounded time scans; the index supports row
lookup. Extend the partition runway through the app's preparation action.
There is no automatic audit-data deletion. Optional, explicitly enabled month-based
cleanup is described in [Design validation](design-validation.md#housekeeping-contract).
It requires a stopped flow and retains the identity ledger and recovery state.

## Fingerprints and fast lookup

For deployed flows, the row-key fingerprint is HMAC-SHA-256 over canonical JSON
containing source identity, source schema, source table and the typed message
key. Canonicalization sorts object keys and removes formatting whitespace.
The lookup path in [audit_service.py](../modules/audit_service.py) obtains the
same key secret and constructs the same namespace as ingestion.

For the example row, enter the complete key in Audit Center:

```json
{"order_id": 1042}
```

Use all columns of a composite key, with the same types as the captured key.
The JSON number `1042` and string `"1042"` produce different fingerprints.
The query filters `flow_id` and `key_hash`, optionally narrows UTC time and
operation, and returns bounded pages of events. Choose one event to compare its
stored images. Confirm the query plan and latency with production-scale data;
an index does not establish a response-time guarantee.

Audit Center presents **Change history** and **Changes over time** in separate
tabs. Both use the filters and operation summary above them. Change history
opens by default; filtering, pagination and page CSV export preserve the selected
tab. Select a history row to compare its before and after images.

Fingerprints do not encrypt the stored JSON or establish tamper-proof history.
Restrict target and export access. Preserve the HMAC key across restarts; key
rotation needs a version/migration strategy or old histories will not match
newly computed lookup hashes. Primary-key updates can move a row to a new key;
the current lookup does not automatically join old/new key aliases. Reuse of a
deleted key also requires row-lifetime context when interpreting a timeline.

## Recovery, DDL and security boundaries

A snapshot supplies a baseline, not earlier row versions. Purged binlogs,
expired unconsumed queue records, or filters can create coverage gaps. Retain
connector offsets and private schema-recovery state across container restarts;
retain processor consumer-group identity and target deduplication state too.
A container restart alone does not prove that a new configuration was applied.

Kafka and OCI Streaming carry the same required image envelope. Their transport
and recovery-state configuration differ; see the [README](../README.md#capture-readiness).
Validate both connector recovery and processor replay on the selected transport.

DDL is archived separately from row events. A structure comparison can show
added/removed columns and available type/default/index/partition differences,
but only where baseline and subsequent metadata establish those properties.
Unknown properties must remain unknown. Adding a column is not an UPDATE event
for every existing row. Privilege changes, failed logins and actor attribution
need a separately configured database-audit source; row images do not provide
complete security evidence. See [schema_history.py](../modules/schema_history.py).

Partition exchanges, partition drops/truncates, table truncation/drop and some
column alterations can affect many rows without individual DML envelopes.
An unchanged schema definition or empty semantic delta does not establish an
unchanged row set. The current schema writer also depends on matching decoded
`tableChanges` entries; it is not a raw-DDL catch-all. See
[DDL-driven data changes and mitigations](limitations.md#ddl-can-change-data-without-producing-row-image-events).

## Acceptance checks before live use

1. Validate observed source settings and writer sessions; register one test table
   with a stable primary key and nullable, JSON, decimal and temporal columns.
2. Verify INSERT, UPDATE and DELETE values against expected images, including
   null transitions and JSON updates. Distinguish snapshots from INSERTs.
3. Restart the processor around a target commit; verify replay produces one
   stored event and identity conflicts stop acknowledgement.
4. Restart Debezium with persistent recovery state; prove continuity through a
   controlled outage and schema change on the selected transport.
5. Search typed simple and composite keys over bounded time ranges; inspect the
   index plan and benchmark representative history volumes.
6. Exercise expired-history recovery, key changes, source failover and schema
   gaps explicitly. Report coverage rather than inventing missing values.
7. On authorized disposable tables, test partition exchange/drop/truncate and
   table-level DDL. Record which statements/schema events the chosen connector
   actually emits and which row images are absent. Do not treat event-chart
   counts or a successful DDL parse as proof of complete row-history coverage.

Local automated checks cover the implementation's baseline behavior. A live
HeatWave-to-transport-to-target capture and capacity test is still required;
illustrative screenshots and local MySQL checks are not evidence of that result.
