# Native capture design and validation

## Runtime contract

Capture runs an embedded Debezium Engine on a private OCI Container Instance.
Its OCI Java SDK publisher sends UTF-8 keys and values with PutMessages. Batches
are bounded; partial errors, unknown destinations and oversized records prevent
Debezium acknowledgment. The complete source offset is committed synchronously
in target `audit_native_offsets`; schema history uses a separate per-flow JDBC
store. A loopback control API manages this engine, not an external Connect cluster.

The Python history writer consumes native GetMessages partition cursors. One
MySQL lease per flow prevents concurrent writers. Target payload, identity ledger
and durable delivery checkpoints commit before in-memory acknowledgment. Resume
uses AFTER_OFFSET at the last actually processed message, including sparse offsets.
Cursor failures and missing retained origins do not reset to latest. Public schema
streams have one partition and are distinct from private recovery history.

Stream ID bindings, source identity, lineage and fingerprint secrets are stable
across restart/image replacement. A migration must preserve the full source
offset bytes, private schema history and target delivery checkpoints. Legacy
workloads are rejected by ordinary native image replacement preflight.

## Initial GTID and large tables

New mappings default to End, capturing metadata and future row changes without
scanning existing rows. Start explicitly requests a source-table snapshot.
Specific GTID accepts multiple UUID groups/intervals, checks executed and purged
sets and validates a complete supported transaction boundary and schema baseline.
The necessary set relationship is `purged ⊆ selected ⊆ executed`.

GTID bootstrap runs schema-only capture. A snapshot-completed notification alone
is insufficient: schema records may still be queued for publication. The bootstrap
waits for the durable checkpoint proving schema publication finished, then stops,
removes initialization flags once and resumes streaming. Regression coverage
exercises delayed publication so initialization cannot race its own shutdown.

## Completed acceptance validation

The native implementation was validated before documentation was updated:

- Python suite: 244 tests, 232 passed and 12 environment-dependent skips locally;
  deployment suite: 233 passed and 11 storage integration skips.
- All 11 storage integration tests passed separately on disposable MySQL.
- Fourteen Java tests passed, including durable offsets, publication failure
  handling, snapshotter behavior and signed OCI SDK HTTP serialization.
- Live native CREATE/UPDATE/DELETE preserved expected BEFORE/AFTER images;
  preexisting baseline rows were not snapshotted in no-data mode.
- Source/writer restart preserved progress without duplicate target events.
- Explicit initial GTID and restart captured only expected later events.
- Native and legacy transport reads matched message keys/values and offsets
  before the state-preserving migration of existing flows.
- Both migrated flows were active with source task and writer running. HTTP and
  Chrome checks covered native configuration, image selection, GTID inspection,
  multiple-group rejection cases, Message Explorer and healthy flow status.
- Disposable validation containers, streams and database objects were removed.

These checks establish the tested paths, not universal production readiness.
Deployment-specific load, outage duration, source DDL transitions, disaster restore
and retention sizing still need rehearsals. The prototype does not implement a
security-audit collector or row reconciliation for DDL-driven partition changes.
See [limitations](limitations.md) and [operations](operations.md).
