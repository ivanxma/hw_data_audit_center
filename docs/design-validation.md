# Technical design validation and refactoring

Review date: 24 October 2026. This review compares the technical design with the
repository implementation. It describes code behavior, not live OCI throughput
or deployment certification. Deployment inventories remain private under
`internal_doc/`. Oracle Linux 9 remains the deployment platform.

## Corrected implementation gaps

| Finding | Correction | Implementation |
| --- | --- | --- |
| Every message committed a separate target transaction and queue offset | Bounded batches, bulk payload/ledger inserts, paced offset commits | `modules/ingestion.py`, `modules/events.py` |
| A missing/expired group offset could silently start at the earliest retained message | Target-side checkpoints, watermark validation and fail-closed offset reset | `modules/checkpoints.py`, `modules/queue_service.py` |
| Fallback hashes could create incompatible row identities | Shared canonical JSON and mandatory HMAC key/source namespace | `modules/fingerprints.py` |
| DML could fall back to a delivery ID without source coordinates | Require valid binlog file/position/row for DML; snapshots retain delivery identity | `modules/events.py` |
| Container control checked app ownership inconsistently | Require both compartment and flow ownership for every lifecycle operation | `modules/workflows.py`, `modules/oci_service.py` |
| Flow state was read before acquiring its action lock | Acquire advisory lock, then read current state; preserve partial deployment IDs | `modules/workflows.py` |
| Stopping a retired flow could clear its retired status | Preserve retired status; reject unknown actions before any mutation | `modules/workflows.py` |
| Independent Connect groups could share config/offset/status topics | Validate state isolation; support per-connector harness/topic overrides | `modules/transport.py`, `modules/workflows.py` |
| Connection aliases bypassed physical source uniqueness or created feedback | Validate physical source/target identities and reserve recovery tables | `modules/mappings.py` |
| Public schema processing could have multiple unordered lanes | Require one schema-topic partition, serialize updates, deduplicate by source position where available | `modules/ingestion.py`, `modules/schema_history.py` |
| Expiry could be undone by queue replay | Persist a monotonic cutoff before partition DDL; writers take a shared state lock and reject older events | `modules/retention.py` |
| Long requests blocked every logged-in user | Serialize connection use per session instead of holding the global lock throughout every request | `app.py`, `modules/session_store.py` |
| General table browsing scanned all history by default | Default unkeyed browsing to a 24-hour window; normalize explicit offsets to UTC; explicit ranges and keyed history remain available | `modules/audit_service.py` |

Removed unused SSH tunnel adapters, permissive TLS aliases, unkeyed row-hash
fallbacks, generic MESSAGE handling, the unused per-row connection-delete route,
duplicated Vault secret resolution and duplicated inherited test cases. Existing
helper entry points still used by verification scripts are retained. Python
formatting and import checks are configured in `pyproject.toml`.

## Ingestion and acknowledgement contract

The writer keeps each assigned partition in an ordered lane. It validates the
batch, commits fresh payloads and their identity-ledger entries together, archives
schema events, then durably stores the batch's next delivery offsets before
acknowledging the queue. These stages are recoverable, not one distributed
transaction. A crash between stages can replay records; identity/content checks
prevent supported duplicates from overwriting history. A failed target or
checkpoint write never advances a queue offset.

On assignment the target checkpoint takes precedence over the consumer group's
offset. The writer checks it against current queue watermarks. Without a saved
offset, replay is allowed only when the retained origin is still zero. Trimmed
history or offsets beyond the queue end stop the writer for coverage review.
Queue recreation must use a new incarnation and an explicit recovery plan.

Revocation or lost ownership discards unacknowledged buffers. A graceful stop
flushes work still owned by that consumer. Schema-topic ordering does not imply
causal order across row/schema topics, cross-table atomic reconstruction, or a
total order across databases. Schema fidelity is still partial where metadata
does not establish every property.

DDL can also change data without any per-row event: partition exchange, partition
drop/truncate and table-level truncation are important examples. The application
does not expand DDL into synthetic row changes, materialize affected row sets,
reconcile source membership or automatically flag every such coverage gap.
Schema storage depends on matching decoded `tableChanges`, so even a logged DDL
statement is not guaranteed a target schema record. An empty structural delta
does not imply unchanged contents. See [audit limitations](limitations.md) for
examples, algorithm nuances, chart/history implications and operating policy.

Default processor settings are:

```json
{
  "batch": {"max_records": 250, "max_bytes": 4194304, "max_wait_ms": 500},
  "offset_commit_interval_ms": 500
}
```

Record count, bytes and wait time bound the buffer. The record limit is at most
1,000. Commit pacing matters for OCI Streaming API limits, including aggregate
traffic from multiple replicas. Set batch size and replica budgets from measured
envelope sizes, DB commit latency, queue throttling and source/queue lag.
No throughput guarantee follows from these defaults.

Batch metrics log counts, bytes and duration without keys or images. SQL I/O has
bounded client timeouts; connections use UTC. A poison record, conflicting
identity or expired event stops consumption for investigation rather than being
silently skipped or acknowledged.

## Multiple sources and Connect state

The current registration/lifecycle model manages one connector and one writer
container per table flow. Multiple source databases are independent flows.
Independent groups must not share Connect state topics. For OCI Streaming,
provide a distinct harness/profile for each connector, for example:

```json
{
  "connect_profiles": {
    "<connector-name-from-registration>": {
      "connect_harness_id": "<same-compartment-harness-ocid>",
      "connect_topics": {
        "config": "<harness-ocid>-config",
        "offset": "<harness-ocid>-offset",
        "status": "<harness-ocid>-status"
      }
    }
  }
}
```

This field belongs inside the selected transport profile. Overrides are limited
to harness/state-topic references. All resource checks still use the VM's
instance principal and the selected VM compartment. A denied read is not
permission to switch credentials or forget a recorded resource.

Both data and public-schema topics must exist. The schema topic has exactly one
partition. The processor keeps a stable `audit-writer-<flow-id>` group, with no
automatic offset storage/commit or topic creation. Kafka and OCI Streaming
retain their distinct authentication, state-storage and throughput contracts.

## Housekeeping contract

No retention policy is enabled in the example configuration. To enable one,
add a policy using the actual flow ID as its key under top-level `retention`:

```json
{
  "retention": {
    "<flow-id>": {
      "enabled": true,
      "retain_months": 3,
      "archive_required": false,
      "legal_hold": false
    }
  }
}
```

This example keeps three whole months plus the current month. Cutoff calculations
use UTC calendar months, not an exact number of days. An explicit false archive
and hold decision is required. Held or archive-required policies fail closed;
verified archive export/restore is not implemented.

Use **More actions → Stop both workloads** and verify both are INACTIVE, then use **Audit
partitions → Clean eligible history**. The backend independently checks
container ownership and inactive state. A legacy writer registration cannot
restart or clean until it has been replaced with a protocol-2 deployment.
External unmanaged writers are outside the lifecycle model and must not write
the registered target.

The cleanup job freezes its target, cutoff, policy digest and monthly partition
list in the target database. It persists the ingestion cutoff under an exclusive
row lock before issuing DDL. Writers hold shared locks through their target
transaction, so a cutoff cannot change midway through a batch. Expiry retries
resume the frozen manifest instead of expanding to a newer date. If DDL succeeded
just before a crash, a missing planned partition is recorded as completed.

Only fully eligible `pYYYYMM` partitions with matching month boundaries can be
dropped. `p_older`, `p_future`, the identity ledger, public/private schema state,
delivery checkpoints and cleanup manifests are retained. A mixed-flow payload
table is refused. Each job is limited to 24 partitions with a short metadata-lock
wait; larger cleanups need an explicitly reviewed staged policy. The initial
job runs synchronously and records durable progress; a fleet job scheduler is
still planned. Physical storage allocation or billing may not shrink after a
successful DROP PARTITION.

The cutoff remains active after partial failure. Late events below it stop the
writer for review; they are not silently discarded. Guard partitions can still
contain old/future data and need separate investigation. The ledger and metadata
continue growing: their purge requires a validated replay-floor and archive
policy and is deliberately not coupled to payload expiry.

## Upgrade existing deployments

1. Stop capture/apply and retain their queue, private schema state and target
   history. Back up the control repository and target evidence.
2. Build and test updated images, then select their verified private-registry
   digests. Existing containers keep their old code/configuration on restart.
3. Run target preparation for each flow through the updated control plane or the
   [target preparation utility](../scripts/prepare_targets.py):
   `.venv/bin/python scripts/prepare_targets.py --flow-id <id>`. Existing payload tables must match the required
   layout; incompatible layouts are refused rather than altered automatically.
4. Replace old writer workloads with the updated image/configuration using the
   reviewed retirement/re-registration path or an explicit deployment migration.
   Preserve lineage, group, queue incarnation and fingerprint key when continuity
   is intended. Recreating a registration generates a fresh connector namespace;
   it does not implicitly adopt another registration's target.
5. Verify checkpoints, schema ordering, full row images, failure recovery and
   typed-key lookup before enabling a retention policy.

The schema evidence identity is now source-coordinate based when available.
Existing schema histories need an explicit deduplication/reconciliation plan
before replay through this version. No existing schema rows are rewritten by
initialization. Row-key HMAC and the existing DML ledger content digest remain
compatible when their original source/lineage/key values are preserved.

## Design capabilities still requiring implementation or live proof

- Shared source connector registry and multi-table dispatch to avoid rereading
  the same binlog for every registered table.
- Lag-based desired-state reconciler, persistent replica ownership, automatic
  scaling and revision-based rolling replacement. The current control model
  tracks one writer container per flow; it does not manage an autoscaled fleet.
- Target sharding/routing migration and per-shard connection/capacity budgets.
- Cursor pagination and preaggregated statistics for large history searches.
- Full structural metadata enrichment, row-key aliases/lifetime identity, HMAC
  key rotation/version routing, and privilege/access audit collectors.
- Verified archive export/restore, legal-hold segregation, live fleet cleanup,
  ledger/metadata retention and a background job scheduler.
- Real HeatWave + selected transport acceptance tests, capacity benchmarks,
  failover behavior, connector image/JDBC modules and OCI authorization.

Do not interpret the architecture deck's proposed capacity figures or features
as implemented service guarantees. The next rollout gate is one real table under
peak change rate, followed by multiple sources and controlled recovery tests.

## Validation

The tests cover image/key validation, target-before-ack ordering, watermark
expiry, rebalances, failed acknowledgements, mapping feedback, physical aliases,
partial deployment retry, ownership checks and retention policy boundaries.
Opt-in real MySQL tests exercise batch replay/conflict atomicity, rollback of
ledger entries after payload failure, resumed partition jobs, retained-identity
replay rejection, schema-event replay, and transaction locking of expiry state.

```bash
.venv/bin/python -m unittest discover -s tests -v
# Disposable local MySQL only; networking must be disabled:
AUDIT_TEST_MYSQL_SOCKET=/path/to/disposable/mysql.sock \
  .venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m compileall -q app.py modules scripts deploy worker.py
.venv/bin/pip check
bash -n setup.sh start_https.sh start_http.sh start_mysql.sh ol9/setup_platform.sh
```

Primary contracts: [Debezium MySQL](https://debezium.io/documentation/reference/3.4/connectors/mysql.html),
[Confluent Python consumer API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html),
[OCI Streaming limits](https://docs.oracle.com/en-us/iaas/Content/Streaming/Concepts/streamingoverview_topic-Limits_on_Streaming_Resources.htm),
[MySQL partition management](https://dev.mysql.com/doc/refman/8.4/en/partitioning-management-range-list.html).


## Connection form simplification

Connections use the supplied host, port, database, username and Vault password
reference. There is no DB system OCID entry or OCI lookup/validation of the IP.
**Test connection** performs a real MySQL login with `SELECT 1` and reads the
server UUID/version. Registration repeats this check before saving.
App-managed OCI resources and Vault references remain compartment constrained;
operator-supplied database endpoints are verified through MySQL rather than OCI
inventory matching.

The default global MySQL mode is REQUIRED: TLS encryption is mandatory, without
certificate-chain or server-name verification and without client certificate or
CA files. The same policy applies to connection tests, target writers, Debezium
and JDBC schema history. Operators may explicitly choose VERIFY_CA or
VERIFY_IDENTITY under mysql_tls.mode when they configure the corresponding
global trust material. Per-connection CA/SSL controls remain removed.

The next authenticated local-admin login migrates the control repository:
existing database resource identities become internal `server_identity` values
without changing prior fingerprint namespaces. Old OCID/CA/SSL columns are then
removed. New registrations use the tested MySQL UUID, reusing an existing
endpoint's identity when one is registered. This migration does not change
passwords, flows or audit payloads. Previous per-connection CA settings are not
applied; configure any required custom trust globally.


## OCI Container Instances platform

The control plane stays on OL9. Both Debezium and the history writer are managed
through OCI Container Instances APIs. Each workload has a private VNIC, a
same-compartment subnet, a digest-pinned private registry image and a resource
principal for approved Vault access. Container Instances lifecycle operations
validate app/flow ownership and compartment before acting.

Private runtime configuration is selected by `AUDIT_CONFIG_FILE`. The same
setting is used by the app, setup scripts, systemd runtime environment and
configuration editor. The source and target database definitions, queue and
recovery-state contracts remain independent of this container platform.

## Snapshot schema evidence and writer recovery

Debezium can emit synthetic DROP and CREATE schema records at the same binlog
position during its initial snapshot. These are preparation and baseline records,
not evidence that a user dropped and recreated the source table. Snapshot schema
identities use the stable queue incarnation, topic, partition, offset and change
ordinal. Streaming DDL retains source-position identity for replay deduplication.
The stored labels are `SNAPSHOT_RESET` and `SNAPSHOT`; subsequent source DDL keeps
its actual change kind. Replay repairs an older snapshot identity only when its
original queue coordinates and stored content match exactly.

Control Center supports separate **Update Debezium image** and **Update history
writer image** actions. Select a new verified image; Update stops only that workload and confirms
INACTIVE before replacement. The application records the replacement instance and retains the
stopped previous instance. Writer consumer groups, delivery checkpoints, schema
history, row fingerprints and retained audit records remain unchanged.

## Setup, IAM and operator feedback review

The consolidated [IAM guide](iam-policy.md) separates VM, container and managed Kafka service identities, including secret metadata access required before bundle retrieval. Setup reruns preserve runtime configuration and custom TLS references. Standalone target preparation loads the same runtime defaults as systemd. A read-only setup/inventory diagnostic reports safe codes without retrieving secrets or mutating OCI.

Control Center distinguishes selected images from deployed images and shows process blocks per flow. Audit charts use explicit UTC ranges, full bounded intervals, legends and selectable chart types. Global database/table counts expose unavailable targets and link into the corresponding bar charts. [Operations](operations.md) documents the final workflow and its limitations.

Normal Stop pauses Debezium while the writer drains its backlog and waits. This intentional pause is distinct from a failed or unexpectedly inactive workload. Start resumes capture; Stop both workloads remains a separate maintenance action. Source binlog and messaging retention still bound the safe pause duration.
