# Streaming, message size and request control

The deployed path is source MySQL binlog → embedded Debezium Engine → native
OCI Streaming → history writer → target audit store. The capture and writer use
OCI SDKs over HTTPS with resource principals. No Object Storage overflow path is
used. Ordinary events keep their original key and full Debezium envelope.

## Why stage changes in a stream?

A durable stream lets capture publish while a writer is stopped, slow or being
replaced. The writer later resumes from its target delivery checkpoint. Source
capture and target apply can progress at different speeds, and short apply-side
interruptions need not force an immediate source replay. Stream retention bounds
this buffer: it is not an unlimited archive or a substitute for source binlogs.

There is a significant dependency in this implementation: the **source offset and
Debezium JDBC schema history are stored in the target database too**. If the entire
target database is unavailable, capture can also pause when it needs that state.
Streaming decouples publishing from row apply, but does not make capture fully
independent of the target database. Preserve enough source binlog retention for
that outage and enough stream retention for the writer's recovery. An independent
capture-state database would be an additional design change.

## Which limits can change?

| Boundary | Current behavior | Operator control |
| --- | --- | --- |
| OCI message and PUT request | Service maximum remains 1 MiB; batching must fit the request too | Application configuration cannot raise the service maximum |
| Publisher request budget | At most 900,000 estimated encoded bytes, including keys and overhead | Fixed conservative application budget |
| Normal event | Original key/value in one message if it fits the publisher budget | No fragmentation for events that fit |
| Oversized event | Lossless framing, with chunks of at most 400,000 packed bytes | Automatic within the logical event limit |
| Logical event | Default 16 MiB for packed key, value and framing header | `streaming.max_event_bytes`: 1–64 MiB |
| Incomplete fragment custody | Default 128 MiB reserved per flow | `streaming.max_pending_fragment_bytes`: at least one maximum event, at most 1 GiB |
| Target database | JSON/document, packet, connection and storage limits still apply | DBA settings and supported service configuration; application limits do not override them |

The publisher budget is deliberately below the service ceiling. An event smaller
than 1 MiB in raw JSON may still need fragments because the key, UTF-8, base64
encoding, full envelope and request structure consume space. Inspect serialized
bytes rather than character count. Oracle describes chunking and external payload
storage as large-message designs; this application implements chunking.
[OCI publishing and large messages](https://docs.oracle.com/en-us/iaas/Content/Streaming/Tasks/publishing.htm),
[PutMessages request limit](https://docs.oracle.com/en-us/iaas/tools/oci-cli/latest/oci_cli_docs/cmdref/streaming/stream/message/put.html).

## Debezium envelope overhead

The `payload` contains the operation, before/after values and source coordinates.
The outer `schema` describes the serialized fields and their types. A two-column
update can therefore carry far more metadata than changed business values. Source
metadata describes the event's origin and replication position; database passwords
and connection secrets are not part of the row message.

This release retains the complete schema and both images for supported updates.
It does not silently remove fields or change typed fingerprint semantics. Debezium
JSON serialization can omit repeated schema descriptions, but a compact mode needs
an explicit schema recovery/versioning contract and compatibility validation.
Such a mode is **not implemented here**. Removing unchanged columns or the BEFORE
image would also change what audit history can establish. Fragmentation preserves
information; it is not compression and adds some bytes and processing work.
[Debezium Engine serialization](https://debezium.io/documentation/reference/development/engine.html).

Snapshots read current source rows directly. End and Specific GTID establish
schema and follow binlog events without a row-copy snapshot. A broad wildcard
scope still increases metadata and change volume; skipping the snapshot does not
remove the cost of reading and serializing large row changes.
[Debezium MySQL connector](https://debezium.io/documentation/reference/3.4/connectors/mysql.html).

## Fragment delivery and recovery

1. The publisher measures the complete key/value. A normal event stays one message.
2. An oversized event is packed with the original key and split. Each part includes
   its index/count, declared size, message identity and part/full SHA-256 checksums.
   A stable routing key keeps the parts of that event in one stream partition.
3. The source checkpoint advances only after every required part was accepted.
   A partially accepted request cannot acknowledge an unpublished source event.
4. The writer commits fragment custody before advancing the delivery checkpoint
   past an incomplete part. A restart can resume from those saved parts.
5. Only complete, validated events enter the normal audit writer. The original
   before/after data, schema and key are reconstructed; partial events never become
   audit rows. Completed payload blobs are released after durable delivery state
   proves completion. Compact receipts remain for replay checks.

A source restart can regenerate processing timestamps in an envelope. That can
produce a different fragment generation for the same underlying source event.
An older incomplete generation is retained even when a later complete generation
arrives. Source-coordinate deduplication protects supported row events, but does
not automatically reconcile every abandoned fragment set. Review retained custody
and source/delivery evidence before explicit cleanup. The pending limit stops
further custody instead of silently discarding incomplete data.

Complete receipts, the event deduplication ledger and audit payloads have different
lifetimes. This release does not automatically prune every receipt. Grouped target
retention also requires coordinated maintenance. Neither source transactions nor
writes across multiple target tables form one distributed atomic transaction.

## Fewer requests before retries

Several small events share one `PutMessages` request while remaining separate OCI
messages. A request is flushed when its byte budget, record count or destination
requires it. Data and schema streams require separate requests. No custom aggregate
wrapper is added around normal events.

| Runtime JSON setting under `streaming` | Default | Purpose |
| --- | ---: | --- |
| `put_max_records` | 500 | Maximum records per engine/publish batch, bounded by bytes |
| `put_requests_per_second` | 4 | Request ceiling per stream |
| `put_bytes_per_second` | 850000 | Smooth estimated encoded PUT bytes per stream |
| `get_max_records` | 1000 | Fetch more queued records in one GET |
| `get_idle_max_ms` | 2000 | Back off empty partition polls up to this delay |
| `get_bytes_per_second` | 1500000 | Pace reads by estimated response bytes per partition |

Publishing uses the lower of the configured byte rate and 850,000 times the stream's
partition count. A hot key can concentrate traffic in one partition, so increasing
the rate or partition count does not guarantee higher usable throughput. Reading
waits at least 250 ms between requests for the same partition and slows further
for byte volume or idle queues. Backoff adds some latency when a quiet queue
becomes active. Each source engine also has a bounded 64 MiB change queue.

Retries remain bounded fallback handling. HTTP success does not prove every PUT
entry succeeded: the publisher checks individual results, including numeric `429`,
and retries only rejected entries. A failed batch retains the source checkpoint.
GET retries retain the same cursor; target checkpoints move only after durable
custody or audit commits. Partition throughput, API management quotas and workload
lifecycle quotas are distinct constraints; identify the service and operation from
the actual error rather than assuming PUT and GET share one global quota.
[OCI Streaming throttling](https://docs.oracle.com/en-us/iaas/Content/Streaming/Reference/troubleshootingstreaming.htm).

The Control Center page polls every 15 seconds, but shared status samples limit OCI
reads across tabs and web workers: up to 45 seconds for settled states, 10 seconds
during transitions. A throttled status read is shared too; the page does not
immediately duplicate it. **Checked** displays the actual sample time.

Save Runtime settings, then choose **Control Center → More actions → Apply stream
settings**. This uses the selected workload images and replaces the writer before
the source while retaining history and checkpoints. An interrupted operation can
be resumed with the same action. Unchanged effective policy/image pairs are kept.
The action permits unchanged or increased message/custody limits; reducing them
requires a separately reviewed drain and retained-fragment reconciliation.

Publisher health counters include PUT requests, accepted physical messages, wire
bytes, throttled entries and logical events. Writer logs report GET requests,
records, bytes, empty responses, retries and throttles. Compare deltas for a known
workload instead of relying on retry counts alone.

## Target size and browse limits

The target stores keys, images and schema as JSON in a fixed audit structure. It
is not a column-for-column replica of the source's wide row. InnoDB page/row rules
therefore differ from the total JSON payload size, and large values can use
out-of-page storage. This does not make JSON or network packets unlimited.
A JSON value is constrained by `max_allowed_packet`; client/server packet settings,
writer memory, serialization expansion and batch size must also be adequate.
[MySQL JSON limits](https://dev.mysql.com/doc/refman/8.4/en/json.html),
[packet limits](https://dev.mysql.com/doc/refman/8.4/en/packet-too-large.html),
[storage requirements](https://dev.mysql.com/doc/refman/8.4/en/storage-requirements.html).

Raising the application event limit cannot raise the database's limits. Test the
largest full update, including both images and schema, under representative load.
The writer bounds batch bytes; one assembled event can exceed that ordinary batch
budget while remaining subject to the logical event ceiling. Audit Center pages
sort lightweight event coordinates before retrieving selected JSON payloads,
avoiding a large JSON sort. Selected payloads are still loaded for the page: many
very large events can produce a heavy browser response.

## Validation scope

Live validation on 28 October 2026 delivered large Unicode CREATE, UPDATE and
DELETE events exactly once in each of the three target layouts, verified partial
writer reassembly recovery, and restarted source and writer without changing
retained history. A separate burst of 100 row events plus one schema event used
**2 PUT requests, 101 physical messages, zero throttled entries**. Normal events
remained single messages. This is a functional batching result for that fixture,
not a general throughput or latency benchmark.
