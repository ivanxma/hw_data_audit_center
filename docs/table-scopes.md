# Source scopes and target layouts

A mapping selects which source row changes belong to a flow and how the writer
organizes their audit history. The target uses fixed audit-event JSON columns;
it does not recreate each source table's columns.

## Source selection

| Source selection | Captured scope |
| --- | --- |
| `sales.orders` | One named base table |
| `sales.*` | Eligible base tables in one database |
| `*.*` | Eligible base tables across source databases |

In **Register table**, select an OCI **Stream pool**, then an ACTIVE **Data stream**
from that pool. Existing flow bindings remain unchanged; no separate Streaming
connection registration is required. Assigned and schema-history streams cannot
be selected for another flow.

Use the database and table selectors in **Register table**. The wildcard is a
whole-name choice; partial patterns such as `sales.order*` are not supported.
Literal names are escaped when constructing Debezium's anchored regular-expression
filters. Internal/system databases are excluded. Registered audit-target schemas
on the same source server are excluded to prevent capture feedback.

Source checks inspect metadata and primary keys, not table contents. Current
inventory is limited to 1,000 tables; a larger result is rejected rather than
silently truncated. Every included table needs a complete stable primary key.
A new table or database that matches the saved scope can be discovered while
capture runs. A newly created keyless table is not silently ignored: unsupported
row events stop the writer for review. Review filters and exclusions when adding
new databases, audit destinations or broad source registrations.

A wildcard mapping can start with no eligible base tables. For `sales.*`, the
database must already exist and be visible to the source account. Source checks
report that capture is waiting for new matching tables. Create each new table
with a stable primary key; its subsequent row changes enter the same running
flow without registering it separately. Debezium reads the new table's DDL from
the binlog to learn its structure. See the [MySQL connector documentation](https://debezium.io/documentation/reference/3.4/connectors/mysql.html).

Broad capture increases schema/change volume and may include sensitive application
data. Choose the narrowest scope that meets the audit requirement. Source account
permissions and metadata visibility must cover the intended databases; a wildcard
does not grant MySQL privileges. Overlapping registrations on the same source and
unsafe source-to-target feedback paths are rejected.

## Target grouping

All three choices use the selected target connection and schema.

| Grouping | Physical audit destination |
| --- | --- |
| **By flow** | One audit table for every source table in the flow |
| **By source database** | One generated audit table per actual source database |
| **By source table** | One generated audit table per actual source database/table pair |

For example, `sales.orders`, `sales.customers` and `billing.orders` produce one,
two or three physical audit tables respectively. By database does **not** create
one target schema per source database. The registered target-table name acts as
the root/prefix for generated destinations. Names include a deterministic hash
of the complete source namespace and flow identity, remain within MySQL's name
limit, and are recorded in durable route/ownership tables.

The writer creates a destination only after claiming ownership. It refuses an
unowned existing physical table rather than treating it as an empty audit store.
Interrupted creation can resume from its saved claim. The root table retains the
flow's retention anchor even when generated destinations hold the row events.

For database/table grouping, the writer prepares a destination when it receives
an in-scope `CREATE TABLE` schema event, even if the source table has no rows.
Row delivery also prepares a missing destination, so independently arriving data
and schema records can recover safely. Replayed schema events reuse the same
owned destination. These destinations use the standard audit-history layout;
they do not mirror the source table's physical columns. Choose **By source table**
to create a separate audit table for every new source table; **By flow** continues
to share one audit table.

Use a separate data stream with a verifiable retained origin for a new flow.
A stream older than its retention period with no delivery checkpoint cannot
prove complete coverage; the writer stops for review. Prepare a fresh stream
before deployment rather than clearing checkpoints or seeking past evidence.

Each event retains its actual source database and table. Row-key hashes include
that namespace, so identical primary keys in different tables remain distinct.
The flow has one writer lease, one source offset namespace, and its prepared data
and schema streams. Grouping does not create a connector or stream per table.
Writes to several physical destinations are recoverable commits, not one atomic
cross-table/source transaction. Replay checks prevent supported duplicate events
when a later grouped write or checkpoint fails.

## Browsing and operating boundaries

Audit Center combines the flow's owned destinations for history and charts. Filter
by source database/table; a primary-key lookup must identify one actual table
(and a database for `*.*`). Audit partitions lets you choose a generated physical
table. Large JSON pages sort event coordinates before loading selected payloads.

End and initial Specific GTID are supported for wildcard mappings without copying
existing rows. Start requests an initial row snapshot across the included scope
and can be expensive. Normal restarts and image replacements retain full offsets.
**Historical repositioning of an already deployed wildcard flow is disabled**
until complete schema-baseline coverage across all included tables can be proven.
The single-table historical preview does not imply that capability for a wildcard.

**Automatic retention cleanup for grouped targets is disabled.** Use a separately
reviewed, coordinated maintenance procedure that preserves the flow's payload,
deduplication, delivery, schema, routing and fragment evidence together. A row
count or an old root table alone is not sufficient cleanup evidence.

Once deployment history exists, source scope and target grouping are locked.
Register a new flow for a changed data path; do not edit identifiers under saved
checkpoints. See [capture position](capture-start.md),
[Streaming and size limits](streaming-and-size-limits.md) and
[coverage limitations](limitations.md).
