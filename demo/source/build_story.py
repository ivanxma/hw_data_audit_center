"""Author the public, illustrative native Streaming demonstration."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SAFE_HARBOR = "The following is intended to outline our general product direction. It is intended for information purposes only, and may not be incorporated into any contract. It is not a commitment to deliver any material, code, or functionality, and should not be relied upon in making purchasing decisions. The development, release, timing, and pricing of any features or functionality described for Oracle’s products may change and remains at the sole discretion of Oracle Corporation."
scenes = []


def add(id, title, duration, narration, caption, **extra):
    scenes.append(
        dict(id=id, title=title, duration=duration, narration=narration, caption=caption, **extra)
    )


add(
    "welcome",
    "Every row has a story",
    10,
    "Welcome to HeatWave Audit Center. Source left, controls in the centre, audit history right. This walkthrough uses illustrative demo data.",
    "Source → OCI Streaming → retained audit history",
    mode="triple",
    card="welcome",
)
add(
    "safe-harbor",
    "Safe harbor statement",
    20,
    "This demonstration explains the current application and its limits. Oracle styling does not imply Oracle authorship or endorsement. The security audit extension shown later is not implemented in this application.",
    "Information only · Current capabilities and extension boundaries are identified",
    mode="full",
    card="safe-harbor",
    text=SAFE_HARBOR,
)
add(
    "architecture",
    "One compartment. One traceable data path.",
    20,
    "The web application runs on Oracle Linux nine. Embedded Debezium Engine and the history writer run on private O C I Container Instances. Debezium reads MySQL binlogs and publishes with the native O C I Java S D K over H T T P S. The Python writer consumes Streaming and commits audit history. Vault holds secrets. Resource access remains compartment scoped.",
    "OL9 control plane · OCI Container Instances · OCI Streaming · HeatWave target",
    mode="full",
    card="architecture",
)
add(
    "create-table",
    "Create a table with a stable row key",
    16,
    "Begin with a demo orders table and a primary key. Creating this table prepares the source before registration. Because capture starts later, its first schema observation is a baseline. It is not a claim that the earlier create statement was captured from the binary log.",
    "CREATE TABLE before capture → schema baseline at initialization",
    mode="triple",
    card="prepare",
    source="CREATE TABLE demo_sales.orders (\n  id BIGINT PRIMARY KEY,\n  status VARCHAR(20) NOT NULL\n    DEFAULT 'new',\n  amount DECIMAL(10,2) NOT NULL\n    DEFAULT 0.00\n);",
    target="baseline",
)
add(
    "configuration",
    "Connect, discover and register",
    18,
    "Register the source and target connections. The Test connection button checks MySQL access using a Vault secret reference. Table mappings load schemas and tables from those connections. Select one registration and choose Edit to change its available settings. Existing capture identities remain protected.",
    "Test connection → discover objects → map source to audit target",
    mode="browser",
    page="configuration?tab=connections",
    target="mapping",
)
add(
    "streaming",
    "Select OCI Streaming resources",
    18,
    "Create or select an active stream pool and data stream. Register the pool connection using principal authentication. The mapping uses that data stream and its own single-partition schema stream. The source publishes with the native O C I S D K, and the writer consumes with the native Python S D K.",
    "Data stream: demo-orders · Schema stream: audit_1 · Principal authentication",
    mode="browser",
    page="configuration?tab=resources&service=streaming",
    target="streams",
)
add(
    "control",
    "Start from now, then verify delivery",
    14,
    "A new flow initialized at End captures schema and future changes, without copying existing rows. Restarts resume saved offsets. Verify messages and target events, not just container status.",
    "New flow: End = no existing-row snapshot · Restart = saved checkpoint",
    mode="browser",
    page="control",
    target="ready",
)
add(
    "insert",
    "INSERT → CREATE with an AFTER image",
    20,
    "Insert order ten forty two. Debezium represents the insert as a create event. Its before image is null; its after image contains the new row. The writer stores the key, source position, queue offset and images. A successful target commit must precede acknowledgement of the message.",
    "CREATE: BEFORE = null · AFTER = inserted row",
    mode="triple",
    page="messages?flow_id=1&event=0",
    source="INSERT INTO demo_sales.orders\n  (id, status, amount)\nVALUES\n  (1042, 'new', 80.00);",
    event=0,
    target="event",
)
add(
    "update",
    "UPDATE → compare complete images",
    22,
    "Now change the order status and amount. The same key links the earlier and later versions. Select the update in Change history, then Compare images. The before image shows new and eighty; the after image shows paid and ninety nine point ninety five. Full row images require the correct source settings.",
    "UPDATE: new / 80.00 → paid / 99.95 · binlog_row_image = FULL",
    mode="triple",
    page="audit?flow_id=1&tab=audit-history&event=1",
    source="UPDATE demo_sales.orders\nSET status = 'paid',\n    amount = 99.95\nWHERE id = 1042;",
    event=1,
    target="event",
)
add(
    "delete",
    "DELETE → retain the final BEFORE image",
    18,
    "Delete the source row. It disappears from the current source table, but the audit record retains its final before image. The after image is null. History remains available for the original insert, the update and the deletion. Deleting source data does not erase audit history.",
    "DELETE: BEFORE = final row · AFTER = null · History remains",
    mode="triple",
    page="audit?flow_id=1&tab=audit-history&event=2",
    source="DELETE FROM demo_sales.orders\nWHERE id = 1042;\n\nSELECT *\nFROM demo_sales.orders\nWHERE id = 1042;\n\n-- Current source row: absent",
    event=2,
    target="event",
)
add(
    "lookup",
    "Find one row across its lifetime",
    20,
    "Search with the typed record key, id ten forty two, and a retained date range. The application computes the same keyed fingerprint from the source identity, schema, table and key. An indexed lookup finds that row’s changes. Separate image fingerprints describe content changes; the event identity supports replay deduplication.",
    "Typed key → stable key fingerprint → indexed, time-bounded history",
    mode="browser",
    page="audit?flow_id=1&tab=audit-history&event=2",
    event=2,
    target="lookup",
)
add(
    "ddl",
    "DDL → compare the table structure",
    26,
    "Add a region column with a default, and widen the amount type. Schema events use the separate schema stream. The target stores the prior definition, new definition and a semantic difference. Here, SQL inspection shows the new column and decimal precision change. Defaults, indexes and partitions can only be compared when the captured metadata contains them. Unknown properties stay unknown.",
    "Separate schema history · Compare captured properties · Unknown stays unknown",
    mode="triple",
    card="ddl",
    source="ALTER TABLE demo_sales.orders\n  ADD COLUMN region VARCHAR(16)\n    DEFAULT 'EMEA',\n  MODIFY amount DECIMAL(14,2)\n    NOT NULL DEFAULT 0.00;",
    target="ddl",
)
add(
    "dcl",
    "Users and privileges need security evidence",
    28,
    "Create a locked demonstration user, grant select, then revoke it. These are account and privilege operations, not row updates. This application does not yet collect them as a security history. Use the MySQL Enterprise Audit plugin with reviewed filters and a separate collector. Retain the actor, account, scope, action, outcome and timestamp. Do not infer complete privilege history from row change capture.",
    "DCL extension is not implemented · Use database audit logs and a separate collector",
    mode="triple",
    card="dcl",
    source="CREATE USER\n  'audit_demo_reader'@'localhost'\n  ACCOUNT LOCK;\n\nGRANT SELECT ON demo_sales.*\nTO 'audit_demo_reader'@'localhost';\n\nREVOKE SELECT ON demo_sales.*\nFROM 'audit_demo_reader'@'localhost';",
    target="dcl",
)
add(
    "global-chart",
    "From all databases to one table",
    20,
    "The Audit Center opens with all databases. A pie chart compares event counts. Choose a database slice or its legend entry to see registered table bars, then select a table to drill into changes over time. These overview counts are a separate sample dataset, not the three earlier demonstration transactions.",
    "All databases pie → database table bars → table changes over time",
    mode="browser",
    page="audit?tab=audit-global",
    target="chart",
)
add(
    "trend-chart",
    "Choose the interval, range and chart",
    20,
    "Choose a custom date range and an interval of minute, hour, day or week. Minute charts cover one hour. Switch between line, bar and stacked bar, and use the operation legend. Counts include snapshots unless you filter them out. Download a chart image or export the loaded table data.",
    "Custom UTC range · Minute / Hour / Day / Week · Line / Bar / Stacked bar",
    mode="browser",
    page="audit?flow_id=1&tab=audit-trends&interval=day&chart_type=line&sample=charts&from=2026-10-17T00:00&to=2026-10-24T00:00",
    target="chart",
)
add(
    "summary",
    "Rows, structures and security: clear boundaries",
    10,
    "Capture row images, compare schemas, and explore history and charts. Security auditing needs a separate collector. This was an illustrative replay.",
    "Row history + schema evidence + charts · Security collector remains a separate extension",
    mode="full",
    card="summary",
)
# Additional native Streaming and recovery chapters follow service setup.
additional = [
    dict(
        id="source-scope",
        title="One table, one database, or all eligible tables",
        duration=28,
        narration="A mapping can capture one table, every eligible table in one database, or eligible tables across databases. System databases and registered audit destinations are excluded. Included tables need stable primary keys. Target grouping chooses one audit table per flow, source database, or source table, all within the selected target schema. These are audit records, not column-for-column source replicas.",
        caption="Source: table, database.* or *.* · Target grouping: flow, database or table",
        mode="browser",
        page="configuration?tab=flows",
    ),
    dict(
        id="gtid-start",
        title="Start after a validated processed G T I D set",
        duration=32,
        narration="End captures new changes without copying existing rows. Start reads current rows for a snapshot. For Specific G T I D, enter transactions already processed. Include all transactions removed from the source logs, and exclude transactions that have not executed. Keep each server identifier and its intervals. Multiple groups are allowed. Also validate transaction order and schema coverage.",
        caption="Purged ⊆ selected ⊆ executed · Already processed, not a transaction to include",
        mode="browser",
        page="configuration?tab=flows",
    ),
    dict(
        id="staging",
        title="A durable buffer for delayed target apply",
        duration=26,
        narration="Streaming retains changes while the writer is stopped, slow, or being replaced. The writer catches up from durable delivery checkpoints, within stream retention. There is an important limit: capture offsets and private Debezium schema history also live in the target database. A full target outage can therefore pause extraction too. Keep source binlogs for that outage window.",
        caption="Stream retention absorbs apply delay · Target-backed capture state remains a dependency",
        mode="full",
        card="explanation",
        points=[
            "Writer delayed: retained stream buffers changes",
            "Restart: resume durable delivery checkpoints",
            "Full target outage: capture state can pause extraction",
        ],
        note="Source binlog retention and stream retention protect different recovery windows.",
    ),
    dict(
        id="message-limits",
        title="Full row images can be much larger than a row",
        duration=28,
        narration="An update carries both images, source metadata, keys and a repeated schema description. The one mebibyte service maximum applies to a message and a P U T request. This publisher uses a lower encoded byte budget. Logical events default to sixteen mebibytes, with bounded incomplete custody. The target database still needs suitable J S O N, packet and memory capacity. The current release preserves the full schema; compact schema-free serialization is not implemented.",
        caption="Full envelope + UTF-8 + encoding overhead · Service and target limits still apply",
        mode="full",
        card="explanation",
        points=[
            "Service: 1 MiB message and PUT request limit",
            "Application: 16 MiB logical event default",
            "Target: JSON, packet, storage and memory limits remain",
        ],
        note="Fragmentation preserves the full event. It does not compress it or raise database limits.",
    ),
    dict(
        id="fragmentation",
        title="Normal events stay single; large events split",
        duration=28,
        narration="An event within budget stays one message. An oversized event is split into checksum-protected parts with stable routing. Source progress is acknowledged only after every part is accepted. The writer saves incomplete parts durably and writes an audit event only after complete reassembly. A writer restart can resume those parts. Interrupted source attempts can leave older incomplete generations, which need retained-evidence review before cleanup.",
        caption="Conditional split → durable part custody → checked reassembly → complete audit event",
        mode="full",
        card="explanation",
        points=[
            "Normal event: original key and envelope, one message",
            "Oversized event: indexed parts and full checksums",
            "Writer: durable custody before delivery progress",
        ],
        note="Incomplete generations are bounded and retained for review; partial events never become audit rows.",
    ),
    dict(
        id="request-control",
        title="Reduce requests before relying on retries",
        duration=30,
        narration="Small events share a request and remain separate messages. Batch limits and byte pacing smooth writes. Larger read batches and slower idle polling reduce calls. Retries handle remaining rate limit rejections. Check every result entry. Our test sent one hundred row events and one schema event in two write requests, with no rejected records. Apply stream settings updates the writer before the source.",
        caption="Validated fixture: 101 messages in 2 PUT calls · Zero throttled entries",
        mode="browser",
        page="configuration/runtime",
    ),
]
insert = next(i for i, scene in enumerate(scenes) if scene["id"] == "control")
scenes[insert:insert] = additional

start = 0
for s in scenes:
    s["start"] = start
    start += s["duration"]
(ROOT / "storyboard.json").write_text(
    json.dumps(
        {
            "title": "HeatWave Audit Center — OCI Streaming demonstration",
            "mode": "Illustrative replay; application templates with isolated fixture data. No live source mutations or OCI delivery claims.",
            "duration": start,
            "width": 2560,
            "height": 1440,
            "scenes": scenes,
        },
        indent=2,
    )
    + "\n"
)
(ROOT / "narration.txt").write_text(
    "\n\n".join(
        f"{s['start'] // 60:02}:{s['start'] % 60:02} — {s['title']}\n{s['narration']}"
        for s in scenes
    )
    + "\n"
)
print("Authored", len(scenes), "scenes,", start, "seconds.")
