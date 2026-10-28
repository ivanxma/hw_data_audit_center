"""Durable, flow-owned target partitions for database/table grouping."""

import hashlib
import re

from .repository import named_lock, query
from .validation import quote


def grouping(flow):
    mode = flow.get("target_grouping") or "flow"
    if mode not in {"flow", "database", "table"}:
        raise ValueError("Unknown target grouping.")
    return mode


def table_name(flow, schema, table):
    mode = grouping(flow)
    if mode == "flow":
        return flow["target_table"]
    # Hash the complete namespace, not a truncated display name. Flow identity
    # prevents two registrations from claiming the same generated destination.
    identity = [str(flow["id"]), flow["target_table"], schema]
    if mode == "table":
        identity.append(table)
    suffix = hashlib.sha256("\0".join(identity).encode()).hexdigest()[:16]
    stem = flow["target_table"] + "_" + schema + ("_" + table if mode == "table" else "")
    return re.sub(r"[^A-Za-z0-9_$]", "_", stem)[:45] + "_" + suffix


def initialize(db, flow):
    query(
        db,
        f"""CREATE TABLE IF NOT EXISTS {quote(flow["target_schema"])}.audit_target_routes (
        flow_id BIGINT NOT NULL, source_schema VARCHAR(64) NOT NULL, source_table VARCHAR(64) NOT NULL,
        target_table VARCHAR(64) NOT NULL, created_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
        PRIMARY KEY(flow_id,source_schema,source_table), INDEX target_lookup(target_table)
        ) ENGINE=InnoDB""",
    )

    query(
        db,
        f"CREATE TABLE IF NOT EXISTS {quote(flow['target_schema'])}.audit_target_route_claims (target_table VARCHAR(64) PRIMARY KEY, flow_id BIGINT NOT NULL) ENGINE=InnoDB",
    )


def ensure(db, flow, schema, table):
    if grouping(flow) == "flow":
        return flow["target_table"]
    from .capture_scope import matches
    from .heatwave import prepare_target

    if not matches(flow, schema, table):
        raise ValueError("Target routing rejected an out-of-scope source table.")
    destination = table_name(flow, schema, table)
    namespace = quote(flow["target_schema"])
    with named_lock(db, "target-route", flow["target_schema"], destination):
        row = query(
            db,
            f"SELECT target_table FROM {namespace}.audit_target_routes WHERE flow_id=%s AND source_schema=%s AND source_table=%s",
            (flow["id"], schema, table),
            one=True,
        )
        if row:
            if row["target_table"] != destination:
                raise ValueError("Stored target route conflicts with its mapping.")
            return destination
        count = query(
            db,
            f"SELECT COUNT(*) AS n FROM {namespace}.audit_target_routes WHERE flow_id=%s",
            (flow["id"],),
            one=True,
        )
        if count["n"] >= 1000:
            raise ValueError(
                "Target route inventory reached 1000 tables. Split capture into smaller scopes."
            )
        owner = query(
            db,
            f"SELECT flow_id FROM {namespace}.audit_target_routes WHERE target_table=%s AND flow_id<>%s LIMIT 1",
            (destination, flow["id"]),
            one=True,
        )
        if owner:
            raise ValueError("Generated target table belongs to another flow.")
        # Never adopt an existing unregistered table: it might contain unrelated history.
        exists = query(
            db,
            "SELECT TABLE_NAME AS name FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s",
            (flow["target_schema"], destination),
            one=True,
        )
        same_flow = query(
            db,
            f"SELECT flow_id FROM {namespace}.audit_target_routes WHERE target_table=%s AND flow_id=%s LIMIT 1",
            (destination, flow["id"]),
            one=True,
        )
        if exists and not same_flow:
            # A prior interrupted create is safe only with explicit ownership proof.
            owner = query(
                db,
                f"SELECT flow_id FROM {namespace}.audit_target_route_claims WHERE target_table=%s",
                (destination,),
                one=True,
            )
            if not owner or owner["flow_id"] != flow["id"]:
                raise ValueError(
                    "Generated audit destination already exists without route ownership."
                )
        query(
            db,
            f"INSERT INTO {namespace}.audit_target_route_claims(target_table,flow_id) VALUES (%s,%s) ON DUPLICATE KEY UPDATE target_table=target_table",
            (destination, flow["id"]),
        )
        claim = query(
            db,
            f"SELECT flow_id FROM {namespace}.audit_target_route_claims WHERE target_table=%s",
            (destination,),
            one=True,
        )
        if claim["flow_id"] != flow["id"]:
            raise ValueError("Target route ownership conflict.")
        prepare_target(db, {**flow, "target_table": destination}, payload_only=True)
        query(
            db,
            f"INSERT INTO {namespace}.audit_target_routes(flow_id,source_schema,source_table,target_table) VALUES (%s,%s,%s,%s)",
            (flow["id"], schema, table, destination),
        )
    return destination


def tables(db, flow):
    if grouping(flow) == "flow":
        return [flow["target_table"]]
    rows = query(
        db,
        f"SELECT DISTINCT target_table FROM {quote(flow['target_schema'])}.audit_target_routes WHERE flow_id=%s ORDER BY target_table LIMIT 1001",
        (flow["id"],),
    )
    if len(rows) > 1000:
        raise ValueError("Target inventory exceeds 1000 tables. Narrow the capture scope.")
    return [r["target_table"] for r in rows]


def relation(db, flow):
    names = tables(db, flow)
    if not names:
        # The root table provides an empty, compatible relation before first delivery.
        names = [flow["target_table"]]
    if len(names) == 1:
        return f"{quote(flow['target_schema'])}.{quote(names[0])}"
    return (
        "("
        + " UNION ALL ".join(
            f"SELECT * FROM {quote(flow['target_schema'])}.{quote(n)} WHERE flow_id={int(flow['id'])}"
            for n in names
        )
        + ") AS audit_rows"
    )


def write(db, flow, events):
    from .events import write_batch

    if grouping(flow) == "flow":
        return write_batch(db, flow, events)
    batches = {}
    resolved = {}
    for event in events:
        source = (event["source_schema"], event["source_table"])
        if source not in resolved:
            resolved[source] = ensure(db, flow, *source)
        table = resolved[source]
        batches.setdefault(table, []).append(event)
    # Each committed batch is deduplicated if a later route fails before delivery
    # checkpointing. The caller acknowledges only after every route has committed.
    return sum(
        write_batch(db, {**flow, "target_table": t, "retention_table": flow["target_table"]}, batch)
        for t, batch in batches.items()
    )
