"""Public schema evidence is archived separately from Debezium's private recovery state."""

import json

from .fingerprints import canonical, digest
from .repository import named_lock, query
from .validation import quote


def initialize(db, schema):
    query(
        db,
        f"""CREATE TABLE IF NOT EXISTS {quote(schema)}.audit_event_dedup (
        event_id CHAR(64) PRIMARY KEY, payload_fingerprint CHAR(64) NOT NULL,
        event_time DATETIME(6) NOT NULL, recorded_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)
    ) ENGINE=InnoDB""",
    )
    query(
        db,
        f"""CREATE TABLE IF NOT EXISTS {quote(schema)}.audit_schema_changes (
        event_id CHAR(64) PRIMARY KEY, flow_id BIGINT NOT NULL,
        source_schema VARCHAR(64) NOT NULL, object_name VARCHAR(255),
        change_kind VARCHAR(24), source_position JSON NOT NULL,
        before_definition JSON, after_definition JSON,
        semantic_delta JSON, ddl TEXT, fidelity VARCHAR(24) NOT NULL,
        observed_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
        INDEX object_history(flow_id,object_name,observed_at,event_id)
    ) ENGINE=InnoDB""",
    )


def difference(before, after, path=""):
    if isinstance(before, dict) and isinstance(after, dict):
        result = []
        for key in sorted(set(before) | set(after)):
            p = path + "/" + str(key)
            if key not in before:
                result.append({"path": p, "kind": "added", "after": after[key]})
            elif key not in after:
                result.append({"path": p, "kind": "removed", "before": before[key]})
            else:
                result.extend(difference(before[key], after[key], p))
        return result
    if before != after:
        return [{"path": path or "/", "kind": "changed", "before": before, "after": after}]
    return []


def normalize(table):
    if not isinstance(table, dict):
        return table
    result = dict(table)
    if isinstance(result.get("columns"), list):
        result["columns"] = {c["name"]: c for c in result["columns"] if c.get("name")}
    return result


def is_snapshot(source):
    return source.get("snapshot") in (True, "true", "last", "incremental")


def schema_identity(flow, source, object_name, ordinal, topic, partition, offset, message_id=None):
    # Snapshot preparation can emit DROP and CREATE for the same table at the
    # same binlog boundary. Those are distinct observations, not binlog DDL.
    if not is_snapshot(source) and source.get("file") is not None and source.get("pos") is not None:
        return digest(
            [
                flow["lineage_id"],
                flow["source_identity"],
                source["file"],
                source["pos"],
                source.get("gtid"),
                object_name,
                ordinal,
            ]
        )
    return digest(
        [flow["lineage_id"], flow["queue_incarnation"], topic, partition, offset, ordinal]
        if message_id is None
        else [
            flow["lineage_id"],
            flow["queue_incarnation"],
            topic,
            "framed-v1",
            message_id,
            ordinal,
        ]
    )


def write_schema_event(db, flow, raw, topic, partition, offset, *, message_id=None):
    if raw is None:
        return
    decoded = json.loads(raw)
    if not isinstance(decoded, dict):
        raise ValueError("Public schema message must be an object.")
    event = decoded.get("payload", decoded)
    if not isinstance(event, dict) or "ddl" not in event:
        raise ValueError("Unsupported public schema envelope.")
    from .capture_scope import matches, matches_schema

    if not matches_schema(flow, event.get("databaseName")):
        return
    changes = event.get("tableChanges", [])
    if not isinstance(changes, list):
        raise ValueError("Schema tableChanges must be a list.")
    namespace = quote(flow["target_schema"])
    with named_lock(db, "schema", flow["target_schema"], flow["id"]):
        old_autocommit = db.autocommit
        db.autocommit = False
        try:
            for ordinal, change in enumerate(changes):
                if not isinstance(change, dict):
                    raise ValueError("Invalid schema table change.")
                object_name = change.get("id", "")
                names = [
                    n.strip().replace("`", "").replace(chr(34), "") for n in object_name.split(",")
                ]
                if not any("." in n and matches(flow, *n.split(".", 1)) for n in names):
                    continue
                after = normalize(change.get("table"))
                source = event.get("source", {})
                if not isinstance(source, dict):
                    raise ValueError("Invalid schema source coordinates.")
                snapshot = is_snapshot(source)
                kind = (
                    ("SNAPSHOT_RESET" if change.get("type") == "DROP" else "SNAPSHOT")
                    if snapshot
                    else change.get("type")
                )
                identity = schema_identity(
                    flow, source, object_name, ordinal, topic, partition, offset, message_id
                )
                existing = query(
                    db,
                    f"SELECT after_definition,ddl,change_kind FROM {namespace}."
                    "audit_schema_changes WHERE event_id=%s",
                    (identity,),
                    one=True,
                )
                if not existing and snapshot:
                    # Repair only the exact previously persisted observation.
                    # Never merge different snapshot records sharing a binlog position.
                    legacy_id = digest(
                        [
                            flow["lineage_id"],
                            flow["source_identity"],
                            source.get("file"),
                            source.get("pos"),
                            source.get("gtid"),
                            object_name,
                            ordinal,
                        ]
                    )
                    legacy = query(
                        db,
                        f"SELECT after_definition,ddl,change_kind,source_position "
                        f"FROM {namespace}.audit_schema_changes WHERE event_id=%s",
                        (legacy_id,),
                        one=True,
                    )
                    if legacy:
                        position = legacy["source_position"]
                        position = json.loads(position) if isinstance(position, str) else position
                        queue = position.get("_audit_queue", {})
                        stored = legacy["after_definition"]
                        stored = json.loads(stored) if isinstance(stored, str) else stored
                        if (
                            queue
                            == {
                                "incarnation": flow["queue_incarnation"],
                                "topic": topic,
                                "partition": partition,
                                "offset": offset,
                            }
                            and stored == after
                            and legacy["ddl"] == event.get("ddl")
                            and legacy["change_kind"] == change.get("type")
                        ):
                            query(
                                db,
                                f"UPDATE {namespace}.audit_schema_changes SET event_id=%s,"
                                "change_kind=%s,fidelity=%s WHERE event_id=%s",
                                (
                                    identity,
                                    kind,
                                    "snapshot_reset" if kind == "SNAPSHOT_RESET" else "baseline",
                                    legacy_id,
                                ),
                            )
                            existing = {**legacy, "change_kind": kind}
                if existing:
                    stored = existing["after_definition"]
                    stored = json.loads(stored) if isinstance(stored, str) else stored
                    if (
                        stored != after
                        or existing["ddl"] != event.get("ddl")
                        or existing["change_kind"] != kind
                    ):
                        raise ValueError("Schema event identity conflicts with stored evidence.")
                    continue
                previous = query(
                    db,
                    f"""SELECT after_definition,source_position FROM {namespace}.audit_schema_changes
                    WHERE flow_id=%s AND object_name=%s ORDER BY
                    CAST(JSON_UNQUOTE(JSON_EXTRACT(source_position,'$._audit_queue.offset')) AS UNSIGNED) DESC,
                    observed_at DESC,event_id DESC LIMIT 1""",
                    (flow["id"], object_name),
                    one=True,
                )
                before = previous["after_definition"] if previous else None
                if isinstance(before, str):
                    before = json.loads(before)
                if previous:
                    position = previous["source_position"]
                    position = json.loads(position) if isinstance(position, str) else position
                    queue = position.get("_audit_queue", {})
                    if queue and (
                        queue.get("incarnation") != flow["queue_incarnation"]
                        or queue.get("topic") != topic
                        or queue.get("partition") != partition
                        or queue.get("offset", -1) >= offset
                    ):
                        raise ValueError(
                            "Schema evidence is out of order or belongs to another queue incarnation."
                        )
                position = {
                    **source,
                    "_audit_queue": {
                        "incarnation": flow["queue_incarnation"],
                        "topic": topic,
                        "partition": partition,
                        "offset": offset,
                    },
                }
                query(
                    db,
                    f"""INSERT INTO {namespace}.audit_schema_changes
                    (event_id,flow_id,source_schema,object_name,change_kind,source_position,before_definition,
                     after_definition,semantic_delta,ddl,fidelity) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        identity,
                        flow["id"],
                        event["databaseName"],
                        object_name,
                        kind,
                        canonical(position),
                        canonical(before) if before is not None else None,
                        canonical(after) if after is not None else None,
                        canonical(difference(before, after)),
                        event.get("ddl"),
                        "snapshot_reset"
                        if kind == "SNAPSHOT_RESET"
                        else "baseline"
                        if snapshot
                        else "partial"
                        if previous
                        else "baseline",
                    ),
                )
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.autocommit = old_autocommit
