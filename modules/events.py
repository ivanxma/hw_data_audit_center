"""Validate Debezium evidence and persist bounded batches with replay protection."""

import json
from datetime import datetime, timezone

from .fingerprints import canonical, digest, row_hash

OPS = {"c": "CREATE", "u": "UPDATE", "d": "DELETE", "r": "SNAPSHOT", "t": "TRUNCATE"}


def decode(raw, key, topic, partition, offset, flow, fingerprint_key):
    if raw is None:
        return None  # Kafka delete tombstone, not a second DELETE.
    obj = json.loads(raw)
    if not isinstance(obj, dict):
        raise ValueError("Change message must be a JSON object.")
    envelope = obj.get("payload", obj)
    if envelope is None:
        return None
    if not isinstance(envelope, dict) or envelope.get("op") not in OPS:
        raise ValueError("Unsupported change envelope; offset will not be committed.")
    source = envelope.get("source", {})
    if not isinstance(source, dict):
        raise ValueError("Change source must be an object.")
    if source.get("db") != flow["source_schema"] or source.get("table") != flow["source_table"]:
        raise ValueError("Message does not belong to the registered table.")
    op = envelope["op"]
    if op in {"u", "d"} and not isinstance(envelope.get("before"), dict):
        raise ValueError("Missing BEFORE image; offset will not be committed.")
    if op in {"c", "u", "r"} and not isinstance(envelope.get("after"), dict):
        raise ValueError("Missing AFTER image; offset will not be committed.")
    if op in {"c", "r"} and envelope.get("before") is not None:
        raise ValueError("Create/snapshot events cannot have a BEFORE image.")
    if op == "d" and envelope.get("after") is not None:
        raise ValueError("Delete events cannot have an AFTER image.")
    record_key = json.loads(key) if key else None
    if isinstance(record_key, dict) and "payload" in record_key:
        record_key = record_key["payload"]
    if op in {"c", "u", "d", "r"} and (not isinstance(record_key, dict) or not record_key):
        raise ValueError("A complete row-key object is required.")
    for image_name in ("before", "after"):
        image = envelope.get(image_name)
        if isinstance(image, dict) and record_key and not set(record_key) <= set(image):
            raise ValueError("Row image does not contain every key column.")
    schema = obj.get("schema")
    if isinstance(schema, dict):
        for definition in schema.get("fields", []):
            image = envelope.get(definition.get("field"))
            if isinstance(image, dict) and isinstance(definition.get("fields"), list):
                required = {field["field"] for field in definition["fields"] if "field" in field}
                if not required <= set(image):
                    raise ValueError(
                        "Row image is missing columns declared by its converter schema."
                    )
    timestamp = source.get("ts_ms")
    if timestamp is None:
        timestamp = envelope.get("ts_ms")
    if timestamp is None:
        raise ValueError("Missing event timestamp.")
    if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)) or timestamp < 0:
        raise ValueError("Invalid event timestamp.")
    # Include target-independent flow identity and immutable queue coordinates.
    capture_id = digest([flow["lineage_id"], flow["queue_incarnation"], topic, partition, offset])
    origin = None
    if (
        op in {"c", "u", "d"}
        and isinstance(source.get("file"), str)
        and source["file"]
        and type(source.get("pos")) is int
        and source["pos"] >= 0
        and type(source.get("row")) is int
        and source["row"] >= 0
    ):
        origin = [
            flow["lineage_id"],
            flow["source_identity"],
            source["file"],
            source["pos"],
            source["row"],
            source.get("gtid"),
            op,
            source["db"],
            source["table"],
        ]
    if op in {"c", "u", "d"} and origin is None:
        raise ValueError("Source-event coordinates are incomplete; no acknowledgement sent.")
    event_id = digest(origin) if origin else capture_id
    return {
        "event_id": event_id,
        "capture_fingerprint": capture_id,
        "key_hash": row_hash(record_key, flow, fingerprint_key) if record_key is not None else None,
        "before_fingerprint": row_hash(envelope.get("before"), flow, fingerprint_key)
        if envelope.get("before") is not None
        else None,
        "after_fingerprint": row_hash(envelope.get("after"), flow, fingerprint_key)
        if envelope.get("after") is not None
        else None,
        "schema_document": obj.get("schema"),
        "schema_fingerprint": digest(obj["schema"]) if obj.get("schema") else None,
        "flow_id": flow["id"],
        "source_schema": source["db"],
        "source_table": source["table"],
        "operation": OPS[op],
        "event_time": datetime.fromtimestamp(timestamp / 1000, timezone.utc).replace(tzinfo=None),
        "record_key": record_key,
        "before_image": envelope.get("before"),
        "after_image": envelope.get("after"),
        "source_metadata": source,
        "topic": topic,
        "partition_id": partition,
        "queue_offset": offset,
    }


JSON_FIELDS = {"record_key", "before_image", "after_image", "source_metadata", "schema_document"}


def payload_digest(event):
    # Preserve the existing ledger digest so previously stored DML replays validate.
    content = {
        key: event[key]
        for key in ("operation", "record_key", "before_image", "after_image", "event_time")
    }
    content["event_time"] = str(content["event_time"])
    return digest(content)


def write_batch(db, flow, events, retries=3):
    """Commit ledger and payload once. Duplicate/conflicting identities never overwrite history."""
    import time

    from .retention import check_ingestion
    from .validation import quote

    if not events:
        return 0
    if len(events) > 1000:
        raise ValueError("Target batch exceeds the 1000-event safety limit.")
    unique = {}
    for event in events:
        if event["flow_id"] != flow["id"]:
            raise ValueError("Batch contains a different flow.")
        identity = event["event_id"]
        fingerprint = payload_digest(event)
        if identity in unique and unique[identity][1] != fingerprint:
            raise ValueError("Source-event identity conflict within batch.")
        unique.setdefault(identity, (event, fingerprint))
    table = f"{quote(flow['target_schema'])}.{quote(flow['target_table'])}"
    guard = f"{quote(flow['target_schema'])}.audit_event_dedup"
    previous_autocommit = db.autocommit
    for attempt in range(retries):
        cur = None
        try:
            db.autocommit = False
            check_ingestion(db, flow, events)
            cur = db.cursor()
            identities = sorted(unique)
            cur.execute(
                f"SELECT event_id,payload_fingerprint FROM {guard} WHERE event_id IN ("
                + ",".join(["%s"] * len(identities))
                + ")",
                identities,
            )
            existing = dict(cur.fetchall())
            fresh = []
            for identity, (event, fingerprint) in unique.items():
                if identity in existing:
                    if existing[identity] != fingerprint:
                        raise ValueError(
                            "Source-event identity conflict. History was not overwritten."
                        )
                else:
                    fresh.append(event)
            if fresh:
                # Stable ledger ordering reduces deadlocks between replaying batches.
                cur.executemany(
                    f"INSERT INTO {guard} (event_id,payload_fingerprint,event_time) VALUES (%s,%s,%s)",
                    [
                        (e["event_id"], unique[e["event_id"]][1], e["event_time"])
                        for e in sorted(fresh, key=lambda e: e["event_id"])
                    ],
                )
                columns = list(fresh[0])
                if any(set(e) != set(columns) for e in fresh):
                    raise ValueError("Inconsistent event fields in batch.")
                sql = (
                    f"INSERT INTO {table} ("
                    + ",".join(quote(c) for c in columns)
                    + ") VALUES ("
                    + ",".join(["%s"] * len(columns))
                    + ")"
                )
                cur.executemany(
                    sql,
                    [
                        [
                            canonical(e[c]) if c in JSON_FIELDS and e[c] is not None else e[c]
                            for c in columns
                        ]
                        for e in fresh
                    ],
                )
            db.commit()
            return len(fresh)
        except Exception as error:
            db.rollback()
            if getattr(error, "errno", None) not in {1062, 1205, 1213} or attempt + 1 >= retries:
                raise
            time.sleep(0.05 * (attempt + 1))
        finally:
            if cur is not None:
                cur.close()
            db.autocommit = previous_autocommit


def write_event(db, flow, event):
    """Single-event entry point retained for storage verification and callers."""
    return write_batch(db, flow, [event])
