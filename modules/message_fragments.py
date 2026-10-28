"""Durable fragment custody, checked reassembly, and compact replay receipts."""

import base64
import hashlib
import json
import math
import re
import struct

from .fingerprints import digest
from .message_policy import CHUNK_BYTES, DEFAULT_EVENT_BYTES, MAX_EVENT_BYTES
from .repository import query
from .validation import quote


def initialize(db, schema):
    ns = quote(schema)
    query(
        db,
        f"""CREATE TABLE IF NOT EXISTS {ns}.audit_message_assemblies (
        group_id CHAR(64) CHARACTER SET ascii PRIMARY KEY,
        flow_id BIGINT NOT NULL, queue_hash CHAR(64) CHARACTER SET ascii NOT NULL,
        topic VARCHAR(249) NOT NULL, partition_id INT NOT NULL,
        message_id CHAR(64) CHARACTER SET ascii NOT NULL,
        checksum CHAR(64) CHARACTER SET ascii NOT NULL,
        chunk_count INT NOT NULL, total_bytes BIGINT NOT NULL,
        state VARCHAR(16) NOT NULL, first_offset BIGINT NOT NULL, last_offset BIGINT NOT NULL,
        event_time DATETIME(6), created_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
        updated_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
        INDEX flow_pending(flow_id,state)
    ) ENGINE=InnoDB""",
    )
    query(
        db,
        f"""CREATE TABLE IF NOT EXISTS {ns}.audit_message_fragments (
        group_id CHAR(64) CHARACTER SET ascii NOT NULL, chunk_index INT NOT NULL,
        chunk_hash CHAR(64) CHARACTER SET ascii NOT NULL, payload MEDIUMBLOB,
        PRIMARY KEY(group_id,chunk_index)
    ) ENGINE=InnoDB""",
    )


def frame(raw, maximum=DEFAULT_EVENT_BYTES):
    if raw is None:
        return None
    prefix = raw.encode() if isinstance(raw, str) else raw
    if not prefix.lstrip().startswith(b'{"_audit_fragment_v1"'):
        return None
    obj = json.loads(raw)
    if set(obj) != {"_audit_fragment_v1"} or not isinstance(obj["_audit_fragment_v1"], dict):
        raise ValueError("Invalid message fragment envelope.")
    item = obj["_audit_fragment_v1"]
    if set(item) != {"id", "index", "count", "total_bytes", "sha256", "chunk_sha256", "data"}:
        raise ValueError("Invalid message fragment fields.")
    for field in ("id", "sha256", "chunk_sha256"):
        if not isinstance(item[field], str) or not re.fullmatch("[0-9a-f]{64}", item[field]):
            raise ValueError("Invalid message fragment digest.")
    for field in ("index", "count", "total_bytes"):
        if type(item[field]) is not int:
            raise ValueError("Invalid message fragment coordinates.")
    if not 8 <= item["total_bytes"] <= min(maximum, MAX_EVENT_BYTES):
        raise ValueError("Fragmented event exceeds the configured logical event limit.")
    if (
        item["count"] != math.ceil(item["total_bytes"] / CHUNK_BYTES)
        or not 0 <= item["index"] < item["count"]
    ):
        raise ValueError("Invalid message fragment count or index.")
    if not isinstance(item["data"], str) or len(item["data"]) > (CHUNK_BYTES + 2) // 3 * 4:
        raise ValueError("Invalid message fragment payload size.")
    try:
        payload = base64.b64decode(item["data"], validate=True)
    except (ValueError, TypeError):
        raise ValueError("Invalid message fragment encoding.") from None
    expected = min(CHUNK_BYTES, item["total_bytes"] - item["index"] * CHUNK_BYTES)
    if len(payload) != expected or hashlib.sha256(payload).hexdigest() != item["chunk_sha256"]:
        raise ValueError("Message fragment length or checksum mismatch.")
    return {**item, "bytes": payload}


def identity(flow, topic, item):
    expected = hashlib.sha256((topic + "\0" + item["sha256"]).encode()).hexdigest()
    if item["id"] != expected:
        raise ValueError("Message fragment identity does not match its stream and checksum.")
    return digest([flow["id"], flow["queue_incarnation"], topic, item["id"]])


def accept(db, flow, topic, partition, offset, item, already_assembled=()):
    """Commit a part before allowing its delivery checkpoint to advance."""
    group = identity(flow, topic, item)
    ns = quote(flow["target_schema"])
    heads, parts = ns + ".audit_message_assemblies", ns + ".audit_message_fragments"
    old = db.autocommit
    try:
        db.autocommit = False
        head = query(db, f"SELECT * FROM {heads} WHERE group_id=%s FOR UPDATE", (group,), one=True)
        if head:
            expected = (
                flow["id"],
                digest(flow["queue_incarnation"]),
                topic,
                partition,
                item["id"],
                item["sha256"],
                item["count"],
                item["total_bytes"],
            )
            actual = tuple(
                head[k]
                for k in [
                    "flow_id",
                    "queue_hash",
                    "topic",
                    "partition_id",
                    "message_id",
                    "checksum",
                    "chunk_count",
                    "total_bytes",
                ]
            )
            if actual != expected:
                raise ValueError("Message fragment metadata conflicts with durable custody.")
        else:
            reserved = query(
                db,
                f"SELECT COALESCE(SUM(total_bytes),0) AS n FROM {heads} WHERE flow_id=%s AND state<>'committed'",
                (flow["id"],),
                one=True,
            )["n"]
            limit = flow.get("message_policy", {}).get("max_pending_bytes", 128 * 1024 * 1024)
            if reserved + item["total_bytes"] > limit:
                raise ValueError(
                    "Pending fragmented events exceed the configured custody budget; delivery paused."
                )
            query(
                db,
                f"""INSERT INTO {heads}
                (group_id,flow_id,queue_hash,topic,partition_id,message_id,checksum,chunk_count,total_bytes,state,first_offset,last_offset)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'collecting',%s,%s)""",
                (
                    group,
                    flow["id"],
                    digest(flow["queue_incarnation"]),
                    topic,
                    partition,
                    item["id"],
                    item["sha256"],
                    item["count"],
                    item["total_bytes"],
                    offset,
                    offset,
                ),
            )
            head = {"state": "collecting", "event_time": None}
        previous = query(
            db,
            f"SELECT chunk_hash FROM {parts} WHERE group_id=%s AND chunk_index=%s",
            (group, item["index"]),
            one=True,
        )
        if previous and previous["chunk_hash"] != item["chunk_sha256"]:
            raise ValueError("Message fragment conflicts with an earlier copy.")
        if head["state"] == "committed":
            if not previous:
                raise ValueError("Committed fragment receipt is incomplete.")
            if head.get("event_time"):
                from .retention import check_ingestion

                check_ingestion(db, flow, [{"event_time": head["event_time"]}])
            db.commit()
            return None
        if not previous:
            query(
                db,
                f"INSERT INTO {parts}(group_id,chunk_index,chunk_hash,payload) VALUES (%s,%s,%s,%s)",
                (group, item["index"], item["chunk_sha256"], item["bytes"]),
            )
        query(
            db,
            f"UPDATE {heads} SET first_offset=LEAST(first_offset,%s),last_offset=GREATEST(last_offset,%s) WHERE group_id=%s",
            (offset, offset, group),
        )
        if head["state"] == "ready" and group in already_assembled:
            db.commit()
            return None
        count = query(
            db,
            f"SELECT COUNT(*) AS n,COALESCE(SUM(OCTET_LENGTH(payload)),0) AS bytes FROM {parts} WHERE group_id=%s",
            (group,),
            one=True,
        )
        if count["n"] != item["count"]:
            db.commit()
            return None
        chunks = query(
            db,
            f"SELECT chunk_index,payload FROM {parts} WHERE group_id=%s ORDER BY chunk_index",
            (group,),
        )
        if count["bytes"] != item["total_bytes"] or [c["chunk_index"] for c in chunks] != list(
            range(item["count"])
        ):
            raise ValueError("Complete fragment set has inconsistent coordinates.")
        packed = b"".join(bytes(c["payload"]) for c in chunks)
        if hashlib.sha256(packed).hexdigest() != item["sha256"]:
            raise ValueError("Reassembled event checksum mismatch.")
        key_size = struct.unpack(">q", packed[:8])[0]
        if key_size < -1 or key_size > len(packed) - 8:
            raise ValueError("Invalid reassembled key length.")
        key = None if key_size == -1 else packed[8 : 8 + key_size]
        value = packed[8 + max(0, key_size) :]
        # Validate UTF-8 without changing bytes or numeric/schema representations.
        value.decode("utf-8")
        if key is not None:
            key.decode("utf-8")
        query(db, f"UPDATE {heads} SET state='ready' WHERE group_id=%s", (group,))
        db.commit()
        return {"group_id": group, "message_id": item["id"], "key": key, "value": value}
    except Exception:
        db.rollback()
        raise
    finally:
        db.autocommit = old


def note_event_time(db, flow, group, timestamp):
    query(
        db,
        f"UPDATE {quote(flow['target_schema'])}.audit_message_assemblies SET event_time=%s WHERE group_id=%s",
        (timestamp, group),
    )


def commit_groups(db, flow, groups):
    """Only after payload/schema writes AND the completion checkpoint commit."""
    if not groups:
        return
    ns = quote(flow["target_schema"])
    old = db.autocommit
    try:
        db.autocommit = False
        for group in sorted(set(groups)):
            head = query(
                db,
                f"SELECT state FROM {ns}.audit_message_assemblies WHERE group_id=%s FOR UPDATE",
                (group,),
                one=True,
            )
            if not head or head["state"] not in {"ready", "committed"}:
                raise ValueError("Cannot commit an incomplete fragmented event.")
            query(
                db,
                f"UPDATE {ns}.audit_message_fragments SET payload=NULL WHERE group_id=%s",
                (group,),
            )
            query(
                db,
                f"UPDATE {ns}.audit_message_assemblies SET state='committed' WHERE group_id=%s",
                (group,),
            )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.autocommit = old


def recover_committed(db, flow):
    ns = quote(flow["target_schema"])
    rows = query(
        db,
        f"""SELECT a.group_id FROM {ns}.audit_message_assemblies a
        JOIN {ns}.audit_delivery_checkpoints c ON c.flow_id=a.flow_id AND c.topic=a.topic AND c.partition_id=a.partition_id
        WHERE a.flow_id=%s AND a.queue_hash=%s AND a.state='ready' AND c.next_offset>a.last_offset""",
        (flow["id"], digest(flow["queue_incarnation"])),
    )
    commit_groups(db, flow, [r["group_id"] for r in rows])
