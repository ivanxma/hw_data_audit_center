"""Durable, one-time requests to reposition a connector without resetting on restart."""

import json
import uuid

from .repository import named_lock, query
from .validation import quote

PENDING = ("pending", "stopped", "applied")


def initialize(db, schema):
    query(
        db,
        f"""CREATE TABLE IF NOT EXISTS {quote(schema)}.audit_capture_requests (
        request_id CHAR(32) PRIMARY KEY, flow_id BIGINT NOT NULL,
        connector_name VARCHAR(150) NOT NULL, selection JSON NOT NULL,
        planned_offset JSON NOT NULL, previous_offsets JSON,
        state VARCHAR(24) NOT NULL, error_code VARCHAR(64), actor VARCHAR(100) NOT NULL,
        created_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
        updated_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
        INDEX pending_capture(flow_id,state,created_at)
    ) ENGINE=InnoDB""",
    )


def latest(db, flow):
    return query(
        db,
        f"SELECT request_id,selection,state,error_code,created_at,updated_at "
        f"FROM {quote(flow['target_schema'])}.audit_capture_requests WHERE flow_id=%s "
        "ORDER BY created_at DESC LIMIT 1",
        (flow["id"],),
        one=True,
    )


def enqueue(db, flow, selection, offset, actor, *, preview=False):
    initialize(db, flow["target_schema"])
    with named_lock(db, "capture", flow["id"], flow["connector_name"]):
        table = quote(flow["target_schema"]) + ".audit_capture_requests"
        if query(
            db,
            f"SELECT request_id FROM {table} WHERE flow_id=%s "
            "AND state IN ('pending','stopped','applied') LIMIT 1",
            (flow["id"],),
            one=True,
        ):
            raise ValueError("A capture position change is already in progress.")
        request_id = uuid.uuid4().hex
        query(
            db,
            f"INSERT INTO {table} (request_id,flow_id,connector_name,selection,planned_offset,state,actor) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (
                request_id,
                flow["id"],
                flow["connector_name"],
                json.dumps(selection),
                json.dumps(offset),
                "prepared" if preview else "pending",
                actor,
            ),
        )
        return request_id


def confirm(db, flow, request_id, actor, revalidate):
    table = quote(flow["target_schema"]) + ".audit_capture_requests"
    with named_lock(db, "capture", flow["id"], flow["connector_name"]):
        row = query(
            db,
            f"SELECT * FROM {table} WHERE request_id=%s AND flow_id=%s "
            "AND connector_name=%s AND actor=%s AND state='prepared' "
            "AND created_at >= UTC_TIMESTAMP() - INTERVAL 5 MINUTE",
            (request_id, flow["id"], flow["connector_name"], actor),
            one=True,
        )
        if not row:
            raise ValueError(
                "Capture preview expired or was already submitted. Preview the position again."
            )
        if pending(db, flow["id"], flow["connector_name"], flow["target_schema"]):
            raise ValueError("Another capture-position operation is already in progress.")
        plan = row["planned_offset"]
        plan = json.loads(plan) if isinstance(plan, str) else plan
        revalidate(plan)
        advance(db, flow["target_schema"], request_id, "pending")
        return row


def advance(db, schema, request_id, state, *, previous=None, error=None):
    table = quote(schema) + ".audit_capture_requests"
    if previous is not None:
        query(
            db,
            f"UPDATE {table} SET state=%s,previous_offsets=%s,error_code=%s WHERE request_id=%s",
            (state, json.dumps(previous), error, request_id),
        )
    else:
        query(
            db,
            f"UPDATE {table} SET state=%s,error_code=%s WHERE request_id=%s",
            (state, error, request_id),
        )


def pending(db, flow_id, connector_name, schema):
    return query(
        db,
        f"SELECT * FROM {quote(schema)}.audit_capture_requests "
        "WHERE flow_id=%s AND connector_name=%s AND state IN ('pending','stopped','applied') "
        "ORDER BY created_at LIMIT 1",
        (flow_id, connector_name),
        one=True,
    )
