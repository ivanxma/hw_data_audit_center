"""Run the supported Kafka Connect offset API on loopback, once per audited request."""

import json
import time

import requests

from .capture_requests import advance, pending
from .capture_start import validate_gtid_boundary
from .mysql_util import open_connection
from .repository import named_lock, query


def open_db(target, secret):
    db, _ = open_connection(
        {"mode": "tcp", **target, "read_timeout": 10, "write_timeout": 10},
        target["username"],
        secret(target["secret_ocid"]),
    )
    return db


class ConnectOffsets:
    def __init__(self, name):
        self.url = "http://127.0.0.1:8083/connectors/" + name

    def call(self, method, suffix, body=None):
        response = requests.request(method, self.url + suffix, json=body, timeout=10)
        response.raise_for_status()
        return response.json() if response.content else None

    def stop(self):
        self.call("PUT", "/stop")
        for _ in range(30):
            status = self.call("GET", "/status")
            if status.get("connector", {}).get("state") == "STOPPED" and not status.get("tasks"):
                return
            time.sleep(1)
        raise ValueError("CONNECTOR_STOP_TIMEOUT")

    def offsets(self):
        return self.call("GET", "/offsets")

    def patch(self, payload):
        self.call("PATCH", "/offsets", payload)

    def resume(self):
        self.call("PUT", "/resume")


def matches(document, expected):
    records = document.get("offsets", [])
    if len(records) != 1 or records[0].get("partition") != expected["partition"]:
        return False
    actual = records[0].get("offset") or {}
    return all(actual.get(key) == value for key, value in expected["offset"].items())


def validate_checkpoint_coverage(source, plan, checkpoint):
    """A durable decoded checkpoint bounds the schema history known to this connector."""
    records = (checkpoint or {}).get("offsets", [])
    if len(records) != 1 or records[0].get("partition") != plan["partition"]:
        raise ValueError("SCHEMA_HISTORY_COVERAGE_UNCONFIRMED")
    captured = (records[0].get("offset") or {}).get("gtids")
    if not captured or not query(
        source,
        "SELECT GTID_SUBSET(%s,%s) AS covered",
        (plan["offset"]["gtids"], captured),
        one=True,
    )["covered"]:
        raise ValueError("SCHEMA_HISTORY_COVERAGE_UNCONFIRMED")
    validate_gtid_boundary(source, plan["offset"]["gtids"], "")


def apply_request(db, schema, row, api, source_check):
    """A crash after PATCH is reconciled while STOPPED; an applied request never patches again."""
    plan = row["planned_offset"]
    plan = json.loads(plan) if isinstance(plan, str) else plan
    rid, state = row["request_id"], row["state"]
    if state == "applied" or plan.get("action") == "resume":
        api.resume()
        advance(db, schema, rid, "complete")
        return
    api.stop()
    current = api.offsets()
    previous = row.get("previous_offsets")
    previous = json.loads(previous) if isinstance(previous, str) else previous
    if state == "pending":
        for record in current.get("offsets", []):
            if (record.get("offset") or {}).get("snapshot") in (True, "true", "INITIAL"):
                raise ValueError("INITIAL_SNAPSHOT_IN_PROGRESS")
        advance(db, schema, rid, "stopped", previous=current)
        previous = current
    elif not matches(current, plan):
        if current != previous:
            raise ValueError("AMBIGUOUS_POSITION_STATE")
    source_check(plan, previous)
    if not matches(current, plan):
        api.patch({"offsets": [plan]})
        if not matches(api.offsets(), plan):
            raise ValueError("OFFSET_VERIFICATION_FAILED")
    advance(db, schema, rid, "applied")
    api.resume()
    advance(db, schema, rid, "complete")


def poll(config, secret):
    control = config.get("capture_control")
    if not control:
        return None
    target = control["target"]
    db = open_db(target, secret)
    try:
        with named_lock(db, "capture", control["flow_id"], config["connector_name"]):
            row = pending(db, control["flow_id"], config["connector_name"], target["database"])
            if not row:
                return None

            def check(plan, checkpoint):
                source = open_db(control["source"], secret)
                try:
                    validate_checkpoint_coverage(source, plan, checkpoint)
                finally:
                    source.close()

            try:
                apply_request(
                    db, target["database"], row, ConnectOffsets(config["connector_name"]), check
                )
                return {"state": "complete", "request_id": row["request_id"]}
            except Exception as error:
                code = (
                    str(error)
                    if type(error) is ValueError
                    and str(error)
                    in {
                        "CONNECTOR_STOP_TIMEOUT",
                        "INITIAL_SNAPSHOT_IN_PROGRESS",
                        "OFFSET_VERIFICATION_FAILED",
                        "AMBIGUOUS_POSITION_STATE",
                        "SCHEMA_HISTORY_COVERAGE_UNCONFIRMED",
                    }
                    else "POSITION_CHANGE_FAILED"
                )
                # No automatic rollback or retry of an ambiguous position change.
                advance(db, target["database"], row["request_id"], "failed", error=code)
                return {"state": "failed", "request_id": row["request_id"], "error_code": code}
    finally:
        db.close()
