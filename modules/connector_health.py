"""Small, non-secret connector-task health records for OCI container logs."""

import json
import time

from .process_errors import record

MARKER = "AUDIT_CONNECTOR_HEALTH "


def summarize(document, now=None):
    connector = document.get("connector", {}).get("state")
    tasks = document.get("tasks", [])
    states = [task.get("state") for task in tasks]
    state = (
        "FAILED"
        if "FAILED" in [connector, *states]
        else (
            "STOPPED"
            if connector in {"STOPPED", "PAUSED"}
            else "RUNNING"
            if connector == "RUNNING" and states and all(s == "RUNNING" for s in states)
            else "STARTING"
        )
    )
    checked_at = time.time() if now is None else now
    result = {"state": state, "tasks": len(tasks), "checked_at": checked_at}
    metrics = document.get("metrics")
    if isinstance(metrics, dict):
        result["metrics"] = {
            k: metrics[k]
            for k in (
                "put_requests",
                "put_records",
                "put_wire_bytes",
                "throttled_records",
                "published_logical_events",
            )
            if type(metrics.get(k)) is int and metrics[k] >= 0
        }
    if state == "FAILED":
        trace = "\n".join(str(task.get("trace", "")) for task in tasks)
        code = (
            "DB_ACCESS_DENIED"
            if "Access denied" in trace
            else "DB_UNREACHABLE"
            if any(
                text in trace
                for text in (
                    "Communications link failure",
                    "Connection refused",
                    "connect timed out",
                )
            )
            else "DB_SCHEMA_MISSING"
            if "Unknown database" in trace
            else "PROCESS_FAILED"
        )
        result["failure"] = record(code, "connector_task", checked_at=checked_at)
    return result


def from_logs(text, now=None):
    now = time.time() if now is None else now
    for line in reversed(text.splitlines()):
        if MARKER not in line:
            continue
        try:
            value = json.loads(line.split(MARKER, 1)[1])
            age = now - value["checked_at"]
            if -10 <= age <= 90 and value["state"] in {
                "RUNNING",
                "FAILED",
                "STARTING",
                "UNAVAILABLE",
                "STOPPED",
            }:
                return value["state"]
        except (ValueError, TypeError, KeyError):
            continue
    # Diagnose older images which do not yet emit task health records.
    if "Unable to find class io.debezium.storage.jdbc.history.JdbcSchemaHistory" in text:
        return "MISSING_JDBC_MODULE"
    return "UNAVAILABLE"
