"""Small, non-secret connector-task health records for OCI container logs."""

import json
import time

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
    return {"state": state, "tasks": len(tasks), "checked_at": time.time() if now is None else now}


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
