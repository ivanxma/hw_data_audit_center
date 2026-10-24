"""Actionable process failures built from safe codes, never raw exception text."""

import json
import re
import time
from datetime import datetime, timezone

MARKER = "AUDIT_PROCESS_ERROR "
STAGES = {
    "configuration": "Load workload configuration",
    "secrets": "Resolve Vault references",
    "target_connect": "Connect to target database",
    "queue_connect": "Connect to messaging",
    "queue_validation": "Validate data/schema topics",
    "checkpoint_recovery": "Recover queue checkpoint",
    "queue_poll": "Read queue messages",
    "decode": "Decode change envelope",
    "target_write": "Commit row history",
    "schema_write": "Commit schema history",
    "checkpoint_write": "Save delivery checkpoint",
    "queue_ack": "Acknowledge queue offsets",
    "connector_task": "Run Debezium connector task",
    "source_connect": "Connect to source database",
    "unknown": "Workload execution",
}
MESSAGES = {
    "DB_UNREACHABLE": (
        "Database connection unavailable",
        "Verify the configured database is ACTIVE, then check endpoint/port, DNS, routes and security rules. Resume the workload after connectivity is restored; preserve its checkpoints.",
    ),
    "DB_CONNECTION_LOST": (
        "Database connection lost",
        "Check for a DB stop/restart or network interruption. Restore connectivity, then resume using existing checkpoints.",
    ),
    "DB_ACCESS_DENIED": (
        "Database access denied",
        "Review the configured database account, Vault password reference and required grants. Do not put credentials in logs.",
    ),
    "DB_SCHEMA_MISSING": (
        "Configured database is missing",
        "Verify the selected schema exists on the intended database before resuming.",
    ),
    "DB_TABLE_MISSING": (
        "Required target table is missing",
        "Review target preparation and table permissions. Do not reset offsets or recreate audit history to bypass the error.",
    ),
    "DB_LAYOUT_ERROR": (
        "Database rejected the query or layout",
        "Review image/schema compatibility and the MySQL code. Apply a reviewed target migration before retrying.",
    ),
    "DB_LOCK_WAIT": (
        "Target database lock wait or deadlock",
        "Review concurrent maintenance and long transactions. Retry from existing checkpoints after resolving contention.",
    ),
    "DB_STORAGE_FULL": (
        "Database storage capacity exhausted",
        "Review target storage capacity and approved retention before resuming.",
    ),
    "DB_VALUE_ERROR": (
        "Target value or packet exceeds a database limit",
        "Review target column types, packet limits and connector type representation. Do not skip the affected event.",
    ),
    "OCI_ACCESS_DENIED": (
        "OCI workload access denied or resource unavailable",
        "Review this container's resource-principal membership and same-compartment permissions for the selected service and Vault secret metadata/bundles. VM access alone does not grant container access.",
    ),
    "OCI_UNAVAILABLE": (
        "OCI service is unavailable or throttling requests",
        "Review service readiness and quotas, then retry with existing checkpoints after access is restored.",
    ),
    "DB_ERROR": (
        "Database operation failed",
        "Review target/source database availability, grants and the reported MySQL code. An older image may not have logged the numeric code; update it for stage-specific diagnostics.",
    ),
    "CHECKPOINT_OUT_OF_RANGE": (
        "Queue checkpoint is outside retained history",
        "Stop for coverage review. Do not silently seek earliest/latest or clear the saved checkpoint.",
    ),
    "ROW_IMAGE_MISSING": (
        "Required row image is missing",
        "Verify source ROW/FULL and complete JSON logging before the transaction. Older incomplete binlogs cannot be repaired by changing settings now.",
    ),
    "EVENT_EXPIRED": (
        "Event is older than the retained-history cutoff",
        "Review late delivery/replay and the durable cleanup cutoff. Do not remove retention state to force ingestion.",
    ),
    "SOURCE_UNREACHABLE": (
        "Debezium could not reconnect to the source",
        "Verify the source DB is ACTIVE and reachable with its configured account. Preserve offsets and schema history while restoring access.",
    ),
    "PROCESS_FAILED": (
        "Workload operation failed",
        "Review the failed step, workload release, permitted service access and readiness. Existing history and checkpoints must be preserved.",
    ),
}
MYSQL_CODES = {
    **dict.fromkeys((2002, 2003, 2005), "DB_UNREACHABLE"),
    **dict.fromkeys((2006, 2013, 2055), "DB_CONNECTION_LOST"),
    **dict.fromkeys((1044, 1045, 1142, 1143, 1227), "DB_ACCESS_DENIED"),
    1049: "DB_SCHEMA_MISSING",
    1146: "DB_TABLE_MISSING",
    **dict.fromkeys((1054, 1064), "DB_LAYOUT_ERROR"),
    **dict.fromkeys((1205, 1213), "DB_LOCK_WAIT"),
    1114: "DB_STORAGE_FULL",
    **dict.fromkeys((1153, 1264, 1292, 1406), "DB_VALUE_ERROR"),
}
SAFE_ERRORS = {
    "Queue checkpoint is outside retained history. Capture coverage must be reviewed.": "CHECKPOINT_OUT_OF_RANGE",
    "No recoverable queue checkpoint. Review coverage before resetting this group.": "CHECKPOINT_OUT_OF_RANGE",
    "Missing BEFORE image; offset will not be committed.": "ROW_IMAGE_MISSING",
    "Missing AFTER image; offset will not be committed.": "ROW_IMAGE_MISSING",
    "Event precedes the retained-history cutoff. Consumption paused; review late data or replay.": "EVENT_EXPIRED",
}


def record(code, stage, *, errno=None, checked_at=None):
    code = code if code in MESSAGES else "PROCESS_FAILED"
    stage = stage if stage in STAGES else "unknown"
    message, guidance = MESSAGES[code]
    stamp = time.time() if checked_at is None else checked_at
    result = dict(
        code=code,
        stage=stage,
        step=STAGES[stage],
        message=message,
        guidance=guidance,
        checked_at=stamp,
    )
    if type(errno) is int and 0 < errno <= 65535:
        result["mysql_errno"] = errno
    return result


def describe(error, stage="unknown", *, checked_at=None):
    errno = getattr(error, "errno", None)
    errno = errno if type(errno) is int else None
    code = MYSQL_CODES.get(errno)
    if not code and type(getattr(error, "status", None)) is int:
        status = error.status
        if status in {401, 403, 404}:
            code = "OCI_ACCESS_DENIED"
        elif status == 429 or status >= 500:
            code = "OCI_UNAVAILABLE"
    if not code:
        code = SAFE_ERRORS.get(str(error))
    if not code and type(error).__name__ in {
        "DatabaseError",
        "OperationalError",
        "InterfaceError",
        "ProgrammingError",
        "IntegrityError",
    }:
        code = "DB_ERROR"
    return record(code, stage, errno=errno, checked_at=checked_at)


def line_time(line):
    match = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.\d+)?Z", line)
    try:
        return (
            datetime.fromisoformat(match[1]).replace(tzinfo=timezone.utc).timestamp()
            if match
            else None
        )
    except ValueError:
        return None


def from_logs(text, role):
    """Parse the latest failure and reconstruct all visible text from a whitelist."""
    for line in reversed(text.splitlines()):
        if "AUDIT_CONNECTOR_HEALTH " in line and role == "Debezium":
            try:
                data = json.loads(line.split("AUDIT_CONNECTOR_HEALTH ", 1)[1])
                failure = data.get("failure")
                stamp = data.get("checked_at", 0)
                if (
                    isinstance(failure, dict)
                    and type(stamp) in (float, int)
                    and 0 <= stamp <= time.time() + 300
                ):
                    return record(failure.get("code"), "connector_task", checked_at=stamp)
            except (ValueError, TypeError, AttributeError):
                pass
        if MARKER in line:
            try:
                data = json.loads(line.split(MARKER, 1)[1])
                stamp = data.get("checked_at")
                if type(stamp) not in (float, int) or not 0 <= stamp <= time.time() + 300:
                    continue
                return record(
                    data.get("code"),
                    data.get("stage"),
                    errno=data.get("mysql_errno"),
                    checked_at=stamp,
                )
            except (ValueError, TypeError, AttributeError):
                continue
        if role == "History writer" and "history_writer_stopped error_type=" in line:
            code = (
                "DB_ERROR"
                if re.search(
                    r"error_type=(?:DatabaseError|OperationalError|InterfaceError|ProgrammingError)\b",
                    line,
                )
                else "PROCESS_FAILED"
            )
            return record(code, "unknown", checked_at=line_time(line) or 0)
        if role == "Debezium" and "Failed to restore connection" in line:
            return record("SOURCE_UNREACHABLE", "source_connect", checked_at=line_time(line) or 0)
    return None
