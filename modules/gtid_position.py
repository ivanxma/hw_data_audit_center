"""Bounded, metadata-only GTID lookup. Never read source table rows."""

import re
import time

from .capture_start import gtid_set, validate_gtid_boundary
from .repository import query


def source_range(db):
    state = query(
        db,
        "SELECT @@GLOBAL.gtid_purged AS purged,@@GLOBAL.gtid_executed AS executed",
        one=True,
    )
    return {"purged": state["purged"], "executed": state["executed"]}


def filename(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", value) or value.startswith("-"):
        raise ValueError("Unexpected binary log name.")
    return value


def locate_gtid(db, requested, baseline=None, *, check_schema=False):
    """Find a committed prefix using file headers, then bounded event metadata.

    Initial capture also rejects any SQL query event after the boundary except
    transaction control. This deliberately refuses unverified historical DDL.
    """
    requested = gtid_set(requested)
    limits = source_range(db)
    validate_gtid_boundary(db, requested, "")
    if baseline:
        validate_gtid_boundary(db, requested, baseline["gtids"])
    files = query(db, "SHOW BINARY LOGS")
    if not files:
        raise ValueError("No retained binary logs are available.")
    deadline = time.monotonic() + 30
    headers = {}

    def bounded():
        if time.monotonic() >= deadline:
            raise ValueError("GTID metadata inspection timed out. Choose a later retained boundary.")

    def header(index):
        bounded()
        if index not in headers:
            name = filename(files[index]["Log_name"])
            rows = query(db, "SHOW BINLOG EVENTS IN '" + name + "' LIMIT 8")
            previous = next((r for r in rows if r["Event_type"] == "Previous_gtids"), None)
            if previous is None:
                raise ValueError("The retained binlog has no readable Previous_gtids header.")
            value = previous["Info"].strip()
            headers[index] = (gtid_set(value) if value else "", int(previous["End_log_pos"]))
        return headers[index]

    # Previous_gtids is a cumulative set. Search headers instead of replaying
    # every file since the original schema baseline (which may be years old).
    low, high, start = 0, len(files) - 1, None
    while low <= high:
        middle = (low + high) // 2
        previous, _ = header(middle)
        subset = query(db, "SELECT GTID_SUBSET(%s,%s) AS covered", (previous, requested), one=True)
        if subset["covered"] == 1:
            start, low = middle, middle + 1
        else:
            high = middle - 1
    if start is None:
        raise ValueError("The requested GTID predates the retained binlog range.")
    executed, position = header(start)
    boundary = None
    transaction = None
    used, scanned_bytes = 0, 0
    for index in range(start, len(files)):
        bounded()
        name = filename(files[index]["Log_name"])
        if index != start:
            previous, position = header(index)
            if executed != previous or transaction:
                raise ValueError("Binlog transaction continuity could not be verified.")
        if executed == requested and boundary is None:
            boundary = {"file": name, "pos": position, "gtids": requested}
        if boundary and not check_schema:
            break
        if index - start >= 128:
            raise ValueError("GTID inspection reached its 128 relevant-file limit. Choose a later boundary.")
        end = int(files[index]["File_size"])
        while position < end:
            bounded()
            rows = query(
                db, "SHOW BINLOG EVENTS IN '" + name + "' FROM " + str(position) + " LIMIT 256"
            )
            if not rows:
                raise ValueError("Retained binlog metadata is incomplete or was purged during inspection.")
            for row in rows:
                pos, following = int(row["Pos"]), int(row["End_log_pos"])
                if pos >= end:
                    break
                used += 1
                scanned_bytes += len(str(row.get("Info", "")))
                if used > 100000 or scanned_bytes > 8 * 1024 * 1024:
                    raise ValueError("GTID metadata inspection exceeded its bounded size. Choose a later boundary.")
                if pos < position or following <= pos or following > end:
                    raise ValueError("Binlog metadata does not contain a complete transaction boundary.")
                kind, info = row["Event_type"], row.get("Info", "")
                sql = re.sub(r"^use (?:`(?:``|[^`])*`|[A-Za-z0-9_$]+);\s*", "", info.strip(), flags=re.I)
                if kind == "Gtid":
                    if transaction:
                        raise ValueError("Binlog transaction completion could not be verified.")
                    match = re.fullmatch(r"SET @@SESSION.GTID_NEXT\s*=\s*'([^']+)'", info.strip())
                    if not match:
                        raise ValueError("Unsupported GTID event metadata.")
                    transaction = gtid_set(match[1])
                elif kind == "Xid" or (kind == "Query" and sql.upper() == "COMMIT"):
                    if not transaction:
                        raise ValueError("Binlog commit has no verified GTID.")
                    executed = gtid_set(executed + ("," if executed else "") + transaction)
                    transaction = None
                    if executed == requested and boundary is None:
                        boundary = {"file": name, "pos": following, "gtids": requested}
                elif kind == "Query" and sql.upper() != "BEGIN":
                    if boundary and check_schema:
                        raise ValueError(
                            "Schema-changing or unsupported SQL exists after this GTID. "
                            "Initial GTID capture needs matching historical schema; choose a later GTID or End."
                        )
                    # An autocommit DDL Query event also completes its GTID.
                    if not transaction:
                        raise ValueError("Binlog SQL has no verified GTID.")
                    executed = gtid_set(executed + ("," if executed else "") + transaction)
                    transaction = None
                    if executed == requested:
                        boundary = {"file": name, "pos": following, "gtids": requested}
                elif kind in {"Anonymous_Gtid", "Transaction_payload", "Incident"}:
                    raise ValueError("Unsupported binlog event prevents verified GTID positioning.")
                position = following
                if boundary and not check_schema:
                    break
            if boundary and not check_schema:
                break
        if boundary and not check_schema:
            break
    if not boundary:
        raise ValueError(
            "The GTID set is not a complete committed transaction-order prefix. "
            "Include all transactions before the requested boundary; arbitrary holes are not supported."
        )
    if check_schema and transaction:
        raise ValueError("The observed binlog ends with an incomplete transaction. Retry the source check.")
    validate_gtid_boundary(db, requested, baseline["gtids"] if baseline else "")
    return {**boundary, "source_range": limits}
