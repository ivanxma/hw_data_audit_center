"""Explicit initial capture choices and validated MySQL executed-GTID boundaries."""

import json
import re
from datetime import datetime, timezone

UUID = r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"


def gtid_set(value):
    """Canonical untagged MySQL GTID set; never confuse this with UUID filtering."""
    if not isinstance(value, str) or len(value) > 65536:
        raise ValueError("Enter a MySQL executed GTID set of at most 64 KiB.")
    groups = {}
    for group in "".join(value.split()).split(","):
        parts = group.split(":")
        if len(parts) < 2 or not re.fullmatch(UUID, parts[0]):
            raise ValueError(
                "Use an executed GTID set such as UUID:1-120. Tagged GTIDs are not supported."
            )
        intervals = groups.setdefault(parts[0].lower(), [])
        for part in parts[1:]:
            if not re.fullmatch(r"[1-9][0-9]*(?:-[1-9][0-9]*)?", part):
                raise ValueError("GTID intervals must contain positive transaction numbers.")
            bounds = [int(n) for n in part.split("-")]
            lo, hi = bounds[0], bounds[-1]
            if lo > hi or hi >= 2**63:
                raise ValueError("GTID interval bounds are invalid.")
            intervals.append((lo, hi))
    rendered = []
    for sid, intervals in sorted(groups.items()):
        merged = []
        for lo, hi in sorted(intervals):
            if merged and lo <= merged[-1][1] + 1:
                merged[-1] = (merged[-1][0], max(hi, merged[-1][1]))
            else:
                merged.append((lo, hi))
        rendered.append(
            sid + ":" + ":".join(str(lo) if lo == hi else f"{lo}-{hi}" for lo, hi in merged)
        )
    return ",".join(rendered)


def utc_datetime(value):
    if not isinstance(value, str) or len(value) > 40:
        raise ValueError("Enter a valid capture datetime in UTC.")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("Enter a valid capture datetime in UTC.") from None
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def selection(value=None):
    if isinstance(value, str):
        value = json.loads(value)
    value = value or {"mode": "start"}
    if not isinstance(value, dict) or value.get("mode") not in {"start", "end", "gtid", "datetime"}:
        raise ValueError("Choose Start, End, Specific GTID or Datetime (UTC).")
    mode = value["mode"]
    if mode == "gtid":
        return {"mode": mode, "value": gtid_set(value.get("value"))}
    if mode == "datetime":
        return {"mode": mode, "value": utc_datetime(value.get("value")).isoformat()}
    return {"mode": mode}


def validate_gtid_boundary(db, requested, baseline):
    """Every required pre-baseline transaction must be excluded from replay."""
    from .repository import query

    requested = "" if requested == "" else gtid_set(requested)
    result = query(
        db,
        "SELECT GTID_SUBSET(%s,@@GLOBAL.gtid_executed) AS executed,"
        "GTID_SUBSET(@@GLOBAL.gtid_purged,%s) AS retained,"
        "GTID_SUBSET(%s,%s) AS covered",
        (requested, requested, baseline, requested),
        one=True,
    )
    if not result or result["executed"] != 1:
        raise ValueError(
            "The requested GTID set includes transactions not executed on this source."
        )
    if result["retained"] != 1:
        raise ValueError(
            "Required binlog transactions have been purged. This position cannot be replayed."
        )
    if result["covered"] != 1:
        raise ValueError(
            "The requested position predates the retained schema baseline. Restore matching schema history before replay."
        )
    return requested
