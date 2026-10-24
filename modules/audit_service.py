import json
from datetime import datetime, timedelta, timezone
from time import monotonic

from .heatwave import connect
from .repository import query
from .validation import bounded, quote

INTERVALS = {"minute": 60, "hour": 3600, "day": 86400, "week": 604800}


def chart_window(filters):
    resolved = dict(filters)
    resolved["to"] = (
        resolved.get("to") or datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    )
    start, end = time_bounds(resolved)
    interval_name = filters.get("interval", "hour")
    interval = INTERVALS.get(interval_name)
    if interval is None:
        raise ValueError("Choose minute, hour, day or week for the chart interval.")
    end = end or datetime.now(timezone.utc).replace(tzinfo=None)
    start = start or end - timedelta(seconds=3600 if interval_name == "minute" else 86400)
    seconds = (end - start).total_seconds()
    if interval_name == "minute" and seconds > 3600:
        raise ValueError(
            "Minute charts cover up to one hour. Choose a one-hour date/time range or a coarser interval."
        )
    if seconds / interval > 999:
        raise ValueError(
            "This range exceeds 1,000 chart buckets. Choose day/week or a shorter date range."
        )
    return start, end, interval


def overview(flows, cloud, filters):
    """Count registered tables independently; unavailable history is never a zero."""
    selected = [
        f
        for f in flows
        if not filters.get("source_id") or str(f["source_id"]) == filters["source_id"]
    ]
    if len(selected) > 100:
        raise ValueError(
            "Global charts support 100 tables per view. Select a source database to narrow the view."
        )
    start, end = time_bounds({**filters, "key": ""})
    end = end or datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    deadline = monotonic() + 15
    for flow in selected:
        item = {
            "flow_id": flow["id"],
            "source_id": flow["source_id"],
            "database": flow["source"]["name"] + " / " + flow["source_schema"],
            "schema": flow["source_schema"],
            "label": flow["source_schema"] + "." + flow["source_table"],
            "count": None,
            "state": "unavailable",
        }
        if monotonic() >= deadline:
            item["state"] = "not counted (time budget)"
            rows.append(item)
            continue
        try:
            with connect(flow["target"], cloud) as db:
                clause, params = (
                    "flow_id=%s AND event_time >= %s AND event_time <= %s",
                    [flow["id"], start, end],
                )
                if filters.get("operation"):
                    clause += " AND operation=%s"
                    params.append(filters["operation"])
                item["count"] = query(
                    db,
                    f"SELECT /*+ MAX_EXECUTION_TIME(5000) */ COUNT(*) AS n FROM {quote(flow['target_schema'])}.{quote(flow['target_table'])} WHERE {clause}",
                    params,
                    one=True,
                )["n"]
                item["state"] = "available"
        except Exception as error:
            item["state"] = (
                "not prepared" if getattr(error, "errno", None) == 1146 else "unavailable"
            )
        rows.append(item)
    return {
        "rows": rows,
        "from": start.isoformat(timespec="seconds"),
        "to": end.isoformat(timespec="seconds"),
        "total": sum(r["count"] for r in rows if r["count"] is not None),
        "unavailable": sum(r["count"] is None for r in rows),
    }


def browse(flow, cloud, filters):
    chart_start, chart_end, interval = chart_window(filters)
    table = f"{quote(flow['target_schema'])}.{quote(flow['target_table'])}"
    clauses = ["flow_id=%s"]
    params = [flow["id"]]
    if filters.get("operation") in {
        "CREATE",
        "UPDATE",
        "DELETE",
        "SNAPSHOT",
        "TRUNCATE",
    }:
        clauses.append("operation=%s")
        params.append(filters["operation"])
    if filters.get("key"):
        from .fingerprints import row_hash

        try:
            typed_key = json.loads(filters["key"])
        except json.JSONDecodeError:
            raise ValueError('Enter a complete primary-key JSON object, for example {"id": 1042}.')
        if not isinstance(typed_key, dict) or not typed_key:
            raise ValueError("Row lookup requires a non-empty primary-key JSON object.")
        from .config import fingerprint_secret

        key = cloud.secret(fingerprint_secret(cloud.config)).encode()
        namespace = {**flow, "source_identity": flow["source"]["server_identity"]}
        clauses.append("key_hash=%s")
        params.append(row_hash(typed_key, namespace, key))
    start, end = time_bounds(filters)
    for value, operator in ((start, ">="), (end, "<=")):
        if value is not None:
            clauses.append("event_time " + operator + " %s")
            params.append(value)
    where = " AND ".join(clauses)
    page = bounded(filters.get("page"))
    sort = {"time": "event_time", "operation": "operation", "table": "source_table"}.get(
        filters.get("sort"), "event_time"
    )
    with connect(flow["target"], cloud) as db:
        total = query(db, f"SELECT COUNT(*) AS n FROM {table} WHERE {where}", params, one=True)["n"]
        rows = query(
            db,
            f"SELECT * FROM {table} WHERE {where} ORDER BY {sort} DESC,event_id DESC LIMIT 50 OFFSET %s",
            params + [(page - 1) * 50],
        )
        for row in rows:
            for field in ("record_key", "before_image", "after_image", "source_metadata"):
                if isinstance(row[field], str):
                    row[field] = json.loads(row[field])
        anchor = 345600 if interval == 604800 else 0  # Monday 1970-01-05 UTC.
        chart_where = where + " AND event_time >= %s AND event_time <= %s"
        chart_params = list(params) + [chart_start, chart_end]
        chart = query(
            db,
            f"""SELECT FLOOR((UNIX_TIMESTAMP(event_time)-%s)/%s)*%s+%s AS bucket,
             operation, COUNT(*) AS n FROM {table} WHERE {chart_where}
             GROUP BY bucket,operation ORDER BY bucket""",
            [anchor, interval, interval, anchor] + chart_params,
        )
        counts = query(
            db,
            f"SELECT operation, COUNT(*) AS n FROM {table} WHERE {where} GROUP BY operation",
            params,
        )
    return {
        "rows": rows,
        "total": total,
        "page": page,
        "chart": chart,
        "interval": interval,
        "counts": counts,
        "from": chart_start.isoformat(timespec="seconds"),
        "to": chart_end.isoformat(timespec="seconds"),
        "chart_start": chart_start.replace(tzinfo=timezone.utc).timestamp(),
        "chart_end": chart_end.replace(tzinfo=timezone.utc).timestamp(),
    }


def time_bounds(filters):
    def parse(value):
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            raise ValueError("Enter an ISO date/time for the history range.") from None
        return parsed.astimezone(timezone.utc).replace(tzinfo=None) if parsed.tzinfo else parsed

    start, end = parse(filters.get("from")), parse(filters.get("to"))
    if start and end and start > end:
        raise ValueError("History range start must not be after its end.")
    if start is None and not filters.get("key"):
        days = {"minute": 1 / 24, "hour": 1, "day": 30, "week": 84}.get(
            filters.get("interval", "hour"), 1
        )
        start = (end or datetime.now(timezone.utc).replace(tzinfo=None)) - timedelta(days=days)
    return start, end
