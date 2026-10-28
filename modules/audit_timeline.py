"""Merge schema and row evidence by source time without sorting large payloads."""

import json
from collections import defaultdict
from datetime import timezone

from .audit_service import chart_window, time_bounds
from .capture_scope import matches, matches_schema, wildcard
from .heatwave import connect
from .repository import query
from .target_routes import tables
from .validation import bounded, quote

LABELS = {
    "CREATE": "INSERT",
    "UPDATE": "UPDATE",
    "DELETE": "DELETE",
    "SNAPSHOT": "Row snapshot",
    "TRUNCATE": "TRUNCATE rows",
    "DDL_CREATE": "CREATE TABLE",
    "DDL_ALTER": "ALTER TABLE",
    "DDL_DROP": "DROP TABLE",
    "DDL_RENAME": "RENAME TABLE",
    "DDL_TRUNCATE": "TRUNCATE TABLE",
    "DDL_SNAPSHOT": "Schema baseline",
    "DDL_SNAPSHOT_RESET": "Schema baseline reset",
}


def text(document, field):
    return f"JSON_UNQUOTE(JSON_EXTRACT({document},'$.{field}'))"


def coordinates(document):
    return (
        f"COALESCE({text(document, 'file')},'') AS source_file,"
        f"COALESCE(CAST({text(document, 'pos')} AS UNSIGNED),0) AS source_pos,"
        f"COALESCE(CAST({text(document, 'row')} AS UNSIGNED),0) AS source_row,"
        f"CAST(SUBSTRING_INDEX(COALESCE({text(document, 'file')},''),'.',-1) AS UNSIGNED) AS file_number"
    )


def relation(db, flow, kind):
    selections = []
    schema = quote(flow["target_schema"])
    if kind != "schema":
        for name in tables(db, flow) or [flow["target_table"]]:
            selections.append(
                "SELECT event_id,event_time,event_time AS payload_time,flow_id,source_schema,source_table,"
                "operation,key_hash,'row' AS event_kind,'source' AS time_basis,"
                f"'{name}' AS origin,CONCAT(source_schema,'.',source_table) AS object_name,"
                + coordinates("source_metadata")
                + f" FROM {schema}.{quote(name)} WHERE flow_id={int(flow['id'])}"
            )
    if kind != "row":
        # Millisecond source timestamps match the row-history time resolution. A
        # schema snapshot establishes a baseline, not its historical CREATE time.
        stamp = f"CAST({text('source_position', 'ts_ms')} AS UNSIGNED)"
        source_time = f"change_kind NOT IN ('SNAPSHOT','SNAPSHOT_RESET') AND {stamp}>0"
        names = "REPLACE(REPLACE(object_name,'`',''),CHAR(34),'')"
        selections.append(
            f"SELECT event_id,CASE WHEN {source_time} THEN TIMESTAMPADD(MICROSECOND,{stamp}*1000,"
            f"CAST('1970-01-01 00:00:00' AS DATETIME(6))) ELSE observed_at END AS event_time,"
            "NULL AS payload_time,flow_id,source_schema,"
            f"SUBSTRING_INDEX({names},'.',-1) AS source_table,CONCAT('DDL_',change_kind) AS operation,"
            f"NULL AS key_hash,'schema' AS event_kind,CASE WHEN {source_time} THEN 'source' ELSE 'recorded' END AS time_basis,"
            f"'schema' AS origin,{names} AS object_name,"
            + coordinates("source_position")
            + f" FROM {schema}.audit_schema_changes WHERE flow_id={int(flow['id'])}"
        )
    return "(" + " UNION ALL ".join(selections) + ") AS audit_timeline"


def browse(flow, cloud, filters):
    kind = filters.get("event_kind", "all")
    direction = filters.get("direction", "asc")
    if kind not in {"all", "row", "schema"} or direction not in {"asc", "desc"}:
        raise ValueError("Choose all, schema or row changes and a valid time order.")
    chart_start, chart_end, interval = chart_window(filters)
    start, end = time_bounds(filters)
    clauses, params = ["flow_id=%s"], [flow["id"]]
    operation = filters.get("operation", "")
    if operation:
        if operation not in LABELS:
            raise ValueError("Choose a supported row or schema operation.")
        clauses.append("operation=%s")
        params.append(operation)
    selected_schema = filters.get("source_database", "").strip()
    selected_table = filters.get("source_table_filter", "").strip()
    if selected_schema and not matches_schema(flow, selected_schema):
        raise ValueError("Choose a database inside this capture scope.")
    if selected_table and not matches(
        flow, selected_schema or flow["source_schema"], selected_table
    ):
        raise ValueError("Choose a database and table inside this capture scope.")
    if selected_schema:
        clauses.append("source_schema=%s")
        params.append(selected_schema)
    if selected_table:
        clauses.append(
            "(source_table=%s OR (event_kind='schema' AND FIND_IN_SET(CONCAT(source_schema,'.',%s),REPLACE(object_name,', ',','))>0))"
        )
        params.extend([selected_table, selected_table])
    if kind != "schema" and filters.get("key"):
        if wildcard(flow) and (
            not selected_table or (flow["source_schema"] == "*" and not selected_schema)
        ):
            raise ValueError("For wildcard row-key lookup, select the source database and table.")
        try:
            key = json.loads(filters["key"])
        except json.JSONDecodeError:
            raise ValueError("Enter a complete primary-key JSON object.") from None
        if not isinstance(key, dict) or not key:
            raise ValueError("Row lookup requires a non-empty primary-key JSON object.")
        from .config import fingerprint_secret
        from .fingerprints import row_hash

        namespace = {
            **flow,
            "source_identity": flow["source"]["server_identity"],
            "source_schema": selected_schema or flow["source_schema"],
            "source_table": selected_table or flow["source_table"],
        }
        digest = row_hash(key, namespace, cloud.secret(fingerprint_secret(cloud.config)).encode())
        clauses.append("(event_kind='schema' OR key_hash=%s)")
        params.append(digest)
    for value, symbol in [(start, ">="), (end, "<=")]:
        if value:
            clauses.append("event_time " + symbol + " %s")
            params.append(value)
    where = " AND ".join(clauses)
    page = bounded(filters.get("page"), 1, 100000)
    with connect(flow["target"], cloud) as db:
        source = relation(db, flow, kind)
        total = query(db, f"SELECT COUNT(*) AS n FROM {source} WHERE {where}", params, one=True)[
            "n"
        ]
        order = direction.upper()
        # Only small scalar coordinates participate in this sort; JSON/DDL is
        # fetched afterwards for the selected 50 events.
        points = query(
            db,
            f"SELECT event_id,event_time,payload_time,event_kind,time_basis,origin,source_file,source_pos,source_row,file_number "
            f"FROM {source} WHERE {where} ORDER BY event_time {order},file_number {order},source_file {order},"
            f"source_pos {order},source_row {order},event_kind {order},event_id {order} LIMIT 50 OFFSET %s",
            [*params, (page - 1) * 50],
        )
        payloads = {}
        groups = defaultdict(list)
        for point in points:
            groups[(point["event_kind"], point["origin"])].append(point)
        for (category, origin), group in groups.items():
            if category == "schema":
                found = query(
                    db,
                    f"SELECT * FROM {quote(flow['target_schema'])}.audit_schema_changes WHERE flow_id=%s AND event_id IN ("
                    + ",".join(["%s"] * len(group))
                    + ")",
                    [flow["id"], *[p["event_id"] for p in group]],
                )
            else:
                found = query(
                    db,
                    f"SELECT * FROM {quote(flow['target_schema'])}.{quote(origin)} WHERE flow_id=%s AND (event_time,event_id) IN ("
                    + ",".join(["(%s,%s)"] * len(group))
                    + ")",
                    [flow["id"], *[v for p in group for v in (p["payload_time"], p["event_id"])]],
                )
            for item in found:
                payloads[(category, origin, item["event_id"])] = item
        rows = []
        for point in points:
            category = point["event_kind"]
            row = payloads.get((category, point["origin"], point["event_id"]))
            if row is None:
                raise ValueError("Timeline evidence changed during paging. Refresh the view.")
            if category == "schema":
                for field in [
                    "source_position",
                    "before_definition",
                    "after_definition",
                    "semantic_delta",
                ]:
                    if isinstance(row.get(field), str):
                        row[field] = json.loads(row[field])
                metadata = row["source_position"]
                queue = metadata.get("_audit_queue", {})
                row.update(
                    operation="DDL_" + row["change_kind"],
                    source_table=metadata.get("table") or row["object_name"],
                    source_metadata=metadata,
                    topic=queue.get("topic", ""),
                    partition_id=queue.get("partition"),
                    queue_offset=queue.get("offset"),
                    record_key=None,
                    before_image=None,
                    after_image=None,
                )
            else:
                for field in ["source_metadata", "record_key", "before_image", "after_image"]:
                    if isinstance(row.get(field), str):
                        row[field] = json.loads(row[field])
            row.update(
                event_kind=category,
                event_time=point["event_time"],
                time_basis=point["time_basis"],
                operation_label=LABELS.get(row["operation"], row["operation"]),
            )
            rows.append(row)
        counts = query(
            db,
            f"SELECT operation,COUNT(*) AS n FROM {source} WHERE {where} GROUP BY operation",
            params,
        )
        anchor = 345600 if interval == 604800 else 0
        chart = query(
            db,
            f"SELECT FLOOR((UNIX_TIMESTAMP(event_time)-%s)/%s)*%s+%s AS bucket,operation,COUNT(*) AS n "
            f"FROM {source} WHERE {where} AND event_time >= %s AND event_time <= %s GROUP BY bucket,operation ORDER BY bucket",
            [anchor, interval, interval, anchor, *params, chart_start, chart_end],
        )
    return {
        "rows": rows,
        "total": total,
        "page": page,
        "counts": counts,
        "chart": chart,
        "interval": interval,
        "event_kind": kind,
        "direction": direction,
        "from": chart_start.isoformat(timespec="seconds"),
        "to": chart_end.isoformat(timespec="seconds"),
        "chart_start": chart_start.replace(tzinfo=timezone.utc).timestamp(),
        "chart_end": chart_end.replace(tzinfo=timezone.utc).timestamp(),
    }
