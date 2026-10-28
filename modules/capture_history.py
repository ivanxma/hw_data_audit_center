"""Find a real schema baseline before permitting historical source positioning."""

import json

from .heatwave import connect
from .repository import query
from .validation import quote


def baseline_record(records, flow):
    expected = flow["source_schema"] + "." + flow["source_table"]
    from .capture_scope import wildcard
    if wildcard(flow):
        raise ValueError('Historical repositioning of wildcard flows requires a verified complete schema baseline for every table. Use initial End/GTID when registering the scope; ordinary restart preserves its complete checkpoint.')
    for record in records:
        if record.get("source", {}).get("server") != "audit_" + str(flow["id"]):
            continue
        position = record.get("position", {})
        if position.get("snapshot") not in (True, "true", "INITIAL"):
            continue
        for change in record.get("tableChanges", []):
            names = change.get("id", "").replace('"', "").replace("`", "").split(",")
            if expected in names and change.get("type") == "CREATE" and change.get("table"):
                if not isinstance(position.get("gtids"), str) or not position.get("file"):
                    raise ValueError("The schema baseline has no recoverable MySQL GTID boundary.")
                return {k: position[k] for k in ("file", "pos", "gtids", "ts_sec") if k in position}
    raise ValueError(
        "No retained initial schema baseline exists for this table. Historical capture needs matching schema history; take an initial baseline first."
    )


def schema_baseline(flow, cloud):
    with connect(flow["target"], cloud) as db:
        table = (
            quote(flow["target_schema"]) + "." + quote("debezium_history_" + str(flow["id"]))
        )
        rows = query(
            db,
            f"SELECT id,history_data,history_data_seq FROM {table} "
            "ORDER BY record_insert_ts,record_insert_seq,id,history_data_seq LIMIT 2048",
        )
    groups = {}
    total = 0
    for row in rows:
        total += len(row["history_data"])
        if total > 16 * 1024 * 1024:
            raise ValueError(
                "Schema baseline inspection exceeded its bounded size. Review history retention."
            )
        groups.setdefault(row["id"], []).append(row)
    records = []
    for fragments in groups.values():
        if [r["history_data_seq"] for r in fragments] != list(range(len(fragments))):
            raise ValueError("Schema history fragments are incomplete.")
        try:
            records.append(json.loads("".join(r["history_data"] for r in fragments)))
        except ValueError:
            raise ValueError(
                "Schema baseline is incomplete or exceeds the inspection limit."
            ) from None
    return baseline_record(records, flow)
