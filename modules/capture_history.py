"""Find a real schema baseline before permitting historical source positioning."""

import json

from .heatwave import connect
from .repository import query
from .transport import backend, transport_profile
from .validation import quote


def baseline_record(records, flow):
    expected = flow["source_schema"] + "." + flow["source_table"]
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
    queue = transport_profile(cloud.config, flow)
    if backend(queue) == "oci_streaming":
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
    from confluent_kafka import TopicPartition

    from .queue_service import create_consumer

    topic = "audit_" + str(flow["id"]) + ".schema-history"
    consumer = create_consumer(queue, cloud.secret, "audit-schema-position-preview")
    try:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)
        if low != 0:
            raise ValueError(
                "Internal schema history has lost its origin. Historical positioning needs a verified baseline."
            )
        consumer.assign([TopicPartition(topic, 0, low)])
        records, total = [], 0
        for _ in range(min(high, 2048)):
            message = consumer.poll(5)
            if message is None or message.error():
                raise ValueError("Internal schema history could not be read completely.")
            raw = message.value()
            if raw:
                total += len(raw)
                if total > 16 * 1024 * 1024:
                    raise ValueError("Schema history exceeds the bounded position inspection size.")
                records.append(json.loads(raw))
                try:
                    return baseline_record(records[-1:], flow)
                except ValueError:
                    pass
            if message.offset() + 1 >= high:
                break
        return baseline_record(records, flow)
    finally:
        consumer.close()  # Direct assignment only; never commit connector/writer offsets.
