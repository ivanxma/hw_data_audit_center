"""Discover accessible database objects using the selected MySQL connection."""

from .heatwave import AUDIT_COLUMNS, connect
from .mappings import SYSTEM_SCHEMAS
from .repository import query
from .validation import identifier


def discover(connection, cloud, schema=None):
    with connect(connection, cloud) as db:
        if not schema:
            rows = query(
                db,
                "SELECT SCHEMA_NAME AS name FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME",
            )
            return {"schemas": [r["name"] for r in rows if r["name"].lower() not in SYSTEM_SCHEMAS]}
        identifier(schema)
        rows = query(
            db,
            """SELECT TABLE_NAME AS name FROM information_schema.TABLES
            WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME""",
            (schema,),
        )
        if connection["role"] == "target":
            columns = query(
                db,
                """SELECT TABLE_NAME AS object_name,COLUMN_NAME AS name
                FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s""",
                (schema,),
            )
            primary = query(
                db,
                """SELECT TABLE_NAME AS object_name,COLUMN_NAME AS name
                FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=%s AND INDEX_NAME='PRIMARY'
                ORDER BY TABLE_NAME,SEQ_IN_INDEX""",
                (schema,),
            )
            by_table, keys = {}, {}
            for row in columns:
                by_table.setdefault(row["object_name"], set()).add(row["name"])
            for row in primary:
                keys.setdefault(row["object_name"], []).append(row["name"])
            for row in rows:
                row["compatible"] = by_table.get(row["name"]) == AUDIT_COLUMNS and keys.get(
                    row["name"]
                ) == ["event_time", "event_id"]
        return {"tables": rows}


def new_audit_name(source_schema, source_table):
    import hashlib

    value = ("all_databases" if source_schema == "*" else source_schema) + "_" + ("all_tables" if source_table == "*" else source_table) + "_history"
    if len(value) > 64:
        value = value[:51] + "_" + hashlib.sha256(value.encode()).hexdigest()[:12]
    return identifier(value)
