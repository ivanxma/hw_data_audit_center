from contextlib import contextmanager

from .mysql_util import open_connection
from .validation import quote

AUDIT_COLUMNS = {
    "event_id",
    "capture_fingerprint",
    "key_hash",
    "before_fingerprint",
    "after_fingerprint",
    "schema_document",
    "schema_fingerprint",
    "flow_id",
    "source_schema",
    "source_table",
    "operation",
    "event_time",
    "record_key",
    "before_image",
    "after_image",
    "source_metadata",
    "topic",
    "partition_id",
    "queue_offset",
    "recorded_at",
}


@contextmanager
def connect(connection, cloud):
    cloud.validate_connection(connection)
    profile = {
        "mode": "tcp",
        "host": connection["host"],
        "port": connection["port"],
        "database": connection["database_name"],
        **cloud.tls_profile(),
        "read_timeout": 30,
        "write_timeout": 30,
    }
    db, _ = open_connection(
        profile, connection["username"], cloud.secret(connection["secret_ocid"])
    )
    try:
        yield db
    finally:
        db.close()


def assess(settings):
    """Observed server variables, never inferred from saved configuration."""
    settings = {k.lower(): str(v) for k, v in settings.items()}
    rules = [
        ("log_bin", lambda v: v.upper() in {"ON", "1"}, "ON", "Binary logging is required."),
        (
            "binlog_format",
            lambda v: v.upper() == "ROW",
            "ROW",
            "Row events are required for data images.",
        ),
        (
            "binlog_row_image",
            lambda v: v.upper() == "FULL",
            "FULL",
            "FULL preserves all available columns in BEFORE and AFTER images.",
        ),
        (
            "binlog_row_value_options",
            lambda v: v.strip() == "",
            "(empty)",
            "Disable PARTIAL_JSON so JSON columns contain complete values.",
        ),
        (
            "binlog_expire_logs_seconds",
            lambda v: v.isdigit() and int(v) > 259200,
            "604800 (7 days)",
            "Use more than 3 days; size retention for the longest outage plus recovery time.",
        ),
        (
            "gtid_mode",
            lambda v: v.upper() == "ON",
            "ON",
            "GTID tracking is required for this HeatWave deployment.",
        ),
        (
            "enforce_gtid_consistency",
            lambda v: v.upper() == "ON",
            "ON",
            "Require GTID-consistent transactions.",
        ),
        (
            "server_id",
            lambda v: v.isdigit() and int(v) > 0,
            "Non-zero, unique",
            "Source and Debezium server IDs must be unique.",
        ),
    ]
    results = [
        {
            "name": key,
            "observed": settings.get(key, "Unavailable"),
            "recommended": expected,
            "ok": key in settings and test(settings[key]),
            "advice": advice,
        }
        for key, test, expected, advice in rules
    ]
    return {"ok": all(r["ok"] for r in results), "checks": results}


def validate_source(connection, schema, table, cloud, *, scope=None):
    from .repository import query

    with connect(connection, cloud) as db:
        rows = query(
            db,
            "SHOW GLOBAL VARIABLES WHERE Variable_name IN ('log_bin','binlog_format','binlog_row_image','binlog_row_value_options','binlog_expire_logs_seconds','server_id','gtid_mode','enforce_gtid_consistency')",
        )
        settings = {r["Variable_name"]: r["Value"] for r in rows}
        result = assess(settings)
        if table == "*":
            from .capture_scope import inventory

            tables = inventory(db, scope or {"source_schema": schema, "source_table": table})
            missing_keys = [
                r["source_schema"] + "." + r["source_table"] for r in tables if not r["has_primary"]
            ]
            primary = (
                []
                if missing_keys
                else [
                    "All " + str(len(tables)) + " eligible tables have a primary key"
                    if tables else "No base tables yet; waiting for new matching tables"
                ]
            )
        else:
            columns = query(
                db,
                """SELECT COLUMN_NAME AS column_name, COLUMN_KEY AS column_key
                FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION""",
                (schema, table),
            )
            primary = [r["column_name"] for r in columns if r["column_key"] == "PRI"]
        result["checks"].append(
            {
                "name": "registered_table_primary_key",
                "observed": ", ".join(primary)
                or (
                    ("Missing primary keys: " + ", ".join(missing_keys[:10]))
                    if table == "*"
                    else "Missing"
                ),
                "recommended": "A stable primary key",
                "ok": bool(primary),
                "advice": "A primary key enables precise record lookup and stable Debezium message keys.",
            }
        )
        # Privileges are visible but never rendered as raw grants (which may expose sensitive account details).
        import re

        granted = set()
        for row in query(db, "SHOW GRANTS FOR CURRENT_USER"):
            for value in row.values():
                match = re.match(r"GRANT (.+?) ON (.+?) TO ", str(value).upper())
                if match:
                    names = {p.strip() for p in match[1].split(",")}
                    granted.update(names - {"ALL PRIVILEGES"})
                    if "ALL PRIVILEGES" in names and match[2] == "*.*":
                        granted.add("ALL GLOBAL PRIVILEGES")
        privileges = (
            "REPLICATION SLAVE",
            "REPLICATION CLIENT",
            "SELECT",
            "RELOAD",
            "SHOW DATABASES",
            "LOCK TABLES",
        )
        missing = [
            p for p in privileges if p not in granted and "ALL GLOBAL PRIVILEGES" not in granted
        ]
        # HeatWave provides the narrower FLUSH_TABLES privilege for snapshot locks.
        # Requiring RELOAD alone incorrectly rejects its supported connector accounts.
        if "RELOAD" in missing and "FLUSH_TABLES" in granted:
            missing.remove("RELOAD")
        result["checks"].append(
            {
                "name": "connector_privileges",
                "observed": "Review grants: " + ", ".join(missing)
                if missing
                else "Required grants found",
                "recommended": "SELECT, RELOAD or FLUSH_TABLES, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES",
                "ok": not missing,
                "advice": "Use a dedicated connector account. Scoped grants may require manual review; the app fails closed.",
            }
        )
        result["ok"] = all(r["ok"] for r in result["checks"])
        return result


def audit_ddl(schema, table, partitioned=True, retention_months=12):
    from .partitions import clauses

    return f"""CREATE TABLE IF NOT EXISTS {quote(schema)}.{quote(table)} (
        event_id CHAR(64) NOT NULL,
        capture_fingerprint CHAR(64) NOT NULL,
        key_hash CHAR(64),
        before_fingerprint CHAR(64),
        after_fingerprint CHAR(64),
        schema_document JSON,
        schema_fingerprint CHAR(64),
        flow_id BIGINT NOT NULL,
        source_schema VARCHAR(64) NOT NULL,
        source_table VARCHAR(64) NOT NULL,
        operation VARCHAR(16) NOT NULL,
        event_time DATETIME(6) NOT NULL,
        recorded_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
        record_key JSON,
        before_image JSON,
        after_image JSON,
        source_metadata JSON NOT NULL,
        topic VARCHAR(249) NOT NULL,
        partition_id INT NOT NULL,
        queue_offset BIGINT NOT NULL,
        PRIMARY KEY (event_time,event_id),
        INDEX row_history(flow_id,key_hash,event_time,event_id),
        INDEX flow_time(flow_id,event_time),
        INDEX operation_time(operation,event_time)
    ) ENGINE=InnoDB""" + (" " + clauses(retention_months=retention_months) if partitioned else "")


def prepare_target(db, flow, *, payload_only=False):
    """Create new objects and verify existing payload tables; never silently migrate history."""
    from .repository import query
    from .retention import initialize as initialize_retention
    from .schema_history import initialize

    schema, table = flow["target_schema"], flow["target_table"]
    query(db, audit_ddl(schema, table))
    columns = query(
        db,
        """SELECT COLUMN_NAME AS name FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s""",
        (schema, table),
    )
    primary = query(
        db,
        """SELECT COLUMN_NAME AS name FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND INDEX_NAME='PRIMARY' ORDER BY SEQ_IN_INDEX""",
        (schema, table),
    )
    if {c["name"] for c in columns} != AUDIT_COLUMNS or [c["name"] for c in primary] != [
        "event_time",
        "event_id",
    ]:
        raise ValueError(
            "Existing audit table has an incompatible layout. An explicit migration is required."
        )
    if payload_only:
        return
    from .target_routes import initialize as initialize_routes

    initialize_routes(db, flow)
    from .message_fragments import initialize as initialize_fragments

    initialize_fragments(db, schema)
    initialize(db, schema)
    initialize_retention(db, schema, flow["id"], table)
    from .checkpoints import initialize as initialize_checkpoints

    initialize_checkpoints(db, schema)
