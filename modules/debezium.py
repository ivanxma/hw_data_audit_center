"""Each flow has one connector and one stable, durable offset/history namespace."""

import re
import hashlib

from .capture_start import selection
from .config import ROOT
from .tls_policy import tls_profile

JDBC_HISTORY_DDL = (
    "CREATE TABLE %s (id VARCHAR(36) NOT NULL,history_data LONGTEXT,"
    "history_data_seq INTEGER,record_insert_ts TIMESTAMP(6) NOT NULL,"
    "record_insert_seq INTEGER NOT NULL,PRIMARY KEY(id,history_data_seq)) "
    "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
)

CAPTURE_CONTROL_FILES = (
    "modules/capture_runtime.py",
    "modules/capture_requests.py",
    "modules/capture_start.py",
)
INITIAL_GTID_FILES = (
    *CAPTURE_CONTROL_FILES,
    "modules/initial_capture.py",
    "modules/gtid_position.py",
    "modules/capture_scope.py",
    "modules/debezium.py",
    "modules/message_policy.py",
    "deploy/debezium_bootstrap.py",
    "deploy/Containerfile.debezium",
    *tuple(
        str(p.relative_to(ROOT))
        for p in sorted((ROOT / "deploy/native-capture").rglob("*"))
        if p.is_file()
    ),
)


def matches_build(manifest, files):
    return all(
        manifest.get(name) == hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
        for name in files
    )


def require_initial_gtid_image(manifest):
    if not matches_build(manifest, INITIAL_GTID_FILES):
        raise ValueError(
            "Native capture requires a newly built Debezium Engine image containing durable native offsets and GTID initialization support. "
            "Build and publish Debezium, then select that release in Workload settings."
        )


def connector_config(flow, config):
    source = flow["source"]
    # Escape database/table names because Debezium include lists use regexes.
    from .capture_scope import filters

    database, table = filters(flow)
    prefix = "audit_" + str(flow["id"])
    ssl_mode = tls_profile(config, container=True)["ssl_mode"]
    start = selection(flow.get("capture_start"))
    if start["mode"] not in {"start", "end", "gtid"}:
        raise ValueError("Initial capture uses Start, End or Specific GTID.")
    settings = {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "tasks.max": "1",
        "database.hostname": source["host"],
        "database.port": str(source["port"]),
        "database.user": source["username"],
        "database.password": "${file:/tmp/audit-runtime/credentials.properties:source_password}",
        "database.ssl.mode": ssl_mode.lower(),
        "database.server.id": str(int(config["debezium"]["server_id_base"]) + flow["id"]),
        "topic.prefix": prefix,
        "database.include.list": database,
        "table.include.list": table,
        "snapshot.mode": "initial" if start["mode"] == "start" else "no_data",
        "include.schema.changes": "true",
        "include.schema.comments": "true",
        "tombstones.on.delete": "true",
        "transforms": "route",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": "^" + re.escape(prefix) + "\\." + table + "$",
        "transforms.route.replacement": flow["topic"],
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "true",
        "value.converter.schemas.enable": "true",
    }

    settings.update(
        {
            "schema.history.internal": "io.debezium.storage.jdbc.history.JdbcSchemaHistory",
            "schema.history.internal.jdbc.connection.url": "jdbc:mysql://"
            + flow["target"]["host"]
            + ":"
            + str(flow["target"]["port"])
            + "/"
            + flow["target_schema"]
            + "?sslMode="
            + ssl_mode,
            "schema.history.internal.jdbc.connection.user": flow["target"]["username"],
            "schema.history.internal.jdbc.connection.password": "${file:/tmp/audit-runtime/credentials.properties:history_password}",
            "schema.history.internal.jdbc.table.name": flow["target_schema"]
            + ".debezium_history_"
            + str(flow["id"]),
            # The storage module's generic VARCHAR(65000) DDL exceeds
            # MySQL row-size limits with utf8mb4. Keep the same JDBC columns.
            "schema.history.internal.jdbc.table.ddl": JDBC_HISTORY_DDL,
        }
    )
    return settings
