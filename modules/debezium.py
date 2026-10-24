"""Each flow has one connector and one stable, durable offset/history namespace."""

import re

from .capture_start import selection
from .tls_policy import tls_profile
from .transport import backend, mechanism, transport_profile

JDBC_HISTORY_DDL = (
    "CREATE TABLE %s (id VARCHAR(36) NOT NULL,history_data LONGTEXT,"
    "history_data_seq INTEGER,record_insert_ts TIMESTAMP(6) NOT NULL,"
    "record_insert_seq INTEGER NOT NULL,PRIMARY KEY(id,history_data_seq)) "
    "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
)


def connector_config(flow, config):
    source = flow["source"]
    q = transport_profile(config, flow)
    # Escape database/table names because Debezium include lists use regexes.
    table = re.escape(flow["source_schema"]) + "\\." + re.escape(flow["source_table"])
    prefix = "audit_" + str(flow["id"])
    ssl_mode = tls_profile(config, container=True)["ssl_mode"]
    start = selection(flow.get("capture_start"))
    if start["mode"] not in {"start", "end"}:
        raise ValueError("Initial capture uses Start or End. GTID and datetime changes require an existing schema baseline through Capture start.")
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
        "database.include.list": re.escape(flow["source_schema"]),
        "table.include.list": table,
        "snapshot.mode": "initial" if start["mode"] == "start" else "no_data",
        "include.schema.changes": "true",
        "include.schema.comments": "true",
        "tombstones.on.delete": "true",
        "schema.history.internal.kafka.bootstrap.servers": q["bootstrap_servers"],
        "schema.history.internal.kafka.topic": prefix + ".schema-history",
        "schema.history.internal.producer.security.protocol": "SASL_SSL",
        "schema.history.internal.consumer.security.protocol": "SASL_SSL",
        "schema.history.internal.producer.sasl.mechanism": mechanism(q),
        "schema.history.internal.consumer.sasl.mechanism": mechanism(q),
        "schema.history.internal.producer.sasl.jaas.config": "${file:/tmp/audit-runtime/credentials.properties:jaas}",
        "schema.history.internal.consumer.sasl.jaas.config": "${file:/tmp/audit-runtime/credentials.properties:jaas}",
        "transforms": "route",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": "^"
        + re.escape(prefix + "." + flow["source_schema"] + "." + flow["source_table"])
        + "$",
        "transforms.route.replacement": flow["topic"],
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "true",
        "value.converter.schemas.enable": "true",
    }

    if backend(q) == "oci_streaming":
        for key in list(settings):
            if key.startswith("schema.history.internal."):
                settings.pop(key)
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
