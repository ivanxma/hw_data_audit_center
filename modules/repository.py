from contextlib import contextmanager

from .config import ROOT


@contextmanager
def cursor(connection):
    cur = connection.cursor(dictionary=True, buffered=True)
    try:
        yield cur
    finally:
        cur.close()


def query(connection, sql, params=(), one=False):
    with cursor(connection) as cur:
        cur.execute(sql, params)
        if not cur.with_rows:
            return cur.lastrowid
        return cur.fetchone() if one else cur.fetchall()


def initialize(connection):
    for statement in (ROOT / "sql/repository.sql").read_text().split(";"):
        if statement.strip():
            query(connection, statement)


def flows(connection):
    return query(
        connection,
        """SELECT f.*, s.name AS source_name, t.name AS target_name
        FROM flows f JOIN connections s ON s.id=f.source_id
        JOIN connections t ON t.id=f.target_id ORDER BY f.updated_at DESC""",
    )


def get_flow(connection, flow_id):
    row = query(connection, "SELECT * FROM flows WHERE id=%s", (flow_id,), one=True)
    if not row:
        raise ValueError("Flow not found.")
    row["source"] = query(
        connection, "SELECT * FROM connections WHERE id=%s", (row["source_id"],), one=True
    )
    row["target"] = query(
        connection, "SELECT * FROM connections WHERE id=%s", (row["target_id"],), one=True
    )
    if row.get("queue_connection_id"):
        row["queue_connection"] = query(
            connection,
            "SELECT * FROM queue_connections WHERE id=%s",
            (row["queue_connection_id"],),
            one=True,
        )
    return row


def record(connection, flow_id, action, outcome, actor):
    query(
        connection,
        "INSERT INTO operations(flow_id,action,outcome,actor) VALUES (%s,%s,%s,%s)",
        (flow_id, action, outcome, actor),
    )


@contextmanager
def named_lock(connection, purpose, *identity):
    """Acquire before reading mutable state; always release this session's advisory lock."""
    from .fingerprints import digest

    name = "audit-" + purpose[:12] + "-" + digest(list(identity))[:40]
    result = query(connection, "SELECT GET_LOCK(%s,0) AS acquired", (name,), one=True)
    if not result or result["acquired"] != 1:
        raise ValueError("An operation is already in progress for this resource.")
    try:
        yield
    finally:
        query(connection, "SELECT RELEASE_LOCK(%s)", (name,))


def migrate_connections(connection):
    """One-way control-repository migration after authenticated local-admin login."""
    with named_lock(connection, "repository", "connections-v2"):
        columns = query(
            connection,
            """SELECT COLUMN_NAME AS name, IS_NULLABLE AS nullable
            FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='connections' """,
        )
        metadata = {row["name"]: row["nullable"] for row in columns}
        if "server_identity" not in metadata:
            query(
                connection, "ALTER TABLE connections ADD COLUMN server_identity VARCHAR(255) NULL"
            )
            metadata["server_identity"] = "YES"
        if metadata["server_identity"] == "YES":
            if "db_system_ocid" in metadata:
                # Existing audit-key namespaces remain byte-for-byte compatible.
                query(
                    connection,
                    "UPDATE connections SET server_identity=db_system_ocid WHERE server_identity IS NULL",
                )
            query(
                connection,
                "UPDATE connections SET server_identity=CONCAT('registered:',UUID()) WHERE server_identity IS NULL",
            )
            query(
                connection,
                "ALTER TABLE connections MODIFY COLUMN server_identity VARCHAR(255) NOT NULL",
            )
        obsolete = [name for name in ("db_system_ocid", "ssl_ca", "ssl_mode") if name in metadata]
        if obsolete:
            query(
                connection,
                "ALTER TABLE connections " + ",".join("DROP COLUMN " + name for name in obsolete),
            )


def migrate_resources(connection):
    """Extend control metadata idempotently after authenticated administrator login."""
    with named_lock(connection, "repository", "resources-v1"):
        # The full initializer only creates missing tables; existing data is retained.
        initialize(connection)
        columns = query(
            connection,
            "SELECT COLUMN_NAME AS name FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='flows'",
        )
        names = {r["name"] for r in columns}
        for name, datatype in [
            ("queue_connection_id", "BIGINT"),
            ("stream_bindings", "JSON"),
            ("capture_start", "JSON"),
            ("source_exclusions", "JSON"),
            ("target_grouping", "VARCHAR(16) NOT NULL DEFAULT 'flow'"),
        ]:
            if name not in names:
                query(connection, "ALTER TABLE flows ADD COLUMN " + name + " " + datatype)

        existing = query(
            connection,
            "SELECT CONSTRAINT_NAME AS name FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=DATABASE() AND TABLE_NAME='flows' AND CONSTRAINT_NAME='queue_connection_ref'",
            one=True,
        )
        if not existing:
            query(
                connection,
                "ALTER TABLE flows ADD CONSTRAINT queue_connection_ref FOREIGN KEY (queue_connection_id) REFERENCES queue_connections(id)",
            )
