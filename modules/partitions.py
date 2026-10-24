"""Monthly audit partitions. Maintenance never deletes historical data implicitly."""

import re
from datetime import date, datetime, timezone

from .repository import query
from .validation import quote


def month(value, offset=0):
    index = value.year * 12 + value.month - 1 + offset
    return date(index // 12, index % 12 + 1, 1)


def clauses(retention_months=12, months_ahead=2, today=None):
    today = today or datetime.now(timezone.utc).date()
    # A lower guard partition is deliberately not eligible for automatic deletion.
    start = month(today, -retention_months)
    definitions = [f"PARTITION p_older VALUES LESS THAN ('{start.isoformat()}')"]
    for i in range(retention_months + months_ahead + 1):
        current = month(start, i)
        end = month(current, 1)
        definitions.append(f"PARTITION p{current:%Y%m} VALUES LESS THAN ('{end.isoformat()}')")
    definitions.append("PARTITION p_future VALUES LESS THAN (MAXVALUE)")
    return "PARTITION BY RANGE COLUMNS(event_time) (" + ",\n".join(definitions) + ")"


def list_partitions(db, schema, table):
    return query(
        db,
        """SELECT PARTITION_NAME AS partition_name, PARTITION_DESCRIPTION AS upper_bound,
        TABLE_ROWS AS estimated_rows, DATA_LENGTH AS data_bytes, INDEX_LENGTH AS index_bytes
        FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
        AND PARTITION_NAME IS NOT NULL ORDER BY PARTITION_ORDINAL_POSITION""",
        (schema, table),
    )


def prepare(db, schema, table, months_ahead=2):
    partitions = list_partitions(db, schema, table)
    if not partitions:
        return []
    names = {p["partition_name"] for p in partitions}
    dated = [p for p in partitions if re.fullmatch(r"p[0-9]{6}", p["partition_name"])]
    if not dated or "p_future" not in names:
        raise ValueError("Unsupported partition layout. Explicit migration is required.")
    latest = max(
        date(int(p["partition_name"][1:5]), int(p["partition_name"][5:7]), 1) for p in dated
    )
    limit = month(datetime.now(timezone.utc).date(), months_ahead)
    additions = []
    definitions = []
    while latest < limit:
        latest = month(latest, 1)
        name = f"p{latest:%Y%m}"
        end = month(latest, 1)
        definitions.append(f"PARTITION {quote(name)} VALUES LESS THAN ('{end.isoformat()}')")
        additions.append(name)
    if definitions:
        definitions.append("PARTITION p_future VALUES LESS THAN (MAXVALUE)")
        query(
            db,
            f"ALTER TABLE {quote(schema)}.{quote(table)} REORGANIZE PARTITION p_future INTO ("
            + ",".join(definitions)
            + ")",
        )
    return additions
