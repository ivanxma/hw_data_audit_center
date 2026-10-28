"""Literal table, database-wide and server-wide capture scopes."""

import json
import re

SYSTEM_SCHEMAS = {"mysql", "sys", "information_schema", "performance_schema"}


def exclusions(flow):
    value = flow.get("source_exclusions") or []
    return set(json.loads(value) if isinstance(value, str) else value)


def wildcard(flow):
    return flow["source_table"] == "*"


def matches_schema(flow, schema):
    return (
        bool(schema)
        and schema.lower() not in SYSTEM_SCHEMAS
        and schema not in exclusions(flow)
        and (flow["source_schema"] == "*" or schema == flow["source_schema"])
    )


def matches(flow, schema, table):
    return (
        matches_schema(flow, schema)
        and bool(table)
        and (flow["source_table"] == "*" or table == flow["source_table"])
    )


def overlaps(a, b):
    if (
        a["source_schema"] != "*"
        and b["source_schema"] != "*"
        and a["source_schema"] != b["source_schema"]
    ):
        return False
    for scope, other in ((a, b), (b, a)):
        if other["source_schema"] != "*" and not matches_schema(scope, other["source_schema"]):
            return False
    return (
        a["source_table"] == "*"
        or b["source_table"] == "*"
        or a["source_table"] == b["source_table"]
    )


def filters(flow):
    schema = re.escape(flow["source_schema"])
    if flow["source_schema"] == "*":
        blocked = "|".join(re.escape(v) for v in sorted(SYSTEM_SCHEMAS | exclusions(flow)))
        schema = "(?!(?:" + blocked + ")$).+"
    table = (
        (".+" if flow["source_schema"] == "*" else re.escape(flow["source_schema"]))
        + r"\."
        + (".+" if wildcard(flow) else re.escape(flow["source_table"]))
    )
    return schema, table


def inventory(db, flow, maximum=1000):
    """Discover base tables and keys without reading business rows; never truncate scope."""
    from .repository import query

    excluded = sorted(SYSTEM_SCHEMAS | exclusions(flow))
    excluded_sql = ",".join(["%s"] * len(excluded))
    rows = query(
        db,
        f"""SELECT t.TABLE_SCHEMA AS source_schema,t.TABLE_NAME AS source_table,
        EXISTS(SELECT 1 FROM information_schema.STATISTICS s WHERE s.TABLE_SCHEMA=t.TABLE_SCHEMA
        AND s.TABLE_NAME=t.TABLE_NAME AND s.INDEX_NAME='PRIMARY') AS has_primary
        FROM information_schema.TABLES t WHERE t.TABLE_TYPE='BASE TABLE'
        AND (%s='*' OR t.TABLE_SCHEMA=%s) AND (%s='*' OR t.TABLE_NAME=%s)
        AND t.TABLE_SCHEMA NOT IN ({excluded_sql})
        ORDER BY t.TABLE_SCHEMA,t.TABLE_NAME LIMIT %s""",
        (
            flow["source_schema"],
            flow["source_schema"],
            flow["source_table"],
            flow["source_table"],
            *excluded,
            maximum + 1,
        ),
    )
    rows = [r for r in rows if matches(flow, r["source_schema"], r["source_table"])]
    if len(rows) > maximum:
        raise ValueError(
            "Capture scope exceeds 1000 tables. Split it into smaller database scopes."
        )
    if not rows:
        raise ValueError("Capture scope has no accessible eligible base tables.")
    return rows
