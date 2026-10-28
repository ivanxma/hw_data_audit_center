"""Sort audit coordinates before fetching large JSON payloads."""

from collections import defaultdict

from .repository import query
from .target_routes import tables
from .validation import quote

METADATA = (
    "event_id",
    "event_time",
    "flow_id",
    "key_hash",
    "operation",
    "source_schema",
    "source_table",
)


def metadata_relation(db, flow):
    names = tables(db, flow) or [flow["target_table"]]
    selections = []
    for name in names:
        # quote() validates the generated identifier before it is also used as
        # a literal origin label; its accepted alphabet contains no quotes.
        physical = quote(flow["target_schema"]) + "." + quote(name)
        selections.append(
            "SELECT "
            + ",".join(quote(c) for c in METADATA)
            + ",'"
            + name
            + "' AS _audit_target_table FROM "
            + physical
            + " WHERE flow_id="
            + str(int(flow["id"]))
        )
    return "(" + " UNION ALL ".join(selections) + ") AS audit_rows"


def payload_page(db, flow, relation, where, params, sort, page):
    coordinates = query(
        db,
        f"SELECT event_time,event_id,_audit_target_table FROM {relation} WHERE {where} ORDER BY {sort} DESC,event_id DESC LIMIT 50 OFFSET %s",
        params + [(page - 1) * 50],
    )
    groups = defaultdict(list)
    for row in coordinates:
        groups[row["_audit_target_table"]].append((row["event_time"], row["event_id"]))
    found = {}
    for name, keys in groups.items():
        rows = query(
            db,
            f"SELECT * FROM {quote(flow['target_schema'])}.{quote(name)} WHERE flow_id=%s AND (event_time,event_id) IN ("
            + ",".join(["(%s,%s)"] * len(keys))
            + ")",
            [flow["id"], *[value for key in keys for value in key]],
        )
        for row in rows:
            found[(name, row["event_time"], row["event_id"])] = row
    return [
        found[key]
        for row in coordinates
        if (key := (row["_audit_target_table"], row["event_time"], row["event_id"])) in found
    ]
