"""Edit registrations without changing deployed capture identities or saved offsets."""

import json
import re
from contextlib import ExitStack

from . import repository as repo
from .capture_start import selection
from .connections import connection_values, verify_connection
from .fingerprints import digest

TABLES = {"connection": "connections", "queue": "queue_connections", "flow": "flows"}
FIELDS = {
    "connection": ("name", "role", "host", "port", "database_name", "username", "secret_ocid"),
    "queue": ("name", "kind", "resource_ocid"),
    "flow": (
        "name",
        "source_id",
        "target_id",
        "source_schema",
        "source_table",
        "target_schema",
        "target_table",
        "queue_connection_id",
        "stream_pool_id",
        "topic",
        "target_grouping",
        "capture_mode",
        "capture_gtid",
    ),
}


def row_values(kind, row):
    values = {name: row.get(name) for name in FIELDS[kind]}
    if kind == "flow":
        values["target_grouping"] = row.get("target_grouping") or "flow"
        capture = selection(row.get("capture_start"))
        values["capture_mode"] = capture["mode"]
        values["capture_gtid"] = capture.get("value", "") if capture["mode"] == "gtid" else ""
    return values


def version(kind, row):
    return digest({**row_values(kind, row), "id": row["id"]})


def related_flows(db, kind, row):
    if kind == "flow":
        return [row]
    if kind == "connection":
        return repo.query(
            db,
            "SELECT * FROM flows WHERE source_id=%s OR target_id=%s ORDER BY id",
            (row["id"], row["id"]),
        )
    return repo.query(
        db, "SELECT * FROM flows WHERE queue_connection_id=%s ORDER BY id", (row["id"],)
    )


def deployed(db, flow):
    # Even a failed deployment can have initialized recovery tables or created a container.
    return bool(
        flow.get("connector_ocid")
        or flow.get("processor_ocid")
        or flow.get("status") != "draft"
        or repo.query(
            db,
            "SELECT id FROM operations WHERE flow_id=%s AND action IN "
            "('deploy','start','resume','restart','update_debezium','update_writer','apply_stream_settings') LIMIT 1",
            (flow["id"],),
            one=True,
        )
    )


def edit_policy(db, kind, row, flows):
    locked, messages = set(), []
    if any(deployed(db, flow) for flow in flows):
        locked.update(set(FIELDS[kind]) - {"name"})
        messages.append(
            "This entry belongs to a flow with deployment history. You can edit its name. "
            "Capture, connection and destination settings are locked to preserve offsets, "
            "row fingerprints and audit history. Register a new flow for a different data path."
        )
    elif kind in {"connection", "queue"}:
        owned_topics = kind == "queue" and repo.query(
            db,
            "SELECT name FROM managed_topics WHERE queue_connection_id=%s LIMIT 1",
            (row["id"],),
            one=True,
        )
        if flows or owned_topics:
            locked.update(
                {"role", "host", "port", "database_name"}
                if kind == "connection"
                else {"kind", "resource_ocid"}
            )
            messages.append(
                "The endpoint is referenced by a mapping or app-managed topics. "
                "You can edit the name and credentials; register another connection to change its endpoint."
            )
    return sorted(locked), " ".join(messages)


def get_entry(db, kind, entry_id):
    if kind not in TABLES:
        raise ValueError("Choose a connection, queue connection or table mapping.")
    row = repo.query(db, "SELECT * FROM " + TABLES[kind] + " WHERE id=%s", (entry_id,), one=True)
    if not row:
        raise ValueError("This registration no longer exists. Refresh Configuration & setup.")
    if kind == "flow" and row.get("queue_connection_id"):
        pool = repo.query(db, "SELECT resource_ocid FROM queue_connections WHERE id=%s",
                          (row["queue_connection_id"],), one=True)
        row["stream_pool_id"] = pool["resource_ocid"] if pool else ""
    return row


def edit_details(db, kind, entry_id):
    row = get_entry(db, kind, entry_id)
    locked, message = edit_policy(db, kind, row, related_flows(db, kind, row))
    return {
        "kind": kind,
        "id": row["id"],
        "values": row_values(kind, row),
        "version": version(kind, row),
        "locked": locked,
        "message": message,
    }


def queue_values(form):
    name = form.get("name", "").strip()
    if not name or len(name) > 100 or form.get("kind", "oci_streaming") != "oci_streaming":
        raise ValueError("Enter a name of up to 100 characters and select OCI Streaming.")
    return {
        "name": name,
        "kind": "oci_streaming",
        "resource_ocid": form.get("resource_ocid", "").strip(),
    }


def connection_identity(db, values, result):
    existing = repo.query(
        db,
        "SELECT server_identity FROM connections WHERE host=%s AND port=%s LIMIT 1",
        (values["host"], values["port"]),
        one=True,
    )
    return existing["server_identity"] if existing else "mysql:" + result["server_uuid"]


def edit_entry(db, cloud_factory, kind, entry_id, form, actor):
    # Mapping creation uses this same lock. Lock related flows before checking deployment
    # history, so an Edit cannot race deployment, image replacement or retirement.
    with repo.named_lock(db, "mappings", "all"), ExitStack() as stack:
        row = get_entry(db, kind, entry_id)
        flows = related_flows(db, kind, row)
        for flow in flows:
            stack.enter_context(repo.named_lock(db, "flow", flow["id"]))
        row = get_entry(db, kind, entry_id)
        flows = related_flows(db, kind, row)
        if form.get("version") != version(kind, row):
            raise ValueError("This entry changed since Edit was opened. Reload it before saving.")
        locked, _ = edit_policy(db, kind, row, flows)
        current = row_values(kind, row)
        changed = {
            name
            for name in FIELDS[kind]
            if name in form
            and str(form[name]).strip() != str(current[name] if current[name] is not None else "")
        }
        if changed.intersection(locked):
            raise ValueError("These settings are locked because this entry is in use. Reload Edit.")
        if not changed:
            return False
        values = {k: str(v) if v is not None else "" for k, v in current.items()}
        values.update({k: form[k].strip() for k in changed})
        name = values["name"]
        if not name or len(name) > (80 if kind == "flow" else 100):
            raise ValueError("Enter a valid name within the field's length limit.")
        if kind == "flow" and not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", name):
            raise ValueError("Use letters, numbers, underscore or hyphen for the flow name.")
        # Renaming is metadata-only, including for a running or retired flow.
        if changed == {"name"}:
            updates = {"name": name}
        elif kind == "connection":
            updates = connection_values(values)
            result = verify_connection(updates, cloud_factory())
            updates["server_identity"] = row["server_identity"]
            if (updates["host"], updates["port"]) != (row["host"], row["port"]):
                updates["server_identity"] = connection_identity(db, updates, result)
            elif row["server_identity"].startswith("mysql:") and (
                row["server_identity"] != "mysql:" + result["server_uuid"]
            ):
                raise ValueError(
                    "The endpoint now identifies a different MySQL server. Register a new connection."
                )
        elif kind == "queue":
            from .queue_connections import profile

            updates = queue_values(values)
            profile(cloud_factory(), updates)
        else:
            updates = mapping_values(db, cloud_factory(), values, entry_id=entry_id)
        repo.query(
            db,
            "UPDATE "
            + TABLES[kind]
            + " SET "
            + ",".join(k + "=%s" for k in updates)
            + " WHERE id=%s",
            [*updates.values(), entry_id],
        )
        repo.record(db, entry_id if kind == "flow" else None, "edit-" + kind, "complete", actor)
        return True


def mapping_values(db, cloud, form, *, entry_id=None):
    """Shared live validation for both a new mapping and an undeployed mapping edit."""
    from .database_objects import discover, new_audit_name
    from .mappings import validate_mapping
    from .queue_connections import pool_reference, profile, topics
    from .validation import identifier

    data = {
        k: form.get(k, "").strip()
        for k in (
            "name",
            "source_schema",
            "source_table",
            "target_schema",
            "target_table",
            "topic",
        )
    }
    if data["target_table"] == "__new__":
        data["target_table"] = new_audit_name(data["source_schema"], data["source_table"])
    if data["source_schema"] == "*" and data["source_table"] != "*":
        raise ValueError("All databases requires all tables. Use *.* or database.*.")
    for key in ("source_schema", "source_table", "target_schema", "target_table"):
        if key.startswith("source_") and data[key] == "*":
            continue
        identifier(data[key])
    data["target_grouping"] = form.get("target_grouping", "flow")
    if data["target_grouping"] not in {"flow", "database", "table"}:
        raise ValueError("Choose target grouping by flow, database or table.")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", data["name"]):
        raise ValueError("Use letters, numbers, underscore or hyphen for the flow name.")
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,249}", data["topic"]) or data["topic"] in {".", ".."}:
        raise ValueError("Enter a valid topic name.")
    validate_flow_identity(db, data, entry_id=entry_id)
    if form.get("stream_pool_id"):
        queue = pool_reference(db, cloud, form["stream_pool_id"].strip())
    else:
        # Preserve existing saved mappings and older clients during the UI transition.
        queue = repo.query(
            db, "SELECT * FROM queue_connections WHERE id=%s",
            (form.get("queue_connection_id"),), one=True,
        )
    if not queue:
        raise ValueError("Choose an ACTIVE OCI stream pool.")
    profile(cloud, queue)
    data["queue_connection_id"] = queue["id"]
    data["transport"] = "streaming"
    if data["topic"] not in {t["name"] for t in topics(cloud, queue) if t["state"] == "ACTIVE"}:
        raise ValueError("Choose an existing ACTIVE OCI stream.")
    candidate = {"id": entry_id}
    for role in ("source", "target"):
        row = repo.query(
            db,
            "SELECT * FROM connections WHERE id=%s AND role=%s",
            (form.get(role + "_id"), role),
            one=True,
        )
        if not row:
            raise ValueError("Select registered source and target connections.")
        candidate[role] = row
        data[role + "_id"] = row["id"]
    capture = selection(
        {"mode": form.get("capture_mode", "end"), "value": form.get("capture_gtid")}
    )
    if capture["mode"] not in {"start", "end", "gtid"}:
        raise ValueError("Choose Start, End or Specific GTID for initial capture.")
    data["capture_start"] = json.dumps(capture)
    if data["source_schema"] != "*":
        source_tables = discover(candidate["source"], cloud, data["source_schema"])["tables"]
        if data["source_table"] != "*" and data["source_table"] not in {
            r["name"] for r in source_tables
        }:
            raise ValueError("Choose an existing source base table.")
    source_system = candidate["source"]["server_identity"]
    namespaces = repo.query(
        db,
        "SELECT DISTINCT t.server_identity,f.target_schema FROM flows f JOIN connections t ON t.id=f.target_id",
    )
    excluded = {r["target_schema"] for r in namespaces if r["server_identity"] == source_system}
    if candidate["target"]["server_identity"] == source_system:
        excluded.add(data["target_schema"])
    data["source_exclusions"] = json.dumps(sorted(excluded) if data["source_schema"] == "*" else [])
    if form.get("target_table") != "__new__":
        target_tables = discover(candidate["target"], cloud, data["target_schema"])["tables"]
        if data["target_table"] not in {r["name"] for r in target_tables if r.get("compatible")}:
            raise ValueError("Choose a compatible audit table or create a new audit table.")
    elif data["target_schema"] not in discover(candidate["target"], cloud)["schemas"]:
        raise ValueError("Choose an accessible target schema.")
    validate_mapping(db, {**candidate, **data})
    if capture["mode"] == "gtid":
        from .gtid_position import locate_gtid
        from .heatwave import connect

        with connect(candidate["source"], cloud) as source:
            locate_gtid(source, capture["value"], check_schema=True)
    return data


def validate_flow_identity(db, data, *, entry_id=None):
    """Report registration conflicts before remote discovery or the unique insert."""
    for field, label in (("name", "Flow name"), ("topic", "Data stream")):
        existing = repo.query(
            db,
            "SELECT id,name FROM flows WHERE " + field + "=%s "
            "AND (%s IS NULL OR id<>%s) LIMIT 1",
            (data[field], entry_id, entry_id),
            one=True,
        )
        if existing:
            detail = (
                "Choose a different flow name or edit the existing mapping."
                if field == "name"
                else "Choose or create a separate data stream for this mapping; each flow owns its stream."
            )
            raise ValueError(
                f"{label} '{data[field]}' is already registered to flow '{existing['name']}' "
                f"(ID {existing['id']}). {detail}"
            )
    if re.fullmatch(r"audit_[0-9]+", data["topic"]):
        raise ValueError(
            "Stream names audit_<flow ID> are reserved for schema history. "
            "Choose or create a separate data stream for row changes."
        )
