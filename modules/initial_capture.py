"""One-time GTID bootstrap with schema-only capture and durable restart guards."""

import json
import time
import uuid
from pathlib import Path

import requests

from .capture_runtime import CaptureControl, matches, open_db
from .capture_start import gtid_set, selection
from .gtid_position import locate_gtid
from .repository import named_lock, query
from .validation import quote


def completed_snapshot(document):
    offsets = document.get("offsets", [])
    if len(offsets) != 1:
        return False
    offset = offsets[0].get("offset") or {}
    # The seed marker is discarded by Debezium when it emits its first offset.
    # Require a persisted final snapshot/streaming offset, not just RUNNING.
    return bool(offset.get("file")) and "audit_initial_seed" not in offset and offset.get(
        "snapshot"
    ) not in (True, "true", "INITIAL", "last")


def schema_checkpoint_ready(document, partition, executed):
    records = document.get("offsets", [])
    if len(records) != 1 or records[0].get("partition") != partition:
        return False
    offset = records[0].get("offset") or {}
    return (bool(offset.get("file")) and "audit_initial_seed" not in offset
            and isinstance(offset.get("gtids"), str) and gtid_set(offset["gtids"]) == executed)


def create_stopped(api, name, connector):
    response = requests.get(api.url + "/config", timeout=10)
    if response.status_code == 404:
        response = requests.post(
            "http://127.0.0.1:8083/capture",
            json={"name": name, "config": connector, "initial_state": "STOPPED"},
            timeout=10,
        )
    response.raise_for_status()
    api.stop()


def bootstrap(config, secret, api=None):
    """Never seed an existing checkpoint or repeat a completed initialization."""
    choice = selection(config["initial_capture"])
    control = config["capture_control"]
    target = control["target"]
    api = api or CaptureControl(config["connector_name"])
    connector = {
        **config["connector"], "snapshot.mode": "custom",
        "snapshot.mode.custom.name": "audit_initial_gtid",
        "audit.initial.token": uuid.uuid4().hex,
    }
    db = open_db(target, secret)
    table = quote(target["database"]) + ".audit_initial_capture"
    try:
        query(
            db,
            f"CREATE TABLE IF NOT EXISTS {table} (flow_id BIGINT PRIMARY KEY,"
            "connector_name VARCHAR(150) NOT NULL, selection JSON NOT NULL,"
            "planned_offset JSON, state VARCHAR(24) NOT NULL,"
            "updated_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)) "
            "ENGINE=InnoDB",
        )
        with named_lock(db, "capture", control["flow_id"], config["connector_name"]):
            row = query(db, f"SELECT * FROM {table} WHERE flow_id=%s", (control["flow_id"],), one=True)
            if row:
                saved = row["selection"]
                saved = json.loads(saved) if isinstance(saved, str) else saved
                if saved != choice or row["connector_name"] != config["connector_name"]:
                    raise ValueError("Initial GTID selection no longer matches its durable initialization.")
                if row["state"] == "complete":
                    current = api.offsets()
                    records = current.get("offsets", [])
                    if (
                        not completed_snapshot(current)
                        or records[0].get("partition") != {"server": config["connector"]["topic.prefix"]}
                        or not query(
                            db, "SELECT GTID_SUBSET(%s,%s) AS covered",
                            (choice["value"], (records[0].get("offset") or {}).get("gtids", "")),
                            one=True,
                        )["covered"]
                    ):
                        raise ValueError("The initialized connector checkpoint is missing or incompatible; refusing to reset it.")
                    api.call("PUT", "/config", config["connector"])
                    return
            create_stopped(api, config["connector_name"], connector)
            current = api.offsets()
            if row is None:
                if current.get("offsets"):
                    raise ValueError("Existing connector offsets cannot be overwritten by initial GTID capture.")
                query(
                    db, f"INSERT INTO {table} (flow_id,connector_name,selection,state) VALUES (%s,%s,%s,'prepared')",
                    (control["flow_id"], config["connector_name"], json.dumps(choice)),
                )
                row = {"state": "prepared", "planned_offset": None}

            def advance(state, plan=None):
                print("AUDIT_INITIAL_CAPTURE " + json.dumps({"state": state}), flush=True)
                query(
                    db, f"UPDATE {table} SET state=%s,planned_offset=COALESCE(%s,planned_offset) WHERE flow_id=%s",
                    (state, json.dumps(plan) if plan else None, control["flow_id"]),
                )

            if row["state"] == "schema_ready":
                saved = row["planned_offset"]
                saved = json.loads(saved) if isinstance(saved, str) else saved
                if not matches(current, saved):
                    records = current.get("offsets", [])
                    if (
                        not completed_snapshot(current)
                        or records[0].get("partition") != saved["partition"]
                        or not query(
                            db, "SELECT GTID_SUBSET(%s,%s) AS covered",
                            (choice["value"], (records[0].get("offset") or {}).get("gtids", "")),
                            one=True,
                        )["covered"]
                    ):
                        raise ValueError("The initialized connector checkpoint changed unexpectedly.")
            else:
                source = open_db(control["source"], secret)
                try:
                    # Hold a metadata read lock through schema recovery. LIMIT 0
                    # acquires the table definition without scanning data rows;
                    # concurrent row writes can continue, table DDL must wait.
                    source.autocommit = False
                    query(source, "START TRANSACTION")
                    scope = config.get('initial_capture_scope')
                    if scope and scope['source_table'] == '*':
                        from .capture_scope import inventory
                        tables = inventory(source, scope)
                    else:
                        tables = [{'source_schema': control['source']['database'], 'source_table':config['initial_capture_table']}]
                    for item in tables:
                        query(source, "SELECT * FROM " + quote(item['source_schema']) + "." + quote(item['source_table']) + " LIMIT 0")
                    boundary = locate_gtid(source, choice["value"], check_schema=True)
                    plan = row.get("planned_offset")
                    plan = json.loads(plan) if isinstance(plan, str) else plan
                    if plan is None:
                        plan = {
                            "partition": {"server": config["connector"]["topic.prefix"]},
                            "offset": {
                                "file": boundary["file"], "pos": boundary["pos"],
                                "gtids": boundary["gtids"], "row": 0, "event": 0,
                                "snapshot": "INITIAL", "audit_initial_seed": uuid.uuid4().hex,
                            },
                        }
                        advance("prepared", plan)
                    if current.get("offsets"):
                        if not matches(current, plan) and row["state"] != "seeded":
                            raise ValueError("Initial GTID checkpoint is ambiguous; no offsets were changed.")
                        records = current["offsets"]
                        if (
                            len(records) != 1 or records[0].get("partition") != plan["partition"]
                            or gtid_set((records[0].get("offset") or {}).get("gtids")) != choice["value"]
                        ):
                            raise ValueError("Initial GTID checkpoint no longer matches its seed; refusing to reset it.")
                    else:
                        if row["state"] == "seeded":
                            raise ValueError("A previously seeded checkpoint is missing; refusing to reset it.")
                        api.patch({"offsets": [plan]})
                        if not matches(api.offsets(), plan):
                            raise ValueError("Initial GTID offset verification failed.")
                    advance("seeded")
                    if not completed_snapshot(current):
                        marker = Path("/tmp/audit-runtime/schema-ready-" + connector["audit.initial.token"])
                        marker.unlink(missing_ok=True)
                        api.call("PUT", "/config", connector)
                        api.resume()
                        # The snapshotter cannot stream rows. Its completion
                        # callback confirms that schema capture has finished.
                        deadline = time.monotonic() + 180
                        try:
                            while time.monotonic() < deadline:
                                if (
                                    marker.is_file() and marker.read_text() == connector["audit.initial.token"]
                                    and schema_checkpoint_ready(api.offsets(), plan["partition"], choice["value"])
                                ):
                                    # Snapshot completion precedes delivery from the engine's
                                    # internal queue. Wait for native publication AND its
                                    # durable source offset before interrupting the engine.
                                    break
                                status = api.call("GET", "/status")
                                if any(task.get("state") == "FAILED" for task in status.get("tasks", [])):
                                    raise ValueError("Initial GTID schema capture failed. Inspect Debezium task logs.")
                                time.sleep(1)
                            else:
                                raise ValueError("Initial GTID schema capture timed out; the checkpoint was preserved.")
                        finally:
                            marker.unlink(missing_ok=True)
                        print('AUDIT_INITIAL_CAPTURE {"state":"schema_observed"}', flush=True)
                        api.stop()  # Flush schema offsets before marking them complete.
                        print('AUDIT_INITIAL_CAPTURE {"state":"schema_flushed"}', flush=True)
                    captured = api.offsets().get("offsets", [])
                    if len(captured) != 1 or captured[0].get("partition") != plan["partition"]:
                        raise ValueError("Initial GTID schema checkpoint could not be verified.")
                    offset = captured[0].get("offset") or {}
                    if "audit_initial_seed" in offset or gtid_set(offset.get("gtids")) != choice["value"]:
                        raise ValueError("Initial GTID schema checkpoint does not match the requested boundary.")
                    finished = {"partition": plan["partition"], "offset": {
                        key: value for key, value in offset.items() if key not in {"snapshot", "snapshot_completed"}
                    }}
                    api.patch({"offsets": [finished]})
                    if not matches(api.offsets(), finished):
                        raise ValueError("Initial GTID completed checkpoint verification failed.")
                    advance("schema_ready", finished)
                except Exception:
                    api.stop()
                    raise
                finally:
                    try:
                        source.rollback()
                    finally:
                        source.close()
            # Persist ordinary no_data mode before marking completion. Later
            # starts preserve their current offsets and never re-run recovery.
            api.call("PUT", "/config", config["connector"])
            api.resume()
            advance("complete")
    finally:
        db.close()
