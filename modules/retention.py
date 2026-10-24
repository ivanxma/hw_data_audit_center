"""Explicit month-based expiry with a durable ingestion gate and resumable manifests."""

import json
import uuid
from datetime import date, datetime, timezone

from .fingerprints import canonical, digest
from .partitions import list_partitions, month
from .repository import query
from .validation import quote


def initialize(db, schema, flow_id, table):
    namespace = quote(schema)
    query(
        db,
        f"""CREATE TABLE IF NOT EXISTS {namespace}.audit_retention_state (
        flow_id BIGINT PRIMARY KEY, target_table VARCHAR(64) NOT NULL,
        expiry_cutoff DATETIME(6), updated_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6)
    ) ENGINE=InnoDB""",
    )
    query(
        db,
        f"""CREATE TABLE IF NOT EXISTS {namespace}.audit_cleanup_jobs (
        job_id CHAR(32) PRIMARY KEY, flow_id BIGINT NOT NULL,
        target_table VARCHAR(64) NOT NULL, policy_digest CHAR(64) NOT NULL,
        cutoff DATETIME(6) NOT NULL, plan JSON NOT NULL,
        completed_partitions JSON NOT NULL, status VARCHAR(24) NOT NULL,
        actor VARCHAR(100) NOT NULL, created_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
        updated_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
        INDEX flow_jobs(flow_id,created_at)
    ) ENGINE=InnoDB""",
    )
    owner = query(
        db,
        f"SELECT flow_id FROM {namespace}.audit_retention_state "
        "WHERE target_table=%s AND flow_id<>%s LIMIT 1",
        (table, flow_id),
        one=True,
    )
    if owner:
        raise ValueError(
            "Target table belongs to a previous flow. Use a new target or an explicit migration."
        )
    query(
        db,
        f"INSERT INTO {namespace}.audit_retention_state (flow_id,target_table) "
        "VALUES (%s,%s) ON DUPLICATE KEY UPDATE flow_id=flow_id",
        (flow_id, table),
    )
    row = query(
        db,
        f"SELECT target_table FROM {namespace}.audit_retention_state WHERE flow_id=%s",
        (flow_id,),
        one=True,
    )
    if row["target_table"] != table:
        raise ValueError("Target retention registration conflicts with this flow.")


def check_ingestion(db, flow, events):
    # Shared row lock lasts through the target transaction. Cleanup activation must wait
    # for existing writes; later writes see the durable, monotonic cutoff.
    row = query(
        db,
        f"SELECT expiry_cutoff,target_table FROM {quote(flow['target_schema'])}."
        "audit_retention_state WHERE flow_id=%s FOR SHARE",
        (flow["id"],),
        one=True,
    )
    if not row or row["target_table"] != flow["target_table"]:
        raise ValueError("Target needs retention-state initialization before ingestion.")
    cutoff = row["expiry_cutoff"]
    if cutoff and any(e["event_time"] < cutoff for e in events):
        raise ValueError(
            "Event precedes the retained-history cutoff. Consumption paused; review late data or replay."
        )


def policy_for(config, flow_id):
    value = config.get("retention", {}).get(str(flow_id))
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("Retention policy must be an object.")
    if value.get("enabled") is not True:
        return None
    months = value.get("retain_months")
    if type(months) is not int or not 1 <= months <= 1200:
        raise ValueError("Retention requires retain_months between 1 and 1200.")
    if value.get("archive_required") is not False:
        raise ValueError(
            "Archive-required expiry is blocked until verified archive support is configured."
        )
    if value.get("legal_hold") is not False:
        raise ValueError("Cleanup is blocked by a hold or an unspecified hold decision.")
    return {k: value[k] for k in ("enabled", "retain_months", "archive_required", "legal_hold")}


def eligible_partitions(partitions, cutoff):
    import re

    eligible = []
    for part in partitions:
        name = part["partition_name"]
        if not re.fullmatch(r"p[0-9]{6}", name or ""):
            continue  # Never delete p_older, p_future or an unknown layout.
        try:
            start = date(int(name[1:5]), int(name[5:7]), 1)
            end = date.fromisoformat(str(part["upper_bound"]).strip("'"))
        except ValueError:
            raise ValueError("Invalid monthly partition metadata.")
        if end != month(start, 1):
            raise ValueError("Partition boundary does not match its month.")
        if end <= cutoff:
            eligible.append(
                {
                    "name": name,
                    "upper_bound": end.isoformat(),
                    "estimated_rows": int(part["estimated_rows"] or 0),
                    "data_bytes": int(part["data_bytes"] or 0),
                    "index_bytes": int(part["index_bytes"] or 0),
                }
            )
    return sorted(eligible, key=lambda p: p["upper_bound"])


def preview(db, flow, policy, today=None):
    cutoff = month(today or datetime.now(timezone.utc).date(), -policy["retain_months"])
    return {
        "cutoff": cutoff.isoformat(),
        "partitions": eligible_partitions(
            list_partitions(db, flow["target_schema"], flow["target_table"]), cutoff
        ),
    }


def cleanup(db, flow, policy, actor):
    """One explicitly requested job; DDL is bounded and each completed step is durable."""
    from .repository import named_lock

    policy = policy_for({"retention": {str(flow["id"]): policy}}, flow["id"])
    if policy is None:
        raise ValueError("An enabled retention policy is required.")
    namespace = quote(flow["target_schema"])
    table = f"{namespace}.{quote(flow['target_table'])}"
    with named_lock(db, "cleanup", flow["target_schema"], flow["target_table"]):
        old_timeout = query(db, "SELECT @@SESSION.lock_wait_timeout AS value", one=True)["value"]
        query(db, "SET SESSION lock_wait_timeout=5")
        job = None
        try:
            jobs = f"{namespace}.audit_cleanup_jobs"
            # Resume the frozen manifest even if another month has since started.
            job = query(
                db,
                f"SELECT * FROM {jobs} WHERE flow_id=%s AND target_table=%s "
                "AND status IN ('planned','expiring','failed') ORDER BY created_at LIMIT 1",
                (flow["id"], flow["target_table"]),
                one=True,
            )
            policy_digest = digest(policy)
            if job and job["policy_digest"] != policy_digest:
                raise ValueError(
                    "An unfinished cleanup uses another policy. Review its manifest before changing scope."
                )
            if not job:
                plan = preview(db, flow, policy)
                if len(plan["partitions"]) > 24:
                    raise ValueError(
                        "More than 24 partitions are eligible. Use an explicitly reviewed staged policy."
                    )
                if not plan["partitions"]:
                    return None
                job = {
                    "job_id": uuid.uuid4().hex,
                    "cutoff": plan["cutoff"],
                    "plan": plan["partitions"],
                    "completed_partitions": [],
                }
                query(
                    db,
                    f"INSERT INTO {jobs} (job_id,flow_id,target_table,policy_digest,cutoff,plan,"
                    "completed_partitions,status,actor) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        job["job_id"],
                        flow["id"],
                        flow["target_table"],
                        policy_digest,
                        job["cutoff"],
                        canonical(job["plan"]),
                        "[]",
                        "planned",
                        actor,
                    ),
                )
            plan = json.loads(job["plan"]) if isinstance(job["plan"], str) else job["plan"]
            done = (
                json.loads(job["completed_partitions"])
                if isinstance(job["completed_partitions"], str)
                else job["completed_partitions"]
            )
            if query(
                db,
                f"SELECT flow_id FROM {table} WHERE flow_id<>%s LIMIT 1",
                (flow["id"],),
                one=True,
            ):
                raise ValueError(
                    "Mixed-flow target table cannot be cleaned by a single-flow policy."
                )
            previous_autocommit = db.autocommit
            try:
                db.autocommit = False
                state = query(
                    db,
                    f"SELECT expiry_cutoff,target_table FROM {namespace}.audit_retention_state "
                    "WHERE flow_id=%s FOR UPDATE",
                    (flow["id"],),
                    one=True,
                )
                if not state or state["target_table"] != flow["target_table"]:
                    raise ValueError("Target retention registration is missing or mismatched.")
                query(
                    db,
                    f"UPDATE {namespace}.audit_retention_state SET expiry_cutoff="
                    "GREATEST(COALESCE(expiry_cutoff,%s),%s) WHERE flow_id=%s",
                    (job["cutoff"], job["cutoff"], flow["id"]),
                )
                db.commit()  # Persist the rejection gate before any implicit-commit DDL.
            except Exception:
                db.rollback()
                raise
            finally:
                db.autocommit = previous_autocommit
            query(db, f"UPDATE {jobs} SET status='expiring' WHERE job_id=%s", (job["job_id"],))
            remaining = {
                p["partition_name"]: p
                for p in list_partitions(db, flow["target_schema"], flow["target_table"])
            }
            cutoff = (
                job["cutoff"].date()
                if isinstance(job["cutoff"], datetime)
                else date.fromisoformat(job["cutoff"])
            )
            valid = {p["name"]: p for p in eligible_partitions(list(remaining.values()), cutoff)}
            for part in plan:
                if part["name"] in done:
                    continue
                if part["name"] in remaining:
                    if (
                        part["name"] not in valid
                        or valid[part["name"]]["upper_bound"] != part["upper_bound"]
                    ):
                        raise ValueError(
                            "Partition layout changed since the cleanup plan was frozen."
                        )
                    query(db, f"ALTER TABLE {table} DROP PARTITION {quote(part['name'])}")
                # A missing planned partition can result from DDL succeeding just before a crash.
                done.append(part["name"])
                query(
                    db,
                    f"UPDATE {jobs} SET completed_partitions=%s WHERE job_id=%s",
                    (canonical(done), job["job_id"]),
                )
            query(db, f"UPDATE {jobs} SET status='complete' WHERE job_id=%s", (job["job_id"],))
            return job["job_id"]
        except Exception:
            if job:
                query(
                    db,
                    f"UPDATE {namespace}.audit_cleanup_jobs SET status='failed' WHERE job_id=%s",
                    (job["job_id"],),
                )
            raise
        finally:
            query(db, "SET SESSION lock_wait_timeout=%s", (old_timeout,))
