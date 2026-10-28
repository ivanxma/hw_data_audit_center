"""Register OCI stream pools and manage app-owned native streams."""

import time

import oci

from . import repository as repo
from .resource_catalog import create, delete_selected, get_resource


def profile(cloud, row):
    if row.get("kind") != "oci_streaming":
        raise ValueError("Only OCI Streaming connections are supported.")
    pool = get_resource(cloud, "stream_pool", row["resource_ocid"])
    if pool.lifecycle_state != "ACTIVE":
        raise ValueError("Selected OCI stream pool must be ACTIVE.")
    return {"kind": "oci_streaming", "region": cloud.config["region"],
            "stream_pool_id": pool.id, "incarnation": pool.id}


def pool_reference(db, cloud, pool_id):
    """Resolve a directly selected pool, retaining legacy flow foreign keys internally.

    Call under the mappings lock. No separate user registration is required.
    """
    import hashlib

    row = {"kind": "oci_streaming", "resource_ocid": pool_id}
    profile(cloud, row)  # Validate the live pool and VM compartment before saving anything.
    existing = repo.query(
        db, "SELECT * FROM queue_connections WHERE kind='oci_streaming' AND resource_ocid=%s ORDER BY id LIMIT 1",
        (pool_id,), one=True,
    )
    if existing:
        return existing
    row["name"] = "pool-" + hashlib.sha256(pool_id.encode()).hexdigest()
    row["id"] = repo.query(
        db, "INSERT INTO queue_connections(name,kind,resource_ocid) VALUES (%s,%s,%s)",
        (row["name"], row["kind"], pool_id),
    )
    return row


def topics(cloud, row):
    selected = profile(cloud, row)
    items = oci.pagination.list_call_get_all_results(
        cloud.streams.list_streams, compartment_id=cloud.compartment,
        stream_pool_id=selected["stream_pool_id"],
    ).data
    for item in items:
        cloud.check(item)
    return [{"name": item.name, "partitions": item.partitions, "state": item.lifecycle_state}
            for item in items if item.lifecycle_state != "DELETED"]


def create_topic(db, cloud, row, name, partitions=1, *, actor="stream-setup"):
    import re

    if not re.fullmatch(r"[A-Za-z0-9._-]{1,249}", name) or name in {".", ".."}:
        raise ValueError("Enter a valid stream name.")
    if not 1 <= partitions <= 1000:
        raise ValueError("Choose between 1 and 1000 partitions.")
    selected = profile(cloud, row)
    create(db, cloud, "stream", {"name": name, "stream_pool_id": selected["stream_pool_id"],
           "partitions": partitions, "retention": 168, "compartment_id": cloud.compartment}, actor)
    repo.query(db, "INSERT INTO managed_topics(queue_connection_id,name) VALUES (%s,%s) "
               "ON DUPLICATE KEY UPDATE name=name", (row["id"], name))


def prepare_schema_topic(db, cloud, flow, actor, *, wait=False):
    """Prepare only a missing schema stream; preserve existing stream identities and records."""
    row = flow.get("queue_connection")
    if not row:
        raise ValueError("Select an OCI Streaming connection in the mapping first.")
    if flow.get("status") == "retired":
        raise ValueError("Retired flows cannot prepare new streams.")
    name = "audit_" + str(flow["id"])
    if name == flow["topic"]:
        raise ValueError("Data and schema streams must have different names.")
    deadline = time.monotonic() + (50 if wait else 0)
    created = False
    while True:
        existing = [stream for stream in topics(cloud, row) if stream["name"] == name]
        if len(existing) > 1:
            raise ValueError("Multiple schema streams match this mapping. Review OCI inventory.")
        if not existing:
            if not created:
                create_topic(db, cloud, row, name, 1, actor=actor)
                created = True
        else:
            stream = existing[0]
            if stream["partitions"] != 1:
                raise ValueError("Schema stream must have exactly one partition.")
            if stream["state"] == "ACTIVE":
                return "Schema stream '" + name + "' is ACTIVE and ready."
            if stream["state"] != "CREATING":
                raise ValueError("Schema stream '" + name + "' is " + stream["state"] + ". Review OCI resources.")
        if not wait:
            return "Schema stream '" + name + "' creation submitted."
        if time.monotonic() >= deadline:
            raise ValueError("Schema stream creation is pending. Retry deployment after it becomes ACTIVE.")
        time.sleep(2)


def delete_topics(db, cloud, row, names, actor):
    import re

    if not 1 <= len(names) <= 25:
        raise ValueError("Select between 1 and 25 streams.")
    selected = profile(cloud, row)
    with repo.named_lock(db, "stream-delete", row["id"]):
        ids = []
        for name in names:
            if not re.fullmatch(r"[A-Za-z0-9._-]{1,249}", name):
                raise ValueError("Invalid stream selection.")
            if repo.query(db, "SELECT id FROM flows WHERE queue_connection_id=%s "
                          "AND (topic=%s OR CONCAT('audit_',id)=%s) LIMIT 1",
                          (row["id"], name, name), one=True):
                raise ValueError("A selected stream is used by a mapping. Nothing was deleted.")
            if not repo.query(db, "SELECT name FROM managed_topics WHERE queue_connection_id=%s AND name=%s",
                              (row["id"], name), one=True):
                raise ValueError("Only streams created by this app can be deleted here.")
            streams = oci.pagination.list_call_get_all_results(
                cloud.streams.list_streams, compartment_id=cloud.compartment,
                stream_pool_id=selected["stream_pool_id"], name=name,
            ).data
            streams = [cloud.check(stream) for stream in streams if stream.lifecycle_state != "DELETED"]
            if len(streams) != 1:
                raise ValueError("Stream no longer exists; refresh inventory.")
            ids.append(streams[0].id)
        delete_selected(db, cloud, "stream", ids, actor)
        for name in names:
            repo.query(db, "DELETE FROM managed_topics WHERE queue_connection_id=%s AND name=%s", (row["id"], name))
