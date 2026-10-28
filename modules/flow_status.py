"""Read container process state without confusing OCI instance lifecycle with health."""

import fcntl
import hashlib
import json
import os
import time
from pathlib import Path

import oci

from .config import ROOT, write_private_json

from .connector_health import from_logs
from .process_errors import from_logs as process_error_from_logs


def image_status(cloud, field, item, container):
    role = "debezium" if field == "connector_ocid" else "processor"
    settings = cloud.config.get(role, {})
    selected, deployed = settings.get("image_url"), getattr(container, "image_url", None)
    item["deployed_image"] = deployed or ""
    item["update_available"] = bool(selected and deployed and selected != deployed)
    item["selected_release"] = settings.get("image_release", "")


def inspect_flow(cloud, flow):
    workloads = []
    for field, label in (("connector_ocid", "Debezium"), ("processor_ocid", "History writer")):
        resource_id = flow.get(field)
        item = {"role": label, "state": "NOT_DEPLOYED", "process": "", "exit_code": None}
        log_container = None
        log_text = None
        log_attempted = False
        if resource_id:
            instance = cloud.owned_container(resource_id, flow["id"])
            item["state"] = instance.lifecycle_state
            if instance.lifecycle_state == "INACTIVE" and instance.containers:
                container = cloud.check(
                    cloud.containers.get_container(instance.containers[0].container_id).data
                )
                if container.container_instance_id != resource_id:
                    raise ValueError("Container does not belong to the registered instance.")
                log_container = container
                role = "debezium" if field == "connector_ocid" else "processor"
                image_status(cloud, field, item, container)
                item["guidance"] = (
                    label
                    + " is stopped. Use Update "
                    + ("Debezium" if role == "debezium" else "history writer")
                    + " image to choose a release; Start runs the currently deployed image."
                    if item["update_available"]
                    else label + " is stopped. Start or Resume runs its existing deployed image."
                )
            if instance.lifecycle_state == "ACTIVE":
                processes = []
                for entry in instance.containers:
                    container = cloud.check(cloud.containers.get_container(entry.container_id).data)
                    if container.container_instance_id != resource_id:
                        raise ValueError("Container does not belong to the registered instance.")
                    processes.append(container)
                if processes:
                    log_container = processes[0]
                    image_status(cloud, field, item, processes[0])
                failed = next(
                    (
                        c
                        for c in processes
                        if c.lifecycle_state == "FAILED"
                        or c.lifecycle_details == "CONTAINER_TERMINATED"
                    ),
                    None,
                )
                if failed:
                    item.update(process="EXITED", exit_code=failed.exit_code)
                elif processes and all(
                    c.lifecycle_details == "CONTAINER_RUNNING" for c in processes
                ):
                    item["process"] = "RUNNING"
                    if field == "connector_ocid":
                        try:
                            log_attempted = True
                            response = cloud.containers.retrieve_logs(
                                instance.containers[0].container_id
                            ).data
                            raw = (
                                response.content
                                if hasattr(response, "content")
                                else response.read()
                            )
                            text = (
                                raw.decode(errors="replace") if isinstance(raw, bytes) else str(raw)
                            )
                            log_text = text
                            item["task_state"] = from_logs(text)
                        except Exception:
                            # Lack of log access cannot establish connector readiness.
                            item["task_state"] = "UNAVAILABLE"
                else:
                    item["process"] = "STARTING"
        if log_container is not None:
            try:
                if log_text is None and not log_attempted:
                    response = cloud.containers.retrieve_logs(
                        log_container.id
                        if hasattr(log_container, "id")
                        else instance.containers[0].container_id
                    ).data
                    raw = response.content if hasattr(response, "content") else response.read()
                    log_text = raw.decode(errors="replace") if isinstance(raw, bytes) else str(raw)
                latest = process_error_from_logs(log_text, label) if log_text is not None else None
                if latest:
                    item["last_error"] = latest
                    if item.get("process") == "EXITED" and not item.get("update_available"):
                        item["guidance"] = latest["guidance"]
            except Exception:
                item["error_log_status"] = "unavailable"
        workloads.append(item)
    states = [w["state"] for w in workloads]
    capture_paused = (
        flow.get("last_action") == "stop"
        and flow["status"] != "retired"
        and states[0] == "INACTIVE"
        and workloads[1]["process"] == "RUNNING"
    )
    if capture_paused:
        workloads[0]["guidance"] = "Capture is paused. Start resumes from saved offsets. " + (
            "Workload default differs; Update Debezium image opens a release picker."
            if workloads[0].get("update_available")
            else "The history writer can continue draining queued records."
        )
        workloads[1]["guidance"] = (
            "Capture is paused. Writer drains queued records, then waits. Check queue lag and target progress."
        )
    if flow["status"] == "retired":
        status = "retired"
    elif any(
        w["process"] == "EXITED"
        or w["state"] == "FAILED"
        or w.get("task_state") in {"FAILED", "MISSING_JDBC_MODULE", "STOPPED"}
        for w in workloads
    ):
        status = "attention"
    elif (
        all(w["process"] == "RUNNING" for w in workloads)
        and workloads[0].get("task_state") == "RUNNING"
    ):
        status = "active"
    elif all(s in {"NOT_DEPLOYED", "DELETED"} for s in states):
        status = "draft"
    elif all(s in {"NOT_DEPLOYED", "DELETED", "INACTIVE"} for s in states):
        status = "stopped"
    elif capture_paused:
        status = "paused"
    elif any(s in {"CREATING", "UPDATING"} for s in states):
        status = "stopping" if flow["status"] == "stopping" else "deploying"
    elif any(s == "DELETING" for s in states):
        status = "stopping"
    elif "ACTIVE" in states and "INACTIVE" in states:
        status = "attention"
    elif workloads[0].get("task_state") == "UNAVAILABLE":
        status = "pending"
    elif all(s == "ACTIVE" for s in states):
        status = "starting"
    else:
        status = "pending"
    return {"id": flow["id"], "status": status, "workloads": workloads}


def snapshot(flow, cloud_factory):
    """Share bounded status reads across browser tabs and Gunicorn processes."""
    directory = ROOT / ".data" / "flow-status"
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    config_path = os.environ.get("AUDIT_CONFIG_FILE")
    revision = Path(config_path).stat().st_mtime_ns if config_path else None
    identity = [
        flow["id"],
        flow.get("connector_ocid"),
        flow.get("processor_ocid"),
        flow["status"],
        flow.get("last_action"),
        revision,
    ]
    key = hashlib.sha256(json.dumps(identity).encode()).hexdigest()
    path = directory / (str(int(flow["id"])) + ".json")
    lock_path = path.with_suffix(".lock")
    with lock_path.open("a") as lock:
        os.chmod(lock_path, 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            cached = json.loads(path.read_text())
        except (FileNotFoundError, ValueError):
            cached = {}
        age = time.time() - cached.get("checked_at", 0)
        if cached.get("key") == key and 0 <= age < cached.get("ttl", 0):
            return cached["result"]
        try:
            result = inspect_flow(cloud_factory(), flow)
        except oci.exceptions.ServiceError as error:
            if error.status not in {429, 500, 502, 503, 504}:
                raise
            result = {
                "id": flow["id"],
                "status": "unavailable",
                "workloads": [],
                "message": "OCI workload status is temporarily unavailable. Checks are paced automatically.",
            }
        checked_at = time.time()
        result["checked_at"] = checked_at
        ttl = 10 if result["status"] in {"starting", "stopping", "deploying"} else 45
        write_private_json(
            path, {"key": key, "checked_at": checked_at, "ttl": ttl, "result": result}
        )
        return result
