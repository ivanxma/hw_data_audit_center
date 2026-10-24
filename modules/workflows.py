"""Flow lifecycle orchestration; routes own HTTP, this service owns transitions."""

import time

from . import repository as repo
from .heatwave import connect, prepare_target, validate_source
from .mappings import validate_mapping


class SourceNotReady(ValueError):
    def __init__(self, flow, result):
        super().__init__("Source replication checks failed.")
        self.flow, self.result = flow, result


class WorkflowService:
    def __init__(self, db, cloud_factory, actor):
        self.db, self.cloud_factory, self.actor = db, cloud_factory, actor

    def _update(self, flow_id, **fields):
        repo.query(
            self.db,
            "UPDATE flows SET " + ",".join(k + "=%s" for k in fields) + " WHERE id=%s",
            [*fields.values(), flow_id],
        )

    def _ready(self, cloud, flow):
        validate_mapping(self.db, flow)
        result = validate_source(flow["source"], flow["source_schema"], flow["source_table"], cloud)
        if not result["ok"]:
            raise SourceNotReady(flow, result)
        cloud.validate_flow(flow)
        from .transport import state_topics, transport_profile

        topics = set(
            state_topics(transport_profile(cloud.config, flow), flow["connector_name"]).values()
        )
        for summary in repo.query(
            self.db, "SELECT id,connector_name,transport FROM flows WHERE id<>%s", (flow["id"],)
        ):
            other = repo.get_flow(self.db, summary["id"])
            if other["transport"] == flow["transport"]:
                other_topics = state_topics(
                    transport_profile(cloud.config, other), other["connector_name"]
                )
                if topics.intersection(other_topics.values()):
                    raise ValueError(
                        "Independent connector groups cannot share Connect state topics or a harness."
                    )
        from .queue_service import validate_registered_topics

        validate_registered_topics(flow, cloud)

    def _wait_stopped(self, cloud, resource_id, flow_id, label, timeout=60):
        deadline = time.monotonic() + timeout
        while True:
            instance = cloud.owned_container(resource_id, flow_id)
            if instance.lifecycle_state == "INACTIVE":
                return True
            if instance.lifecycle_state not in {"ACTIVE", "UPDATING", "STOPPING"}:
                raise ValueError(
                    f"{label} did not stop cleanly. Inspect its OCI state before retrying Update."
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(2, remaining))

    def action(self, flow_id, action, *, image_release_id=None, expected_instance_id=None):
        if action not in {
            "deploy",
            "refresh",
            "terminate",
            "delete",
            "start",
            "stop",
            "stop_all",
            "resume",
            "restart",
            "retire",
            "update_debezium",
            "update_writer",
        }:
            raise ValueError("Unknown workflow action.")
        with repo.named_lock(self.db, "flow", flow_id):
            # Read only after taking the lock; another request may have deployed meanwhile.
            flow = repo.get_flow(self.db, flow_id)
            try:
                if action == "delete":
                    if (
                        flow["status"] != "retired"
                        or flow["processor_ocid"]
                        or flow["connector_ocid"]
                    ):
                        raise ValueError(
                            "Retire and terminate both containers before deleting the registration."
                        )
                    repo.query(self.db, "DELETE FROM flows WHERE id=%s", (flow_id,))
                    status = "complete"
                else:
                    cloud = self.cloud_factory()
                    if action in {"update_debezium", "update_writer"}:
                        from .workload_images import select

                        role, field = (
                            ("debezium", "connector_ocid")
                            if action == "update_debezium"
                            else ("processor", "processor_ocid")
                        )
                        if not expected_instance_id or flow[field] != expected_instance_id:
                            raise ValueError(
                                "The deployed workload changed. Reopen Update image and choose a release again."
                            )
                        select(cloud, role, image_release_id)
                    status = self._operate(cloud, flow, action)
                    # Refresh must not erase the pause intent used by live health checks.
                    self._update(
                        flow_id,
                        status=status,
                        **({} if action == "refresh" else {"last_action": action}),
                    )
                repo.record(
                    self.db,
                    flow_id,
                    action,
                    "submitted" if status != "complete" else status,
                    self.actor,
                )
                return status
            except Exception:
                repo.record(self.db, flow_id, action, "failed", self.actor)
                raise

    def _operate(self, cloud, flow, action):
        flow_id = flow["id"]
        if action in {"update_debezium", "update_writer"}:
            field, role, label, create = (
                ("connector_ocid", "debezium", "Debezium", cloud.create_debezium)
                if action == "update_debezium"
                else ("processor_ocid", "processor", "History writer", cloud.create_processor)
            )
            if flow["status"] == "retired" or not flow[field]:
                raise ValueError(f"An existing, non-retired {label} deployment is required.")
            old_id = flow[field]
            old = cloud.owned_container(old_id, flow_id)
            if old.lifecycle_state not in {"ACTIVE", "INACTIVE"}:
                raise ValueError(
                    f"Wait for {label}'s current OCI operation to finish before updating its image."
                )
            current_images = []
            for item in old.containers:
                container = cloud.check(cloud.containers.get_container(item.container_id).data)
                if container.container_instance_id != old_id:
                    raise ValueError("Container does not belong to the registered instance.")
                current_images.append(container.image_url)
            if cloud.config[role]["image_url"] in current_images:
                raise ValueError(
                    f"This {label} image is already deployed. Choose a different published release."
                )
            self._ready(cloud, flow)
            cloud.validate_workload(cloud.config[role], require_image=True)
            if old.lifecycle_state == "ACTIVE":
                cloud.container_action(old_id, "stop", flow_id)
                self._update(flow_id, status="stopping", last_action=action)
                if not self._wait_stopped(cloud, old_id, flow_id, label):
                    # Never overlap old/new consumers after an unconfirmed shutdown.
                    # The operator can retry once OCI reports INACTIVE.
                    return "stopping"
            # Recheck ownership/state immediately before creation, including inactive retries.
            if cloud.owned_container(old_id, flow_id).lifecycle_state != "INACTIVE":
                raise ValueError(f"{label} must be INACTIVE before creating its replacement.")
            new_id = create(flow, replacement_of=old_id)
            if new_id == old_id:
                raise ValueError(
                    f"This {label} image is already deployed. Choose a different published release."
                )
            self._update(flow_id, **{field: new_id, "status": "deploying"})
            # Keep the stopped previous instance for rollback/explicit retirement.
            # Never delete a resource as a side effect of an image update.
            return "deploying"
        if action == "deploy":
            if flow["status"] == "retired":
                raise ValueError("Retired flows cannot be redeployed.")
            self._ready(cloud, flow)
            with connect(flow["target"], cloud) as target:
                prepare_target(target, flow)
            for field, create in (
                ("connector_ocid", cloud.create_debezium),
                ("processor_ocid", cloud.create_processor),
            ):
                if not flow[field]:
                    value = create(flow)
                    self._update(flow_id, **{field: value, "status": "deploying"})
                    flow[field] = value
            return "deploying"
        if action == "refresh":
            from .flow_status import inspect_flow

            result = inspect_flow(cloud, flow)
            for field in ("connector_ocid", "processor_ocid"):
                if flow[field]:
                    # OCI 404 can also mean denied access. Never clear ownership on 404.
                    obj = cloud.owned_container(flow[field], flow_id)
                    if obj.lifecycle_state == "DELETED":
                        self._update(flow_id, **{field: None})
            return result["status"]
        if action == "terminate":
            if flow["status"] != "retired":
                raise ValueError("Retire the flow before terminating containers.")
            for field in ("connector_ocid", "processor_ocid"):
                if flow[field]:
                    resource = cloud.owned_container(flow[field], flow_id)
                    if resource.lifecycle_state not in {"DELETING", "DELETED"}:
                        cloud.containers.delete_container_instance(flow[field])
            return "retired"
        if not flow["connector_ocid"] and not flow["processor_ocid"]:
            raise ValueError("Deploy the flow before changing its lifecycle.")
        if action == "stop":
            if not flow["connector_ocid"]:
                raise ValueError("There is no deployed Debezium workload to pause.")
            cloud.container_action(flow["connector_ocid"], "stop", flow_id)
            # Leave the writer consuming its existing group until it catches up.
            return "retired" if flow["status"] == "retired" else "stopping"
        if action in {"start", "resume", "restart"}:
            if flow["status"] == "retired":
                raise ValueError("Retired flows cannot be restarted.")
            if not flow["connector_ocid"] or not flow["processor_ocid"]:
                raise ValueError("Complete the partial deployment before starting it.")
            self._ready(cloud, flow)
            # A pre-refactor writer does not enforce the retained-history gate.
            cloud.require_writer_protocol(flow["processor_ocid"], flow_id)
        operation = (
            "restart"
            if action == "restart"
            else "start"
            if action in {"start", "resume"}
            else "stop"
        )
        # Stop extraction first; start apply first so queued changes can drain.
        fields = (
            ("processor_ocid", "connector_ocid")
            if operation == "start"
            else ("connector_ocid", "processor_ocid")
        )
        for field in fields:
            if flow[field]:
                cloud.container_action(flow[field], operation, flow_id)
        if action == "retire" or flow["status"] == "retired":
            return "retired"
        return "starting" if action in {"start", "resume", "restart"} else "stopping"
