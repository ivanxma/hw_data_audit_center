"""Choose a verified app-built release for one flow without changing defaults."""

import re

import oci

from . import image_builds
from .image_registry import destination

ROLES = {
    "debezium": ("connector_ocid", "Debezium"),
    "processor": ("processor_ocid", "History writer"),
}


def releases(cloud, role, *, available_only=False):
    if role not in ROLES:
        raise ValueError("Choose Debezium or the history writer.")
    settings = cloud.config.get(role, {})
    repository_id = settings.get("repository_id")
    if not repository_id:
        raise ValueError("Configure this workload's private registry repository first.")
    repository, prefix = destination(cloud, repository_id)
    available = None
    if available_only:
        images = oci.pagination.list_call_get_all_results(
            cloud.registry.list_container_images,
            cloud.compartment,
            repository_id=repository_id,
        ).data
        if not isinstance(images, list):
            images = images.items
        available = {}
        for image in images:
            cloud.check(image)
            if image.repository_id != repository_id:
                raise ValueError("Image does not belong to the configured repository.")
            if image.lifecycle_state == "AVAILABLE":
                available[image.id] = image.digest
    rows = []
    for row in image_builds.inventory():
        if row["status"] != "complete" or row["role"] != role:
            continue
        job = image_builds.load(image_builds.job_path(row["id"]))
        digest = job.get("digest", "")
        if (
            job.get("status") != "complete"
            or job.get("role") != role
            or job.get("runtime_protocol") != 3
            or job.get("compartment_id") != cloud.compartment
            or job.get("region") != cloud.config["region"]
            or job.get("repository_id") != repository_id
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest)
            or not job.get("image_id")
            or (available is not None and available.get(job.get("image_id")) != digest)
        ):
            continue
        rows.append(
            dict(
                id=row["id"],
                tag=job["tag"],
                created_at=job["created_at"],
                repository=repository.display_name,
                digest=digest,
                image_id=job["image_id"],
                image_url=prefix + "@" + digest,
                default=settings.get("image_url") == prefix + "@" + digest,
            )
        )
    return rows


def choices(cloud, flow, role):
    if role not in ROLES:
        raise ValueError("Choose Debezium or the history writer.")
    field, label = ROLES[role]
    if flow["status"] == "retired" or not flow.get(field):
        raise ValueError(f"An existing, non-retired {label} deployment is required.")
    instance = cloud.owned_container(flow[field], flow["id"])
    if instance.lifecycle_state not in {"ACTIVE", "INACTIVE"}:
        raise ValueError(
            "Wait for the current OCI operation to finish, then reopen image selection."
        )
    current = []
    for item in instance.containers:
        container = cloud.check(cloud.containers.get_container(item.container_id).data)
        if container.container_instance_id != flow[field]:
            raise ValueError("Container does not belong to the registered instance.")
        current.append(container.image_url)
    return dict(
        role=role,
        label=label,
        flow_name=flow["name"],
        instance_id=flow[field],
        current_images=current,
        releases=releases(cloud, role, available_only=True),
    )


def select(cloud, role, release_id):
    if not release_id:
        raise ValueError("Choose a published image release in the Update image dialog.")
    release = next((r for r in releases(cloud, role) if r["id"] == release_id), None)
    if not release:
        raise ValueError(
            "This release is not a completed build for the workload's configured repository and VM compartment."
        )
    settings = {
        **cloud.config[role],
        "image_id": release["image_id"],
        "image_url": release["image_url"],
        "image_release": release["tag"],
    }
    # Live OCI availability, repository and digest checks happen before any stop.
    cloud.validate_workload(settings, require_image=True)
    cloud.config = {**cloud.config, role: settings}
    return release
