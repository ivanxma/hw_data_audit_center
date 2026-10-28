"""Compartment-scoped OCI service inventory and explicit, guarded provisioning."""

import json
import uuid
from datetime import datetime, timedelta, timezone

import oci

from . import repository as repo
from .config import app_instance_id
from .fingerprints import digest

# kind: (service tab, label, client attribute, list/get/delete method)
SERVICES = {
    "stream_pool": (
        "streaming",
        "Stream pools",
        "streams",
        "list_stream_pools",
        "get_stream_pool",
        "delete_stream_pool",
    ),
    "stream": ("streaming", "Streams", "streams", "list_streams", "get_stream", "delete_stream"),
    "container": (
        "containers",
        "OCI Container Instances",
        "containers",
        "list_container_instances",
        "get_container_instance",
        "delete_container_instance",
    ),
    "repository": (
        "registry",
        "Container Registry",
        "registry",
        "list_container_repositories",
        "get_container_repository",
        "delete_container_repository",
    ),
    "secret": (
        "vault",
        "Vault secrets",
        "vault",
        "list_secrets",
        "get_secret",
        "schedule_secret_deletion",
    ),
    "vault": ("vault", "Vaults", "kms", "list_vaults", "get_vault", "schedule_vault_deletion"),
    "heatwave": (
        "databases",
        "HeatWave DB systems",
        "mysql",
        "list_db_systems",
        "get_db_system",
        "delete_db_system",
    ),
}
TABS = [
    ("streaming", "OCI Streaming"),
    ("containers", "OCI Container Instances"),
    ("registry", "Container Registry"),
    ("vault", "Vault"),
    ("databases", "HeatWave"),
]


def client_for(cloud, kind):
    if kind not in SERVICES:
        raise ValueError("Select a supported OCI service.")
    name = SERVICES[kind][2]
    return (
        cloud.client(oci.key_management.KmsVaultClient) if name == "kms" else getattr(cloud, name)
    )


def get_resource(cloud, kind, resource_id):
    return cloud.check(getattr(client_for(cloud, kind), SERVICES[kind][4])(resource_id).data)


def registered_resources(db):
    if db is None:
        return set()
    return {
        row["resource_ocid"]
        for row in repo.query(
            db,
            "SELECT resource_ocid FROM resource_requests WHERE status='submitted' AND resource_ocid IS NOT NULL",
        )
    }


def owned_by_app(resource, registered=()):
    tags = getattr(resource, "freeform_tags", None) or {}
    if tags.get("managed-by") != "heatwave-audit":
        return False
    if tags.get("audit-app-id"):
        return tags["audit-app-id"] == app_instance_id()
    # Legacy ownership requires this installation's successful provisioning record.
    return resource.id in registered


def inventory(cloud, kind, db=None):
    method = getattr(client_for(cloud, kind), SERVICES[kind][3])
    rows = oci.pagination.list_call_get_all_results(method, compartment_id=cloud.compartment).data
    if not isinstance(rows, list):
        rows = rows.items
    result = []
    registered = registered_resources(db)
    for row in rows:
        cloud.check(row)
        if getattr(row, "lifecycle_state", None) == "DELETED":
            continue
        result.append(
            {
                "id": row.id,
                "name": getattr(row, "display_name", None)
                or getattr(row, "name", None)
                or getattr(row, "secret_name", None)
                or row.id,
                "kind": kind,
                "state": getattr(row, "lifecycle_state", "UNKNOWN"),
                "managed": owned_by_app(row, registered),
            }
        )
    return result


def integer(form, name, default, low, high):
    try:
        value = int(form.get(name, default))
    except (TypeError, ValueError):
        raise ValueError("Invalid numeric setting: " + name) from None
    if not low <= value <= high:
        raise ValueError("Value out of range: " + name)
    return value


def build_spec(kind, form):
    name = form.get("name", "").strip()
    if not name or len(name) > 100:
        raise ValueError("Enter a resource name of at most 100 characters.")
    if kind == "stream_pool":
        result = {"name": name}
        if form.get("subnet_id"):
            result["private_endpoint_details"] = {"subnet_id": form["subnet_id"]}
    elif kind == "stream":
        result = {
            "name": name,
            "stream_pool_id": form.get("stream_pool_id", ""),
            "partitions": integer(form, "partitions", 1, 1, 1000),
            "retention_in_hours": integer(form, "retention", 168, 24, 168),
        }
    elif kind == "repository":
        result = {"display_name": name, "is_public": False}
    elif kind == "secret":
        import base64

        value = form.get("value", "")
        if not value:
            raise ValueError("Enter a secret value.")
        result = {
            "secret_name": name,
            "vault_id": form.get("vault_id", ""),
            "key_id": form.get("key_id", ""),
            "secret_content": {
                "content_type": "BASE64",
                "content": base64.b64encode(value.encode()).decode(),
            },
        }
    elif kind == "vault":
        result = {"display_name": name, "vault_type": "DEFAULT"}
    elif kind == "heatwave":
        result = {
            "display_name": name,
            "admin_username": form.get("admin_username", "").strip(),
            "admin_password_secret_ocid": form.get("admin_password_secret_ocid", ""),
            "availability_domain": form.get("availability_domain", "").strip(),
            "shape_name": form.get("shape_name", "").strip(),
            "mysql_version": form.get("mysql_version", "").strip(),
            "subnet_id": form.get("subnet_id", ""),
            "data_storage_size_in_gbs": integer(form, "storage", 50, 50, 131072),
            "is_highly_available": False,
        }
        configuration = form.get("configuration_id", "")
        if not configuration:
            raise ValueError("Choose a configuration for the selected DB system shape.")
        if configuration != "__default__":
            result["configuration_id"] = configuration
        if not all(
            (
                result["admin_username"],
                result["availability_domain"],
                result["shape_name"],
                result["mysql_version"],
            )
        ):
            raise ValueError("Complete the DB system settings.")
    else:
        raise ValueError("Create this resource through its dedicated workflow.")
    return result


def create(db, cloud, kind, form, actor):
    if form.get("compartment_id") != cloud.compartment:
        raise ValueError("Select the VM compartment. Other compartments are not permitted.")
    spec = build_spec(kind, form)
    if kind == "heatwave":
        from .heatwave_resources import validate_creation

        validate_creation(cloud, spec)
    name = spec.get("name") or spec.get("display_name") or spec.get("secret_name")
    with repo.named_lock(db, "create", kind, name):
        existing = repo.query(
            db, "SELECT * FROM resource_requests WHERE kind=%s AND name=%s", (kind, name), one=True
        )
        checksum = digest(spec)
        if kind == "secret":
            import hashlib
            import hmac

            from .config import ROOT
            from .fingerprints import canonical

            checksum = hmac.new(
                (ROOT / ".flask_secret_key").read_bytes(), canonical(spec).encode(), hashlib.sha256
            ).hexdigest()
        if existing and existing["status"] == "rejected":
            token = uuid.uuid4().hex
            repo.query(
                db,
                "UPDATE resource_requests SET request_token=%s,spec_digest=%s,status=%s,created_at=UTC_TIMESTAMP() WHERE request_token=%s",
                (token, checksum, "pending", existing["request_token"]),
            )
            existing = {
                **existing,
                "request_token": token,
                "spec_digest": checksum,
                "created_at": datetime.now(timezone.utc).replace(tzinfo=None),
            }
        if existing:
            if existing["resource_ocid"]:
                if kind == "stream":
                    # A repeated topic submission must not duplicate an accepted stream.
                    stream = get_resource(cloud, kind, existing["resource_ocid"])
                    if not owned_by_app(stream, {existing["resource_ocid"]}):
                        raise ValueError(
                            "The previously submitted stream is not managed by this app."
                        )
                    if any(
                        getattr(stream, field) != spec[field]
                        for field in ("name", "stream_pool_id", "partitions", "retention_in_hours")
                    ):
                        raise ValueError(
                            "A stream was already submitted under this name with different settings. "
                            "Select it from inventory or use a different name."
                        )
                    if stream.lifecycle_state not in {"CREATING", "ACTIVE"}:
                        raise ValueError(
                            "The previously submitted stream is "
                            + stream.lifecycle_state
                            + ". Refresh its OCI state or use a different name."
                        )
                    repo.record(db, None, "create-stream", "reused", actor)
                    return stream.id
                raise ValueError("This resource was already submitted. Refresh its OCI state.")
            if existing["spec_digest"] != checksum:
                raise ValueError(
                    "A pending request has different settings. Review it before retrying."
                )
            if datetime.now(timezone.utc).replace(tzinfo=None) - existing["created_at"] > timedelta(
                hours=23
            ):
                raise ValueError(
                    "The request is too old for a safe automatic retry. Review OCI inventory first."
                )
            token = existing["request_token"]
        else:
            token = uuid.uuid4().hex
            repo.query(
                db,
                "INSERT INTO resource_requests(request_token,kind,name,spec_digest,status) VALUES (%s,%s,%s,%s,%s)",
                (token, kind, name, checksum, "pending"),
            )
        try:
            resource_id = cloud.provision(kind, spec, retry_token=token)
        except oci.exceptions.ServiceError as error:
            if error.status in {400, 401, 403, 404}:
                repo.query(
                    db,
                    "UPDATE resource_requests SET status=%s WHERE request_token=%s",
                    ("rejected", token),
                )
            raise
        repo.query(
            db,
            "UPDATE resource_requests SET resource_ocid=%s,status=%s WHERE request_token=%s",
            (resource_id, "submitted", token),
        )
        repo.record(db, None, "create-" + kind, "submitted", actor)
        return resource_id


def in_use(db, cloud, kind, resource_id, resource):
    config = json.dumps(cloud.config)
    if resource_id in config:
        return True
    if repo.query(
        db,
        "SELECT q.id FROM queue_connections q WHERE q.resource_ocid=%s AND "
        "(EXISTS(SELECT 1 FROM flows f WHERE f.queue_connection_id=q.id) OR "
        "EXISTS(SELECT 1 FROM managed_topics m WHERE m.queue_connection_id=q.id)) LIMIT 1",
        (resource_id,),
        one=True,
    ):
        return True
    if repo.query(
        db, "SELECT id FROM connections WHERE secret_ocid=%s LIMIT 1", (resource_id,), one=True
    ):
        return True
    if kind == "container":
        flow = repo.query(
            db,
            "SELECT id,status FROM flows WHERE connector_ocid=%s OR processor_ocid=%s",
            (resource_id, resource_id),
            one=True,
        )
        return bool(flow and flow["status"] != "retired")
    if kind == "stream":
        rows = repo.query(db, "SELECT topic FROM flows")
        if any(r["topic"] == resource.name for r in rows):
            return True
    if kind == "stream_pool":
        streams = oci.pagination.list_call_get_all_results(
            cloud.streams.list_streams,
            compartment_id=cloud.compartment,
            stream_pool_id=resource_id,
        ).data
        if any(s.lifecycle_state != "DELETED" for s in streams):
            return True
    if kind == "vault":
        secrets = oci.pagination.list_call_get_all_results(
            cloud.vault.list_secrets, cloud.compartment, vault_id=resource_id
        ).data
        if secrets:
            return True
    if kind == "heatwave":
        for endpoint in resource.endpoints or []:
            if repo.query(
                db,
                "SELECT id FROM connections WHERE host=%s LIMIT 1",
                (endpoint.ip_address,),
                one=True,
            ):
                return True
    return False


def delete_selected(db, cloud, kind, ids, actor):
    if kind not in SERVICES or not 1 <= len(ids) <= 25:
        raise ValueError("Select between 1 and 25 resources.")
    with repo.named_lock(db, "catalog-delete", "all"):
        checked = []
        registered = registered_resources(db)
        for resource_id in ids:
            resource = get_resource(
                cloud, kind, resource_id
            )  # Always re-read and compartment-check.
            if not owned_by_app(resource, registered):
                raise ValueError(
                    "Only resources created by this app installation can be deleted here."
                )
            if in_use(db, cloud, kind, resource_id, resource):
                raise ValueError("A selected resource is still referenced. Nothing was deleted.")
            checked.append(resource)
        for resource in checked:
            method = getattr(client_for(cloud, kind), SERVICES[kind][5])
            if kind == "secret":
                method(
                    resource.id,
                    oci.vault.models.ScheduleSecretDeletionDetails(
                        time_of_deletion=datetime.now(timezone.utc) + timedelta(days=8)
                    ),
                )
            elif kind == "vault":
                method(
                    resource.id,
                    oci.key_management.models.ScheduleVaultDeletionDetails(
                        time_of_deletion=datetime.now(timezone.utc) + timedelta(days=8)
                    ),
                )
            else:
                method(resource.id)
            repo.record(db, None, "delete-" + kind, "submitted", actor)
