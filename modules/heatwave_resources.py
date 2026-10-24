"""Live MySQL service choices and compartment-checked DB system lifecycle actions."""

import uuid

import oci

from . import repository as repo
from .service_errors import oci_failure


def choices(cloud, kind, shape_name=None, availability_domain=None):
    client = cloud.client(oci.mysql.MysqlaasClient)
    if kind == "mysql_shape":
        if not availability_domain:
            return []
        rows = oci.pagination.list_call_get_all_results(
            client.list_shapes,
            cloud.compartment,
            is_supported_for=["DBSYSTEM"],
            availability_domain=availability_domain,
        ).data
        return sorted(
            [
                {"id": row.name, "name": row.name + " · " + str(row.memory_size_in_gbs) + " GB RAM"}
                for row in rows
                if "DBSYSTEM" in row.is_supported_for
            ],
            key=lambda row: row["id"],
        )
    if kind == "mysql_version":
        families = oci.pagination.list_call_get_all_results(
            client.list_versions, cloud.compartment
        ).data
        return [
            {"id": version.version, "name": version.version + " · " + family.version_family}
            for family in families
            for version in family.versions
        ]
    if kind == "mysql_configuration":
        if not shape_name:
            return []
        rows = oci.pagination.list_call_get_all_results(
            client.list_configurations,
            cloud.compartment,
            shape_name=shape_name,
            lifecycle_state="ACTIVE",
            type=["CUSTOM"],
        ).data
        result = [{"id": "__default__", "name": "OCI default for " + shape_name}]
        for row in rows:
            # Oracle's global default templates are used implicitly, never attached as a foreign resource.
            if row.type != "CUSTOM":
                continue
            cloud.check(row)
            if row.shape_name == shape_name and row.lifecycle_state == "ACTIVE":
                result.append({"id": row.id, "name": row.display_name + " · " + row.shape_name})
        return result
    raise ValueError("Select a supported MySQL service option.")


def validate_creation(cloud, spec):
    from .service_options import choices as general_choices

    domain = spec.get("availability_domain")
    shape = spec.get("shape_name")
    if domain not in {row["id"] for row in general_choices(cloud, "availability_domain")}:
        raise ValueError("Choose an availability domain returned by OCI in this region.")
    if shape not in {
        row["id"] for row in choices(cloud, "mysql_shape", availability_domain=domain)
    }:
        raise ValueError("Choose an available DB system shape for this availability domain.")
    if spec.get("mysql_version") not in {row["id"] for row in choices(cloud, "mysql_version")}:
        raise ValueError("Choose a MySQL version currently available from OCI.")
    configuration_id = spec.get("configuration_id")
    if configuration_id:
        config = cloud.check(
            cloud.client(oci.mysql.MysqlaasClient).get_configuration(configuration_id).data
        )
        if (
            config.shape_name != shape
            or config.lifecycle_state != "ACTIVE"
            or config.type != "CUSTOM"
        ):
            raise ValueError(
                "Choose an active configuration for the selected shape in the VM compartment."
            )


def lifecycle(db, cloud, ids, action, actor):
    if action not in {"start", "stop"} or not 1 <= len(ids) <= 25 or len(set(ids)) != len(ids):
        raise ValueError("Select between 1 and 25 distinct DB systems and choose Start or Stop.")
    required = "INACTIVE" if action == "start" else "ACTIVE"
    # DB systems report INACTIVE after shutdown; UI and server use the same OCI state.
    with repo.named_lock(db, "catalog-delete", "all"):
        checked = []
        for resource_id in ids:
            response = cloud.mysql.get_db_system(resource_id)
            row = cloud.check(response.data)
            if row.lifecycle_state != required:
                raise ValueError(
                    f"{row.display_name} is {row.lifecycle_state}. "
                    f"{action.capitalize()} requires {required}. "
                    "Refresh the DB state and wait for any active operation to finish."
                )
            checked.append(row)
        results = []
        for row in checked:
            kwargs = {
                "opc_retry_token": uuid.uuid4().hex,
                "retry_strategy": oci.retry.NoneRetryStrategy(),
            }
            try:
                # HeatWave lifecycle POSTs intentionally omit optional If-Match.
                # GET representation/version ETags have caused repeat 412 failures.
                if action == "start":
                    response = cloud.mysql.start_db_system(row.id, **kwargs)
                else:
                    response = cloud.mysql.stop_db_system(
                        row.id,
                        oci.mysql.models.StopDbSystemDetails(shutdown_type="FAST"),
                        **kwargs,
                    )
                request_id = response.headers.get("opc-work-request-id", "")
                repo.record(db, None, action + "-heatwave", "submitted", actor)
                results.append(
                    {"name": row.display_name, "status": "submitted", "request_id": request_id}
                )
            except oci.exceptions.ServiceError as error:
                diagnostic = oci_failure(error)
                repo.record(db, None, action + "-heatwave", "failed", actor)
                results.append(
                    {
                        "name": row.display_name,
                        "status": "failed",
                        "message": (
                            f"Could not {action} {row.display_name}. "
                            f"Last checked DB state: {row.lifecycle_state}. "
                            + diagnostic["message"]
                            + " "
                            + diagnostic["guidance"]
                        ),
                        "request_id": diagnostic["request_id"],
                    }
                )
        return results
