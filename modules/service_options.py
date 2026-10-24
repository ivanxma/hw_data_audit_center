"""Selectable references resolved only within the VM compartment."""

import oci

from .resource_catalog import get_resource, inventory


def choices(
    cloud, kind, vault_id=None, repository_id=None, shape_name=None, availability_domain=None
):
    if kind in {"mysql_shape", "mysql_configuration", "mysql_version"}:
        from .heatwave_resources import choices as mysql_choices

        return mysql_choices(cloud, kind, shape_name, availability_domain)
    if kind == "availability_domain":
        client = cloud.client(oci.identity.IdentityClient)
        rows = client.list_availability_domains(cloud.compartment).data
        return [{"id": r.name, "name": r.name} for r in rows]
    if kind == "container_shape":
        rows = oci.pagination.list_call_get_all_results(
            cloud.containers.list_container_instance_shapes, cloud.compartment
        ).data
        if not isinstance(rows, list):
            rows = rows.items
        return [{"id": r.name, "name": r.name} for r in rows]
    if kind == "image":
        if not repository_id:
            return []
        get_resource(cloud, "repository", repository_id)
        rows = oci.pagination.list_call_get_all_results(
            cloud.registry.list_container_images, cloud.compartment, repository_id=repository_id
        ).data
        if not isinstance(rows, list):
            rows = rows.items
        result = []
        for row in rows:
            cloud.check(row)
            if row.lifecycle_state in {"DELETED", "DELETING"}:
                continue
            if row.repository_id != repository_id:
                raise ValueError("Image does not belong to the selected repository.")
            result.append(
                {"id": row.id, "name": row.version or row.display_name, "digest": row.digest}
            )
        return sorted(result, key=lambda row: row["name"], reverse=True)
    if kind == "subnet":
        rows = oci.pagination.list_call_get_all_results(
            cloud.network.list_subnets, compartment_id=cloud.compartment
        ).data
        result = []
        for row in rows:
            cloud.check(row)
            cloud.check(cloud.network.get_vcn(row.vcn_id).data)
            result.append({"id": row.id, "name": row.display_name + " · " + row.cidr_block})
        return result
    if kind == "key":
        if not vault_id:
            return []
        vault = get_resource(cloud, "vault", vault_id)
        client = oci.key_management.KmsManagementClient(
            cloud.sdk_config, service_endpoint=vault.management_endpoint, signer=cloud.signer
        )
        rows = oci.pagination.list_call_get_all_results(
            client.list_keys, compartment_id=cloud.compartment
        ).data
        result = []
        for row in rows:
            cloud.check(row)
            if row.lifecycle_state == "ENABLED":
                # KeySummary omits key_shape; fetch details from this vault's endpoint.
                key = cloud.check(client.get_key(row.id).data)
                if key.lifecycle_state == "ENABLED" and key.key_shape.algorithm == "AES":
                    result.append({"id": key.id, "name": key.display_name})
        return result
    return [
        r
        for r in inventory(cloud, kind)
        if r["state"] not in {"DELETED", "DELETING", "PENDING_DELETION"}
    ]
