"""Resolve native OCI streams through the VM's scoped control-plane identity."""

import json

import oci


def resolve_streams(cloud, flow, queue):
    if queue.get("kind") != "oci_streaming":
        raise ValueError("Select an OCI Streaming connection.")
    pool = cloud.check(cloud.streams.get_stream_pool(queue["stream_pool_id"]).data)
    if pool.lifecycle_state != "ACTIVE":
        raise ValueError("Stream pool must be ACTIVE.")
    if pool.private_endpoint_settings:
        settings = pool.private_endpoint_settings
        subnet = cloud.check(cloud.network.get_subnet(settings.subnet_id).data)
        cloud.check(cloud.network.get_vcn(subnet.vcn_id).data)
        for nsg in settings.nsg_ids or []:
            cloud.check(cloud.network.get_network_security_group(nsg).data)
    names = (flow["topic"], "audit_" + str(flow["id"]))
    if names[0] == names[1]:
        raise ValueError("Data and schema streams must have different names.")
    previous = flow.get("stream_bindings") or {}
    if isinstance(previous, str):
        previous = json.loads(previous)
    streams = {}
    for name in names:
        rows = oci.pagination.list_call_get_all_results(
            cloud.streams.list_streams, compartment_id=cloud.compartment,
            stream_pool_id=pool.id, name=name,
        ).data
        rows = [cloud.check(row) for row in rows if row.lifecycle_state != "DELETED"]
        if len(rows) != 1:
            raise ValueError("Select exactly one existing OCI stream named '" + name + "'.")
        stream = cloud.check(cloud.streams.get_stream(rows[0].id).data)
        if stream.stream_pool_id != pool.id or stream.lifecycle_state != "ACTIVE":
            raise ValueError("Required OCI stream '" + name + "' is " + stream.lifecycle_state + "; it must be ACTIVE in the selected pool.")
        if name == names[1] and stream.partitions != 1:
            raise ValueError("Schema stream must have exactly one partition.")
        if previous and previous.get(name) != stream.id:
            raise ValueError("The registered stream identity changed. Review capture coverage before redeploying.")
        streams[name] = {
            "id": stream.id, "partitions": stream.partitions,
            "messages_endpoint": stream.messages_endpoint,
            "time_created": stream.time_created.isoformat(),
            "retention_in_hours": stream.retention_in_hours,
        }
    return {
        "kind": "oci_streaming", "region": cloud.config["region"],
        "stream_pool_id": pool.id, "incarnation": pool.id, "streams": streams,
    }
