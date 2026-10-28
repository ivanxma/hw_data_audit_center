"""Native OCI Streaming profiles; deployment identities stay stable across restarts."""


def backend(queue):
    if queue.get("kind") != "oci_streaming":
        raise ValueError("Only native OCI Streaming connections are supported.")
    return "oci_streaming"


def uses_principal(queue):
    backend(queue)
    return True


def transport_profile(config, flow):
    if flow.get("transport") != "streaming":
        raise ValueError("Select an OCI Streaming connection.")
    profile = dict(config.get("streaming", {}))
    profile.update(flow.get("queue_connection") or {})
    backend(profile)
    pool = profile.get("stream_pool_id") or profile.get("resource_ocid")
    if not pool:
        raise ValueError("Select a verified OCI stream pool.")
    return {"kind": "oci_streaming", "region": config["region"],
            "stream_pool_id": pool, "incarnation": pool}
