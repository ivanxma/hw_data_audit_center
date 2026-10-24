"""Transport profiles are explicit: managed OCI Kafka and OCI Streaming are different services."""

import re


def backend(queue):
    value = queue.get("kind")
    if value not in {"oci_kafka", "oci_streaming"}:
        raise ValueError("Select an explicit oci_kafka or oci_streaming transport profile.")
    return value


def uses_principal(queue):
    kind = backend(queue)
    mode = queue.get("auth_mode")
    if mode is None:
        mode = (
            "sasl"
            if kind == "oci_kafka" or queue.get("secret_ocid") or queue.get("username")
            else "principal"
        )
    if mode not in {"principal", "sasl"} or (kind == "oci_kafka" and mode != "sasl"):
        raise ValueError("Select a supported messaging authentication mode.")
    return mode == "principal"


def principal_jaas(queue, *, resource_principal=True):
    pool = queue.get("stream_pool_id", "")
    if backend(queue) != "oci_streaming" or not re.fullmatch(
        r"ocid1\.streampool\.[A-Za-z0-9._-]+", pool
    ):
        raise ValueError("A verified OCI stream pool is required for principal authentication.")
    provider = (
        "ResourcePrincipalsLoginModule" if resource_principal else "InstancePrincipalsLoginModule"
    )
    return "com.oracle.bmc.auth.sasl." + provider + ' required intent="streamPoolId:' + pool + '";'


def mechanism(queue):
    if uses_principal(queue):
        return "OCI-RSA-SHA256"
    return "SCRAM-SHA-512" if backend(queue) == "oci_kafka" else "PLAIN"


def jaas(queue, username, password):
    def escape(value):
        if not isinstance(value, str) or any(ord(c) < 32 for c in value):
            raise ValueError("Queue credentials must not contain control characters.")
        return value.replace("\\", "\\\\").replace('"', '\\"')

    module = (
        "scram.ScramLoginModule"
        if mechanism(queue).startswith("SCRAM")
        else "plain.PlainLoginModule"
    )
    return f'org.apache.kafka.common.security.{module} required username="{escape(username)}" password="{escape(password)}";'


def state_topics(queue, name):
    backend(queue)
    configured = queue.get("connect_topics", {})
    if backend(queue) == "oci_streaming":
        harness = queue.get("connect_harness_id", "")
        if not harness.startswith("ocid1.connectharness."):
            raise ValueError("OCI Streaming requires its Kafka Connect harness OCID.")
        for kind in ("config", "offset", "status"):
            if configured.get(kind) != harness + "-" + kind:
                raise ValueError("Use the exact OCI harness-generated " + kind + " topic name.")
    else:
        configured = {
            kind: configured.get(kind) or name + "-" + kind
            for kind in ("config", "offset", "status")
        }
    for value in configured.values():
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,249}", value) or value in {".", ".."}:
            raise ValueError("Invalid Connect state topic.")
    return configured


def producer_settings(queue):
    if backend(queue) == "oci_streaming":
        return {
            "enable.idempotence": "false",
            "acks": "all",
            "max.in.flight.requests.per.connection": "1",
            "retries": "2147483647",
            "max.request.size": "1048576",
        }
    return {
        "enable.idempotence": "true",
        "acks": "all",
        "max.in.flight.requests.per.connection": "5",
        "retries": "2147483647",
    }


def transport_profile(config, flow):
    """A separate Connect state namespace is required for each independently managed connector."""
    profile = dict(config.get(flow["transport"], {}))
    if flow.get("queue_connection"):
        profile.update(flow["queue_connection"])
    if flow.get("connect_harness_id") and profile.get("kind") == "oci_streaming":
        harness = flow["connect_harness_id"]
        profile.update(
            connect_harness_id=harness,
            connect_topics={kind: harness + "-" + kind for kind in ("config", "offset", "status")},
        )
    override = profile.pop("connect_profiles", {}).get(flow["connector_name"], {})
    if not isinstance(override, dict) or set(override) - {"connect_harness_id", "connect_topics"}:
        raise ValueError("Connect profile can override harness and state topics only.")
    profile.update(override)
    return profile
