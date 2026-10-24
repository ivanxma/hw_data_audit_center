"""Bind registered messaging credentials to verified same-compartment services."""

from .resource_catalog import get_resource
from .validation import ocid


def profile(cloud, row):
    kind = row["kind"]
    resource = get_resource(
        cloud, "stream_pool" if kind == "oci_streaming" else "kafka_cluster", row["resource_ocid"]
    )
    if resource.lifecycle_state != "ACTIVE":
        raise ValueError("Selected messaging service must be ACTIVE.")
    from .transport import uses_principal

    principal = uses_principal(row)
    if not principal:
        if not row.get("username") or not row.get("secret_ocid"):
            raise ValueError("Kafka/SASL authentication requires a username and credential secret.")
        cloud.check(cloud.vault.get_secret(ocid(row["secret_ocid"], "vaultsecret")).data)
    result = {
        "kind": kind,
        "username": row["username"],
        "secret_ocid": row["secret_ocid"],
        "incarnation": resource.id,
        "auth_mode": "principal" if principal else "sasl",
    }
    if kind == "oci_streaming":
        if not resource.kafka_settings or not resource.kafka_settings.bootstrap_servers:
            raise ValueError("Stream pool Kafka endpoint is not ready.")
        result.update(
            stream_pool_id=resource.id, bootstrap_servers=resource.kafka_settings.bootstrap_servers
        )
    else:
        allowed = [
            u.url.removeprefix("SASL_SSL://").removeprefix("SSL://")
            for u in resource.kafka_bootstrap_urls or []
        ]
        if not allowed:
            raise ValueError("Kafka bootstrap endpoints are not ready.")
        result.update(
            cluster_id=resource.id, bootstrap_servers=",".join(allowed), replication_factor=3
        )
    return result


def topics(cloud, row):
    selected = profile(cloud, row)
    if selected["kind"] == "oci_streaming":
        import oci

        items = oci.pagination.list_call_get_all_results(
            cloud.streams.list_streams,
            compartment_id=cloud.compartment,
            stream_pool_id=selected["stream_pool_id"],
        ).data
        for item in items:
            cloud.check(item)
        return [
            {"name": i.name, "partitions": i.partitions, "state": i.lifecycle_state}
            for i in items
            if i.lifecycle_state != "DELETED"
        ]
    from confluent_kafka import Consumer

    from .queue_service import consumer_config

    consumer = Consumer(
        consumer_config(selected, cloud.secret(selected["secret_ocid"]), "audit-catalog")
    )
    try:
        metadata = consumer.list_topics(timeout=10)
        return [
            {"name": name, "partitions": len(topic.partitions), "state": "ACTIVE"}
            for name, topic in sorted(metadata.topics.items())
            if not topic.error and not name.startswith("__")
        ]
    finally:
        consumer.close()


def create_topic(db, cloud, row, name, partitions=1, *, actor="topic-setup"):
    import re

    from . import repository as repo
    from .resource_catalog import create

    if not re.fullmatch(r"[A-Za-z0-9._-]{1,249}", name) or name in {".", ".."}:
        raise ValueError("Enter a valid topic name.")
    if not 1 <= partitions <= 1000:
        raise ValueError("Choose between 1 and 1000 partitions.")
    selected = profile(cloud, row)
    if selected["kind"] == "oci_streaming":
        create(
            db,
            cloud,
            "stream",
            {
                "name": name,
                "stream_pool_id": selected["stream_pool_id"],
                "partitions": partitions,
                "retention": 168,
                "compartment_id": cloud.compartment,
            },
            actor,
        )
    else:
        from confluent_kafka.admin import AdminClient, NewTopic

        from .queue_service import consumer_config

        settings = consumer_config(
            selected, cloud.secret(selected["secret_ocid"]), "audit-topic-admin"
        )
        settings = {
            key: value
            for key, value in settings.items()
            if key
            in {
                "bootstrap.servers",
                "security.protocol",
                "sasl.mechanism",
                "sasl.username",
                "sasl.password",
                "enable.ssl.certificate.verification",
                "ssl.endpoint.identification.algorithm",
                "socket.timeout.ms",
            }
        }
        cluster = get_resource(cloud, "kafka_cluster", selected["cluster_id"])
        factor = min(3, cluster.broker_shape.node_count)
        with repo.named_lock(db, "topic-create", selected["cluster_id"], name):
            future = AdminClient(settings).create_topics(
                [
                    NewTopic(
                        name,
                        num_partitions=partitions,
                        replication_factor=factor,
                        config={"cleanup.policy": "delete", "retention.ms": "604800000"},
                    )
                ],
                request_timeout=20,
            )[name]
            try:
                future.result(timeout=25)
            except Exception as error:
                from confluent_kafka import KafkaError, KafkaException

                if (
                    not isinstance(error, KafkaException)
                    or error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS
                ):
                    raise
                raise ValueError(
                    "This topic already exists. Select it from the inventory; ownership was not changed."
                ) from None
    repo.query(
        db,
        "INSERT INTO managed_topics(queue_connection_id,name) VALUES (%s,%s) ON DUPLICATE KEY UPDATE name=name",
        (row["id"], name),
    )


def prepare_schema_topic(db, cloud, flow, actor):
    """Prepare just the schema-change topic; never replace a topic or its contents."""
    row = flow.get("queue_connection")
    if not row:
        raise ValueError("Select a queue connection in the mapping first.")
    if flow.get("status") == "retired":
        raise ValueError("Retired flows cannot prepare new topics.")
    name = "audit_" + str(flow["id"])
    if name == flow["topic"]:
        raise ValueError("The data topic and schema-change topic must have different names.")
    existing = [t for t in topics(cloud, row) if t["name"] == name]
    if len(existing) > 1:
        raise ValueError("Multiple schema topics match this mapping. Review OCI inventory.")
    if existing:
        topic = existing[0]
        if topic["partitions"] != 1:
            raise ValueError("Schema-change topic '" + name + "' must have exactly one partition.")
        if topic["state"] == "ACTIVE":
            return "Schema-change topic '" + name + "' is ACTIVE and ready."
        if topic["state"] == "CREATING":
            return (
                "Schema-change topic '"
                + name
                + "' is being created. Wait for ACTIVE before deployment."
            )
        raise ValueError(
            "Schema-change topic '"
            + name
            + "' is "
            + topic["state"]
            + ". Review OCI resources before retrying."
        )
    create_topic(db, cloud, row, name, 1, actor=actor)
    return (
        "Schema-change topic '"
        + name
        + "' creation submitted. Wait for ACTIVE in OCI resources, then deploy."
    )


def delete_topics(db, cloud, row, names, actor):
    import re

    from . import repository as repo
    from .resource_catalog import delete_selected

    if not 1 <= len(names) <= 25:
        raise ValueError("Select between 1 and 25 topics.")
    selected = profile(cloud, row)
    with repo.named_lock(db, "topic-delete", row["id"]):
        for name in names:
            if not re.fullmatch(r"[A-Za-z0-9._-]{1,249}", name):
                raise ValueError("Invalid topic selection.")
            if repo.query(
                db,
                "SELECT id FROM flows WHERE queue_connection_id=%s AND (topic=%s OR CONCAT('audit_',id)=%s) LIMIT 1",
                (row["id"], name, name),
                one=True,
            ):
                raise ValueError("A selected topic is used by a mapping. Nothing was deleted.")
            if not repo.query(
                db,
                "SELECT name FROM managed_topics WHERE queue_connection_id=%s AND name=%s",
                (row["id"], name),
                one=True,
            ):
                raise ValueError("Only topics created by this app can be deleted here.")
        if selected["kind"] == "oci_streaming":
            import oci

            ids = []
            for name in names:
                streams = oci.pagination.list_call_get_all_results(
                    cloud.streams.list_streams,
                    compartment_id=cloud.compartment,
                    stream_pool_id=selected["stream_pool_id"],
                    name=name,
                ).data
                streams = [s for s in streams if s.lifecycle_state != "DELETED"]
                if len(streams) != 1:
                    raise ValueError("Stream no longer exists; refresh inventory.")
                ids.append(cloud.check(streams[0]).id)
            delete_selected(db, cloud, "stream", ids, actor)
        else:
            from confluent_kafka.admin import AdminClient

            from .queue_service import consumer_config

            settings = consumer_config(
                selected, cloud.secret(selected["secret_ocid"]), "audit-topic-admin"
            )
            settings = {
                key: value
                for key, value in settings.items()
                if key
                in {
                    "bootstrap.servers",
                    "security.protocol",
                    "sasl.mechanism",
                    "sasl.username",
                    "sasl.password",
                    "enable.ssl.certificate.verification",
                    "ssl.endpoint.identification.algorithm",
                    "socket.timeout.ms",
                }
            }
            futures = AdminClient(settings).delete_topics(names, request_timeout=20)
            for future in futures.values():
                future.result(timeout=25)
        repo.record(db, None, "delete-topics", "submitted", actor)
