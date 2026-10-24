import json
import uuid

from confluent_kafka import Consumer, TopicPartition

from .transport import mechanism


def create_consumer(queue, secret, group, *, resource_principal=False):
    from .transport import uses_principal

    if uses_principal(queue):
        from .principal_kafka import PrincipalConsumer

        return PrincipalConsumer(queue, group, resource_principal=resource_principal)
    return Consumer(consumer_config(queue, secret(queue["secret_ocid"]), group))


def consumer_config(queue, password, group):
    return {
        "bootstrap.servers": queue["bootstrap_servers"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": mechanism(queue),
        "sasl.username": queue["username"],
        "sasl.password": password,
        "group.id": group,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "error",
        "allow.auto.create.topics": False,
        "partition.assignment.strategy": "range",
        "socket.timeout.ms": 10000,
        "session.timeout.ms": 30000,
        "enable.ssl.certificate.verification": True,
        "ssl.endpoint.identification.algorithm": "https",
    }


def explore(flow, cloud, limit=25):
    queue = cloud.validate_flow(flow)
    consumer = create_consumer(queue, cloud.secret, "audit-explorer-" + uuid.uuid4().hex)
    try:
        metadata = consumer.list_topics(flow["topic"], timeout=8)
        topic = metadata.topics.get(flow["topic"])
        if not topic or topic.error:
            raise ValueError("Topic is not available.")
        records = []
        for part in list(topic.partitions)[:16]:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(flow["topic"], part), timeout=5
            )
            consumer.assign([TopicPartition(flow["topic"], part, max(low, high - limit))])
            while len(records) < limit:
                msg = consumer.poll(1)
                if msg is None:
                    break
                if msg.error():
                    raise ValueError("Queue read failed.")
                if msg.offset() >= high:
                    break
                records.append(
                    {
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "key": msg.key().decode(errors="replace") if msg.key() else None,
                        "value": json.loads(msg.value()) if msg.value() else None,
                    }
                )
                if msg.offset() + 1 >= high:
                    break
        return records
    finally:
        # Explorer uses direct assignment and never commits or joins the writer consumer group.
        consumer.close()


def validate_registered_topics(flow, cloud):
    from .ingestion import validate_topics
    from .transport import transport_profile

    queue = transport_profile(cloud.config, flow)
    consumer = create_consumer(queue, cloud.secret, "audit-preflight-" + str(flow["id"]))
    try:
        validate_topics(consumer, {**flow, "schema_topic": "audit_" + str(flow["id"])})
    finally:
        consumer.close()  # Metadata only; never joins/commits the writer group.
