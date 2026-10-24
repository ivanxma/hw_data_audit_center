"""Kafka consumer backed by Oracle's Java SASL provider for OCI principals.

Keeps Kafka partitions, group offsets and the existing target checkpoint protocol.
The JVM is shared within a process; each consumer belongs to one Python thread.
"""

import os
import threading
from pathlib import Path
from types import SimpleNamespace

from confluent_kafka import TopicPartition

from .config import ROOT

_JVM_LOCK = threading.Lock()


def java_runtime():
    import jpype

    with _JVM_LOCK:
        if not jpype.isJVMStarted():
            location = Path(
                os.environ.get(
                    "AUDIT_STREAMING_JAVA_LIB_DIR",
                    "/opt/streaming-libs"
                    if Path("/opt/streaming-libs").is_dir()
                    else str(ROOT / ".data/java/streaming-libs"),
                )
            )
            jars = sorted(str(path) for path in location.glob("*.jar"))
            if not jars:
                raise ValueError(
                    "OCI Streaming principal support is not installed. Run scripts/install_streaming_java.py on the OL9 VM and rebuild workload images."
                )
            jpype.startJVM(
                "-Xms32m",
                "-Xmx256m",
                "-Dorg.slf4j.simpleLogger.defaultLogLevel=error",
                classpath=jars,
                convertStrings=True,
            )
            jpype.JClass("com.oracle.bmc.auth.sasl.OciSaslClientProvider").initialize()
    return jpype


class PrincipalMessage:
    def __init__(self, record):
        self.record = record

    def topic(self):
        return str(self.record.topic())

    def partition(self):
        return int(self.record.partition())

    def offset(self):
        return int(self.record.offset())

    def key(self):
        value = self.record.key()
        return bytes(value) if value is not None else None

    def value(self):
        value = self.record.value()
        return bytes(value) if value is not None else None

    def error(self):
        return None


class PrincipalConsumer:
    def __init__(self, queue, group, *, resource_principal=False):
        from .transport import principal_jaas

        self.java = java_runtime()
        self.jclass = self.java.JClass
        self.duration = self.jclass("java.time.Duration")
        self.partition = self.jclass("org.apache.kafka.common.TopicPartition")
        props = self.jclass("java.util.Properties")()
        settings = {
            "bootstrap.servers": queue["bootstrap_servers"],
            "group.id": group,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "OCI-RSA-SHA256",
            "sasl.jaas.config": principal_jaas(queue, resource_principal=resource_principal),
            "key.deserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer",
            "value.deserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer",
            "enable.auto.commit": "false",
            "auto.offset.reset": "none",
            "allow.auto.create.topics": "false",
            "partition.assignment.strategy": "org.apache.kafka.clients.consumer.RangeAssignor",
            "max.poll.records": "1",
            "max.partition.fetch.bytes": "1048576",
            "ssl.endpoint.identification.algorithm": "https",
            "default.api.timeout.ms": "15000",
            "request.timeout.ms": "15000",
            "session.timeout.ms": "30000",
        }
        for key, value in settings.items():
            props.setProperty(key, value)
        self.client = self.jclass("org.apache.kafka.clients.consumer.KafkaConsumer")(props)
        self.in_assignment = False
        self.listener = None

    def _parts(self, parts):
        values = self.jclass("java.util.ArrayList")()
        for part in parts:
            values.add(self.partition(part.topic, part.partition))
        return values

    def list_topics(self, topic=None, timeout=10):
        duration = self.duration.ofMillis(int(timeout * 1000))
        if topic is None:
            items = self.client.listTopics(duration)
            values = [(str(entry.getKey()), entry.getValue()) for entry in items.entrySet()]
        else:
            values = [(topic, self.client.partitionsFor(topic, duration))]
        return SimpleNamespace(
            topics={
                name: SimpleNamespace(
                    error=None, partitions={int(p.partition()): None for p in parts}
                )
                for name, parts in values
                if parts is not None
            }
        )

    def get_watermark_offsets(self, part, timeout=10, cached=False):
        parts = self._parts([part])
        duration = self.duration.ofMillis(int(timeout * 1000))
        key = parts.get(0)
        return int(self.client.beginningOffsets(parts, duration).get(key)), int(
            self.client.endOffsets(parts, duration).get(key)
        )

    def committed(self, parts, timeout=10):
        keys = self._parts(parts)
        result = self.client.committed(
            self.jclass("java.util.HashSet")(keys), self.duration.ofMillis(int(timeout * 1000))
        )
        return [
            TopicPartition(
                part.topic,
                part.partition,
                int(result.get(key).offset()) if result.get(key) is not None else -1,
            )
            for part, key in zip(parts, keys)
        ]

    def assign(self, parts):
        keys = self._parts(parts)
        # Java has already assigned the partitions before invoking its callback.
        # Seeking preserves the subscription; manual assign would cancel the group.
        if not self.in_assignment:
            self.client.assign(keys)
        for part, key in zip(parts, keys):
            self.client.seek(key, self.java.JLong(part.offset))

    def subscribe(self, topics, on_assign, on_revoke, on_lost):
        consumer = self

        def convert(parts):
            return [TopicPartition(str(p.topic()), int(p.partition())) for p in parts]

        class Listener:
            def onPartitionsAssigned(self, parts):
                consumer.in_assignment = True
                try:
                    on_assign(consumer, convert(parts))
                finally:
                    consumer.in_assignment = False

            def onPartitionsRevoked(self, parts):
                on_revoke(consumer, convert(parts))

            def onPartitionsLost(self, parts):
                on_lost(consumer, convert(parts))

        self.listener = self.java.JProxy(
            "org.apache.kafka.clients.consumer.ConsumerRebalanceListener", inst=Listener()
        )
        names = self.jclass("java.util.ArrayList")()
        for topic in topics:
            names.add(topic)
        self.client.subscribe(names, self.listener)

    def poll(self, timeout=0.1):
        records = self.client.poll(self.duration.ofMillis(int(timeout * 1000)))
        if records.count() > 1:
            raise RuntimeError("Unexpected consumer batch size; no acknowledgement sent.")
        return PrincipalMessage(records.iterator().next()) if records.count() else None

    def commit(self, offsets, asynchronous=False):
        if asynchronous:
            raise ValueError("Principal consumer requires synchronous acknowledgement.")
        values = self.jclass("java.util.HashMap")()
        offset_type = self.jclass("org.apache.kafka.clients.consumer.OffsetAndMetadata")
        for part in offsets:
            values.put(
                self.partition(part.topic, part.partition),
                offset_type(self.java.JLong(part.offset)),
            )
        self.client.commitSync(values, self.duration.ofSeconds(15))
        return offsets

    def close(self):
        self.client.close(self.duration.ofSeconds(10))
