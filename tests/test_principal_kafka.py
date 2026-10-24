import unittest
from types import SimpleNamespace as Obj
from unittest.mock import MagicMock, patch

from confluent_kafka import TopicPartition

from modules.principal_kafka import PrincipalConsumer, PrincipalMessage
from modules.queue_connections import profile
from modules.queue_service import create_consumer
from modules.transport import mechanism, principal_jaas, uses_principal

POOL = "ocid1.streampool.oc1.region.fixture"
QUEUE = {
    "kind": "oci_streaming",
    "stream_pool_id": POOL,
    "bootstrap_servers": "streaming.example.invalid:9092",
    "username": "",
    "secret_ocid": "",
}


class PrincipalKafkaTests(unittest.TestCase):
    def test_new_streaming_uses_principals_and_preserves_existing_sasl_connections(self):
        self.assertTrue(uses_principal(QUEUE))
        self.assertEqual(mechanism(QUEUE), "OCI-RSA-SHA256")
        legacy = {**QUEUE, "username": "existing-user", "secret_ocid": "existing-token"}
        self.assertFalse(uses_principal(legacy))
        self.assertEqual(mechanism(legacy), "PLAIN")
        self.assertFalse(uses_principal({"kind": "oci_kafka"}))
        self.assertIn("ResourcePrincipalsLoginModule", principal_jaas(QUEUE))
        self.assertIn(
            "InstancePrincipalsLoginModule", principal_jaas(QUEUE, resource_principal=False)
        )
        self.assertNotIn("password", principal_jaas(QUEUE))
        with self.assertRaises(ValueError):
            principal_jaas({**QUEUE, "stream_pool_id": POOL + '"; invalid'})

    def test_principal_consumer_never_reads_queue_credentials(self):
        secret = MagicMock()
        with patch("modules.principal_kafka.PrincipalConsumer") as factory:
            create_consumer(QUEUE, secret, "writer", resource_principal=True)
            factory.assert_called_once_with(QUEUE, "writer", resource_principal=True)
        secret.assert_not_called()

    def test_principal_profile_checks_pool_without_requiring_vault(self):
        cloud = MagicMock()
        pool = Obj(
            id=POOL,
            lifecycle_state="ACTIVE",
            kafka_settings=Obj(bootstrap_servers="streaming.example.invalid:9092"),
        )
        with patch("modules.queue_connections.get_resource", return_value=pool):
            result = profile(cloud, {**QUEUE, "resource_ocid": POOL})
        self.assertEqual(result["auth_mode"], "principal")
        cloud.vault.get_secret.assert_not_called()

    def test_java_rebalance_seeks_without_cancelling_the_group_subscription(self):
        consumer = PrincipalConsumer.__new__(PrincipalConsumer)
        consumer.client = MagicMock()
        consumer.java = Obj(JLong=int)
        consumer._parts = MagicMock(return_value=["partition"])
        consumer.in_assignment = True
        consumer.assign([TopicPartition("orders", 0, 42)])
        consumer.client.assign.assert_not_called()
        consumer.client.seek.assert_called_once_with("partition", 42)
        consumer.in_assignment = False
        consumer.assign([TopicPartition("orders", 0, 42)])
        consumer.client.assign.assert_called_once_with(["partition"])

    def test_poll_refuses_unexpected_batch_instead_of_dropping_records(self):
        consumer = PrincipalConsumer.__new__(PrincipalConsumer)
        consumer.duration = MagicMock()
        consumer.client = MagicMock()
        consumer.client.poll.return_value.count.return_value = 2
        with self.assertRaises(RuntimeError):
            consumer.poll(0.1)
        consumer.client.commitSync.assert_not_called()

    def test_tombstones_and_offsets_are_preserved(self):
        record = MagicMock()
        record.key.return_value = b"key"
        record.value.return_value = None
        record.offset.return_value = 42
        record.partition.return_value = 3
        record.topic.return_value = "orders"
        message = PrincipalMessage(record)
        self.assertEqual(message.key(), b"key")
        self.assertIsNone(message.value())
        self.assertEqual(
            (message.topic(), message.partition(), message.offset()), ("orders", 3, 42)
        )
