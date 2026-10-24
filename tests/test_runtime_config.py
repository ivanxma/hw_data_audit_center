import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from modules.config import load_config, save_config
from modules.oci_service import Cloud


class RuntimeConfigTests(unittest.TestCase):
    def test_processor_payload_uses_only_runtime_connection_fields(self):
        cloud = Cloud.__new__(Cloud)
        cloud.compartment = "scope"
        cloud.validate_flow = MagicMock()
        cloud.network = MagicMock()
        cloud.registry = MagicMock()
        cloud.containers = MagicMock()
        subnet = cloud.network.get_subnet.return_value.data
        subnet.compartment_id = "scope"
        subnet.vcn_id = "vcn"
        cloud.network.get_vcn.return_value.data.compartment_id = "scope"
        repository = cloud.registry.get_container_repository.return_value.data
        repository.compartment_id = "scope"
        repository.is_public = False
        repository.namespace = "namespace"
        repository.display_name = "worker"
        cloud.config = {
            "region": "region",
            "fingerprint_secret_ocid": "ocid1.vaultsecret.test.key",
            "mysql_tls": {"mode": "REQUIRED"},
            "processor": {
                "subnet_id": "subnet",
                "repository_id": "repository",
                "registry_endpoint": "registry.example.invalid",
                "image_url": "registry.example.invalid/namespace/worker@sha256:" + "a" * 64,
                "availability_domain": "ad",
                "shape": "CI.Standard.E4.Flex",
            },
            "streaming": {"kind": "oci_streaming", "incarnation": "pool"},
        }
        flow = {
            "id": 1,
            "name": "orders",
            "connector_name": "audit-orders",
            "source_schema": "sales",
            "source_table": "orders",
            "target_schema": "audit",
            "target_table": "history",
            "topic": "orders",
            "transport": "streaming",
            "source": {"server_identity": "source"},
            "target": {
                "host": "target.example.invalid",
                "port": 3306,
                "username": "writer",
                "secret_ocid": "ocid1.vaultsecret.test.database",
                "database_name": "audit",
                "created_at": datetime(2026, 10, 24),
                "private_note": "must-not-ship",
            },
        }
        cloud.create_processor(flow)
        details = cloud.containers.create_container_instance.call_args.args[0]
        encoded = details.containers[0].environment_variables["AUDIT_WORKER_CONFIG"]
        payload = json.loads(encoded)
        self.assertEqual(
            payload["target"],
            {
                "host": "target.example.invalid",
                "port": 3306,
                "username": "writer",
                "secret_ocid": "ocid1.vaultsecret.test.database",
                "ssl_mode": "REQUIRED",
                "ssl_ca": None,
            },
        )
        self.assertNotIn("created_at", encoded)
        self.assertNotIn("must-not-ship", encoded)
        self.assertEqual(payload["schema_topic"], "audit_1")
        self.assertEqual(
            payload["fingerprint_secret_ocid"], cloud.config["fingerprint_secret_ocid"]
        )

    def test_app_configuration_uses_the_app_specific_environment(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "deployment.json"
            value = {"compartment_id": "scope", "region": "region", "processor": {"ocpus": 2}}
            path.write_text(json.dumps(value))
            with patch.dict(os.environ, {"AUDIT_CONFIG_FILE": str(path)}, clear=True):
                self.assertEqual(load_config(required=True), value)
                updated = {**value, "processor": {"ocpus": 4}}
                save_config(updated, "scope", "region")
                self.assertEqual(load_config(required=True), updated)
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)
                with self.assertRaises(ValueError):
                    save_config({**value, "compartment_id": "another"}, "scope", "region")

    def test_missing_configuration_fails_closed(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(load_config(), {})
            with self.assertRaisesRegex(ValueError, "AUDIT_CONFIG_FILE"):
                load_config(required=True)

    def test_workload_uses_private_container_instances_and_stable_retry_token(self):
        cloud = Cloud.__new__(Cloud)
        cloud.compartment = "scope"
        cloud.network = MagicMock()
        cloud.registry = MagicMock()
        cloud.vault = MagicMock()
        cloud.containers = MagicMock()
        subnet = MagicMock()
        subnet.compartment_id = "scope"
        subnet.vcn_id = "vcn"
        cloud.network.get_subnet.return_value.data = subnet
        cloud.network.get_vcn.return_value.data.compartment_id = "scope"
        repository = MagicMock()
        repository.compartment_id = "scope"
        repository.is_public = False
        repository.namespace = "namespace"
        repository.display_name = "worker"
        cloud.registry.get_container_repository.return_value.data = repository
        cloud.vault.get_secret.return_value.data.compartment_id = "scope"
        cloud.containers.create_container_instance.return_value.data.id = "created-instance"
        settings = {
            "subnet_id": "subnet",
            "repository_id": "repository",
            "registry_endpoint": "registry.example.invalid",
            "image_url": "registry.example.invalid/namespace/worker@sha256:" + "a" * 64,
            "image_pull_secret_ocid": "secret",
            "availability_domain": "ad",
            "shape": "CI.Standard.E4.Flex",
        }
        flow = {"id": 1, "connector_name": "audit-test"}
        payload = {"compartment_id": "scope", "writer_protocol": 2}
        first = cloud.create_workload(
            flow, settings, payload, "history-writer", ["python", "worker.py"]
        )
        token = cloud.containers.create_container_instance.call_args.kwargs["opc_retry_token"]
        second = cloud.create_workload(
            flow, settings, payload, "history-writer", ["python", "worker.py"]
        )
        self.assertEqual(first, second)
        self.assertEqual(
            token, cloud.containers.create_container_instance.call_args.kwargs["opc_retry_token"]
        )
        details = cloud.containers.create_container_instance.call_args.args[0]
        self.assertEqual(details.compartment_id, "scope")
        self.assertFalse(details.vnics[0].is_public_ip_assigned)
        self.assertFalse(details.containers[0].is_resource_principal_disabled)
        self.assertEqual(details.freeform_tags["flow-id"], "1")
        replacement_tokens = []
        for previous_instance in ("old-instance", "old-instance", "later-instance"):
            cloud.create_workload(
                flow,
                settings,
                payload,
                "history-writer",
                ["python", "worker.py"],
                replacement_of=previous_instance,
            )
            replacement_tokens.append(
                cloud.containers.create_container_instance.call_args.kwargs["opc_retry_token"]
            )
        self.assertEqual(replacement_tokens[0], replacement_tokens[1])
        self.assertNotEqual(token, replacement_tokens[0])
        self.assertNotEqual(replacement_tokens[0], replacement_tokens[2])
        # A new principal-based workload has no mandatory Vault image credential.
        cloud.vault.get_secret.reset_mock()
        settings.pop("image_pull_secret_ocid")
        settings["image_pull_auth"] = "resource_principal"
        cloud.create_workload(flow, settings, payload, "history-writer", ["python", "worker.py"])
        cloud.vault.get_secret.assert_not_called()
        details = cloud.containers.create_container_instance.call_args.args[0]
        self.assertIsNone(details.image_pull_secrets)
        settings["image_id"] = "published-image"
        image = cloud.registry.get_container_image.return_value.data
        image.compartment_id = "scope"
        image.lifecycle_state = "AVAILABLE"
        image.repository_id = "repository"
        image.digest = "sha256:" + "a" * 64
        cloud.validate_workload(settings, require_image=True)
        for field, invalid in (
            ("compartment_id", "another-compartment"),
            ("lifecycle_state", "DELETING"),
            ("repository_id", "another-repository"),
            ("digest", "sha256:" + "b" * 64),
        ):
            original = getattr(image, field)
            setattr(image, field, invalid)
            with self.subTest(field=field), self.assertRaises(ValueError):
                cloud.validate_workload(settings, require_image=True)
            setattr(image, field, original)
