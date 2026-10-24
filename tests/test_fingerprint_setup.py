import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace as Obj
from unittest.mock import MagicMock, patch

import oci
import test_core
from test_service_catalog import cloud

from modules.config import load_config, save_config
from modules.fingerprint_setup import setup
from modules.service_options import choices


class FingerprintSetupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "deployment.json"
        self.config = {"compartment_id": "scope", "region": "region", "processor": {"ocpus": 2}}
        self.path.write_text(json.dumps(self.config))
        self.c = cloud()
        self.c.config = self.config
        self.c.sdk_config = {"region": "region"}
        self.c.signer = MagicMock()
        self.c.client = MagicMock()
        self.vault = Obj(
            id="vault",
            compartment_id="scope",
            lifecycle_state="ACTIVE",
            management_endpoint="https://vault.example.invalid",
        )
        self.c.client.return_value.get_vault.return_value.data = self.vault
        self.kms = MagicMock()
        self.kms.get_key.return_value.data = Obj(
            id="key",
            compartment_id="scope",
            lifecycle_state="ENABLED",
            key_shape=Obj(algorithm="AES"),
            display_name="encryption-key",
        )
        self.secret = Obj(
            id="ocid1.vaultsecret.test.fingerprint",
            secret_name="audit-row-fingerprint-" + "a" * 32,
            compartment_id="scope",
            vault_id="vault",
            key_id="key",
            lifecycle_state="ACTIVE",
            freeform_tags={
                "managed-by": "heatwave-audit",
                "audit-app-id": "a" * 32,
                "audit-purpose": "row-fingerprint",
            },
        )
        self.c.vault.get_secret.return_value.data = self.secret
        self.c.vault.create_secret.return_value.data = self.secret
        self.c.secret = MagicMock(return_value="1" * 64)
        for target, value in (
            ("modules.fingerprint_setup.ROOT", self.root),
            ("modules.fingerprint_setup.app_instance_id", lambda: "a" * 32),
        ):
            context = patch(target, value)
            context.start()
            self.addCleanup(context.stop)
        context = patch.dict(os.environ, {"AUDIT_CONFIG_FILE": str(self.path)})
        context.start()
        self.addCleanup(context.stop)
        context = patch("oci.key_management.KmsManagementClient", return_value=self.kms)
        context.start()
        self.addCleanup(context.stop)
        context = patch("oci.pagination.list_call_get_all_results", return_value=Obj(data=[]))
        self.listing = context.start()
        self.addCleanup(context.stop)

    def test_setup_saves_only_reference_and_retries_never_rotate(self):
        result = setup(self.c, "vault", "key")
        self.assertTrue(result["created"])
        self.assertEqual(load_config()["fingerprint_secret_ocid"], self.secret.id)
        self.assertEqual(load_config()["processor"], self.config["processor"])
        details = self.c.vault.create_secret.call_args.args[0]
        import base64

        generated = base64.b64decode(details.secret_content.content).decode()
        self.assertRegex(generated, r"^[0-9a-f]{64}$")
        self.assertEqual(details.freeform_tags["audit-purpose"], "row-fingerprint")
        for path in self.root.rglob("*"):
            if path.is_file():
                self.assertNotIn(generated, path.read_text())
        self.assertNotIn(generated, str(result))
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
        self.assertFalse(setup(self.c, "unused", "unused")["created"])
        self.c.vault.create_secret.assert_called_once()

    def test_lost_create_response_reconciles_the_tagged_secret(self):
        self.c.vault.create_secret.side_effect = TimeoutError("ambiguous create")
        with self.assertRaises(TimeoutError):
            setup(self.c, "vault", "key")
        self.assertNotIn("fingerprint_secret_ocid", load_config())
        self.listing.return_value.data = [self.secret]
        self.assertFalse(setup(self.c, "vault", "key")["created"])
        self.c.vault.create_secret.assert_called_once()

    def test_foreign_or_disabled_keys_cannot_create_a_secret(self):
        for attr, value in (("compartment_id", "foreign"), ("lifecycle_state", "DISABLED")):
            with self.subTest(attr=attr):
                key = self.kms.get_key.return_value.data
                old = getattr(key, attr)
                setattr(key, attr, value)
                with self.assertRaises(ValueError):
                    setup(self.c, "vault", "key")
                setattr(key, attr, old)
        self.c.vault.create_secret.assert_not_called()

    def test_unowned_or_deleted_existing_secret_is_not_adopted_or_recreated(self):
        self.listing.return_value.data = [self.secret]
        self.secret.freeform_tags = {}
        with self.assertRaises(ValueError):
            setup(self.c, "vault", "key")
        self.secret.freeform_tags = {
            "managed-by": "heatwave-audit",
            "audit-app-id": "a" * 32,
            "audit-purpose": "row-fingerprint",
        }
        self.secret.lifecycle_state = "PENDING_DELETION"
        with self.assertRaises(ValueError):
            setup(self.c, "vault", "key")
        self.c.vault.create_secret.assert_not_called()
        self.assertNotIn("fingerprint_secret_ocid", load_config())

    def test_saved_reference_cannot_be_removed_replaced_or_overwritten_by_stale_settings(self):
        setup(self.c, "vault", "key")
        current = load_config()
        for value in (self.config, {**current, "fingerprint_secret_ocid": "another"}):
            with self.assertRaisesRegex(ValueError, "cannot be removed or replaced"):
                save_config(value, "scope", "region")
        with self.assertRaisesRegex(ValueError, "changed during"):
            save_config(current, "scope", "region", expected=self.config)
        save_config({**current, "processor": {"ocpus": 4}}, "scope", "region")
        self.assertEqual(load_config()["fingerprint_secret_ocid"], self.secret.id)

    def test_key_dropdown_reads_key_details_instead_of_missing_summary_shape(self):
        self.listing.return_value.data = [
            Obj(id="key", compartment_id="scope", lifecycle_state="ENABLED")
        ]
        self.assertEqual(
            choices(self.c, "key", vault_id="vault"), [{"id": "key", "name": "encryption-key"}]
        )
        self.kms.get_key.assert_called_once_with("key")

    def test_create_uses_real_sdk_argument_validation(self):
        sdk = oci.vault.VaultsClient.__new__(oci.vault.VaultsClient)
        sdk.base_client = MagicMock()
        sdk.retry_strategy = oci.retry.NoneRetryStrategy()
        sdk.base_client.get_preferred_retry_strategy.return_value = sdk.retry_strategy
        sdk.base_client.call_api.return_value = Obj(data=self.secret)
        self.c.vault.create_secret = sdk.create_secret
        self.assertTrue(setup(self.c, "vault", "key")["created"])
        args = sdk.base_client.call_api.call_args.kwargs
        self.assertEqual(args["body"].compartment_id, "scope")
        self.assertEqual(args["body"].vault_id, "vault")


class FingerprintWebTests(unittest.TestCase):
    setUp = test_core.WebTests.setUp
    auth = test_core.WebTests.auth

    def test_setup_requires_authentication_admin_and_csrf(self):
        self.assertEqual(self.client.post("/configuration/fingerprint/setup").status_code, 400)
        self.auth()
        with patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()):
            self.assertEqual(self.client.post("/configuration/fingerprint/setup").status_code, 400)
            self.assertEqual(
                self.client.post(
                    "/configuration/fingerprint/setup", data={"csrf": "test-form-token"}
                ).status_code,
                403,
            )
