import base64
import json
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace as Obj
from unittest.mock import MagicMock, patch

import test_core
from mysql.connector import Error
from werkzeug.datastructures import MultiDict

from modules import account_setup as setup


def form(role="source"):
    return MultiDict(
        [
            ("name", "example"),
            ("role", role),
            ("host", "source.example.invalid"),
            ("port", "3306"),
            ("database_name", "sales"),
            ("username", "setup_admin"),
            ("secret_ocid", "ocid1.vaultsecret.example.admin"),
            ("account_setup_id", "a" * 32),
            ("account_username", "cdc_reader" if role == "source" else "history_writer"),
            ("account_host", "%"),
            ("account_schemas", "sales"),
            ("account_schemas", "archive"),
            ("account_vault_id", "ocid1.vault.example.selected"),
            ("account_key_id", "ocid1.key.example.selected"),
            ("account_password", "Example-test-password-42!"),
        ]
    )


class AccountSetupTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.stack.enter_context(patch.object(setup, "ROOT", self.root))
        self.stack.enter_context(patch.object(setup, "app_instance_id", return_value="example-app"))
        self.cloud = MagicMock()
        self.cloud.compartment = "scope"
        self.cloud.config = {"region": "example-region"}
        self.db = MagicMock()
        self.stack.enter_context(patch.object(setup, "connect", return_value=self.db))
        self.db.__enter__.return_value = self.db
        self.exists = False
        self.unmanaged = False
        self.partial_revokes = 1
        self.fail_grant = False
        self.calls = []
        self.values = form()
        self.test_connection = self.stack.enter_context(
            patch.object(setup, "verify_connection", return_value={"server_uuid": "server"})
        )
        self.stack.enter_context(patch.object(setup, "query", side_effect=self.query))
        self.secret_calls = 0

        def secret(cloud, state, save, password):
            self.secret_calls += 1
            state["secret_ocid"] = "ocid1.vaultsecret.example.dedicated"
            save()
            return "Example-test-password-42!"

        self.vault = self.stack.enter_context(
            patch.object(setup, "vault_secret", side_effect=secret)
        )
        self.addCleanup(self.stack.close)

    def query(self, db, sql, params=(), **kwargs):
        self.calls.append((sql, params))
        if "@@server_uuid" in sql:
            return {"id": "server", "partial_revokes": self.partial_revokes}
        if "SCHEMATA" in sql:
            return [{"name": "sales"}, {"name": "archive"}, {"name": "sales_data"}]
        if sql.startswith("SHOW CREATE USER"):
            if not self.exists:
                raise Error("No such user", errno=1396)
            return [
                {
                    "statement": "unmanaged"
                    if self.unmanaged
                    else 'ATTRIBUTE {"audit-app-id":"example-app","audit-setup-id":"'
                    + "a" * 32
                    + '"}'
                }
            ]
        if sql.startswith("CREATE USER"):
            self.exists = True
            return 0
        if sql.startswith("GRANT ") and self.fail_grant:
            self.fail_grant = False
            raise Error("Sensitive driver text must not escape", errno=1227)
        if sql.startswith("SHOW GRANTS"):
            return [
                {"grant": "GRANT " + ", ".join(privileges) + " ON " + scope + " TO `user`@`%`"}
                for scope, privileges in setup.plan(
                    self.values["role"],
                    sorted(set(self.values.getlist("account_schemas"))),
                    maintenance=self.values.get("account_maintenance") == "on",
                )
            ]
        return 0

    def test_source_user_is_scoped_and_admin_is_never_registered_or_persisted(self):
        result = setup.create(self.cloud, self.values, "operator")
        self.assertEqual(result["username"], "cdc_reader")
        self.assertEqual(result["secret_ocid"], "ocid1.vaultsecret.example.dedicated")
        self.assertEqual(self.test_connection.call_args.args[0]["username"], "cdc_reader")
        self.assertEqual(
            self.test_connection.call_args.args[0]["secret_ocid"], result["secret_ocid"]
        )
        sql = "\n".join(row[0] for row in self.calls)
        self.assertIn(
            "REPLICATION SLAVE, REPLICATION CLIENT, SHOW DATABASES, FLUSH_TABLES ON *.*", sql
        )
        self.assertIn("SELECT, LOCK TABLES ON `sales`.*", sql)
        self.assertNotIn("ALL PRIVILEGES", sql)
        self.assertNotIn("SUPER", sql)
        self.assertNotIn("WITH GRANT OPTION", sql)
        create = next(row for row in self.calls if row[0].startswith("CREATE USER"))
        self.assertIn("REQUIRE SSL ATTRIBUTE %s", create[0])
        self.assertNotIn(self.values["account_password"], create[0])
        state_path = next((self.root / ".data/account-setups").glob("*.json"))
        saved = state_path.read_text()
        for sensitive in (
            self.values["account_password"],
            "setup_admin",
            "ocid1.vaultsecret.example.admin",
        ):
            self.assertNotIn(sensitive, saved)
            self.assertNotIn(sensitive, json.dumps(result))
        self.assertEqual(state_path.stat().st_mode & 0o777, 0o600)

    def test_target_account_has_no_global_or_source_grants_and_optional_maintenance(self):
        self.values = form("target")
        self.values["account_maintenance"] = "on"
        setup.create(self.cloud, self.values, "operator")
        sql = "\n".join(row[0] for row in self.calls if row[0].startswith("GRANT "))
        self.assertIn("SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP ON `sales`.*", sql)
        self.assertNotIn("*.*", sql)
        self.assertNotIn("REPLICATION", sql)
        baseline = setup.plan("target", ["sales"])
        self.assertNotIn("ALTER", baseline[0][1])
        self.assertNotIn("DROP", baseline[0][1])

    def test_existing_unmanaged_user_is_never_reset_or_granted(self):
        self.exists = self.unmanaged = True
        with self.assertRaisesRegex(setup.SetupFailure, "already exists"):
            setup.create(self.cloud, self.values, "operator")
        self.vault.assert_not_called()
        self.assertFalse(
            any(
                sql.startswith(("CREATE USER", "GRANT ", "ALTER USER", "DROP USER"))
                for sql, _ in self.calls
            )
        )

    def test_partial_grant_failure_is_safe_and_retry_reuses_owned_account(self):
        self.fail_grant = True
        with self.assertRaises(setup.SetupFailure) as error:
            setup.create(self.cloud, self.values, "operator")
        self.assertIn("least-privilege grants", str(error.exception))
        self.assertNotIn("Sensitive driver text", str(error.exception))
        self.assertNotIn(self.values["account_password"], str(error.exception))
        self.assertIn("ocid1.vaultsecret.example.dedicated", str(error.exception))
        self.values.pop("account_password")
        self.assertTrue(setup.create(self.cloud, self.values, "operator")["ok"])
        self.assertEqual(sum(sql.startswith("CREATE USER") for sql, _ in self.calls), 1)
        self.assertFalse(any(sql.startswith("DROP") for sql, _ in self.calls))
        # A completed retry tests only the dedicated credentials, never the admin.
        self.calls.clear()
        setup.create(self.cloud, self.values, "operator")
        self.assertEqual(self.calls, [])

    def test_setup_request_cannot_be_reused_with_changed_scope_or_actor(self):
        setup.create(self.cloud, self.values, "operator")
        with self.assertRaisesRegex(ValueError, "fields changed"):
            setup.create(self.cloud, self.values, "another-operator")
        self.values.setlist("account_schemas", ["sales"])
        with self.assertRaisesRegex(ValueError, "fields changed"):
            setup.create(self.cloud, self.values, "operator")

    def test_rejects_system_schema_and_unbounded_or_injected_account(self):
        for key, value in [
            ("account_username", "u'; DROP USER x"),
            ("account_host", "x'@'host"),
            ("account_setup_id", "../unsafe"),
        ]:
            original = self.values[key]
            self.values[key] = value
            with self.assertRaises(ValueError):
                setup.create(self.cloud, self.values, "operator")
            self.values[key] = original
        self.values.setlist("account_schemas", ["mysql"])
        with self.assertRaisesRegex(ValueError, "system databases"):
            setup.create(self.cloud, self.values, "operator")
        self.vault.assert_not_called()

    def test_wildcard_database_scope_is_not_broadened(self):
        self.partial_revokes = 0
        self.values.setlist("account_schemas", ["sales", "sales_data"])
        with self.assertRaisesRegex(setup.SetupFailure, "partial_revokes"):
            setup.create(self.cloud, self.values, "operator")
        self.vault.assert_not_called()

    def test_grant_verification_rejects_extra_privileges_and_grant_option(self):
        allowed = setup.plan("target", ["sales"])
        for grant in [
            "GRANT ALL PRIVILEGES ON *.* TO user",
            "GRANT SELECT ON `other`.* TO user",
            "GRANT role TO user",
            "GRANT SELECT ON `sales`.* TO user WITH GRANT OPTION",
        ]:
            with self.assertRaises(setup.SetupFailure):
                setup.check_grants([{"grant": grant}], allowed)


class AccountVaultTests(unittest.TestCase):
    def test_password_is_only_in_vault_request_and_retries_reuse_secret(self):
        cloud = MagicMock()
        cloud.compartment = "scope"
        cloud.check.side_effect = lambda obj: obj
        state = dict(vault_id="vault", key_id="key", setup_id="b" * 32)
        tags = {
            "managed-by": "heatwave-audit",
            "audit-app-id": "example-app",
            "audit-setup-id": state["setup_id"],
        }
        vault = Obj(
            id="vault",
            lifecycle_state="ACTIVE",
            management_endpoint="https://vault.example.invalid",
        )
        key = Obj(id="key", lifecycle_state="ENABLED", key_shape=Obj(algorithm="AES"))
        secret = Obj(
            id="secret",
            vault_id="vault",
            key_id="key",
            lifecycle_state="ACTIVE",
            freeform_tags=tags,
        )
        cloud.vault.create_secret.return_value.data = secret
        cloud.vault.get_secret.return_value.data = secret
        cloud.secret.return_value = "Example-test-password-42!"
        with (
            patch.object(setup, "app_instance_id", return_value="example-app"),
            patch.object(setup, "get_resource", return_value=vault),
            patch.object(setup.oci.key_management, "KmsManagementClient") as client,
            patch.object(
                setup.oci.pagination, "list_call_get_all_results", return_value=Obj(data=[])
            ),
        ):
            client.return_value.get_key.return_value.data = key
            save = MagicMock()
            self.assertEqual(
                setup.vault_secret(cloud, state, save, "Example-test-password-42!"),
                "Example-test-password-42!",
            )
            details = cloud.vault.create_secret.call_args.args[0]
            self.assertEqual(
                base64.b64decode(details.secret_content.content).decode(),
                "Example-test-password-42!",
            )
            self.assertNotIn("Example-test-password-42!", json.dumps(state))
            self.assertEqual(state["secret_ocid"], "secret")
            self.assertEqual(
                setup.vault_secret(cloud, state, save, ""), "Example-test-password-42!"
            )
            cloud.vault.create_secret.assert_called_once()


class AccountSetupWebTests(unittest.TestCase):
    setUp = test_core.WebTests.setUp
    auth = test_core.WebTests.auth

    def test_account_creation_requires_admin_csrf_and_does_not_register_admin(self):
        self.assertEqual(self.client.post("/connections/create-account").status_code, 400)
        self.auth()
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(self.mod, "ROOT", Path(directory)),
            patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()),
            patch.object(self.mod, "Cloud"),
            patch.object(self.mod.repo, "named_lock"),
            patch.object(self.mod.repo, "record"),
            patch.object(self.mod.repo, "query") as query,
            patch(
                "modules.account_setup.create",
                return_value={
                    "ok": True,
                    "username": "cdc_reader",
                    "secret_ocid": "ocid1.vaultsecret.example.dedicated",
                },
            ) as create,
        ):
            self.assertEqual(
                self.client.post(
                    "/connections/create-account", data={"csrf": "test-form-token"}
                ).status_code,
                403,
            )
            create.assert_not_called()
            (Path(directory) / "profiles.json").write_text(
                json.dumps({"admin_users": ["operator"]})
            )
            self.assertEqual(self.client.post("/connections/create-account").status_code, 400)
            response = self.client.post(
                "/connections/create-account",
                data={"csrf": "test-form-token", "account_setup_id": "a" * 32},
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json["username"], "cdc_reader")
            query.assert_not_called()
            create.assert_called_once()
            response = self.client.post(
                "/configuration",
                data={"csrf": "test-form-token", "action": "connection", "create_account": "on"},
            )
            self.assertEqual(response.status_code, 400)
            query.assert_not_called()
