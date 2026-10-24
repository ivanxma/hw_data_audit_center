import unittest
from unittest.mock import MagicMock, patch

import test_core

from modules.connections import connection_values, verify_connection
from modules.oci_service import Cloud

FORM = {
    "name": "Source",
    "role": "source",
    "host": "db.example.invalid",
    "port": "3306",
    "username": "reader",
    "database_name": "sales",
    "secret_ocid": "ocid1.vaultsecret.test.reference",
}


class ConnectionTests(unittest.TestCase):
    def test_no_resource_id_or_ca_is_needed_or_accepted(self):
        values = connection_values({**FORM, "db_system_ocid": "ignored", "ssl_ca": "/ignored"})
        self.assertNotIn("db_system_ocid", values)
        self.assertNotIn("ssl_ca", values)
        self.assertEqual(values["host"], FORM["host"])
        with self.assertRaises(ValueError):
            connection_values({**FORM, "port": "0"})

    def test_validation_never_looks_up_database_by_ip(self):
        cloud = Cloud.__new__(Cloud)
        cloud.compartment = "scope"
        cloud.mysql = MagicMock()
        cloud.vault = MagicMock()
        cloud.vault.get_secret.return_value.data.compartment_id = "scope"
        cloud.validate_connection(connection_values(FORM))
        cloud.mysql.get_db_system.assert_not_called()
        cloud.mysql.list_db_systems.assert_not_called()

    def test_test_connection_uses_global_trust_and_closes_connection(self):
        cloud = MagicMock()
        cloud.tls_profile.return_value = {"ssl_mode": "VERIFY_IDENTITY", "ssl_ca": "/global/ca.pem"}
        cloud.secret.return_value = "fixture-password"
        db = MagicMock()
        with (
            patch("modules.connections.open_connection", return_value=(db, None)) as opened,
            patch(
                "modules.connections.query",
                side_effect=[{"connected": 1}, {"server_uuid": "uuid", "server_version": "9.x"}],
            ),
        ):
            result = verify_connection(connection_values(FORM), cloud)
        self.assertEqual(result["server_uuid"], "uuid")
        self.assertEqual(opened.call_args.args[0]["ssl_ca"], "/global/ca.pem")
        db.close.assert_called_once()


class ConnectionWebTests(unittest.TestCase):
    setUp = test_core.WebTests.setUp
    auth = test_core.WebTests.auth

    def test_test_endpoint_requires_admin_and_csrf(self):
        self.auth()
        with patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()):
            self.assertEqual(
                self.client.post(
                    "/connections/test", data={**FORM, "csrf": "test-form-token"}
                ).status_code,
                403,
            )
        self.assertEqual(self.client.post("/connections/test", data=FORM).status_code, 400)

    def test_test_result_is_safe_and_does_not_save(self):
        self.auth()
        with (
            patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()),
            patch(
                "modules.connections.verify_connection",
                return_value={"server_uuid": "uuid", "server_version": "9.x"},
            ),
            patch.object(self.mod, "Cloud"),
            patch("modules.repository.query") as query,
            patch("builtins.open"),
            patch.object(self.mod, "ROOT") as root,
        ):
            root.__truediv__.return_value.exists.return_value = True
            root.__truediv__.return_value.read_text.return_value = '{"admin_users":["operator"]}'
            response = self.client.post(
                "/connections/test", data={**FORM, "csrf": "test-form-token"}
            )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json["ok"])
        query.assert_not_called()
        self.assertNotIn(b"fixture-password", response.data)
        self.assertNotIn(b"server_uuid", response.data)

    def test_add_form_has_test_button_and_no_ocid_or_ca_entry(self):
        from flask import g, render_template

        with self.app.test_request_context("/configuration"):
            g.record = {"username": "operator", "csrf": "token"}
            text = render_template(
                "configuration.html",
                connections=[],
                flows=[],
                resources={},
                provisioned=[],
                configured=False,
                setup_sql="",
            )
        self.assertNotIn('name="db_system_ocid"', text)
        self.assertNotIn('name="ssl_ca"', text)
        self.assertIn("Test connection", text)


class GlobalTlsPolicyTests(unittest.TestCase):
    def test_default_is_encrypted_without_certificate_files(self):
        from modules.mysql_util import build_tls_kwargs
        from modules.tls_policy import tls_profile

        self.assertEqual(tls_profile({}), {"ssl_mode": "REQUIRED", "ssl_ca": None})
        policy = tls_profile({"mysql_tls": {"vm_ca_bundle": "/stale/path"}})
        self.assertIsNone(policy["ssl_ca"])
        kwargs = build_tls_kwargs(policy)
        self.assertFalse(kwargs["ssl_disabled"])
        self.assertFalse(kwargs["ssl_verify_cert"])
        self.assertFalse(kwargs["ssl_verify_identity"])
        self.assertIn("client_flags", kwargs)

    def test_plaintext_is_not_a_global_policy(self):
        from modules.tls_policy import tls_profile

        with self.assertRaises(ValueError):
            tls_profile({"mysql_tls": {"mode": "DISABLED"}})

    def test_debezium_and_history_share_the_global_policy(self):
        from modules.debezium import connector_config

        flow = {
            "id": 1,
            "transport": "streaming",
            "connector_name": "test-connector",
            "source_schema": "sales",
            "source_table": "orders",
            "target_schema": "history",
            "topic": "audit.orders",
            "source": {"host": "db.example.invalid", "port": 3306, "username": "reader"},
            "target": {"host": "target.example.invalid", "port": 3306, "username": "writer"},
        }
        config = {
            "streaming": {
                "kind": "oci_streaming",
                "bootstrap_servers": "queue.example.invalid:9092",
            },
            "debezium": {"server_id_base": 54000},
        }
        settings = connector_config(flow, config)
        self.assertEqual(settings["snapshot.mode"], "initial")
        self.assertEqual(
            connector_config({**flow, "capture_start": '{"mode":"end"}'}, config)["snapshot.mode"],
            "no_data",
        )
        self.assertEqual(settings["database.ssl.mode"], "required")
        self.assertTrue(
            settings["schema.history.internal.jdbc.connection.url"].endswith("sslMode=REQUIRED")
        )


class ConnectionFailureTests(unittest.TestCase):
    def test_driver_messages_are_never_exposed(self):
        from modules.connections import connection_failure

        for number, expected in [
            (1045, "authentication"),
            (1049, "database"),
            (2026, "tls"),
            (2003, "network"),
        ]:
            error = RuntimeError("secret-fixture-value must not be displayed")
            error.errno = number
            code, message = connection_failure(error)
            self.assertEqual(code, expected)
            self.assertNotIn("secret-fixture-value", message)
