import json
import unittest
from unittest.mock import MagicMock, patch

from modules.events import decode, write_event
from modules.heatwave import assess
from modules.mysql_util import ConnectionCache, build_tls_kwargs, escape_sql_literal
from modules.oci_service import Cloud
from modules.validation import quote

FLOW = {
    "id": 1,
    "source_identity": "source-A",
    "lineage_id": "capture-A",
    "queue_incarnation": "queue-A",
    "source_schema": "sales",
    "source_table": "orders",
    "target_schema": "history",
    "target_table": "orders",
}


def event(op="u"):
    return {
        "op": op,
        "source": {
            "db": "sales",
            "table": "orders",
            "ts_ms": 1700000000000,
            "file": "binlog.000001",
            "pos": 123,
            "row": 0,
        },
        "before": {"id": 1, "a": "old"},
        "after": {"id": 1, "a": "new"},
    }


class CoreTests(unittest.TestCase):
    def test_retention_and_json(self):
        settings = {
            "log_bin": "ON",
            "binlog_format": "ROW",
            "binlog_row_image": "FULL",
            "binlog_row_value_options": "",
            "binlog_expire_logs_seconds": "604800",
            "server_id": "1",
            "gtid_mode": "ON",
            "enforce_gtid_consistency": "ON",
        }
        self.assertTrue(assess(settings)["ok"])
        for key, value in [
            ("binlog_expire_logs_seconds", "259200"),
            ("binlog_row_image", "MINIMAL"),
            ("binlog_row_value_options", "PARTIAL_JSON"),
            ("gtid_mode", "OFF"),
        ]:
            self.assertFalse(assess({**settings, key: value})["ok"])
        self.assertFalse(assess({})["ok"])

    def test_replay_and_images(self):
        raw = json.dumps(event())
        a = decode(raw, b'{"id":1}', "topic", 0, 10, FLOW, b"test-key")
        self.assertEqual(
            a["event_id"], decode(raw, b'{"id":1}', "topic", 0, 10, FLOW, b"test-key")["event_id"]
        )
        self.assertEqual(
            a["event_id"], decode(raw, b'{"id":1}', "topic", 0, 11, FLOW, b"test-key")["event_id"]
        )
        self.assertEqual(a["before_image"]["a"], "old")
        self.assertIsNone(decode(None, None, "topic", 0, 12, FLOW, b"test-key"))

    def test_reject_incomplete_or_wrong_route(self):
        for bad in [
            {**event(), "before": None},
            {**event(), "source": {"db": "wrong", "table": "orders"}},
            {"ddl": "ALTER TABLE orders ADD x INT"},
        ]:
            with self.assertRaises(ValueError):
                decode(json.dumps(bad), None, "topic", 0, 1, FLOW, b"test-key")

    def test_transaction_cleanup(self):
        db = MagicMock()
        db.cursor.return_value.execute.side_effect = RuntimeError("fail")
        with patch("modules.retention.check_ingestion"), self.assertRaises(RuntimeError):
            write_event(
                db, FLOW, decode(json.dumps(event()), b'{"id":1}', "t", 0, 1, FLOW, b"test-key")
            )
        db.rollback.assert_called_once()
        self.assertTrue(db.autocommit)
        db.commit.assert_not_called()

    def test_connection_cache_and_health(self):
        connection = MagicMock()
        with patch("modules.mysql_util.open_connection", return_value=(connection, None)) as opened:
            cache = ConnectionCache()
            self.assertIs(
                cache.borrow("x", {"mode": "socket", "socket": "/tmp/test"}, "u", "p"), connection
            )
            self.assertIs(
                cache.borrow("x", {"mode": "socket", "socket": "/tmp/test"}, "u", "p"), connection
            )
            self.assertEqual(opened.call_count, 1)
            self.assertEqual(connection.cursor.return_value.execute.call_count, 2)
            cache.close("x")
            connection.close.assert_called_once()

    def test_tls(self):
        self.assertTrue(build_tls_kwargs({"ssl_mode": "DISABLED"})["ssl_disabled"])
        self.assertIn("client_flags", build_tls_kwargs({"ssl_mode": "REQUIRED"}))
        self.assertTrue(build_tls_kwargs({"ssl_mode": "VERIFY_CA"})["ssl_verify_cert"])
        self.assertTrue(build_tls_kwargs({"ssl_mode": "VERIFY_IDENTITY"})["ssl_verify_identity"])

    def test_literal_and_identifier(self):
        self.assertEqual(escape_sql_literal(None), "NULL")
        for char, replacement in [
            ("'", "\\'"),
            ("\\", "\\\\"),
            ("\x00", "\\0"),
            ("\n", "\\n"),
            ("\r", "\\r"),
            ("\x1a", "\\Z"),
        ]:
            self.assertEqual(escape_sql_literal(char), "'" + replacement + "'")
        with self.assertRaises(ValueError):
            quote("x; DROP DATABASE y")

    def test_compartment_guard(self):
        cloud = Cloud.__new__(Cloud)
        cloud.compartment = "expected"
        resource = MagicMock()
        resource.compartment_id = "other"
        with self.assertRaises(ValueError):
            cloud.check(resource)


class WebTests(unittest.TestCase):
    def setUp(self):
        import app

        self.mod = app
        self.app = app.create_app({"TESTING": True, "SESSION_COOKIE_SECURE": False})
        self.client = self.app.test_client()

    def auth(self):
        sid = self.mod.SESSIONS.create(
            "local-admin-profile",
            "operator",
            "never-render-this-password",
            {"mode": "socket", "socket": "/tmp/test"},
        )
        self.mod.SESSIONS._records[sid]["csrf"] = "test-form-token"
        with self.client.session_transaction() as s:
            s["sid"] = sid
        return sid

    def test_cookie_and_login_secrecy(self):
        r = self.client.get("/login")
        self.assertEqual(r.status_code, 200)
        self.assertNotIn(b"mysql.sock", r.data)
        self.assertNotIn(b"127.0.0.1", r.data)
        sid = self.auth()
        cookie = self.client.get_cookie("heatwave_audit").value
        payload = self.app.session_interface.get_signing_serializer(self.app).loads(cookie)
        self.assertEqual(payload, {"sid": sid})
        self.assertNotIn("password", cookie)

    def test_missing_state_and_csrf(self):
        sid = self.auth()
        self.mod.SESSIONS.clear(sid)
        self.assertEqual(self.client.get("/dashboard").status_code, 302)
        self.assertEqual(
            self.client.post("/login", data={"username": "x", "password": "x"}).status_code, 400
        )

    def test_auth_and_admin_routes(self):
        self.assertEqual(self.client.get("/control").status_code, 302)
        self.auth()
        with patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()):
            self.assertEqual(self.client.get("/configuration").status_code, 403)

    def test_templates(self):

        from flask import g, render_template

        with self.app.test_request_context("/audit"):
            g.record = {"username": "operator", "csrf": "token"}
            context = {
                "flows": [],
                "operations": [],
                "connection_count": 0,
                "connections": [],
                "resources": {},
                "provisioned": [],
                "configured": False,
                "setup_sql": "SQL",
                "records": [],
                "flow": {**FLOW, "name": "Example", "source": {}},
                "result": None,
            }
            for template in [
                "dashboard.html",
                "configuration.html",
                "control.html",
                "messages.html",
                "audit.html",
                "validation.html",
                "password.html",
            ]:
                text = render_template(template, **context)
                self.assertNotIn("never-render-this-password", text)
                self.assertNotIn("__PLACEHOLDER__", text)
            context["result"] = {
                "rows": [],
                "total": 0,
                "page": 1,
                "chart": [],
                "interval": 3600,
                "counts": [],
            }
            render_template(
                "audit.html", **context, export_args={"export": "csv"}, prev_args={}, next_args={}
            )

    def test_streaming_registration_does_not_store_submitted_credentials(self):
        import tempfile
        from pathlib import Path

        self.auth()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "profiles.json").write_text(json.dumps({"admin_users": ["operator"]}))
            with (
                patch.object(self.mod, "ROOT", root),
                patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()),
                patch.object(self.mod, "Cloud"),
                patch("modules.queue_connections.profile") as profile,
                patch.object(self.mod.repo, "query") as query,
            ):
                response = self.client.post(
                    "/queue-connections",
                    data={
                        "csrf": "test-form-token",
                        "name": "Streaming",
                        "kind": "oci_streaming",
                        "resource_ocid": "ocid1.streampool.oc1.region.fixture",
                        "username": "obsolete-user",
                        "secret_ocid": "obsolete-token",
                    },
                )
                self.assertEqual(response.status_code, 302)
                self.assertNotIn("username", profile.call_args.args[1])
                self.assertNotIn("secret_ocid", profile.call_args.args[1])
                self.assertEqual(len(query.call_args.args[2]), 3)


class TransportAndStorageTests(unittest.TestCase):
    def test_only_native_streaming_profiles_are_supported(self):
        from modules.transport import backend, uses_principal

        self.assertEqual(backend({"kind": "oci_streaming"}), "oci_streaming")
        self.assertTrue(uses_principal({"kind": "oci_streaming"}))
        with self.assertRaises(ValueError):
            backend({"kind": "oci_kafka"})

    def test_new_queue_offset_same_source_event(self):
        envelope = event()
        envelope["source"].update(file="binlog.000001", pos=123, row=0, gtid="source:7")
        one = decode(
            json.dumps(envelope), b'{"id":1}', "topic", 0, 10, FLOW, b"test-fingerprint-key"
        )
        two = decode(
            json.dumps(envelope), b'{"id":1}', "topic", 0, 11, FLOW, b"test-fingerprint-key"
        )
        self.assertEqual(one["event_id"], two["event_id"])
        self.assertNotEqual(one["capture_fingerprint"], two["capture_fingerprint"])
        self.assertEqual(one["key_hash"], two["key_hash"])

    def test_schema_semantic_difference(self):
        from modules.schema_history import difference, normalize

        before = normalize({"columns": [{"name": "x", "typeName": "INT"}]})
        after = normalize(
            {"columns": [{"name": "x", "typeName": "BIGINT"}, {"name": "y", "typeName": "VARCHAR"}]}
        )
        delta = difference(before, after)
        self.assertTrue(any(d["path"] == "/columns/x/typeName" for d in delta))
        self.assertTrue(any(d["path"] == "/columns/y" and d["kind"] == "added" for d in delta))

    def test_monthly_partition_keys(self):
        from modules.heatwave import audit_ddl

        sql = audit_ddl("audit", "history")
        self.assertIn("PRIMARY KEY (event_time,event_id)", sql)
        self.assertIn("PARTITION BY RANGE COLUMNS(event_time)", sql)
        self.assertIn("MAXVALUE", sql)


class BootstrapRotationTests(unittest.TestCase):
    setUp = WebTests.setUp
    auth = WebTests.auth

    def test_rotation_blocks_workspace_and_logs_out(self):
        import tempfile
        from pathlib import Path

        self.auth()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "profiles.json").write_text(
                json.dumps({"admin_users": ["operator"], "rotation_required": ["operator"]})
            )
            with (
                patch.object(self.mod, "ROOT", root),
                patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()),
            ):
                response = self.client.get("/dashboard")
                self.assertEqual(response.status_code, 302)
                self.assertTrue(response.location.endswith("/password"))
                response = self.client.post(
                    "/password",
                    data={
                        "csrf": "test-form-token",
                        "password": "replacement-password-for-test",
                        "confirmation": "replacement-password-for-test",
                    },
                )
                self.assertEqual(response.status_code, 302)
                self.assertTrue(response.location.endswith("/login"))
                self.assertEqual(
                    json.loads((root / "profiles.json").read_text())["rotation_required"], []
                )
                with self.client.session_transaction() as state:
                    self.assertNotIn("sid", state)


if __name__ == "__main__":
    unittest.main()
