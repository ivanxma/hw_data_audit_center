import unittest
from unittest.mock import MagicMock, patch

import test_core

from modules import configuration_entries as entries

CONNECTION = {
    "id": 1,
    "name": "Source",
    "role": "source",
    "host": "db.example.invalid",
    "port": 3306,
    "database_name": "sales",
    "username": "reader",
    "secret_ocid": "ocid1.vaultsecret.test.ref",
    "server_identity": "mysql:stable-source",
}
QUEUE = {
    "id": 3,
    "name": "Queue",
    "kind": "oci_streaming",
    "resource_ocid": "pool",
    "username": "",
    "secret_ocid": "",
}
FLOW = {
    "id": 7,
    "name": "orders",
    "source_id": 1,
    "target_id": 2,
    "source_schema": "sales",
    "source_table": "orders",
    "target_schema": "history",
    "target_table": "orders_history",
    "topic": "orders",
    "queue_connection_id": 3,
    "connect_harness_id": "harness",
    "connector_name": "stable-connector",
    "capture_start": '{"mode":"end"}',
    "status": "draft",
    "connector_ocid": None,
    "processor_ocid": None,
}


class EditTests(unittest.TestCase):
    def setUp(self):
        self.db, self.factory = MagicMock(), MagicMock()
        self.query = self.enterContext(patch.object(entries.repo, "query"))
        self.lock = self.enterContext(patch.object(entries.repo, "named_lock"))
        self.record = self.enterContext(patch.object(entries.repo, "record"))
        self.get = self.enterContext(patch.object(entries, "get_entry"))
        self.related = self.enterContext(patch.object(entries, "related_flows", return_value=[]))
        self.query.return_value = None

    def save(self, kind, row, changes, *, token=None):
        self.get.return_value = row.copy()
        return entries.edit_entry(
            self.db,
            self.factory,
            kind,
            row["id"],
            {"version": entries.version(kind, row) if token is None else token, **changes},
            "operator",
        )

    def update_call(self):
        return next(
            call for call in self.query.call_args_list if call.args[1].startswith("UPDATE ")
        )

    def test_running_flow_rename_preserves_connector_offset_and_lifecycle(self):
        row = {**FLOW, "status": "active", "connector_ocid": "container"}
        self.related.return_value = [row]
        self.assertTrue(self.save("flow", row, {"name": "renamed"}))
        self.assertEqual(
            self.update_call().args[1:], ("UPDATE flows SET name=%s WHERE id=%s", ["renamed", 7])
        )
        self.factory.assert_not_called()
        self.lock.assert_any_call(self.db, "flow", 7)
        self.record.assert_called_once_with(self.db, 7, "edit-flow", "complete", "operator")

    def test_running_flow_rejects_forged_path_fields(self):
        self.related.return_value = [{**FLOW, "status": "active"}]
        for kind, row, changes in [
            ("flow", FLOW, {"source_table": "other"}),
            ("flow", FLOW, {"capture_gtid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-32"}),
            ("connection", CONNECTION, {"secret_ocid": "ocid1.vaultsecret.test.other"}),
            ("queue", QUEUE, {"resource_ocid": "other-pool"}),
        ]:
            with self.subTest(kind=kind), self.assertRaisesRegex(ValueError, "locked"):
                self.save(kind, row, changes)
        self.factory.assert_not_called()
        self.query.assert_not_called()

    def test_stale_form_cannot_overwrite_newer_changes(self):
        with self.assertRaisesRegex(ValueError, "changed since Edit"):
            self.save("connection", CONNECTION, {"name": "old-form"}, token="stale")
        self.query.assert_not_called()
        self.record.assert_not_called()

    def test_connection_change_retests_and_keeps_fingerprint_identity(self):
        with patch.object(
            entries, "verify_connection", return_value={"server_uuid": "stable-source"}
        ) as verify:
            self.save("connection", CONNECTION, {"username": "replacement_reader"})
        self.assertEqual(verify.call_args.args[0]["username"], "replacement_reader")
        self.assertIn("mysql:stable-source", self.update_call().args[2])

    def test_failed_connection_test_never_saves(self):
        with patch.object(
            entries, "verify_connection", side_effect=ValueError("Connection test failed")
        ):
            with self.assertRaisesRegex(ValueError, "Connection test"):
                self.save("connection", CONNECTION, {"host": "new.example.invalid"})
        self.query.assert_not_called()

    def test_same_endpoint_replaced_server_cannot_change_history_identity(self):
        with patch.object(entries, "verify_connection", return_value={"server_uuid": "different"}):
            with self.assertRaisesRegex(ValueError, "different MySQL"):
                self.save("connection", CONNECTION, {"username": "reader2"})
        self.query.assert_not_called()

    def test_draft_references_lock_endpoint_but_allow_tested_credentials(self):
        self.related.return_value = [FLOW]
        with self.assertRaisesRegex(ValueError, "locked"):
            self.save("connection", CONNECTION, {"role": "target"})
        with patch.object(
            entries, "verify_connection", return_value={"server_uuid": "stable-source"}
        ) as verify:
            self.save("connection", CONNECTION, {"username": "reader2"})
        verify.assert_called_once()

    def test_failed_deployment_locks_draft_settings(self):
        self.query.return_value = {"id": 42}  # Prior deploy attempt in operation history.
        self.related.return_value = [FLOW]
        with self.assertRaisesRegex(ValueError, "locked"):
            self.save("flow", FLOW, {"capture_mode": "start"})
        self.assertFalse(any(c.args[1].startswith("UPDATE ") for c in self.query.call_args_list))

    def test_queue_scope_is_validated_before_save(self):
        with patch(
            "modules.queue_connections.profile",
            side_effect=ValueError("outside the VM compartment"),
        ):
            with self.assertRaisesRegex(ValueError, "outside the VM compartment"):
                self.save("queue", QUEUE, {"resource_ocid": "foreign"})
        self.assertFalse(any(c.args[1].startswith("UPDATE ") for c in self.query.call_args_list))

    def test_topic_ownership_cannot_be_rebound_to_another_pool(self):
        self.query.return_value = {"name": "owned-topic"}
        with self.assertRaisesRegex(ValueError, "locked"):
            self.save("queue", QUEUE, {"resource_ocid": "different-pool"})

    def test_draft_mapping_reuses_validation_and_preserves_connector_name(self):
        self.related.return_value = [FLOW]
        with patch.object(
            entries, "mapping_values", return_value={"topic": "new-topic"}
        ) as validate:
            self.save("flow", FLOW, {"topic": "new-topic"})
        self.assertEqual(validate.call_args.kwargs, {"entry_id": 7})
        self.assertNotIn("connector_name", self.update_call().args[1])
        self.assertNotIn("status", self.update_call().args[1])

    def test_no_changes_do_not_write_or_call_oci(self):
        self.assertFalse(self.save("connection", CONNECTION, {"name": "Source"}))
        self.factory.assert_not_called()
        self.query.assert_not_called()
        self.record.assert_not_called()


class EditWebTests(unittest.TestCase):
    setUp = test_core.WebTests.setUp
    auth = test_core.WebTests.auth

    def test_edit_requires_login_admin_and_csrf(self):
        self.assertEqual(
            self.client.get("/configuration?edit=connection&entry_id=1").status_code, 302
        )
        self.auth()
        with patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()):
            self.assertEqual(self.client.post("/configuration/edit/connection/1").status_code, 400)
            self.assertEqual(
                self.client.post(
                    "/configuration/edit/connection/1", data={"csrf": "test-form-token"}
                ).status_code,
                403,
            )

    def test_admin_save_uses_selected_entry_and_returns_to_its_tab(self):
        self.auth()
        with (
            patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()),
            patch.object(self.mod, "ROOT") as root,
            patch.object(entries, "edit_entry", return_value=True) as edit,
        ):
            root.__truediv__.return_value.exists.return_value = True
            root.__truediv__.return_value.read_text.return_value = '{"admin_users":["operator"]}'
            response = self.client.post(
                "/configuration/edit/queue/3",
                data={"csrf": "test-form-token", "name": "New name", "version": "revision"},
            )
        self.assertEqual(response.status_code, 302)
        self.assertIn("tab=resources", response.location)
        self.assertIn("service=queues", response.location)
        self.assertEqual(edit.call_args.args[2:4], ("queue", 3))
        self.assertNotIn(b"never-render-this-password", response.data)
