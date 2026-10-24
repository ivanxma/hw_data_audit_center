import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import test_core

from modules.session_store import ServerSessionStore


class NotificationStoreTests(unittest.TestCase):
    def test_isolation_bounded_history_and_clear_preserves_deduplication(self):
        store = ServerSessionStore()
        one = store.create("profile", "one", "private-password", {})
        two = store.create("profile", "two", "private-password", {})
        for number in range(60):
            store.notify(one, "Event " + str(number))
        self.assertEqual(len(store.notifications(one)), 50)
        self.assertEqual(store.notifications(two), [])
        store.notify(one, "Build completed", "success", "build:fixture:complete")
        store.clear_notifications(one)
        store.notify(one, "Build completed", "success", "build:fixture:complete")
        self.assertEqual(store.notifications(one), [])
        store.clear(one)
        store.notify(one, "After logout")
        self.assertEqual(store.notifications(one), [])


class NotificationWebTests(unittest.TestCase):
    setUp = test_core.WebTests.setUp
    auth = test_core.WebTests.auth

    def test_notifications_require_login_and_clear_requires_csrf(self):
        self.assertEqual(self.client.get("/notifications").status_code, 302)
        sid = self.auth()
        self.mod.SESSIONS.notify(sid, "Example operation failed", "error")
        with patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()):
            self.assertEqual(len(self.client.get("/notifications").json["items"]), 1)
            self.assertEqual(self.client.post("/notifications/clear").status_code, 400)
            self.assertEqual(len(self.mod.SESSIONS.notifications(sid)), 1)
            response = self.client.post("/notifications/clear", data={"csrf": "test-form-token"})
            self.assertEqual(response.json, {"items": []})
            self.assertEqual(self.client.get("/notifications").json, {"items": []})
        cookie = self.client.get_cookie("heatwave_audit").value
        payload = self.app.session_interface.get_signing_serializer(self.app).loads(cookie)
        self.assertEqual(payload, {"sid": sid})

    def test_background_build_events_are_owned_and_clearing_does_not_readd_them(self):
        self.auth()
        jobs = [
            {
                "id": "own",
                "actor": "operator",
                "status": "failed",
                "tag": "historywriter-fixture",
                "message": "Example build failure",
            },
            {
                "id": "foreign",
                "actor": "someone-else",
                "status": "failed",
                "tag": "other",
                "message": "Not this user's event",
            },
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "profiles.json").write_text('{"admin_users":["operator"]}')
            with (
                patch.object(self.mod, "ROOT", root),
                patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()),
                patch("modules.image_builds.inventory", return_value=jobs),
            ):
                response = self.client.get("/notifications")
                self.assertEqual(len(response.json["items"]), 1)
                self.assertIn("historywriter-fixture", response.json["items"][0]["message"])
                self.client.post("/notifications/clear", data={"csrf": "test-form-token"})
                self.assertEqual(self.client.get("/notifications").json, {"items": []})
