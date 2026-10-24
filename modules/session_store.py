import secrets
import threading
import time


class ServerSessionStore:
    def __init__(self, ttl_seconds=8 * 60 * 60):
        self.ttl_seconds = ttl_seconds
        self._lock = threading.RLock()
        self._records = {}

    def create(self, profile_name, username, password, profile):
        connection_id = secrets.token_urlsafe(32)
        now = int(time.time())
        record = {
            "connection_id": connection_id,
            "csrf": secrets.token_urlsafe(32),
            "request_lock": threading.RLock(),
            "profile_name": profile_name,
            "username": username,
            "password": password,
            "profile": dict(profile),
            "connection_summary": self._connection_summary(profile),
            "created_at": now,
            "expires_at": now + self.ttl_seconds,
            "notifications": [],
            "notification_keys": [],
        }
        with self._lock:
            self._records[connection_id] = record
        return connection_id

    def get(self, connection_id):
        if not connection_id:
            return None
        with self._lock:
            record = self._records.get(connection_id)
            if not record:
                return None
            if int(record.get("expires_at") or 0) <= int(time.time()):
                self._records.pop(connection_id, None)
                return None
            return dict(record)

    def clear(self, connection_id):
        if not connection_id:
            return
        with self._lock:
            self._records.pop(connection_id, None)

    def notify(self, connection_id, message, category="info", event_key=None):
        with self._lock:
            record = self.get(connection_id)
            if not record or not record.get("username"):
                return
            keys = record["notification_keys"]
            if event_key and event_key in keys:
                return
            if event_key:
                keys.append(event_key)
                del keys[:-500]
            items = record["notifications"]
            items.insert(
                0,
                {
                    "id": secrets.token_hex(12),
                    "message": str(message)[:4000],
                    "category": category if category in {"success", "error", "warning"} else "info",
                    "created_at": int(time.time()),
                },
            )
            del items[50:]

    def notifications(self, connection_id):
        with self._lock:
            record = self.get(connection_id)
            return [dict(item) for item in record["notifications"]] if record else []

    def clear_notifications(self, connection_id):
        with self._lock:
            record = self.get(connection_id)
            if record:
                record["notifications"].clear()

    def prune_expired(self):
        now = int(time.time())
        with self._lock:
            expired = [
                sid
                for sid, record in self._records.items()
                if int(record.get("expires_at") or 0) <= now
            ]
            for sid in expired:
                self._records.pop(sid, None)
            return expired

    @staticmethod
    def _connection_summary(profile):
        mode = str(profile.get("mode") or "tcp").lower()
        if mode == "socket":
            return "socket profile"
        host = profile.get("host") or "127.0.0.1"
        port = profile.get("port") or 3306
        return f"{host}:{port}"
