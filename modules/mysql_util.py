import json
import os
import threading
from dataclasses import dataclass

try:
    import mysql.connector
    from mysql.connector.constants import ClientFlag
except ImportError:  # pragma: no cover - setup installs the dependency.
    mysql = None
    ClientFlag = None

    class _MissingMySQLError(Exception):
        pass

    MySQLError = _MissingMySQLError
else:
    MySQLError = mysql.connector.Error


def require_mysql_connector():
    if mysql is None:
        raise RuntimeError("mysql-connector-python is not installed.")


def normalized_ssl_mode(profile):
    value = str(profile.get("ssl_mode") or profile.get("sslMode") or "REQUIRED").strip().upper()
    aliases = {
        "REQUIRED": "REQUIRED",
        "VERIFY_CA": "VERIFY_CA",
        "VERIFY_IDENTITY": "VERIFY_IDENTITY",
        "DISABLED": "DISABLED",
    }
    if value not in aliases:
        raise ValueError("Unsupported MySQL TLS mode.")
    return aliases[value]


def build_tls_kwargs(profile):
    ssl_mode = normalized_ssl_mode(profile)
    if ssl_mode == "DISABLED":
        return {"ssl_disabled": True}

    kwargs = {
        "ssl_disabled": False,
        "client_flags": [ClientFlag.SSL],
        "ssl_verify_cert": False,
        "ssl_verify_identity": False,
    }
    if profile.get("ssl_ca"):
        kwargs["ssl_ca"] = os.path.expanduser(str(profile["ssl_ca"]))
    if profile.get("ssl_cert"):
        kwargs["ssl_cert"] = os.path.expanduser(str(profile["ssl_cert"]))
    if profile.get("ssl_key"):
        kwargs["ssl_key"] = os.path.expanduser(str(profile["ssl_key"]))
    if ssl_mode in {"VERIFY_CA", "VERIFY_IDENTITY"}:
        kwargs["ssl_verify_cert"] = True
    if ssl_mode == "VERIFY_IDENTITY":
        kwargs["ssl_verify_identity"] = True
    return kwargs


def build_connection_kwargs(profile, username, password):
    require_mysql_connector()
    mode = str(profile.get("mode") or "tcp").strip().lower()
    if mode not in {"socket", "tcp"}:
        raise ValueError("Use a socket or TCP MySQL connection.")
    kwargs = {
        "user": username,
        "password": password,
        "connection_timeout": int(profile.get("connect_timeout") or 10),
        "autocommit": True,
    }
    for option in ("read_timeout", "write_timeout"):
        if profile.get(option) is not None:
            kwargs[option] = int(profile[option])
    kwargs["time_zone"] = "+00:00"
    if profile.get("database"):
        kwargs["database"] = profile["database"]

    if mode == "socket":
        socket_path = str(profile.get("socket") or "").strip()
        if not socket_path:
            raise ValueError("Socket profile is missing a socket path.")
        kwargs["unix_socket"] = socket_path
    else:
        kwargs["host"] = str(profile.get("host") or "127.0.0.1").strip()
        kwargs["port"] = int(profile.get("port") or 3306)
        kwargs.update(build_tls_kwargs(profile))

    return kwargs


def open_connection(profile, username, password):
    """Open a socket or TCP connection; require negotiated TLS for encrypted modes."""
    kwargs = build_connection_kwargs(profile, username, password)
    connection = mysql.connector.connect(**kwargs)
    if (
        str(profile.get("mode") or "tcp").lower() != "socket"
        and normalized_ssl_mode(profile) != "DISABLED"
    ):
        cursor = None
        try:
            cursor = connection.cursor()
            cursor.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
            row = cursor.fetchone()
            if not row or not row[1]:
                raise ValueError("TLS encryption was not negotiated.")
        except Exception:
            connection.close()
            raise
        finally:
            if cursor is not None:
                cursor.close()
    return connection, None


def profile_signature(profile, username):
    public_profile = {
        key: value
        for key, value in dict(profile).items()
        if key.lower()
        not in {
            "password",
            "passwd",
            "token",
            "secret",
            "private_key_content",
            "private_key_passphrase",
        }
    }
    return json.dumps(
        {"profile": public_profile, "username": username}, sort_keys=True, default=str
    )


@dataclass
class CachedConnection:
    signature: str
    connection: object


class ConnectionCache:
    def __init__(self):
        self._lock = threading.RLock()
        self._connections = {}

    def borrow(self, connection_id, profile, username, password):
        signature = profile_signature(profile, username)
        with self._lock:
            cached = self._connections.get(connection_id)
            if cached and cached.signature == signature:
                if self._healthy(cached.connection):
                    return cached.connection
                self._close_cached(cached)
                self._connections.pop(connection_id, None)
            elif cached:
                self._close_cached(cached)
                self._connections.pop(connection_id, None)

            connection, _ = open_connection(profile, username, password)
            cached = CachedConnection(signature=signature, connection=connection)
            try:
                self._health_check(connection)
            except Exception:
                self._close_cached(cached)
                raise
            self._connections[connection_id] = cached
            return connection

    def close(self, connection_id):
        with self._lock:
            cached = self._connections.pop(connection_id, None)
            if cached:
                self._close_cached(cached)

    def close_all(self):
        with self._lock:
            for cached in list(self._connections.values()):
                self._close_cached(cached)
            self._connections.clear()

    def _healthy(self, connection):
        try:
            self._health_check(connection)
            return True
        except Exception:
            return False

    @staticmethod
    def _health_check(connection):
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        finally:
            cursor.close()

    @staticmethod
    def _close_cached(cached):
        try:
            cached.connection.close()
        except Exception:
            pass


def escape_sql_literal(value):
    if value is None:
        return "NULL"
    replacements = {
        "\\": "\\\\",
        "\0": "\\0",
        "\n": "\\n",
        "\r": "\\r",
        "\b": "\\b",
        "\t": "\\t",
        "\x1a": "\\Z",
        "'": "\\'",
        '"': '\\"',
    }
    text = str(value)
    return "'" + "".join(replacements.get(ch, ch) for ch in text) + "'"
