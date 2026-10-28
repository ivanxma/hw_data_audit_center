"""Private runtime configuration. No OCI API-key authentication fallback."""

import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_config(required=False):
    path = os.environ.get("AUDIT_CONFIG_FILE")
    if not path:
        if required:
            raise ValueError("Set AUDIT_CONFIG_FILE to the private deployment JSON on this VM.")
        return {}
    config = json.loads(Path(path).expanduser().read_text())
    if not isinstance(config, dict):
        raise ValueError("Private configuration must be a JSON object.")
    return config


def local_profile():
    return {
        "name": "local-admin-profile",
        "mode": "socket",
        "socket": str(ROOT / ".data/run/mysql.sock"),
        "database": "audit_control",
    }


def fingerprint_secret(config):
    from .validation import ocid

    value = config.get("fingerprint_secret_ocid")
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            "The row-fingerprint key is not configured. Open Runtime settings → Row-history fingerprint key "
            "and choose Generate and configure key. This saves fingerprint_secret_ocid for deployment "
            "and row lookup. Keep the same key for existing audit history."
        )
    return ocid(value, "vaultsecret")


def save_config(value, compartment, region, *, expected=None):
    """Authenticated admin settings contain references, never credential values."""
    if (
        not isinstance(value, dict)
        or value.get("compartment_id") != compartment
        or value.get("region") != region
    ):
        raise ValueError("Configuration must retain this VM compartment and region.")

    def inspect(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k.lower() in {
                    "password",
                    "passwd",
                    "admin_password",
                    "token",
                    "private_key",
                    "private_key_content",
                    "credentials",
                    "sasl.jaas.config",
                }:
                    raise ValueError(
                        "Use Vault secret references, not plaintext credentials in settings."
                    )
                inspect(v)
        elif isinstance(obj, list):
            for item in obj:
                inspect(item)
        elif isinstance(obj, str) and ("-----BEGIN " in obj or "password=" in obj.lower()):
            raise ValueError("Credential material is not permitted in deployment settings.")

    inspect(value)
    from .message_policy import policy

    policy(value)
    import fcntl

    path = Path(os.environ["AUDIT_CONFIG_FILE"]).expanduser()
    lock_path = path.with_name(path.name + ".lock")
    with lock_path.open("a") as lock:
        os.chmod(lock_path, 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX)
        current = json.loads(path.read_text()) if path.exists() else {}
        if expected is not None and current != expected:
            raise ValueError("Runtime settings changed during this operation. Refresh and retry.")
        original = current.get("fingerprint_secret_ocid")
        if original and value.get("fingerprint_secret_ocid") != original:
            raise ValueError(
                "The configured row-fingerprint secret cannot be removed or replaced. "
                "Existing row history requires the original key."
            )
        write_private_json(path, value)


def write_private_json(path, value):
    import tempfile

    fd, temp = tempfile.mkstemp(prefix=".audit-config-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(value, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(temp, 0o600)
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def app_instance_id():
    """Stable ownership identity for this installation; never shared in source control."""
    import uuid

    path = ROOT / ".data/app-instance-id"
    path.parent.mkdir(mode=0o700, exist_ok=True)
    if not path.exists():
        import tempfile

        fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".app-identity-")
        try:
            with os.fdopen(fd, "w") as handle:
                handle.write(uuid.uuid4().hex)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                pass
        finally:
            os.unlink(temporary)
    value = path.read_text().strip()
    if len(value) != 32 or any(c not in "0123456789abcdef" for c in value):
        raise ValueError("The app installation identity is invalid. Restore its private state.")
    return value
