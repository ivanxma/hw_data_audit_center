"""One-time, compartment-scoped setup of the stable row-history HMAC secret."""

import base64
import fcntl
import os
import re
import secrets
import time
import uuid

import oci

from .config import ROOT, app_instance_id, load_config, save_config
from .resource_catalog import get_resource


def setup(cloud, vault_id, key_id):
    """Create or reconcile this installation's secret; never rotate an existing key."""
    folder = ROOT / ".data"
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = folder / "fingerprint-setup.lock"
    with path.open("a") as lock:
        os.chmod(path, 0o600)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError(
                "Fingerprint-key setup is already in progress. Refresh shortly."
            ) from None
        current = load_config(required=True)
        if (
            current.get("compartment_id") != cloud.compartment
            or current.get("region") != cloud.config["region"]
        ):
            raise ValueError(
                "Runtime configuration must remain within this VM compartment and region."
            )
        if current.get("fingerprint_secret_ocid"):
            # A retry or stale setup form must never create another key.
            secret = get_resource(cloud, "secret", current["fingerprint_secret_ocid"])
            if secret.lifecycle_state != "ACTIVE":
                raise ValueError(
                    "The configured fingerprint secret is not ACTIVE. Restore its availability; do not replace it."
                )
            if not cloud.secret(secret.id):
                raise ValueError(
                    "The configured fingerprint secret is empty. Restore its original value."
                )
            return {"id": secret.id, "name": secret.secret_name, "created": False}

        vault = get_resource(cloud, "vault", vault_id)
        if vault.lifecycle_state != "ACTIVE":
            raise ValueError("Select an ACTIVE vault in the VM compartment.")
        client = oci.key_management.KmsManagementClient(
            cloud.sdk_config,
            service_endpoint=vault.management_endpoint,
            signer=cloud.signer,
            timeout=(5, 30),
            retry_strategy=oci.retry.NoneRetryStrategy(),
        )
        key = cloud.check(client.get_key(key_id).data)
        if key.lifecycle_state != "ENABLED" or key.key_shape.algorithm != "AES":
            raise ValueError("Select an ENABLED AES encryption key from this vault.")

        identity = app_instance_id()
        name = "audit-row-fingerprint-" + identity
        tags = {
            "managed-by": "heatwave-audit",
            "audit-app-id": identity,
            "audit-purpose": "row-fingerprint",
        }
        matches = oci.pagination.list_call_get_all_results(
            cloud.vault.list_secrets, compartment_id=cloud.compartment, vault_id=vault.id, name=name
        ).data
        for item in matches:
            cloud.check(item)
            if item.secret_name != name or item.vault_id != vault.id:
                raise ValueError(
                    "Fingerprint secret inventory did not match the selected vault and name."
                )
        if len(matches) > 1:
            raise ValueError(
                "Multiple fingerprint secrets match this installation. Review Vault inventory."
            )
        created = not matches
        if matches:
            secret = get_resource(cloud, "secret", matches[0].id)
            if (
                any((secret.freeform_tags or {}).get(k) != v for k, v in tags.items())
                or secret.key_id != key.id
            ):
                raise ValueError(
                    "The existing fingerprint secret does not match this app and encryption key. Nothing was changed."
                )
        else:
            # 256 bits of entropy, ASCII encoded for existing writer/lookup compatibility.
            # The value exists only in memory and OCI Vault; it is never returned or logged.
            value = secrets.token_hex(32)
            details = oci.vault.models.CreateSecretDetails(
                compartment_id=cloud.compartment,
                vault_id=vault.id,
                key_id=key.id,
                secret_name=name,
                description="Stable row-history fingerprint key. Preserve for the lifetime of audit history; do not rotate.",
                freeform_tags=tags,
                enable_auto_generation=False,
                secret_content=oci.vault.models.Base64SecretContentDetails(
                    content=base64.b64encode(value.encode()).decode()
                ),
            )
            secret = cloud.check(
                cloud.vault.create_secret(
                    details,
                    opc_retry_token=uuid.uuid4().hex,
                    retry_strategy=oci.retry.NoneRetryStrategy(),
                ).data
            )
            del value, details

        for attempt in range(10):
            secret = get_resource(cloud, "secret", secret.id)
            if (
                secret.vault_id != vault.id
                or secret.key_id != key.id
                or any((secret.freeform_tags or {}).get(k) != v for k, v in tags.items())
            ):
                raise ValueError(
                    "Fingerprint secret ownership or Vault attachment changed. Nothing was configured."
                )
            if secret.lifecycle_state == "ACTIVE":
                break
            if secret.lifecycle_state != "CREATING":
                raise ValueError(
                    "The fingerprint secret is "
                    + secret.lifecycle_state
                    + ". Restore its availability before retrying."
                )
            if attempt < 9:
                time.sleep(1)
        else:
            raise ValueError(
                "The fingerprint secret is still being created. Run setup again shortly to finish; the existing secret will be reused."
            )
        value = cloud.secret(secret.id)
        if not re.fullmatch(r"[0-9a-f]{64}", value):
            raise ValueError(
                "The app fingerprint secret has unexpected content. Restore its original value; it was not configured."
            )
        del value
        current = load_config(required=True)
        save_config(
            {**current, "fingerprint_secret_ocid": secret.id},
            cloud.compartment,
            cloud.config["region"],
            expected=current,
        )
        return {"id": secret.id, "name": secret.secret_name, "created": created}
