"""One-time, narrowly granted database users; passwords are stored only in Vault."""

import base64
import json
import re
import time

import oci

from .config import ROOT, app_instance_id, write_private_json
from .connections import connection_failure, connection_values, verify_connection
from .heatwave import connect
from .mappings import SYSTEM_SCHEMAS
from .repository import query
from .resource_catalog import get_resource
from .validation import identifier, ocid, quote


class SetupFailure(ValueError):
    pass


def parameters(form):
    setup_id = form.get("account_setup_id", "")
    if not re.fullmatch(r"[a-f0-9]{32}", setup_id):
        raise ValueError("Reopen Add connection to begin a new account setup.")
    username = form.get("account_username", "").strip()
    host = form.get("account_host", "").strip()
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,31}", username):
        raise ValueError(
            "Use a dedicated username of 1–32 letters, digits, underscores or hyphens, starting with a letter."
        )
    if not host or len(host) > 255 or not re.fullmatch(r"[A-Za-z0-9_.:%/\-]+", host):
        raise ValueError(
            "Enter a MySQL account host pattern permitting the VM and worker containers."
        )
    schemas = sorted(set(form.getlist("account_schemas")))
    if not schemas or len(schemas) > 50:
        raise ValueError("Select between 1 and 50 databases for the dedicated account.")
    for schema in schemas:
        identifier(schema)
        if schema.lower() in SYSTEM_SCHEMAS or (
            form.get("role") == "target" and schema.lower().startswith("mysql_")
        ):
            raise ValueError(
                "Dedicated account creation does not grant access to system databases."
            )
    return dict(
        setup_id=setup_id,
        username=username,
        account_host=host,
        schemas=schemas,
        vault_id=ocid(form.get("account_vault_id"), "vault"),
        key_id=ocid(form.get("account_key_id"), "key"),
        maintenance=form.get("account_maintenance") == "on",
    )


def plan(role, schemas, *, maintenance=False):
    if role == "source":
        return [
            ("*.*", ["REPLICATION SLAVE", "REPLICATION CLIENT", "SHOW DATABASES", "FLUSH_TABLES"])
        ] + [(quote(schema) + ".*", ["SELECT", "LOCK TABLES"]) for schema in schemas]
    if role != "target":
        raise ValueError("Select source or target.")
    privileges = ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE"]
    if maintenance:
        privileges += ["ALTER", "DROP"]
    return [(quote(schema) + ".*", privileges) for schema in schemas]


def check_grants(rows, grants):
    expected = {scope.replace("`", ""): set(privileges) for scope, privileges in grants}
    actual = {}
    for row in rows:
        for raw in row.values():
            match = re.match(r"^GRANT (.+?) ON (.+?) TO ", str(raw), re.I)
            if not match or "WITH GRANT OPTION" in str(raw).upper():
                raise SetupFailure(
                    "The dedicated account has unexpected role or grant-option access. Review it manually; the app will not revoke unrelated grants."
                )
            names = {name.strip().upper() for name in match[1].split(",")} - {"USAGE"}
            if not names:
                continue
            scope = match[2].replace("`", "")
            if not names <= expected.get(scope, set()):
                raise SetupFailure(
                    "The dedicated account has privileges outside the selected plan. Review it manually; no broad grants are removed automatically."
                )
            actual.setdefault(scope, set()).update(names)
    if any(not privileges <= actual.get(scope, set()) for scope, privileges in expected.items()):
        raise SetupFailure("The dedicated account is missing part of the selected privilege plan.")


def owned_statement(db, username, host, marker):
    try:
        rows = query(db, "SHOW CREATE USER %s@%s", (username, host))
    except Exception as error:
        if getattr(error, "errno", None) in {1396, 1141}:
            return False
        raise
    text = " ".join(str(v) for row in rows for v in row.values())
    if marker not in text or app_instance_id() not in text or "audit-setup-id" not in text:
        raise SetupFailure(
            "This MySQL account already exists and was not created by this setup. Choose a new username; its password and grants were not changed."
        )
    return True


def vault_secret(cloud, state, save, password):
    vault = get_resource(cloud, "vault", state["vault_id"])
    if vault.lifecycle_state != "ACTIVE":
        raise SetupFailure("Choose an ACTIVE Vault in the VM compartment.")
    client = oci.key_management.KmsManagementClient(
        cloud.sdk_config,
        service_endpoint=vault.management_endpoint,
        signer=cloud.signer,
        timeout=(5, 30),
        retry_strategy=oci.retry.NoneRetryStrategy(),
    )
    key = cloud.check(client.get_key(state["key_id"]).data)
    if key.lifecycle_state != "ENABLED" or key.key_shape.algorithm != "AES":
        raise SetupFailure("Choose an ENABLED AES encryption key in the selected Vault.")
    name = "audit-account-" + state["setup_id"]
    tags = {
        "managed-by": "heatwave-audit",
        "audit-app-id": app_instance_id(),
        "audit-setup-id": state["setup_id"],
    }
    if not state.get("secret_ocid"):
        matches = oci.pagination.list_call_get_all_results(
            cloud.vault.list_secrets,
            compartment_id=cloud.compartment,
            vault_id=vault.id,
            name=name,
        ).data
        if len(matches) > 1:
            raise SetupFailure("Multiple secrets match this setup. Review the Vault inventory.")
        if matches:
            secret = cloud.check(cloud.vault.get_secret(matches[0].id).data)
        else:
            if not 12 <= len(password) <= 256 or "\0" in password:
                raise SetupFailure(
                    "Enter a new dedicated-account password of 12–256 characters. It is sent only to MySQL and Vault."
                )
            details = oci.vault.models.CreateSecretDetails(
                compartment_id=cloud.compartment,
                vault_id=vault.id,
                key_id=key.id,
                secret_name=name,
                freeform_tags=tags,
                enable_auto_generation=False,
                description="Dedicated audit database account credential; no administrator credential.",
                secret_content=oci.vault.models.Base64SecretContentDetails(
                    content=base64.b64encode(password.encode()).decode()
                ),
            )
            secret = cloud.check(
                cloud.vault.create_secret(
                    details,
                    opc_retry_token=state["setup_id"],
                    retry_strategy=oci.retry.NoneRetryStrategy(),
                ).data
            )
        if (
            secret.vault_id != vault.id
            or secret.key_id != key.id
            or any((secret.freeform_tags or {}).get(k) != v for k, v in tags.items())
        ):
            raise SetupFailure("The secret does not match this setup, Vault and encryption key.")
        state["secret_ocid"] = secret.id
        save()
    for attempt in range(10):
        secret = cloud.check(cloud.vault.get_secret(state["secret_ocid"]).data)
        if (
            secret.vault_id != vault.id
            or secret.key_id != key.id
            or any((secret.freeform_tags or {}).get(k) != v for k, v in tags.items())
        ):
            raise SetupFailure("The secret ownership or Vault attachment changed; setup stopped.")
        if secret.lifecycle_state == "ACTIVE":
            return cloud.secret(secret.id)
        if secret.lifecycle_state != "CREATING":
            raise SetupFailure(
                "The account secret is not ACTIVE. Restore its availability before retrying."
            )
        if attempt < 9:
            time.sleep(1)
    raise SetupFailure(
        "The account secret is still being created. Retry this setup shortly; the same secret will be reused."
    )


def create(cloud, form, actor):
    admin = connection_values(form)
    values = parameters(form)
    if values["username"] == admin["username"]:
        raise ValueError("Use a dedicated username different from the setup administrator.")
    if admin["database_name"] not in values["schemas"]:
        raise ValueError("Include the connection default database in the selected databases.")
    if admin["role"] == "source" and values["maintenance"]:
        raise ValueError("Target maintenance permissions cannot be granted to a source account.")
    folder = ROOT / ".data/account-setups"
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = folder / (values["setup_id"] + ".json")
    intent = {
        **values,
        "actor": actor,
        "role": admin["role"],
        "host": admin["host"],
        "port": admin["port"],
        "database_name": admin["database_name"],
        "compartment_id": cloud.compartment,
        "region": cloud.config["region"],
    }
    state = json.loads(path.read_text()) if path.exists() else dict(intent)
    if any(state.get(key) != value for key, value in intent.items()):
        raise ValueError(
            "The setup fields changed. Begin a new account setup instead of reusing this request."
        )

    def save():
        write_private_json(path, state)

    def result():
        return dict(
            ok=True,
            username=state["username"],
            secret_ocid=state["secret_ocid"],
            schemas=state["schemas"],
            grants=[
                dict(scope=scope, privileges=names)
                for scope, names in plan(
                    state["role"], state["schemas"], maintenance=state["maintenance"]
                )
            ],
            message="Dedicated account created and tested. The connection now uses its username and Vault secret; the administrator is not registered.",
        )

    if state.get("complete"):
        dedicated = {**admin, "username": state["username"], "secret_ocid": state["secret_ocid"]}
        verify_connection(dedicated, cloud)
        return result()
    stage = "administrator connection"
    try:
        with connect(admin, cloud) as db:
            identity = query(
                db, "SELECT @@server_uuid AS id, @@partial_revokes AS partial_revokes", one=True
            )
            if state.get("server_identity") and state["server_identity"] != identity["id"]:
                raise SetupFailure("The database server identity changed. Setup was stopped.")
            if not identity["partial_revokes"] and any(
                "_" in schema for schema in values["schemas"]
            ):
                raise SetupFailure(
                    "Enable partial_revokes before scoped grants to database names containing underscores, or provision the account manually. No wildcard-expanded grant is issued."
                )
            state["server_identity"] = identity["id"]
            save()
            schemas = {
                row["name"]
                for row in query(db, "SELECT SCHEMA_NAME AS name FROM information_schema.SCHEMATA")
            }
            if not set(values["schemas"]) <= schemas:
                raise SetupFailure(
                    "A selected database is unavailable to the administrator. Reload the database list."
                )
            stage = "account ownership check"
            exists = owned_statement(
                db, values["username"], values["account_host"], values["setup_id"]
            )
            stage = "Vault password storage"
            password = vault_secret(cloud, state, save, form.get("account_password", ""))
            if not 12 <= len(password) <= 256 or "\0" in password:
                raise SetupFailure(
                    "The saved account credential is invalid. Restore its intended Vault value before retrying."
                )
            if not exists:
                stage = "MySQL account creation"
                attributes = json.dumps(
                    {"audit-app-id": app_instance_id(), "audit-setup-id": values["setup_id"]}
                )
                query(
                    db,
                    "CREATE USER %s@%s IDENTIFIED BY %s REQUIRE SSL ATTRIBUTE %s",
                    (values["username"], values["account_host"], password, attributes),
                )
            del password
            stage = "least-privilege grants"
            grants = plan(admin["role"], values["schemas"], maintenance=values["maintenance"])
            for scope, privileges in grants:
                query(
                    db,
                    "GRANT " + ", ".join(privileges) + " ON " + scope + " TO %s@%s",
                    (values["username"], values["account_host"]),
                )
            stage = "grant verification"
            check_grants(
                query(db, "SHOW GRANTS FOR %s@%s", (values["username"], values["account_host"])),
                grants,
            )
        stage = "dedicated account connection test"
        dedicated = {**admin, "username": values["username"], "secret_ocid": state["secret_ocid"]}
        verify_connection(dedicated, cloud)
        state["complete"] = True
        save()
        return result()
    except Exception as error:
        if isinstance(error, SetupFailure):
            message = str(error)
        elif isinstance(error, oci.exceptions.ServiceError):
            from .service_errors import oci_failure

            diagnostic = oci_failure(error)
            message = (
                diagnostic["message"]
                + " "
                + "Check same-compartment Vault secret creation/read and encryption-key access for the VM instance principal. "
                + ("Request: " + diagnostic["request_id"] + "." if diagnostic["request_id"] else "")
            )
        else:
            code, message = connection_failure(error)
            if getattr(error, "errno", None):
                message = "MySQL " + str(error.errno) + ": " + message
            if getattr(error, "errno", None) in {1142, 1227, 1044}:
                message = "The administrator needs CREATE USER and grantable privileges from the displayed plan. SUPER is not required."
            elif getattr(error, "errno", None) == 1396:
                message = "MySQL could not create this account. It may already exist; the app does not reset existing accounts."
            elif getattr(error, "errno", None) == 1819:
                message = "The password does not satisfy the server password policy. Begin a new setup with a compliant password."
        reference = (
            " Saved credential secret: " + state["secret_ocid"] + "."
            if state.get("secret_ocid")
            else ""
        )
        raise SetupFailure(
            "Account setup failed at "
            + stage
            + ": "
            + message
            + reference
            + " Retry unchanged setup fields to resume. No existing account or secret is deleted automatically."
        ) from None
