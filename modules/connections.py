"""Connection registration and actual MySQL reachability/authentication checks."""

from .mysql_util import open_connection
from .repository import query
from .validation import identifier, ocid


def connection_values(form):
    values = {
        name: form.get(name, "").strip()
        for name in ("name", "host", "username", "database_name", "secret_ocid")
    }
    if not all(values.values()):
        raise ValueError("Complete all required connection fields.")
    if form.get("role") not in {"source", "target"}:
        raise ValueError("Select source or target.")
    identifier(values["database_name"])
    ocid(values["secret_ocid"], "vaultsecret")
    try:
        port = int(form.get("port", "3306"))
    except (TypeError, ValueError):
        raise ValueError("Enter a port between 1 and 65535.") from None
    if not 1 <= port <= 65535:
        raise ValueError("Enter a port between 1 and 65535.")
    values.update(role=form["role"], port=port)
    return values


def verify_connection(connection, cloud):
    cloud.validate_connection(connection)
    db, _ = open_connection(
        {
            "mode": "tcp",
            "host": connection["host"],
            "port": connection["port"],
            "database": connection["database_name"],
            **cloud.tls_profile(),
            "connect_timeout": 10,
            "read_timeout": 10,
            "write_timeout": 10,
        },
        connection["username"],
        cloud.secret(connection["secret_ocid"]),
    )
    try:
        query(db, "SELECT 1 AS connected", one=True)
        result = query(
            db, "SELECT @@server_uuid AS server_uuid, VERSION() AS server_version", one=True
        )
        if not result or not result.get("server_uuid"):
            raise ValueError("The server did not return a stable MySQL identity.")
        return result
    finally:
        db.close()


def connection_failure(error):
    """Return actionable categories without exposing driver strings or credentials."""
    import oci

    number = getattr(error, "errno", None)
    if isinstance(error, oci.exceptions.ServiceError):
        return (
            "vault_access",
            "The VM cannot read the Vault secret. Check its compartment and secret-bundle access.",
        )
    if number == 1045:
        return (
            "authentication",
            "MySQL rejected the username or Vault password. Check the account and allowed client hosts.",
        )
    if number == 1049:
        return "database", "The selected database does not exist or is not accessible."
    if number in {1044, 1142, 1227}:
        return (
            "privileges",
            "MySQL authentication succeeded, but the account lacks permission for this database or test.",
        )
    if number == 2026:
        return (
            "tls",
            "TLS handshake failed. Check the global MySQL TLS policy and any configured trust material.",
        )
    if number in {2002, 2003, 2005, 2013, 2055} or isinstance(
        error, (TimeoutError, ConnectionError)
    ):
        return (
            "network",
            "Cannot reach the MySQL endpoint. Check host, port, routing and security rules.",
        )
    if isinstance(error, ValueError) and "outside the VM compartment" in str(error):
        return "vault_scope", "The Vault secret is outside the VM compartment."
    return (
        "connection",
        "Connection test failed. Check the endpoint, Vault access and database account.",
    )
