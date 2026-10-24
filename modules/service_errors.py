"""Actionable OCI failures without exposing response bodies or request credentials."""

import re


def mysql_failure(error, endpoint):
    """Explain driver codes without exposing SQL, row values, accounts or endpoints."""
    code = getattr(error, "errno", None)
    code = code if isinstance(code, int) and 0 < code <= 65535 else None
    label = {
        "validation": "Source validation",
        "flow_action": "Flow operation",
        "prepare_flow_topics": "Schema topic preparation",
        "audit": "Audit history lookup",
        "audit_partitions": "Audit partition lookup",
    }.get(endpoint, "Database operation")
    message = label + " failed" + (" (MySQL " + str(code) + ")" if code else "") + "."
    status = 503
    if code == 1146:
        guidance = (
            "An audit table is unavailable. For a new mapping, complete source checks, prepare "
            "the schema topic and deploy from Control Center to initialize the target tables. "
            "For an existing deployment, verify the target schema and table access."
            if endpoint in {"audit", "audit_partitions"}
            else "A required database table is unavailable. Check repository setup and the selected source/target objects."
        )
        status = 409
    elif code in {1044, 1045, 1142, 1143, 1227}:
        guidance = "MySQL denied database access. Check the selected database account, Vault password secret and required database grants."
        status = 403
    elif code == 1049:
        guidance = "The configured database does not exist. Check the database name in the selected connection."
        status = 409
    elif code in {1054, 1064}:
        guidance = "MySQL rejected an application query or table layout. Use the operation and MySQL code shown here to investigate compatibility."
        status = 500
    elif code in {2002, 2003, 2005}:
        guidance = (
            "Cannot connect to the configured database. Verify that its DB system is ACTIVE, "
            "then check the configured endpoint/port, DNS, routes and security rules. "
            "An INACTIVE DB system must be started before retrying."
        )
    elif code in {2006, 2013, 2055}:
        guidance = "The database connection was lost. Check DB availability or a restart, then reconnect and retry the operation."
    elif code == 1062:
        guidance = "A registration with these unique values already exists. Refresh and select the existing registration."
        status = 409
    else:
        guidance = "Check database availability and the selected connection. Use the MySQL code shown here when investigating."
    return {"message": message, "guidance": guidance, "response_status": status, "code": code}


def oci_failure(error):
    operation = getattr(error, "operation_name", "") or ""
    normalized = operation.replace("_", "").lower()
    resource = None
    label = "OCI service request"
    for fragment, title, policy_type in (
        ("dbsystem", "HeatWave DB systems", None),
        ("streampool", "stream pools", "stream-pools"),
        ("connectharness", "Connect configurations", "connect-harness"),
        ("kafkaclusterconfig", "Kafka configurations", None),
        ("kafkacluster", "Kafka clusters", None),
        ("subnet", "subnets", None),
        ("vcn", "VCNs", None),
        ("stream", "streams", "streams"),
    ):
        if fragment in normalized:
            resource = policy_type
            verb = next(
                (
                    v
                    for v in ("create", "list", "get", "delete", "update", "start", "stop")
                    if normalized.startswith(v)
                ),
                "access",
            )
            label = verb.capitalize() + " " + title
            break
    code = getattr(error, "code", "")
    allowed_codes = {
        "NotAuthorizedOrNotFound",
        "NotAuthorized",
        "NotAuthenticated",
        "InvalidParameter",
        "MissingParameter",
        "LimitExceeded",
        "QuotaExceeded",
        "TooManyRequests",
        "Conflict",
        "IncorrectState",
        "InternalServerError",
        "ServiceUnavailable",
        "NotFound",
        "NoEtagMatch",
        "PreconditionFailed",
    }
    code = code if code in allowed_codes else "ServiceError"
    status = getattr(error, "status", None)
    status = status if isinstance(status, int) and 100 <= status <= 599 else 503
    request_id = getattr(error, "request_id", "") or ""
    if not re.fullmatch(r"[A-Za-z0-9/_.:-]{1,256}", request_id):
        request_id = ""
    policy = ""
    if status in {401, 403, 404}:
        guidance = (
            "OCI could not authorize this operation or locate the resource in the VM compartment. "
            "Ask the OCI administrator to check the VM instance principal's dynamic-group membership "
            "and permissions in that compartment."
        )
        if resource:
            guidance += " Access to streams alone does not grant access to stream pools or Connect configurations."
            policy = (
                "Allow dynamic-group <VM_DYNAMIC_GROUP> to manage "
                + resource
                + " in compartment id <VM_COMPARTMENT_OCID>"
            )
        if "kafkacluster" in normalized:
            guidance += (
                " OCI Streaming stream-family permissions do not cover OCI Streaming with Apache Kafka. "
                "Listing requires KAFKA_CLUSTER_INSPECT and KAFKA_CLUSTER_CONFIG_INSPECT. "
                "If these are already granted, verify the policy scope and service availability in the VM region."
            )
            permissions = ["KAFKA_CLUSTER_INSPECT", "KAFKA_CLUSTER_CONFIG_INSPECT"]
            if not normalized.startswith("list"):
                permissions = [
                    prefix + suffix
                    for prefix in ("KAFKA_CLUSTER_", "KAFKA_CLUSTER_CONFIG_")
                    for suffix in ("INSPECT", "READ", "CREATE", "UPDATE", "DELETE")
                ]
            policy = (
                "Allow dynamic-group <VM_DYNAMIC_GROUP> to {"
                + ", ".join(permissions)
                + "} in compartment id <VM_COMPARTMENT_OCID>"
            )
        response_status = 403
    elif code in {"LimitExceeded", "QuotaExceeded"}:
        guidance = "Check this service's regional quota and capacity before retrying."
        response_status = 409
    elif status == 429 or status >= 500:
        guidance = "OCI is temporarily unavailable or throttling requests. Refresh the inventory before retrying the same creation request."
        response_status = 503
    elif status == 412:
        guidance = (
            "OCI rejected the HeatWave operation precondition. This app does not send an "
            "ETag/If-Match header for HeatWave operations. Refresh the DB state and check "
            "for an active start/stop/update work request. If it repeats, retain this request "
            "ID for service investigation; changing IAM or a database password will not fix it."
            if "dbsystem" in normalized
            else "OCI rejected the resource version check. Refresh the resource list, wait for any active operation to finish, and retry."
        )
        response_status = 409
    elif status == 409:
        guidance = "Refresh the resource state and resolve the conflict before retrying."
        response_status = 409
    else:
        guidance = "Review the selected resource settings and referenced resources. Use the OCI request ID when investigating the rejected request."
        response_status = 400
    return {
        "message": f"{label} failed: OCI {code} (HTTP {status}).",
        "guidance": guidance,
        "policy": policy,
        "request_id": request_id,
        "code": code,
        "operation": label,
        "response_status": response_status,
    }
