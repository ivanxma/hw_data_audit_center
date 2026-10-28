"""Creation fields for the OCI services exposed by Configuration & setup."""


def field(name, label, kind="text", **kwargs):
    return {"name": name, "label": label, "type": kind, **kwargs}


NAME = field("name", "Name", required=True)
SUBNET = field("subnet_id", "Subnet", "select", options="subnet", required=True)
FORMS = {
    "stream_pool": [
        NAME,
        field("subnet_id", "Private endpoint subnet (optional)", "select", options="subnet"),
    ],
    "stream": [
        NAME,
        field("stream_pool_id", "Stream pool", "select", options="stream_pool", required=True),
        field("partitions", "Partitions", "number", value=1, min=1, required=True),
        field(
            "retention", "Retention (hours)", "number", value=168, min=24, max=168, required=True
        ),
    ],
    "repository": [NAME],
    "vault": [NAME],
    "secret": [
        NAME,
        field("vault_id", "Vault", "select", options="vault", required=True),
        field("key_id", "Encryption key", "select", options="key", required=True),
        field("value", "Secret value", "password", required=True),
    ],
    "heatwave": [
        NAME,
        field("admin_username", "Administrator username", required=True),
        field(
            "admin_password_secret_ocid",
            "Administrator password secret",
            "select",
            options="secret",
            required=True,
        ),
        SUBNET,
        field(
            "availability_domain",
            "Availability domain",
            "select",
            options="availability_domain",
            required=True,
        ),
        field("shape_name", "DB system shape", "select", options="mysql_shape", required=True),
        field(
            "configuration_id",
            "Configuration for selected shape",
            "select",
            options="mysql_configuration",
            required=True,
        ),
        field(
            "mysql_version",
            "MySQL HeatWave version",
            "select",
            options="mysql_version",
            required=True,
        ),
        field("storage", "Storage (GB)", "number", value=50, min=50, required=True),
    ],
    "container": [field("flow_id", "Registered flow", "flow", required=True)],
}
