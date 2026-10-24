"""One global MySQL TLS policy for web tests, readers and container writers."""

MODES = {"REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY"}


def tls_profile(config, *, container=False):
    settings = config.get("mysql_tls", {})
    mode = str(settings.get("mode") or "REQUIRED").upper()
    if mode not in MODES:
        raise ValueError("Global MySQL TLS mode must require encryption.")
    profile = {"ssl_mode": mode, "ssl_ca": None}
    if mode != "REQUIRED":
        profile["ssl_ca"] = (
            settings.get("container_ca_bundle" if container else "vm_ca_bundle") or None
        )
    return profile
