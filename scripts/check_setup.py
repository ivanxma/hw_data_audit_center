#!/usr/bin/env python3
"""Read-only OL9 setup and optional same-compartment OCI inventory checks."""

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from modules.config import ROOT, load_config  # noqa: E402
from modules.runtime_env import load_runtime_defaults  # noqa: E402
from modules.service_errors import oci_failure  # noqa: E402


def check(rows, name, action):
    try:
        result = action()
        rows.append({"check": name, "status": "passed", **(result or {})})
    except Exception as error:
        item = {"check": name, "status": "failed", "error_type": type(error).__name__}
        if hasattr(error, "status") and hasattr(error, "code"):
            safe = oci_failure(error)
            item.update(code=safe["code"], request_id=safe["request_id"], guidance=safe["guidance"])
        rows.append(item)  # Never print raw SDK exceptions, endpoint details or secret values.


def require(condition):
    if not condition:
        raise ValueError("Check failed")


def local_checks():
    rows = []
    check(rows, "python_3_12_or_newer", lambda: require(sys.version_info >= (3, 12)))
    check(
        rows,
        "private_runtime_file",
        lambda: require(
            (ROOT / ".runtime.env").is_file() and not (ROOT / ".runtime.env").stat().st_mode & 0o077
        ),
    )
    check(
        rows, "runtime_defaults", lambda: load_runtime_defaults(ROOT / ".runtime.env", os.environ)
    )

    def private_config():
        config = load_config(required=True)
        from modules.validation import ocid

        ocid(config.get("compartment_id"), "compartment")
        require(bool(config.get("region")))
        require(not Path(os.environ["AUDIT_CONFIG_FILE"]).expanduser().stat().st_mode & 0o077)

    check(rows, "private_deployment_configuration", private_config)
    for binary in ("java", "mvn", "podman"):
        check(rows, binary + "_installed", lambda name=binary: require(bool(shutil.which(name))))
    for binary in ("mysqld", "mysql", "mysqlbinlog"):
        path = ROOT / ".embedded/mysql-server/current/bin" / binary
        check(
            rows,
            "embedded_" + binary,
            lambda p=path: require(p.is_file() and os.access(p, os.X_OK)),
        )
    check(
        rows,
        "streaming_principal_jars",
        lambda: require(
            any((ROOT / ".data/java/streaming-libs").glob("oci-java-sdk-addons-sasl-*.jar"))
        ),
    )
    for service in ("local-mysql", "https", "image-builder"):

        def active(name=service):
            result = subprocess.run(
                ["systemctl", "is-active", "heatwave-audit-" + name + ".service"],
                capture_output=True,
                timeout=10,
                check=False,
            )
            require(result.returncode == 0)

        check(rows, "service_" + service, active)
    return rows


def cloud_checks(cloud, transport):
    import oci

    from modules.resource_catalog import SERVICES, client_for

    rows = []
    kinds = ["container", "repository", "vault", "secret", "heatwave"]
    if transport in {"streaming", "both"}:
        kinds += ["stream_pool", "stream", "connect_harness"]
    if transport in {"kafka", "both"}:
        kinds += ["kafka_cluster", "kafka_config"]
    for kind in kinds:

        def inspect(name=kind):
            method = getattr(client_for(cloud, name), SERVICES[name][3])
            data = oci.pagination.list_call_get_all_results(
                method, compartment_id=cloud.compartment
            ).data
            items = data if isinstance(data, list) else data.items
            for item in items:
                cloud.check(item)
            return {
                "visible_resources": sum(
                    getattr(r, "lifecycle_state", "") != "DELETED" for r in items
                )
            }

        check(rows, "inventory_" + kind, inspect)
    for role in ("debezium", "processor"):

        def selected(name=role):
            settings = cloud.config.get(name, {})
            subnet = cloud.check(cloud.network.get_subnet(settings["subnet_id"]).data)
            cloud.check(cloud.network.get_vcn(subnet.vcn_id).data)
            repo = cloud.check(
                cloud.registry.get_container_repository(settings["repository_id"]).data
            )
            require(not repo.is_public)
            image = cloud.check(cloud.registry.get_container_image(settings["image_id"]).data)
            require(
                image.repository_id == repo.id
                and settings["image_url"].endswith("@" + image.digest)
            )

        check(rows, "selected_" + role + "_resources", selected)
    secret_id = cloud.config.get("fingerprint_secret_ocid")
    check(
        rows,
        "fingerprint_secret_metadata",
        lambda: require(
            bool(secret_id)
            and cloud.check(cloud.vault.get_secret(secret_id).data).lifecycle_state == "ACTIVE"
        ),
    )
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--oci", action="store_true", help="Use the VM instance principal; no resource mutations."
    )
    parser.add_argument("--transport", choices=("streaming", "kafka", "both"), default="streaming")
    args = parser.parse_args()
    rows = local_checks()
    if args.oci:
        from modules.oci_service import Cloud

        clients = []
        check(rows, "vm_identity_compartment_and_region", lambda: clients.append(Cloud()))
        if clients:
            rows.extend(cloud_checks(clients[0], args.transport))
    print(
        json.dumps(
            {
                "checks": rows,
                "scope": "local" if not args.oci else "VM instance principal / VM compartment",
                "limits": "Read/list checks do not prove create/delete grants, container-principal permissions, MySQL login or end-to-end delivery. Secret values are not retrieved.",
            },
            indent=2,
        )
    )
    return 1 if any(r["status"] == "failed" for r in rows) else 0


if __name__ == "__main__":
    raise SystemExit(main())
