"""Private OCIR destinations and principal-based authentication."""

import base64
import json

import oci
import requests


def endpoint(region):
    code = next(
        (key for key, value in oci.regions.REGIONS_SHORT_NAMES.items() if value == region), None
    )
    if not code:
        raise ValueError("No OCIR endpoint is configured for this VM region.")
    return code + ".ocir.io"


def destination(cloud, repository_id):
    repository = cloud.check(cloud.registry.get_container_repository(repository_id).data)
    if repository.is_public:
        raise ValueError("Select a private registry repository in the VM compartment.")
    return repository, endpoint(
        cloud.config["region"]
    ) + "/" + repository.namespace + "/" + repository.display_name


def pull_mode(settings):
    mode = settings.get("image_pull_auth") or (
        "vault" if settings.get("image_pull_secret_ocid") else "resource_principal"
    )
    if mode not in {"resource_principal", "vault"}:
        raise ValueError("Select resource principal or Vault image authentication.")
    return mode


def write_push_auth(cloud, repository_id, path):
    """Exchange the VM identity for a short-lived, repository-scoped OCIR token."""
    repository, image_prefix = destination(cloud, repository_id)
    host = image_prefix.split("/", 1)[0]
    response = requests.get(
        "https://" + host + "/20180419/docker/token",
        params={
            "service": host,
            "scope": "repository:"
            + repository.namespace
            + "/"
            + repository.display_name
            + ":pull,push",
        },
        auth=cloud.signer,
        headers={"Accept": "application/json"},
        timeout=(5, 30),
        allow_redirects=False,
    )
    if response.status_code != 200:
        raise ValueError(
            "OCIR did not grant the VM a publish token. Check its compartment-scoped repository permissions."
        )
    token = response.json().get("token")
    if not isinstance(token, str) or not token:
        raise ValueError("OCIR returned no publish token.")
    encoded = base64.b64encode(("BEARER_TOKEN:" + token).encode()).decode()
    with path.open("x") as handle:
        path.chmod(0o600)
        json.dump({"auths": {host: {"auth": encoded}}}, handle)
    return image_prefix
