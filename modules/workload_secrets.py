"""Resource-principal secret resolution for container workloads only."""

import base64

import oci


class WorkloadSecrets:
    def __init__(self, config):
        signer = oci.auth.signers.get_resource_principals_signer()
        sdk = {"region": config["region"]}
        self.compartment = config["compartment_id"]
        self.vault = oci.vault.VaultsClient(sdk, signer=signer, timeout=(5, 30))
        self.secrets = oci.secrets.SecretsClient(sdk, signer=signer, timeout=(5, 30))

    def __call__(self, secret_id):
        if self.vault.get_secret(secret_id).data.compartment_id != self.compartment:
            raise ValueError("Secret is outside the deployment compartment.")
        content = self.secrets.get_secret_bundle(secret_id).data.secret_bundle_content
        if content.content_type != "BASE64":
            raise ValueError("Unsupported Vault secret encoding.")
        return base64.b64decode(content.content, validate=True).decode()
