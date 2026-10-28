"""All control-plane mutations are VM instance-principal and compartment constrained."""

import base64
import json

import oci
import requests

from .config import ROOT, app_instance_id, load_config
from .transport import transport_profile
from .validation import ocid


class Cloud:
    def __init__(self):
        self.config = load_config(required=True)
        metadata = requests.get(
            "http://169.254.169.254/opc/v2/instance/",
            headers={"Authorization": "Bearer Oracle"},
            timeout=3,
        ).json()
        self.compartment = ocid(self.config.get("compartment_id"), "compartment")
        if metadata["compartmentId"] != self.compartment:
            raise ValueError(
                "Private compartment does not match the VM compartment. No action allowed."
            )
        if self.config.get("region") != metadata["region"]:
            # IMDS canonicalRegionName is present on newer images.
            if self.config.get("region") != metadata.get("canonicalRegionName"):
                raise ValueError("Configured region must match the VM region.")
        self.signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        self.sdk_config = {"region": self.config["region"]}
        self.containers = self.client(oci.container_instances.ContainerInstanceClient)
        self.streams = self.client(oci.streaming.StreamAdminClient)
        self.vault = self.client(oci.vault.VaultsClient)
        self.secrets = self.client(oci.secrets.SecretsClient)
        self.mysql = self.client(oci.mysql.DbSystemClient)
        self.network = self.client(oci.core.VirtualNetworkClient)
        self.registry = self.client(oci.artifacts.ArtifactsClient)
        self.kms = None

    def client(self, cls):
        return cls(
            self.sdk_config,
            signer=self.signer,
            timeout=(5, 30),
            retry_strategy=oci.retry.NoneRetryStrategy(),
        )

    def check(self, obj):
        if obj.compartment_id != self.compartment:
            raise ValueError("Resource is outside the VM compartment. Action refused.")
        return obj

    def secret(self, secret_id):
        self.check(self.vault.get_secret(ocid(secret_id, "vaultsecret")).data)
        content = self.secrets.get_secret_bundle(secret_id).data.secret_bundle_content.content
        return base64.b64decode(content).decode()

    def validate_connection(self, connection):
        # Database endpoints are supplied by the operator and tested over MySQL.
        # Only app-managed OCI resources/secret references are compartment checked.
        self.check(self.vault.get_secret(connection["secret_ocid"]).data)

    def tls_profile(self, *, container=False):
        from .tls_policy import tls_profile

        return tls_profile(self.config, container=container)

    def validate_flow(self, flow):
        self.validate_connection(flow["source"])
        self.validate_connection(flow["target"])
        if flow.get("queue_connection") and "resource_ocid" in flow["queue_connection"]:
            from .queue_connections import profile

            flow["queue_connection"] = profile(self, flow["queue_connection"])
        transport = transport_profile(self.config, flow)
        from .config import fingerprint_secret
        from .streaming_service import resolve_streams

        self.check(self.vault.get_secret(fingerprint_secret(self.config)).data)
        return resolve_streams(self, flow, transport)

    def owned_container(self, resource_id, flow_id):
        instance = self.check(self.containers.get_container_instance(resource_id).data)
        tags = instance.freeform_tags or {}
        if (
            tags.get("managed-by") != "heatwave-audit"
            or tags.get("flow-id") != str(flow_id)
            or tags.get("audit-app-id") != app_instance_id()
        ):
            raise ValueError("Container ownership does not match this flow.")
        return instance

    def require_writer_protocol(self, resource_id, flow_id):
        instance = self.owned_container(resource_id, flow_id)
        if (instance.freeform_tags or {}).get("writer-protocol") != "3":
            raise ValueError(
                "Migrate this flow to the native OCI Streaming writer before restarting it."
            )
        return instance

    def container_action(self, resource_id, action, flow_id):
        instance = self.owned_container(resource_id, flow_id)
        if (action == "start" and instance.lifecycle_state == "ACTIVE") or (
            action == "stop" and instance.lifecycle_state == "INACTIVE"
        ):
            return None
        if action in {"start", "restart"}:
            tags = instance.freeform_tags or {}
            protocol = (
                "writer-protocol"
                if tags.get("workload-role") == "history-writer"
                else "capture-control-protocol"
            )
            if tags.get(protocol) != "3":
                raise ValueError(
                    "Migrate this flow to native OCI Streaming before starting its legacy workload."
                )
        if action == "start":
            return self.containers.start_container_instance(resource_id)
        if action == "restart":
            return self.containers.restart_container_instance(resource_id)
        if action == "stop":
            return self.containers.stop_container_instance(resource_id)
        raise ValueError("Unsupported container action.")

    def create_processor(self, flow, *, replacement_of=None):
        queue = self.validate_flow(flow)
        self.require_native_image("processor")
        self.require_message_image("processor")
        self.validate_scope_image(flow, "processor")
        settings = self.config["processor"]
        self.check(self.network.get_subnet(settings["subnet_id"]).data)
        self.check(self.registry.get_container_repository(settings["repository_id"]).data)
        source = flow["source"]
        # Repository rows include timestamps and control-plane metadata. Send only
        # the connection fields consumed by the worker, plus global TLS settings.
        target = {key: flow["target"][key] for key in ("host", "port", "username", "secret_ocid")}
        target.update(self.tls_profile(container=True))
        payload = {
            k: flow[k]
            for k in (
                "id",
                "name",
                "source_schema",
                "source_table",
                "target_schema",
                "target_table",
                "topic",
                "transport",
            )
        }
        from .message_policy import policy

        payload["message_policy"] = policy(self.config)
        payload["message_protocol"] = 1
        payload["source_exclusions"] = flow.get("source_exclusions") or []
        payload["target_grouping"] = flow.get("target_grouping") or "flow"
        payload["target"] = target
        payload["source_identity"] = source["server_identity"]
        payload["lineage_id"] = flow.get("lineage_id") or flow["connector_name"]
        payload["queue_incarnation"] = transport_profile(self.config, flow)["incarnation"]
        payload["writer_protocol"] = 3
        from dataclasses import asdict

        from .ingestion import BatchSettings

        payload["batch"] = asdict(BatchSettings.from_config(settings.get("batch", {})))
        payload["schema_topic"] = "audit_" + str(flow["id"])
        payload["fingerprint_secret_ocid"] = self.config["fingerprint_secret_ocid"]
        payload["compartment_id"] = self.compartment
        payload["region"] = self.config["region"]
        payload["queue"] = queue
        return self.create_workload(
            flow,
            settings,
            payload,
            "history-writer",
            ["python", "worker.py"],
            replacement_of=replacement_of,
        )

    def create_debezium(self, flow, *, replacement_of=None):
        self.require_message_image("processor")
        queue = self.validate_flow(flow)
        from .debezium import connector_config

        settings = self.config["debezium"]
        payload = {
            "compartment_id": self.compartment,
            "region": self.config["region"],
            "queue": queue,
            "streams": queue["streams"],
            "runtime_protocol": 3,
            "source_secret_ocid": flow["source"]["secret_ocid"],
            "history_secret_ocid": flow["target"]["secret_ocid"],
            "connector_name": flow["connector_name"],
            "connector": connector_config(flow, self.config),
        }
        from .message_policy import policy

        payload["message_policy"] = policy(self.config)
        payload["message_protocol"] = 1
        payload["state_jdbc_url"] = payload["connector"][
            "schema.history.internal.jdbc.connection.url"
        ]
        from .debezium import require_initial_gtid_image
        from .capture_requests import initialize
        from .heatwave import connect
        from .repository import query
        from .validation import quote

        capture_manifest = self.debezium_build_manifest()
        require_initial_gtid_image(capture_manifest)
        with connect(flow["target"], self) as db:
            initialize(db, flow["target_schema"])
            if flow.get("connector_ocid"):
                offsets = query(
                    db,
                    "SELECT offset_key FROM "
                    + quote(flow["target_schema"])
                    + ".audit_native_offsets WHERE flow_id=%s",
                    (flow["id"],),
                )
                if not offsets:
                    raise ValueError(
                        "Migrate the existing capture checkpoint before deploying its native replacement."
                    )
        control = {"flow_id": flow["id"]}
        for role, database in (
            ("source", flow["source_schema"]),
            ("target", flow["target_schema"]),
        ):
            control[role] = {k: flow[role][k] for k in ("host", "port", "username", "secret_ocid")}
            control[role].update(
                database=(flow[role].get("database_name") or None) if database == "*" else database,
                **self.tls_profile(container=True),
            )
        payload["capture_control"] = control
        from .capture_start import selection

        initial = selection(flow.get("capture_start"))
        if initial["mode"] == "gtid":
            require_initial_gtid_image(capture_manifest)
            from .gtid_position import locate_gtid
            from .heatwave import connect

            if not flow.get("connector_ocid"):
                with connect(flow["source"], self) as source:
                    locate_gtid(source, initial["value"], check_schema=True)
            payload["initial_capture"] = initial
            payload["initial_capture_table"] = flow["source_table"]
            payload["initial_capture_scope"] = {
                k: flow.get(k) for k in ("source_schema", "source_table", "source_exclusions")
            }
        return self.create_workload(
            flow, settings, payload, "debezium", None, replacement_of=replacement_of
        )

    def debezium_build_manifest(self, role="debezium"):
        from .image_builds import inventory, job_path, load

        settings = self.config[role]
        release = next(
            (
                r
                for r in inventory()
                if r["status"] == "complete"
                and r["role"] == role
                and r["image_id"] == settings.get("image_id")
                and settings["image_url"].endswith("@" + str(r.get("digest", "")))
            ),
            None,
        )
        return load(job_path(release["id"]))["manifest"] if release else {}

    def validate_initial_capture_image(self, flow):
        from .capture_start import selection
        from .debezium import require_initial_gtid_image

        require_initial_gtid_image(self.debezium_build_manifest())
        if flow.get("processor_ocid"):
            writer = self.owned_container(flow["processor_ocid"], flow["id"])
            if (writer.freeform_tags or {}).get("message-protocol") != "1":
                raise ValueError(
                    "Update the history writer image first. Oversized-message capture requires a writer with durable fragment reassembly."
                )
        if flow.get("connector_ocid"):
            instance = self.owned_container(flow["connector_ocid"], flow["id"])
            if (instance.freeform_tags or {}).get("capture-control-protocol") != "3":
                raise ValueError(
                    "Migrate this flow's existing capture checkpoint before updating to the native image."
                )

    def require_message_image(self, role):
        from .debezium import matches_build

        files = (
            (
                "modules/message_policy.py",
                "modules/message_fragments.py",
                "modules/native_streaming.py",
                "modules/ingestion.py",
                "modules/events.py",
                "modules/schema_history.py",
                "worker.py",
            )
            if role == "processor"
            else (
                "modules/message_policy.py",
                "deploy/native-capture/src/main/java/local/heatwave/audit/MessageFraming.java",
                "deploy/native-capture/src/main/java/local/heatwave/audit/StreamingPublisher.java",
            )
        )
        if not matches_build(self.debezium_build_manifest(role), files):
            raise ValueError(
                "Build and select a current " + role + " image with oversized-message support."
            )

    def validate_scope_image(self, flow, role):
        if flow["source_table"] != "*" and (flow.get("target_grouping") or "flow") == "flow":
            return
        from .debezium import matches_build

        files = (
            ("modules/capture_scope.py", "modules/debezium.py", "modules/initial_capture.py")
            if role == "debezium"
            else (
                "modules/capture_scope.py",
                "modules/target_routes.py",
                "modules/events.py",
                "modules/schema_history.py",
                "modules/ingestion.py",
                "modules/heatwave.py",
                "modules/retention.py",
            )
        )
        if not matches_build(self.debezium_build_manifest(role), files):
            raise ValueError(
                "Build and select a current "
                + role
                + " image with wildcard capture and target grouping support."
            )

    def require_native_image(self, role):
        from .workload_images import releases

        settings = self.config[role]
        if not any(
            row["image_id"] == settings.get("image_id")
            and row["image_url"] == settings.get("image_url")
            for row in releases(self, role)
        ):
            raise ValueError(
                "Build and select a verified native OCI Streaming image for this workload before deployment."
            )

    def validate_workload(self, settings, *, require_image=False):
        """Verify deployment attachments before stopping an existing workload."""
        repository = self.check(
            self.registry.get_container_repository(settings["repository_id"]).data
        )
        if repository.is_public:
            raise ValueError("Workload images must come from a private registry repository.")
        expected = (
            settings["registry_endpoint"]
            + "/"
            + repository.namespace
            + "/"
            + repository.display_name
        )
        import re

        if not re.fullmatch(re.escape(expected) + r"@sha256:[0-9a-f]{64}", settings["image_url"]):
            raise ValueError(
                "Use a digest-pinned image from the verified same-compartment repository."
            )
        if require_image and not settings.get("image_id"):
            raise ValueError("Select and save a published registry image in workload settings.")
        if settings.get("image_id"):
            image = self.check(self.registry.get_container_image(settings["image_id"]).data)
            if (
                image.lifecycle_state != "AVAILABLE"
                or image.repository_id != settings["repository_id"]
                or settings["image_url"] != expected + "@" + image.digest
            ):
                raise ValueError(
                    "The selected registry image is unavailable or its digest changed."
                )
        subnet = self.check(self.network.get_subnet(settings["subnet_id"]).data)
        self.check(self.network.get_vcn(subnet.vcn_id).data)
        from .image_registry import pull_mode

        image_pull_secrets = None
        if pull_mode(settings) == "vault":
            image_secret = settings.get("image_pull_secret_ocid")
            if not image_secret:
                raise ValueError(
                    "Select a Vault image pull secret or use resource principal authentication."
                )
            self.check(self.vault.get_secret(image_secret).data)
            image_pull_secrets = [
                oci.container_instances.models.CreateVaultImagePullSecretDetails(
                    registry_endpoint=settings["registry_endpoint"], secret_id=image_secret
                )
            ]
        return image_pull_secrets

    def create_workload(self, flow, settings, payload, name, command, *, replacement_of=None):
        image_pull_secrets = self.validate_workload(settings)
        # Only non-secret settings and Vault OCIDs enter OCI container metadata.
        details = oci.container_instances.models.CreateContainerInstanceDetails(
            compartment_id=self.compartment,
            display_name=name + "-" + str(flow["id"]),
            availability_domain=settings["availability_domain"],
            shape=settings["shape"],
            shape_config=oci.container_instances.models.CreateContainerInstanceShapeConfigDetails(
                ocpus=settings.get("ocpus", 1), memory_in_gbs=settings.get("memory_in_gbs", 4)
            ),
            vnics=[
                oci.container_instances.models.CreateContainerVnicDetails(
                    subnet_id=settings["subnet_id"], is_public_ip_assigned=False
                )
            ],
            containers=[
                oci.container_instances.models.CreateContainerDetails(
                    display_name=name,
                    image_url=settings["image_url"],
                    environment_variables={"AUDIT_WORKER_CONFIG": json.dumps(payload)},
                    is_resource_principal_disabled=False,
                    command=command,
                )
            ],
            image_pull_secrets=image_pull_secrets,
            freeform_tags={
                "managed-by": "heatwave-audit",
                "audit-app-id": app_instance_id(),
                "flow-id": str(flow["id"]),
                "workload-role": name,
                "writer-protocol": str(payload.get("writer_protocol", "none")),
                "message-protocol": str(payload.get("message_protocol", "none")),
                "capture-control-protocol": str(payload.get("runtime_protocol", "none")),
            },
            graceful_shutdown_timeout_in_seconds=120,
            container_restart_policy="ALWAYS",
        )
        from .fingerprints import digest

        identity = [flow["connector_name"], flow["id"], name, payload, settings["image_url"]]
        if replacement_of:
            # Stable for retries of this replacement, distinct when an older image
            # is deliberately deployed again from a different registered instance.
            identity.append(replacement_of)
        retry_token = digest(identity)
        return self.containers.create_container_instance(
            details, opc_retry_token=retry_token
        ).data.id

    def provision(self, kind, spec, retry_token=None):
        """Create typed, allowlisted resources after verifying every attachment's compartment."""
        models = {
            "stream_pool": (
                self.streams,
                "create_stream_pool",
                oci.streaming.models.CreateStreamPoolDetails,
            ),
            "stream": (self.streams, "create_stream", oci.streaming.models.CreateStreamDetails),
            "repository": (
                self.registry,
                "create_container_repository",
                oci.artifacts.models.CreateContainerRepositoryDetails,
            ),
            "heatwave": (self.mysql, "create_db_system", oci.mysql.models.CreateDbSystemDetails),
            "vault": (
                self.client(oci.key_management.KmsVaultClient),
                "create_vault",
                oci.key_management.models.CreateVaultDetails,
            ),
        }
        models["secret"] = (self.vault, "create_secret", oci.vault.models.CreateSecretDetails)
        if kind not in models:
            raise ValueError("Unsupported resource kind.")
        spec = dict(spec)
        if spec.get("compartment_id", self.compartment) != self.compartment:
            raise ValueError("Resource compartment mismatch.")
        spec["compartment_id"] = self.compartment
        spec["freeform_tags"] = {
            **spec.get("freeform_tags", {}),
            "managed-by": "heatwave-audit",
            "audit-app-id": app_instance_id(),
        }
        if kind == "heatwave":
            from .heatwave_resources import validate_creation

            validate_creation(self, spec)
        # Every reference that can attach an existing OCI resource must be verified.
        checks = {
            "subnet_id": self.network.get_subnet,
            "vcn_id": self.network.get_vcn,
            "stream_pool_id": self.streams.get_stream_pool,
            "configuration_id": self.client(oci.mysql.MysqlaasClient).get_configuration,
            "backup_id": self.client(oci.mysql.DbBackupsClient).get_backup,
            "vault_id": self.client(oci.key_management.KmsVaultClient).get_vault,
        }

        if kind == "secret":
            vault = self.check(
                self.client(oci.key_management.KmsVaultClient)
                .get_vault(spec.get("vault_id", ""))
                .data
            )
            key_client = oci.key_management.KmsManagementClient(
                self.sdk_config, service_endpoint=vault.management_endpoint, signer=self.signer
            )
            self.check(key_client.get_key(spec.get("key_id", "")).data)
            checks["key_id"] = key_client.get_key

        def validate_refs(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if key == "subnets" and isinstance(value, list):
                        for subnet_id in value:
                            subnet = self.check(self.network.get_subnet(subnet_id).data)
                            self.check(self.network.get_vcn(subnet.vcn_id).data)
                    if key == "nsg_ids" and isinstance(value, list):
                        for nsg_id in value:
                            self.check(self.network.get_network_security_group(nsg_id).data)
                    if key.endswith("_id") and value and key != "compartment_id":
                        if key not in checks:
                            raise ValueError("Unverified resource attachment: " + key)
                        resource = self.check(checks[key](value).data)
                        if key == "subnet_id":
                            self.check(self.network.get_vcn(resource.vcn_id).data)
                    validate_refs(value)
            elif isinstance(obj, list):
                for value in obj:
                    validate_refs(value)

        password_secret = spec.pop("admin_password_secret_ocid", None)
        validate_refs(spec)
        if kind == "stream" and spec.get("stream_pool_id"):
            # OCI derives the compartment from the verified pool; these fields are exclusive.
            spec.pop("compartment_id")
        if password_secret:
            spec["admin_password"] = self.secret(password_secret)
        if kind == "repository" and spec.get("is_public"):
            raise ValueError("Audit image repositories must be private.")
        client, method, model = models[kind]
        # SDK's deserializer supports typed nested structures; reject unknown fields.
        data = build_model(model, spec)
        if kind == "stream":
            return self._create_stream(data, retry_token)
        kwargs = {"opc_retry_token": retry_token} if retry_token else {}
        return getattr(client, method)(data, **kwargs).data.id

    def _create_stream(self, details, request_token):
        """CreateStream has no retry-token parameter; reconcile by pool/name and request tag."""
        if not details.stream_pool_id:
            raise ValueError("Select a stream pool before creating a stream.")
        rows = oci.pagination.list_call_get_all_results(
            self.streams.list_streams,
            compartment_id=self.compartment,
            stream_pool_id=details.stream_pool_id,
            name=details.name,
        ).data
        matches = []
        for row in rows:
            self.check(row)
            if row.stream_pool_id != details.stream_pool_id or row.name != details.name:
                raise ValueError("Stream inventory does not match the selected pool and name.")
            if row.lifecycle_state != "DELETED":
                matches.append(row)
        if matches:
            if len(matches) != 1:
                raise ValueError("Multiple matching streams were returned. Refresh OCI inventory.")
            stream = matches[0]
            tags = stream.freeform_tags or {}
            if (
                not request_token
                or tags.get("audit-request") != request_token
                or tags.get("managed-by") != "heatwave-audit"
            ):
                raise ValueError(
                    "A stream with this name already exists in the selected pool. Select it from inventory or use a different name."
                )
            if (
                stream.partitions != details.partitions
                or stream.retention_in_hours != details.retention_in_hours
            ):
                raise ValueError(
                    "The submitted stream settings differ from this request. Refresh OCI inventory."
                )
            if stream.lifecycle_state not in {"CREATING", "ACTIVE"}:
                raise ValueError(
                    "The submitted stream is not ready for reuse. Refresh its OCI state."
                )
            return stream.id
        if request_token:
            details.freeform_tags = {**details.freeform_tags, "audit-request": request_token}
        # Avoid implicit retries after an ambiguous response. The next user retry reconciles above.
        kwargs = {"retry_strategy": oci.retry.NoneRetryStrategy()}
        if request_token:
            kwargs["opc_request_id"] = request_token
        stream = self.check(self.streams.create_stream(details, **kwargs).data)
        if stream.stream_pool_id != details.stream_pool_id:
            raise ValueError("Created stream does not match the verified stream pool.")
        return stream.id


def build_model(model, values):
    if model is oci.vault.models.SecretContentDetails and values.get("content_type") == "BASE64":
        model = oci.vault.models.Base64SecretContentDetails
    obj = model()
    for key, value in values.items():
        if key not in obj.swagger_types:
            raise ValueError("Unknown OCI resource field: " + key)
        typ = obj.swagger_types[key]
        if isinstance(value, dict) and not typ.startswith("dict("):
            module = __import__(model.__module__.rsplit(".", 1)[0], fromlist=[typ])
            value = build_model(getattr(module, typ), value)
        elif isinstance(value, list) and typ.startswith("list["):
            name = typ[5:-1]
            if name not in {"str", "int", "float"}:
                module = __import__(model.__module__.rsplit(".", 1)[0], fromlist=[name])
                value = [build_model(getattr(module, name), item) for item in value]
        setattr(obj, key, value)
    return obj
