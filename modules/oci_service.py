"""All control-plane mutations are VM instance-principal and compartment constrained."""

import base64
import hashlib
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
        self.kafka = self.client(oci.managed_kafka.KafkaClusterClient)
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
        from .transport import backend, state_topics

        selected = backend(transport)
        state_topics(transport, flow["connector_name"])
        if selected == "oci_streaming":
            harness = self.check(
                self.streams.get_connect_harness(transport["connect_harness_id"]).data
            )
            if harness.lifecycle_state != "ACTIVE":
                raise ValueError("Connect harness must be ACTIVE.")
            pool = self.check(self.streams.get_stream_pool(transport["stream_pool_id"]).data)
            if pool.lifecycle_state != "ACTIVE":
                raise ValueError("Stream pool must be ACTIVE.")
            if pool.private_endpoint_settings:
                subnet = self.check(
                    self.network.get_subnet(pool.private_endpoint_settings.subnet_id).data
                )
                self.check(self.network.get_vcn(subnet.vcn_id).data)
                for nsg_id in pool.private_endpoint_settings.nsg_ids or []:
                    self.check(self.network.get_network_security_group(nsg_id).data)
            expected = pool.kafka_settings.bootstrap_servers if pool.kafka_settings else None
            if not expected or transport["bootstrap_servers"] != expected:
                raise ValueError("Bootstrap endpoint must match the selected OCI stream pool.")
            schema_topic = "audit_" + str(flow["id"])
            if flow["topic"] == schema_topic:
                raise ValueError(
                    "The data topic and schema-change topic must have different names."
                )
            for topic in (flow["topic"], schema_topic):
                streams = oci.pagination.list_call_get_all_results(
                    self.streams.list_streams,
                    compartment_id=self.compartment,
                    name=topic,
                    stream_pool_id=transport["stream_pool_id"],
                ).data
                streams = [s for s in streams if s.lifecycle_state != "DELETED"]
                for stream in streams:
                    self.check(stream)
                if not streams:
                    action = (
                        "Choose Prepare schema topic for this mapping in Control Center."
                        if topic == schema_topic
                        else "Create the data stream in the selected pool under OCI resources."
                    )
                    raise ValueError(
                        "Required stream '"
                        + topic
                        + "' is missing from the selected pool. "
                        + action
                    )
                if len(streams) != 1:
                    raise ValueError(
                        "Multiple streams match '" + topic + "'. Review OCI inventory."
                    )
                if streams[0].lifecycle_state != "ACTIVE":
                    raise ValueError(
                        "Required stream '"
                        + topic
                        + "' is "
                        + streams[0].lifecycle_state
                        + ". Refresh OCI resources and wait for ACTIVE before deployment."
                    )
                if topic == schema_topic and streams[0].partitions != 1:
                    raise ValueError(
                        "Schema-change stream '" + topic + "' must have exactly one partition."
                    )
        else:
            cluster = self.check(self.kafka.get_kafka_cluster(transport["cluster_id"]).data)
            if cluster.lifecycle_state != "ACTIVE":
                raise ValueError("Managed Kafka cluster must be ACTIVE.")
            allowed = {
                u.url.removeprefix("SASL_SSL://").removeprefix("SSL://")
                for u in cluster.kafka_bootstrap_urls or []
            }
            supplied = {v.strip() for v in transport["bootstrap_servers"].split(",")}
            if not supplied or not supplied <= allowed:
                raise ValueError(
                    "Bootstrap endpoints must match the verified managed Kafka cluster."
                )
            for attachment in cluster.access_subnets or []:
                for subnet_id in attachment.subnets or []:
                    subnet = self.check(self.network.get_subnet(subnet_id).data)
                    self.check(self.network.get_vcn(subnet.vcn_id).data)
        from .transport import uses_principal

        if not uses_principal(transport):
            self.check(self.vault.get_secret(transport["secret_ocid"]).data)
        from .config import fingerprint_secret

        self.check(self.vault.get_secret(fingerprint_secret(self.config)).data)
        if not transport.get("incarnation"):
            raise ValueError("Configure a stable queue incarnation ID before deployment.")
        return transport

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
        if (instance.freeform_tags or {}).get("writer-protocol") != "2":
            raise ValueError(
                "Legacy writer must be replaced with the retention-aware image before restart or cleanup."
            )
        return instance

    def container_action(self, resource_id, action, flow_id):
        instance = self.owned_container(resource_id, flow_id)
        if (action == "start" and instance.lifecycle_state == "ACTIVE") or (
            action == "stop" and instance.lifecycle_state == "INACTIVE"
        ):
            return None
        if action == "start":
            return self.containers.start_container_instance(resource_id)
        if action == "restart":
            return self.containers.restart_container_instance(resource_id)
        if action == "stop":
            return self.containers.stop_container_instance(resource_id)
        raise ValueError("Unsupported container action.")

    def create_processor(self, flow, *, replacement_of=None):
        self.validate_flow(flow)
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
        payload["target"] = target
        payload["source_identity"] = source["server_identity"]
        payload["lineage_id"] = flow.get("lineage_id") or flow["connector_name"]
        payload["queue_incarnation"] = transport_profile(self.config, flow)["incarnation"]
        payload["writer_protocol"] = 2
        from dataclasses import asdict

        from .ingestion import BatchSettings

        payload["batch"] = asdict(BatchSettings.from_config(settings.get("batch", {})))
        interval = settings.get("offset_commit_interval_ms", 500)
        if type(interval) is not int or not 250 <= interval <= 5000:
            raise ValueError("Offset commit interval must be between 250 and 5000 ms.")
        payload["offset_commit_interval_ms"] = interval
        payload["schema_topic"] = "audit_" + str(flow["id"])
        payload["fingerprint_secret_ocid"] = self.config["fingerprint_secret_ocid"]
        payload["compartment_id"] = self.compartment
        payload["region"] = self.config["region"]
        payload["queue"] = transport_profile(self.config, flow)
        return self.create_workload(
            flow,
            settings,
            payload,
            "history-writer",
            ["python", "worker.py"],
            replacement_of=replacement_of,
        )

    def create_debezium(self, flow, *, replacement_of=None):
        self.validate_flow(flow)
        from .debezium import connector_config

        settings = self.config["debezium"]
        payload = {
            "compartment_id": self.compartment,
            "region": self.config["region"],
            "queue": transport_profile(self.config, flow),
            "source_secret_ocid": flow["source"]["secret_ocid"],
            "history_secret_ocid": flow["target"]["secret_ocid"],
            "connector_name": flow["connector_name"],
            "connector": connector_config(flow, self.config),
        }
        from .image_builds import inventory, job_path, load

        release = next(
            (
                r
                for r in inventory()
                if r["status"] == "complete"
                and r["role"] == "debezium"
                and r["image_id"] == settings.get("image_id")
                and settings["image_url"].endswith("@" + str(r.get("digest", "")))
            ),
            None,
        )
        capture_manifest = load(job_path(release["id"]))["manifest"] if release else {}
        capture_files = (
            "modules/capture_runtime.py",
            "modules/capture_requests.py",
            "modules/capture_start.py",
        )
        if release and all(
            capture_manifest.get(name) == hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
            for name in capture_files
        ):
            from .capture_requests import initialize
            from .heatwave import connect

            with connect(flow["target"], self) as db:
                initialize(db, flow["target_schema"])
            control = {"flow_id": flow["id"]}
            for role, database in (
                ("source", flow["source_schema"]),
                ("target", flow["target_schema"]),
            ):
                control[role] = {
                    k: flow[role][k] for k in ("host", "port", "username", "secret_ocid")
                }
                control[role].update(database=database, **self.tls_profile(container=True))
            payload["capture_control"] = control
        return self.create_workload(
            flow, settings, payload, "debezium", None, replacement_of=replacement_of
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
                "writer-protocol": "2" if name == "history-writer" else "none",
                "capture-control-protocol": "2" if payload.get("capture_control") else "none",
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
            "kafka_cluster": (
                self.kafka,
                "create_kafka_cluster",
                oci.managed_kafka.models.CreateKafkaClusterDetails,
            ),
            "connect_harness": (
                self.streams,
                "create_connect_harness",
                oci.streaming.models.CreateConnectHarnessDetails,
            ),
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
        models["kafka_config"] = (
            self.kafka,
            "create_kafka_cluster_config",
            oci.managed_kafka.models.CreateKafkaClusterConfigDetails,
        )
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
            "cluster_config_id": self.kafka.get_kafka_cluster_config,
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
