import copy
import json
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace as Obj
from unittest.mock import MagicMock, patch

import test_core

from modules.oci_service import Cloud
from modules.workflows import WorkflowService
from modules.workload_images import choices, releases, select


class WorkloadImageTests(unittest.TestCase):
    def setUp(self):
        self.cloud = Cloud.__new__(Cloud)
        self.cloud.compartment = "scope"
        self.cloud.config = {
            "region": "uk-london-1",
            "debezium": dict(
                repository_id="repo",
                image_url="old-image",
                subnet_id="subnet",
                registry_endpoint="lhr.ocir.io",
            ),
            "processor": dict(image_url="other-workload-image"),
        }
        self.cloud.registry = MagicMock()
        self.cloud.network = MagicMock()
        self.cloud.registry.get_container_repository.return_value.data = Obj(
            id="repo",
            compartment_id="scope",
            namespace="namespace",
            display_name="images",
            is_public=False,
        )
        self.job = dict(
            id="a" * 32,
            status="complete",
            role="debezium",
            runtime_protocol=3,
            compartment_id="scope",
            region="uk-london-1",
            repository_id="repo",
            tag="debezium-example",
            created_at="2026-10-24T12:00:00+00:00",
            image_id="image",
            digest="sha256:" + "1" * 64,
        )
        self.cloud.registry.get_container_image.return_value.data = Obj(
            id="image",
            compartment_id="scope",
            repository_id="repo",
            lifecycle_state="AVAILABLE",
            digest=self.job["digest"],
        )
        self.cloud.network.get_subnet.return_value.data = Obj(compartment_id="scope", vcn_id="vcn")
        self.cloud.network.get_vcn.return_value.data = Obj(compartment_id="scope")
        self.stack = ExitStack()
        self.image_inventory = self.stack.enter_context(
            patch(
                "modules.workload_images.oci.pagination.list_call_get_all_results",
                return_value=Obj(data=[self.cloud.registry.get_container_image.return_value.data]),
            )
        )
        self.stack.enter_context(
            patch("modules.image_builds.inventory", side_effect=lambda: [dict(self.job)])
        )
        self.stack.enter_context(
            patch("modules.image_builds.job_path", return_value=Path("/unused"))
        )
        self.stack.enter_context(
            patch("modules.image_builds.load", side_effect=lambda path: dict(self.job))
        )
        self.addCleanup(self.stack.close)

    def test_selection_is_pinned_and_request_local(self):
        original = self.cloud.config
        saved = copy.deepcopy(original)
        release = select(self.cloud, "debezium", self.job["id"])
        self.assertEqual(original, saved)
        self.assertEqual(
            self.cloud.config["debezium"]["image_url"],
            "lhr.ocir.io/namespace/images@" + self.job["digest"],
        )
        self.assertEqual(self.cloud.config["debezium"]["image_release"], release["tag"])
        self.assertEqual(self.cloud.config["processor"], saved["processor"])

    def test_wrong_component_repository_scope_region_status_or_digest_is_rejected(self):
        for key, value in [
            ("role", "processor"),
            ("repository_id", "foreign"),
            ("compartment_id", "foreign"),
            ("region", "foreign"),
            ("status", "failed"),
            ("runtime_protocol", 2),
            ("digest", "bad"),
        ]:
            with self.subTest(key=key):
                old = self.job[key]
                self.job[key] = value
                with self.assertRaisesRegex(ValueError, "not a completed"):
                    select(self.cloud, "debezium", self.job["id"])
                self.job[key] = old
        self.cloud.registry.get_container_image.assert_not_called()

    def test_live_deleted_mismatched_or_foreign_images_fail_before_config_changes(self):
        image = self.cloud.registry.get_container_image.return_value.data
        original = copy.deepcopy(self.cloud.config)
        for key, value in [
            ("lifecycle_state", "DELETED"),
            ("digest", "sha256:" + "2" * 64),
            ("repository_id", "foreign"),
            ("compartment_id", "foreign"),
        ]:
            with self.subTest(key=key):
                old = getattr(image, key)
                setattr(image, key, value)
                with self.assertRaises(ValueError):
                    select(self.cloud, "debezium", self.job["id"])
                self.assertEqual(self.cloud.config, original)
                setattr(image, key, old)

    def test_unconfigured_or_foreign_repository_is_rejected(self):
        self.cloud.registry.get_container_repository.return_value.data.compartment_id = "foreign"
        with self.assertRaisesRegex(ValueError, "outside"):
            releases(self.cloud, "debezium")
        with self.assertRaisesRegex(ValueError, "Configure"):
            releases(self.cloud, "processor")

    def test_picker_hides_deleted_images_and_images_missing_from_registry(self):
        self.cloud.registry.get_container_image.return_value.data.lifecycle_state = "DELETED"
        self.assertEqual(releases(self.cloud, "debezium", available_only=True), [])
        self.image_inventory.return_value.data = []
        self.assertEqual(releases(self.cloud, "debezium", available_only=True), [])

    def test_choices_validate_current_attachment_and_do_not_mutate_cloud(self):
        self.cloud.owned_container = MagicMock(
            return_value=Obj(lifecycle_state="ACTIVE", containers=[Obj(container_id="process")])
        )
        self.cloud.containers = MagicMock()
        self.cloud.containers.get_container.return_value.data = Obj(
            compartment_id="scope", container_instance_id="current", image_url="old-image"
        )
        flow = dict(id=1, name="example", status="active", connector_ocid="current")
        data = choices(self.cloud, flow, "debezium")
        self.assertEqual(data["instance_id"], "current")
        self.assertEqual(data["current_images"], ["old-image"])
        self.assertEqual(data["releases"][0]["id"], self.job["id"])
        self.assertEqual(self.cloud.config["debezium"]["image_url"], "old-image")
        self.cloud.containers.get_container.return_value.data.container_instance_id = "wrong"
        with self.assertRaisesRegex(ValueError, "does not belong"):
            choices(self.cloud, flow, "debezium")

    def test_locked_action_requires_selection_and_rejects_stale_instance_before_stopping(self):
        service = WorkflowService(MagicMock(), lambda: self.cloud, "operator")
        flow = dict(id=1, status="active", connector_ocid="current")
        with (
            patch("modules.workflows.repo.named_lock"),
            patch("modules.workflows.repo.get_flow", return_value=flow),
            patch("modules.workflows.repo.record"),
            patch.object(service, "_operate") as operate,
        ):
            with self.assertRaisesRegex(ValueError, "workload changed"):
                service.action(
                    1,
                    "update_debezium",
                    image_release_id=self.job["id"],
                    expected_instance_id="previous",
                )
            with self.assertRaisesRegex(ValueError, "Choose a published"):
                service.action(1, "update_debezium", expected_instance_id="current")
            operate.assert_not_called()

    def test_locked_action_uses_selected_release_without_saving_defaults(self):
        service = WorkflowService(MagicMock(), lambda: self.cloud, "operator")
        original = self.cloud.config
        with (
            patch("modules.workflows.repo.named_lock"),
            patch(
                "modules.workflows.repo.get_flow", return_value=dict(id=1, connector_ocid="current")
            ),
            patch("modules.workflows.repo.record"),
            patch.object(service, "_operate", return_value="deploying") as operate,
            patch.object(service, "_update"),
        ):
            self.assertEqual(
                service.action(
                    1,
                    "update_debezium",
                    image_release_id=self.job["id"],
                    expected_instance_id="current",
                ),
                "deploying",
            )
            self.assertEqual(operate.call_args.args[0].config["debezium"]["image_id"], "image")
            self.assertEqual(original["debezium"]["image_url"], "old-image")


class StreamSettingsTests(unittest.TestCase):
    def setUp(self):
        from modules.message_policy import policy

        self.cloud = MagicMock()
        self.cloud.config = {
            "processor": {"image_url": "writer-image"},
            "debezium": {"image_url": "source-image"},
        }
        self.flow = dict(
            id=1, status="active", processor_ocid="writer-old", connector_ocid="source-old"
        )
        self.previous = policy({})
        self.instances = {
            key: Obj(
                id=key,
                lifecycle_state="INACTIVE",
                containers=[Obj(container_id=key + "-container")],
            )
            for key in ["writer-old", "source-old"]
        }
        self.cloud.owned_container.side_effect = lambda key, flow_id: self.instances[key]
        self.cloud.check.side_effect = lambda value: value

        def container(key):
            owner = key.removesuffix("-container")
            return Obj(
                data=Obj(
                    container_instance_id=owner,
                    image_url="writer-image" if owner.startswith("writer") else "source-image",
                    environment_variables={
                        "AUDIT_WORKER_CONFIG": json.dumps({"message_policy": self.previous})
                    },
                )
            )

        self.cloud.containers.get_container.side_effect = container
        self.cloud.create_processor.return_value = "writer-new"
        self.cloud.create_debezium.return_value = "source-new"
        self.service = WorkflowService(MagicMock(), lambda: self.cloud, "operator")
        self.service._ready = MagicMock()
        self.service._update = MagicMock()

    def test_unchanged_policy_and_images_do_not_replace_workloads(self):
        self.service._apply_stream_settings(self.cloud, self.flow)
        self.cloud.create_processor.assert_not_called()
        self.cloud.create_debezium.assert_not_called()

    def test_writer_replaced_first_and_its_new_id_used_for_source(self):
        self.cloud.config["streaming"] = {"put_requests_per_second": 2}
        order = []
        self.cloud.create_processor.side_effect = lambda flow, **kw: (
            order.append("writer") or "writer-new"
        )

        def source(flow, **kw):
            self.assertEqual(flow["processor_ocid"], "writer-new")
            order.append("source")
            return "source-new"

        self.cloud.create_debezium.side_effect = source
        self.service._apply_stream_settings(self.cloud, self.flow)
        self.assertEqual(order, ["writer", "source"])
        self.assertEqual(self.flow["connector_ocid"], "source-new")

    def test_reduced_limits_fail_before_stopping_or_replacing(self):
        self.cloud.config["streaming"] = {"max_event_bytes": 8 * 1024 * 1024}
        with self.assertRaisesRegex(ValueError, "reducing message limits"):
            self.service._apply_stream_settings(self.cloud, self.flow)
        self.cloud.container_action.assert_not_called()
        self.cloud.create_processor.assert_not_called()

    def test_source_preflight_failure_leaves_writer_untouched(self):
        self.cloud.validate_initial_capture_image.side_effect = ValueError(
            "incompatible checkpoint"
        )
        self.cloud.config["streaming"] = {"put_requests_per_second": 2}
        with self.assertRaisesRegex(ValueError, "incompatible checkpoint"):
            self.service._apply_stream_settings(self.cloud, self.flow)
        self.cloud.container_action.assert_not_called()
        self.cloud.create_processor.assert_not_called()


class WorkloadImageWebTests(unittest.TestCase):
    setUp = test_core.WebTests.setUp
    auth = test_core.WebTests.auth

    def test_image_choices_require_admin_and_are_read_only(self):
        self.assertEqual(self.client.get("/flows/1/images/debezium").status_code, 302)
        self.auth()
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(self.mod, "ROOT", Path(directory)),
            patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()),
            patch.object(self.mod.repo, "get_flow", return_value={"id": 1}) as flow,
            patch("modules.workload_images.choices", return_value={"releases": []}) as choose,
            patch.object(self.mod, "Cloud"),
        ):
            self.assertEqual(self.client.get("/flows/1/images/debezium").status_code, 403)
            choose.assert_not_called()
            (Path(directory) / "profiles.json").write_text(
                json.dumps({"admin_users": ["operator"]})
            )
            response = self.client.get("/flows/1/images/debezium")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers["Cache-Control"], "no-store")
            flow.assert_called_once()
            choose.assert_called_once()
            self.assertEqual(self.client.get("/flows/1/images/unknown").status_code, 404)
