import unittest
from types import SimpleNamespace as Obj
from unittest.mock import MagicMock, patch

import oci

from modules.heatwave_resources import choices, lifecycle, validate_creation
from modules.oci_service import Cloud
from modules.resource_catalog import build_spec, delete_selected, owned_by_app


def cloud():
    c = Cloud.__new__(Cloud)
    c.compartment = "scope"
    c.config = {}
    c.mysql = MagicMock()
    c.client = MagicMock()
    return c


class HeatWaveResourceTests(unittest.TestCase):
    def test_lifecycle_routes_do_not_shadow_heatwave_create_or_delete(self):
        from app import create_app

        routes = create_app({"TESTING": True}).url_map.bind("localhost")
        self.assertEqual(
            routes.match("/configuration/services/heatwave/create", method="POST")[0],
            "create_service",
        )
        self.assertEqual(
            routes.match("/configuration/services/heatwave/delete", method="POST")[0],
            "delete_services",
        )
        self.assertEqual(
            routes.match("/configuration/services/heatwave/actions/stop", method="POST")[0],
            "heatwave_lifecycle",
        )

    def test_choices_are_live_and_scope_filtered(self):
        c = cloud()
        client = c.client.return_value
        client.list_configurations.__name__ = "list_configurations"
        rows = [
            Obj(
                id="custom",
                compartment_id="scope",
                display_name="CDC configuration",
                shape_name="MySQL.2",
                type="CUSTOM",
                lifecycle_state="ACTIVE",
            ),
            Obj(
                id="different",
                compartment_id="scope",
                display_name="Large shape",
                shape_name="MySQL.4",
                type="CUSTOM",
                lifecycle_state="ACTIVE",
            ),
            Obj(id="default", compartment_id="service", type="DEFAULT"),
        ]
        with patch(
            "oci.pagination.list_call_get_all_results", return_value=Obj(data=rows)
        ) as listing:
            result = choices(c, "mysql_configuration", shape_name="MySQL.2")
            self.assertEqual([r["id"] for r in result], ["__default__", "custom"])
            self.assertEqual(listing.call_args.kwargs["shape_name"], "MySQL.2")
            self.assertEqual(listing.call_args.args[1], "scope")
            rows[0].compartment_id = "foreign"
            with self.assertRaises(ValueError):
                choices(c, "mysql_configuration", shape_name="MySQL.2")
        with patch(
            "oci.pagination.list_call_get_all_results",
            return_value=Obj(
                data=[Obj(version_family="8.4 - LTS", versions=[Obj(version="8.4.test")])]
            ),
        ):
            self.assertEqual(choices(c, "mysql_version")[0]["id"], "8.4.test")

    def test_creation_passes_version_and_custom_configuration_and_rejects_mismatched_shape(self):
        c = cloud()
        form = {
            "name": "test-db",
            "admin_username": "admin",
            "shape_name": "MySQL.2",
            "availability_domain": "AD-1",
            "mysql_version": "8.4.test",
            "configuration_id": "custom",
        }
        spec = build_spec("heatwave", form)
        self.assertEqual(spec["configuration_id"], "custom")
        self.assertEqual(spec["mysql_version"], "8.4.test")
        model = oci.mysql.models.CreateDbSystemDetails(
            **{key: value for key, value in spec.items() if key != "admin_password_secret_ocid"}
        )
        self.assertEqual(model.shape_name, "MySQL.2")
        default = build_spec("heatwave", {**form, "configuration_id": "__default__"})
        self.assertNotIn("configuration_id", default)
        c.client.return_value.get_configuration.return_value.data = Obj(
            compartment_id="scope", type="CUSTOM", lifecycle_state="ACTIVE", shape_name="MySQL.4"
        )
        with (
            patch("modules.service_options.choices", return_value=[{"id": "AD-1"}]),
            patch(
                "modules.heatwave_resources.choices",
                side_effect=[[{"id": "MySQL.2"}], [{"id": "8.4.test"}]],
            ),
        ):
            with self.assertRaisesRegex(ValueError, "configuration"):
                validate_creation(c, spec)

    def test_lifecycle_checks_every_compartment_and_state_before_mutating(self):
        c = cloud()
        one = Obj(id="one", display_name="One", compartment_id="scope", lifecycle_state="ACTIVE")
        two = Obj(id="two", display_name="Two", compartment_id="foreign", lifecycle_state="ACTIVE")
        c.mysql.get_db_system.side_effect = [
            Obj(data=one, headers={"etag": "1"}),
            Obj(data=two, headers={"etag": "2"}),
        ]
        with patch("modules.heatwave_resources.repo.named_lock"), self.assertRaises(ValueError):
            lifecycle(MagicMock(), c, ["one", "two"], "stop", "admin")
        c.mysql.stop_db_system.assert_not_called()
        two.compartment_id = "scope"
        two.lifecycle_state = "INACTIVE"
        c.mysql.get_db_system.side_effect = [Obj(data=one, headers={}), Obj(data=two, headers={})]
        with patch("modules.heatwave_resources.repo.named_lock"), self.assertRaises(ValueError):
            lifecycle(MagicMock(), c, ["one", "two"], "stop", "admin")
        c.mysql.stop_db_system.assert_not_called()

    def test_start_stop_use_sdk_contract_and_graceful_fast_shutdown(self):
        c = cloud()
        row = Obj(
            id="db", display_name="Database", compartment_id="scope", lifecycle_state="ACTIVE"
        )
        c.mysql.get_db_system.return_value = Obj(data=row, headers={"etag": "version"})
        c.mysql.stop_db_system.return_value.headers = {"opc-work-request-id": "work"}
        with (
            patch("modules.heatwave_resources.repo.named_lock"),
            patch("modules.heatwave_resources.repo.record"),
        ):
            result = lifecycle(MagicMock(), c, ["db"], "stop", "admin")
            self.assertEqual(result[0]["status"], "submitted")
            args = c.mysql.stop_db_system.call_args
            self.assertEqual(args.args[1].shutdown_type, "FAST")
            self.assertNotIn("if_match", args.kwargs)
            row.lifecycle_state = "INACTIVE"
            lifecycle(MagicMock(), c, ["db"], "start", "admin")
            c.mysql.start_db_system.assert_called_once()
            self.assertNotIn("if_match", c.mysql.start_db_system.call_args.kwargs)

    def test_deletion_distinguishes_this_installation_from_other_apps_and_legacy_provenance(self):
        row = Obj(
            id="resource",
            compartment_id="scope",
            freeform_tags={"managed-by": "heatwave-audit", "audit-app-id": "this-app"},
        )
        with patch("modules.resource_catalog.app_instance_id", return_value="this-app"):
            self.assertTrue(owned_by_app(row))
            row.freeform_tags["audit-app-id"] = "another-app"
            self.assertFalse(owned_by_app(row, {"resource"}))
            row.freeform_tags.pop("audit-app-id")
            self.assertFalse(owned_by_app(row))
            self.assertTrue(owned_by_app(row, {"resource"}))
            c = cloud()
            c.mysql.get_db_system.return_value.data = row
            with (
                patch("modules.resource_catalog.repo.named_lock"),
                patch("modules.resource_catalog.registered_resources", return_value=set()),
                self.assertRaises(ValueError),
            ):
                delete_selected(MagicMock(), c, "heatwave", ["resource"], "admin")
            c.mysql.delete_db_system.assert_not_called()

    def test_lifecycle_ignores_response_etag_and_reports_failed_state_without_retry(self):
        c = cloud()
        row = Obj(
            id="db", display_name="Example DB", compartment_id="scope", lifecycle_state="INACTIVE"
        )
        c.mysql.get_db_system.return_value = Obj(data=row, headers={"etag": "version--gzip"})
        c.mysql.start_db_system.side_effect = oci.exceptions.ServiceError(
            412,
            "NoEtagMatch",
            {"opc-request-id": "example-request"},
            "private service detail",
            operation_name="start_db_system",
        )
        with (
            patch("modules.heatwave_resources.repo.named_lock"),
            patch("modules.heatwave_resources.repo.record"),
        ):
            result = lifecycle(MagicMock(), c, ["db"], "start", "admin")
        self.assertEqual(c.mysql.start_db_system.call_count, 1)
        self.assertNotIn("if_match", c.mysql.start_db_system.call_args.kwargs)
        self.assertEqual(result[0]["status"], "failed")
        self.assertIn("Last checked DB state: INACTIVE", result[0]["message"])
        self.assertIn("does not send", result[0]["message"])
        self.assertNotIn("private service detail", str(result))
