import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from modules.runtime_env import load_runtime_defaults, read_runtime
from scripts.check_setup import check, cloud_checks
from scripts.install_services import runtime_settings


class SetupReviewTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which("systemd-analyze"), "Requires the OL9 systemd parser")
    def test_generated_units_are_accepted_with_spaces_in_checkout_path(self):
        from scripts import install_image_builder, install_services

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "app checkout"
            for name in ("etc", ".data", "scripts"):
                (root / name).mkdir(parents=True)
            config = root / "private.json"
            config.write_text(
                json.dumps(
                    {"region": "example-1", "compartment_id": "ocid1.compartment.test.scope"}
                )
            )
            for name in ("start_mysql.sh", "start_https.sh", "scripts/start_image_builder.sh"):
                (root / name).write_text("#!/bin/sh\nexit 0\n")
                (root / name).chmod(0o700)
            units = []

            def capture(name, body):
                path = root / ".data" / name
                path.write_text(body)
                units.append(path)

            with (
                patch.object(install_services, "ROOT", root),
                patch.object(install_services, "install", capture),
                patch.dict(os.environ, {"AUDIT_CONFIG_FILE": str(config)}, clear=True),
                patch("sys.argv", ["install_services.py"]),
            ):
                install_services.main()
            with (
                patch.object(install_image_builder, "ROOT", root),
                patch.object(install_image_builder, "install", capture),
                patch.object(install_image_builder, "inventory", return_value=[]),
                patch.object(install_image_builder.subprocess, "run"),
            ):
                install_image_builder.main()
            result = subprocess.run(
                ["systemd-analyze", "verify", *map(str, units)], capture_output=True, text=True
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertNotIn("path is not absolute", result.stderr)

    def test_rerun_preserves_saved_path_port_and_explicit_override(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "private settings.json"
            config.write_text(
                json.dumps(
                    {"region": "example-region-1", "compartment_id": "ocid1.compartment.test.scope"}
                )
            )
            (root / ".runtime.env").write_text(
                "AUDIT_CONFIG_FILE='" + str(config) + "'\nAUDIT_HTTPS_PORT=8443\n"
            )
            private, values = runtime_settings(root, {})
            self.assertEqual(private, config.resolve())
            self.assertEqual(values["AUDIT_HTTPS_PORT"], "8443")
            self.assertEqual(
                runtime_settings(root, {"AUDIT_HTTPS_PORT": "9443"})[1]["AUDIT_HTTPS_PORT"], "9443"
            )
            with self.assertRaises(ValueError):
                runtime_settings(root, {"AUDIT_HTTPS_PORT": "70000"})
            with self.assertRaises(ValueError):
                runtime_settings(root, {"SSL_CERT_FILE": "/missing.crt"})

    def test_runtime_defaults_are_literal_and_do_not_override_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.env"
            path.write_text("AUDIT_CONFIG_FILE='/saved/path'\nLITERAL='$(do-not-execute)'\n")
            environment = {"AUDIT_CONFIG_FILE": "/explicit/path"}
            load_runtime_defaults(path, environment)
            self.assertEqual(environment["AUDIT_CONFIG_FILE"], "/explicit/path")
            self.assertEqual(environment["LITERAL"], "$(do-not-execute)")
            path.write_text("source /another/file\n")
            with self.assertRaises(ValueError):
                read_runtime(path)

    def test_diagnostic_masks_error_payload_and_checks_every_resource_scope(self):
        rows = []
        check(rows, "private", MagicMock(side_effect=RuntimeError("password=not-for-output")))
        self.assertNotIn("not-for-output", json.dumps(rows))
        cloud = MagicMock()
        cloud.compartment = "scope"
        cloud.config = {}
        cloud.check.side_effect = ValueError("outside")
        with patch("oci.pagination.list_call_get_all_results") as listing:
            listing.return_value.data = [MagicMock()]
            rows = cloud_checks(cloud, "streaming")
        self.assertTrue(all(r["status"] == "failed" for r in rows))
        cloud.secrets.get_secret_bundle.assert_not_called()
        cloud.containers.create_container_instance.assert_not_called()
