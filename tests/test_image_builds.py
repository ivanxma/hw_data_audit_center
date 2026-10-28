import base64
import json
import os
import shlex
import shutil
import subprocess
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace as Obj
from unittest.mock import MagicMock, patch

from modules import image_build_worker as worker
from modules import image_builds as builds
from modules.config import ROOT
from modules.image_registry import destination, write_push_auth
from modules.oci_service import Cloud


def cloud():
    c = Cloud.__new__(Cloud)
    c.compartment = "scope"
    c.config = {"region": "uk-london-1"}
    c.registry = MagicMock()
    c.signer = MagicMock()
    c.registry.get_container_repository.return_value.data = Obj(
        id="repo",
        compartment_id="scope",
        is_public=False,
        namespace="namespace",
        display_name="audit",
    )
    return c


class ImageBuildTests(unittest.TestCase):
    def test_builder_restart_preserves_the_rootless_container_mount_namespace(self):
        from scripts import install_image_builder as installer

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".runtime.env").write_text("")
            (root / ".data").mkdir()
            with (
                patch.object(installer, "ROOT", root),
                patch.object(installer, "inventory", return_value=[]),
                patch.object(installer.pwd, "getpwuid", return_value=Obj(pw_name="appuser", pw_gid=1234)),
                patch.object(installer.grp, "getgrgid", return_value=Obj(gr_name="appgroup")),
                patch.object(installer, "install") as install,
                patch.object(installer.subprocess, "run"),
            ):
                installer.main()
            unit = install.call_args.args[1]
            self.assertIn("PrivateTmp=false", unit)
            self.assertIn("RuntimeDirectoryPreserve=yes", unit)
            self.assertIn("ProtectSystem=strict", unit)
            self.assertIn(" /var/tmp\n", unit)

    def test_initial_gtid_release_contains_the_snapshotter_and_runtime_before_deployment(self):
        from modules.debezium import require_initial_gtid_image

        with tempfile.TemporaryDirectory() as tmp:
            manifest, _ = builds.snapshot(ROOT, Path(tmp) / "context", "debezium")
            require_initial_gtid_image(manifest)
            manifest.pop("modules/initial_capture.py")
            with self.assertRaisesRegex(ValueError, "newly built Debezium"):
                require_initial_gtid_image(manifest)

    def test_launcher_loads_private_runtime_from_its_checkout(self):
        with tempfile.TemporaryDirectory(prefix="audit launcher ") as tmp:
            root = Path(tmp)
            (root / "scripts").mkdir()
            (root / ".venv/bin").mkdir(parents=True)
            launcher = root / "scripts/start_image_builder.sh"
            shutil.copyfile(ROOT / "scripts/start_image_builder.sh", launcher)
            config = str(root / "private settings.json")
            (root / ".runtime.env").write_text("AUDIT_CONFIG_FILE=" + shlex.quote(config) + "\n")
            stub = root / ".venv/bin/python"
            stub.write_text('#!/bin/sh\nprintf "%s" "$AUDIT_CONFIG_FILE"\n')
            stub.chmod(0o700)
            env = dict(os.environ)
            env.pop("AUDIT_CONFIG_FILE", None)
            result = subprocess.run(
                ["bash", str(launcher)],
                cwd="/",
                env=env,
                capture_output=True,
                text=True,
                check=True,
            )
            self.assertEqual(result.stdout, config)
            (root / ".runtime.env").unlink()
            result = subprocess.run(
                ["bash", str(launcher)], cwd="/", env=env, capture_output=True, text=True
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Set AUDIT_CONFIG_FILE", result.stderr)

    def test_release_naming_and_private_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            context = Path(tmp) / "context"
            manifest, revision = builds.snapshot(ROOT, context, "processor")
            tag = builds.release_tag(
                "processor", revision, datetime(2026, 10, 23, 18, 15, tzinfo=timezone.utc)
            )
            self.assertEqual(tag, "historywriter-20261023T181500Z-" + revision[:12])
            self.assertIn("worker.py", manifest)
            self.assertNotIn(".runtime.env", manifest)
            self.assertFalse(
                any(name.startswith((".data/", "internal_doc/", "tls/")) for name in manifest)
            )
            job = {"manifest": manifest, "revision": revision, "role": "processor", "tag": tag}
            worker.validate_context(Path(tmp), job)
            (context / "worker.py").write_text("changed")
            with self.assertRaisesRegex(ValueError, "changed"):
                worker.validate_context(Path(tmp), job)

    def test_repository_scope_and_principal_token_permissions(self):
        c = cloud()
        with (
            tempfile.TemporaryDirectory() as tmp,
            patch("modules.image_registry.requests.get") as get,
        ):
            get.return_value.status_code = 200
            get.return_value.json.return_value = {"token": "private-test-token"}
            path = Path(tmp) / "auth.json"
            self.assertEqual(write_push_auth(c, "repo", path), "lhr.ocir.io/namespace/audit")
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(
                get.call_args.kwargs["params"]["scope"], "repository:namespace/audit:pull,push"
            )
            self.assertIs(get.call_args.kwargs["auth"], c.signer)
            self.assertFalse(get.call_args.kwargs["allow_redirects"])
            auth = json.loads(path.read_text())["auths"]["lhr.ocir.io"]["auth"]
            self.assertEqual(base64.b64decode(auth).decode(), "BEARER_TOKEN:private-test-token")
            c.registry.get_container_repository.return_value.data.compartment_id = "foreign"
            with self.assertRaises(ValueError):
                destination(c, "repo")
            with self.assertRaises(ValueError):
                builds.enqueue(c, "processor", "repo", "scope", "admin")
            c.registry.get_container_repository.return_value.data.compartment_id = "scope"
            c.registry.get_container_repository.return_value.data.is_public = True
            with self.assertRaises(ValueError):
                destination(c, "repo")

    def test_failed_push_removes_token_and_marks_ambiguous_release(self):
        c = cloud()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            manifest, revision = builds.snapshot(ROOT, path / "context", "processor")
            job = {
                "id": "a" * 32,
                "role": "processor",
                "repository_id": "repo",
                "compartment_id": "scope",
                "region": "uk-london-1",
                "tag": builds.release_tag("processor", revision),
                "revision": revision,
                "manifest": manifest,
                "status": "queued",
            }
            job["image_url"] = "lhr.ocir.io/namespace/audit:" + job["tag"]
            builds.save(path, job)

            def auth(_cloud, _repository, target):
                target.write_text("private-test-token")
                return "lhr.ocir.io/namespace/audit"

            def run(_path, *args, **kwargs):
                if args[0] == "push":
                    raise subprocess.CalledProcessError(1, "podman")

            with (
                patch.object(worker, "Cloud", return_value=c),
                patch.object(worker, "images", return_value=[]),
                patch.object(worker, "run", side_effect=run),
                patch.object(worker, "write_push_auth", side_effect=auth),
                patch.object(worker.shutil, "disk_usage", return_value=Obj(free=10 * 1024**3)),
            ):
                worker.process(path)
            self.assertFalse((path / "push-auth.json").exists())
            self.assertFalse((path / "context").exists())
            result = builds.load(path)
            self.assertEqual(result["status"], "failed")
            self.assertIn("may exist", result["message"])
            self.assertNotIn("private-test-token", (path / "job.json").read_text())

    def test_no_shell_or_plaintext_auth_in_podman_command(self):
        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.object(worker, "ROOT", Path(tmp)),
            patch("modules.image_build_worker.subprocess.run") as run,
        ):
            (Path(tmp) / ".data").mkdir()
            worker.run(
                Path(tmp),
                "push",
                "--authfile",
                "/private/auth.json",
                "local",
                "docker://registry/image:tag",
            )
            args = run.call_args.args[0]
            self.assertEqual(args[0], "/usr/bin/podman")
            self.assertNotIn("--creds", args)
            self.assertNotIn("shell", run.call_args.kwargs)
            environment = run.call_args.kwargs["env"]
            temporary = Path(environment["TMPDIR"])
            self.assertEqual(temporary, worker.runtime_directory() / "tmp")
            self.assertTrue(temporary.is_dir())
            self.assertEqual(temporary.stat().st_mode & 0o777, 0o700)

    def test_service_uses_volatile_runtime_with_persistent_image_storage(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(worker, "ROOT", Path(tmp)):
            runtime = Path(tmp) / "volatile"
            (Path(tmp) / ".data").mkdir()
            with patch.dict(os.environ, {"AUDIT_IMAGE_RUNTIME_DIR": str(runtime)}):
                args = worker.podman("info")
                self.assertEqual(args[args.index("--runroot") + 1], str(runtime / "storage"))
                self.assertEqual(args[args.index("--tmpdir") + 1], str(runtime / "libpod/tmp"))
                self.assertEqual(
                    args[args.index("--root") + 1],
                    str(Path(tmp) / ".data/image-builder/storage-v2"),
                )

    def test_build_endpoints_require_login_and_csrf(self):
        from app import create_app

        client = create_app({"TESTING": True}).test_client()
        self.assertEqual(client.get("/configuration/image-builds").status_code, 302)
        self.assertEqual(
            client.get("/configuration/image-builds/" + "a" * 32 + "/log").status_code, 302
        )
        self.assertEqual(client.post("/configuration/image-builds", data={}).status_code, 400)
