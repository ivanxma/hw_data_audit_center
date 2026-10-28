import json
import os
import time
import unittest
from types import SimpleNamespace as Obj
from unittest.mock import MagicMock, patch

from mysql.connector import DatabaseError

from modules.connector_health import summarize
from modules.process_errors import MARKER, describe, from_logs, record


class ProcessErrorTests(unittest.TestCase):
    def test_mysql_failure_explains_target_connect_without_disclosing_exception(self):
        result = describe(
            DatabaseError("password=DO-NOT-EXPOSE row private", errno=2003), "target_connect"
        )
        self.assertEqual(result["code"], "DB_UNREACHABLE")
        self.assertEqual(result["mysql_errno"], 2003)
        self.assertIn("ACTIVE", result["guidance"])
        self.assertEqual(result["step"], "Connect to target database")
        self.assertNotIn("DO-NOT-EXPOSE", json.dumps(result))
        self.assertNotIn("row private", json.dumps(result))

    def test_latest_error_is_safe_even_when_log_message_is_untrusted(self):
        now = time.time()
        old = record("DB_ACCESS_DENIED", "target_connect", checked_at=now - 10)
        latest = {
            **record("DB_UNREACHABLE", "target_connect", errno=2003, checked_at=now),
            "message": "<script>password=private</script>",
            "guidance": "private SQL",
        }
        text = "\n".join([MARKER + json.dumps(old), MARKER + json.dumps(latest)])
        got = from_logs(text, "History writer")
        self.assertEqual(got["mysql_errno"], 2003)
        self.assertEqual(got["checked_at"], now)
        self.assertNotIn("private", json.dumps(got))
        self.assertNotIn("<script>", json.dumps(got))

    def test_legacy_writer_error_retains_real_log_time(self):
        got = from_logs(
            "2026-10-24T21:04:32.499Z stderr F ERROR history_writer_stopped error_type=DatabaseError; details",
            "History writer",
        )
        self.assertEqual(got["code"], "DB_ERROR")
        self.assertEqual(got["checked_at"], 1792875872)
        self.assertNotIn("details", json.dumps(got))

    def test_malformed_records_do_not_break_health(self):
        text = (
            MARKER
            + "{not-json}\n"
            + MARKER
            + json.dumps({"code": ["unsafe"], "stage": "x", "checked_at": time.time()})
        )
        self.assertIsNone(from_logs(text, "History writer"))

    def test_connector_failure_keeps_safe_cause_without_trace(self):
        health = summarize(
            {
                "connector": {"state": "RUNNING"},
                "tasks": [{"state": "FAILED", "trace": "Access denied password=PRIVATE row data"}],
            }
        )
        self.assertEqual(health["failure"]["code"], "DB_ACCESS_DENIED")
        self.assertNotIn("PRIVATE", json.dumps(health))
        got = from_logs("AUDIT_CONNECTOR_HEALTH " + json.dumps(health), "Debezium")
        self.assertEqual(got["stage"], "connector_task")

    def test_worker_logs_failed_step_before_any_queue_consumption(self):
        import worker

        config = dict(
            writer_protocol=3,
            fingerprint_secret_ocid="example-key",
            target_schema="example_schema",
            target=dict(
                host="example.invalid",
                port=3306,
                ssl_mode="REQUIRED",
                username="test",
                secret_ocid="example-secret",
            ),
        )
        secrets = MagicMock(return_value="dummy-value")
        with (
            patch.dict(os.environ, {"AUDIT_WORKER_CONFIG": json.dumps(config)}),
            patch("worker.WorkloadSecrets", return_value=secrets),
            patch("worker.open_connection", side_effect=DatabaseError("PRIVATE", errno=2003)),
            patch("worker.NativeConsumer") as consumer,
        ):
            with self.assertLogs(level="ERROR") as output, self.assertRaises(DatabaseError):
                worker.main()
        consumer.assert_not_called()
        self.assertNotIn("PRIVATE", str(output.output))
        event = json.loads(output.output[0].split(MARKER, 1)[1])
        self.assertEqual(event["stage"], "target_connect")
        self.assertEqual(event["mysql_errno"], 2003)

    def test_failed_process_status_contains_latest_database_error(self):
        from modules.flow_status import inspect_flow

        cloud = MagicMock()
        cloud.config = {}
        cloud.check.side_effect = lambda x: x
        cloud.owned_container.return_value = Obj(
            lifecycle_state="ACTIVE", containers=[Obj(container_id="writer-process")]
        )
        cloud.containers.get_container.return_value.data = Obj(
            id="writer-process",
            container_instance_id="writer",
            lifecycle_state="ACTIVE",
            lifecycle_details="CONTAINER_TERMINATED",
            exit_code=1,
        )
        cloud.containers.retrieve_logs.return_value.data.content = (
            MARKER + json.dumps(record("DB_UNREACHABLE", "target_connect", errno=2003))
        ).encode()
        result = inspect_flow(
            cloud, dict(id=1, status="active", connector_ocid=None, processor_ocid="writer")
        )
        self.assertEqual(result["status"], "attention")
        writer = result["workloads"][1]
        self.assertEqual(writer["last_error"]["mysql_errno"], 2003)
        self.assertIn("ACTIVE", writer["guidance"])

    def test_debezium_image_runtime_imports_from_packaged_files_only(self):
        import shutil
        import subprocess
        import sys
        import tempfile
        from pathlib import Path

        root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary)
            (target / "modules").mkdir()
            for line in (root / "deploy/Containerfile.debezium").read_text().splitlines():
                if not line.startswith("COPY "):
                    continue
                for item in line.split()[1:-1]:
                    if item == "modules":
                        shutil.copytree(root / item, target / item, dirs_exist_ok=True)
                    elif item.startswith("modules/") and item.endswith(".py"):
                        shutil.copy2(root / item, target / item)
                    elif item == "deploy/debezium_bootstrap.py":
                        shutil.copy2(root / item, target / "debezium_bootstrap.py")
            result = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    "-c",
                    "import sys; sys.path.insert(0,sys.argv[1]); import debezium_bootstrap; import modules.capture_runtime",
                    str(target),
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
