import unittest

import oci
import test_core
from mysql.connector import ProgrammingError

from modules.service_errors import mysql_failure, oci_failure


def denied():
    return oci.exceptions.ServiceError(
        404,
        "NotAuthorizedOrNotFound",
        {"opc-request-id": "request-fixture/part"},
        "sensitive-response-body-password",
        operation_name="create_stream_pool",
    )


class ServiceErrorTests(unittest.TestCase):
    def test_mysql_missing_table_points_to_initialization_without_leaking_sql(self):
        result = mysql_failure(
            ProgrammingError("private schema, password and SQL", errno=1146), "audit"
        )
        self.assertIn("MySQL 1146", result["message"])
        self.assertIn("initialize the target tables", result["guidance"])
        self.assertEqual(result["response_status"], 409)
        self.assertNotIn("private schema", str(result))

    def test_native_stream_diagnostics_are_compartment_scoped_and_hide_service_body(self):
        for operation, title in (("list_streams", "streams"), ("list_stream_pools", "stream pools")):
            with self.subTest(operation=operation):
                result = oci_failure(oci.exceptions.ServiceError(404, "NotAuthorizedOrNotFound", {},
                                     "private service detail", operation_name=operation))
                self.assertIn(title, result["message"])
                self.assertIn("in compartment id <VM_COMPARTMENT_OCID>", result["policy"])
                self.assertNotIn("in tenancy", result["policy"])
                self.assertNotIn("private service detail", str(result))

    def test_scope_guidance_preserves_diagnostics_without_raw_response(self):
        result = oci_failure(denied())
        self.assertEqual(result["response_status"], 403)
        self.assertIn("stream-pools", result["policy"])
        self.assertIn("in compartment id <VM_COMPARTMENT_OCID>", result["policy"])
        self.assertNotIn("in tenancy", result["policy"])
        self.assertEqual(result["request_id"], "request-fixture/part")
        self.assertNotIn("sensitive-response-body", str(result))


class ServiceErrorWebTests(unittest.TestCase):
    setUp = test_core.WebTests.setUp

    def test_mysql_errors_preserve_safe_code_and_operation_in_html_and_json(self):
        @self.app.route("/test-mysql-error")
        def mysql_error():
            raise ProgrammingError("secret password and SQL", errno=1142)

        for headers in ({}, {"Accept": "application/json"}):
            response = self.client.get("/test-mysql-error", headers=headers)
            self.assertEqual(response.status_code, 403)
            self.assertIn(b"MySQL 1142", response.data)
            self.assertIn(b"Database operation", response.data)
            self.assertNotIn(b"secret password", response.data)

    def test_html_and_json_explain_the_service_failure_without_leaking_body(self):
        @self.app.route("/test-service-error")
        def failure():
            raise denied()

        for headers in ({}, {"Accept": "application/json"}):
            response = self.client.get("/test-service-error", headers=headers)
            self.assertEqual(response.status_code, 403)
            self.assertIn(b"NotAuthorizedOrNotFound", response.data)
            self.assertIn(b"stream-pools", response.data)
            self.assertNotIn(b"sensitive-response-body", response.data)
