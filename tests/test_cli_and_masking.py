"""Tests for CLI options, peer masking in Check 3, and render_text summary."""

import unittest
from unittest.mock import MagicMock
from dataproc_gateway_diagnostics.checks import check_launch_timeouts
from dataproc_gateway_diagnostics.cli import render_text
from dataproc_gateway_diagnostics.client import GatewayDiagnosticClient
from dataproc_gateway_diagnostics.models import CheckResult, GatewayDiagnosticReport, Status


class TestCliAndMasking(unittest.TestCase):
    def test_cli_transport_defaults_to_gateway(self):
        """Verify GatewayDiagnosticClient defaults to transport='gateway' and allow_job_submission=False."""
        client = GatewayDiagnosticClient(
            cluster_name="test-cluster",
            project_id="test-proj",
            region="us-central1",
        )
        self.assertEqual(client.transport, "gateway")
        self.assertFalse(client.allow_job_submission)
        self.assertEqual(client.active_transport, "Method 1: Component Gateway HTTPS")

    def test_cli_billing_project_header(self):
        """Verify billing_project sets X-Goog-User-Project only when explicitly provided."""
        client_default = GatewayDiagnosticClient(
            cluster_name="test-cluster",
            project_id="test-proj",
            region="us-central1",
            billing_project=None,
        )
        self.assertIsNone(client_default.billing_project)

        client_custom = GatewayDiagnosticClient(
            cluster_name="test-cluster",
            project_id="test-proj",
            region="us-central1",
            billing_project="custom-billing-proj",
        )
        self.assertEqual(client_custom.billing_project, "custom-billing-proj")

    def test_peer_masking_in_check_3(self):
        """Verify that under my_sessions_only, kernel IDs in launch timeout logs are masked for other users."""
        client = MagicMock()
        client.cluster_name = "test-cluster"
        client.project_id = "test-proj"
        client.log_entries.return_value = [
            {
                "timestamp": "2026-10-01T10:00:00Z",
                "textPayload": "[W 261001 10:00:00 KernelID: '12345678-abcd-1111-2222-333344445555'] launch timeout: 120",
            }
        ]

        res = check_launch_timeouts(
            client,
            lookback_days=7,
            my_sessions_only=True,
            scoped_user="ds_user_1",
        )
        self.assertEqual(res.status, Status.FAIL)
        self.assertEqual(len(res.metrics["affected_kernel_ids"]), 1)
        # Confirms kernel ID is masked
        self.assertIn("12345678...[masked]", res.metrics["affected_kernel_ids"])

    def test_render_text_summary_on_error(self):
        """Verify render_text prints 'Review error details above.' when overall status is ERROR."""
        report = GatewayDiagnosticReport(
            tool_version="0.2.0",
            project_id="test-proj",
            region_id="us-central1",
            cluster_name="test-cluster",
            active_account="test@example.com",
            cluster_uuid="123",
            cluster_state="RUNNING",
            image_version="2.3",
            generated_at="2026-10-01 12:00:00 UTC",
        )
        err_check = CheckResult(check_id=1, name="Check 1", status=Status.ERROR, summary="Access denied")
        report.checks = [err_check]

        output = render_text(report)
        self.assertNotIn("No action required.", output)
        self.assertIn("Review error details above.", output)


if __name__ == "__main__":
    unittest.main()
