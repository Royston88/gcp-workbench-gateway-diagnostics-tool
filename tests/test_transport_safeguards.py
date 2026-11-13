"""Unit tests verifying transport safeguards, zero inadvertent job submissions, and proxy redaction."""

import errno
import unittest
import urllib.error
from unittest.mock import MagicMock, patch

from dataproc_gateway_diagnostics.client import (
    DiagnosticError,
    GatewayDiagnosticClient,
    _redact_proxy,
)


class TestTransportSafeguards(unittest.TestCase):
    def setUp(self):
        self.mock_snapshot = {
            "kernels": [{"id": "k-123"}],
            "yarn_metrics": {"availableMB": 4096},
            "yarn_scheduler": {"type": "capacityScheduler"},
            "yarn_apps": [{"id": "app_1", "state": "RUNNING"}],
            "gateway_log_entries": [],
            "errors": {},
        }

    def test_gateway_transport_submits_zero_jobs_on_unreachable_gateway(self):
        """Under default transport='gateway', all getters MUST raise DiagnosticError and submit 0 jobs."""
        client = GatewayDiagnosticClient(
            cluster_name="test-cluster",
            project_id="test-proj",
            region="us-central1",
            transport="gateway",
            allow_job_submission=False,
        )
        client._get_master_snapshot = MagicMock(return_value=self.mock_snapshot)

        # Simulate connection-level network failure (e.g. Errno 101 ENETUNREACH)
        client.yarn = MagicMock(
            side_effect=DiagnosticError(
                "Network error for https://gateway.test/ws/v1/cluster: [Errno 101] Network is unreachable"
            )
        )
        client._get_json = MagicMock(
            side_effect=DiagnosticError(
                "Network error for https://gateway.test/api/kernels: [Errno 101] Network is unreachable"
            )
        )
        client._endpoint = MagicMock(return_value="https://gateway.test")

        # 1. yarn_metrics
        with self.assertRaises(DiagnosticError) as cm:
            client.yarn_metrics()
        self.assertIn("Component Gateway is unreachable over the network", str(cm.exception))
        self.assertIn("--transport=spark-job", str(cm.exception))

        # 2. yarn_scheduler (MUST NOT call _get_master_snapshot!)
        with self.assertRaises(DiagnosticError) as cm:
            client.yarn_scheduler()
        self.assertIn("Component Gateway is unreachable over the network", str(cm.exception))

        # 3. yarn_apps (MUST NOT call _get_master_snapshot!)
        with self.assertRaises(DiagnosticError) as cm:
            client.yarn_apps()
        self.assertIn("Component Gateway is unreachable over the network", str(cm.exception))

        # 4. kernels (MUST NOT call _get_master_snapshot!)
        with self.assertRaises(DiagnosticError) as cm:
            client.kernels()
        self.assertIn("Component Gateway is unreachable over the network", str(cm.exception))

        # STRICT ASSERTION: Zero calls to _get_master_snapshot
        self.assertEqual(client._get_master_snapshot.call_count, 0)
        self.assertFalse(client._gateway_unreachable)

    def test_auto_transport_without_allow_job_submits_zero_jobs(self):
        """Under transport='auto' with allow_job_submission=False, zero jobs are submitted."""
        client = GatewayDiagnosticClient(
            cluster_name="test-cluster",
            project_id="test-proj",
            region="us-central1",
            transport="auto",
            allow_job_submission=False,
        )
        client._get_master_snapshot = MagicMock(return_value=self.mock_snapshot)
        client.yarn = MagicMock(
            side_effect=DiagnosticError(
                "Network error for https://gateway.test/ws/v1/cluster: [Errno 101] Network is unreachable"
            )
        )
        client._get_json = MagicMock(
            side_effect=DiagnosticError(
                "Network error for https://gateway.test/api/kernels: [Errno 101] Network is unreachable"
            )
        )
        client._endpoint = MagicMock(return_value="https://gateway.test")

        with self.assertRaises(DiagnosticError) as cm:
            client.yarn_metrics()
        self.assertIn("--allow-job-submission", str(cm.exception))

        with self.assertRaises(DiagnosticError):
            client.yarn_scheduler()

        with self.assertRaises(DiagnosticError):
            client.yarn_apps()

        with self.assertRaises(DiagnosticError):
            client.kernels()

        self.assertEqual(client._get_master_snapshot.call_count, 0)
        self.assertFalse(client._gateway_unreachable)

    def test_auto_transport_with_allow_job_falls_back_cleanly(self):
        """Under transport='auto' with allow_job_submission=True, snapshot is retrieved on network error."""
        client = GatewayDiagnosticClient(
            cluster_name="test-cluster",
            project_id="test-proj",
            region="us-central1",
            transport="auto",
            allow_job_submission=True,
        )
        client._get_master_snapshot = MagicMock(return_value=self.mock_snapshot)
        client.yarn = MagicMock(
            side_effect=DiagnosticError(
                "Network error for https://gateway.test/ws/v1/cluster: [Errno 101] Network is unreachable"
            )
        )
        client._endpoint = MagicMock(return_value="https://gateway.test")

        metrics = client.yarn_metrics()
        self.assertEqual(metrics, {"availableMB": 4096})
        self.assertTrue(client._gateway_unreachable)
        self.assertEqual(client._get_master_snapshot.call_count, 1)

    def test_proxy_redaction(self):
        """Verify _redact_proxy removes basic auth credentials and leaves unauthenticated proxies untouched."""
        # Authenticated proxies
        self.assertEqual(
            _redact_proxy("http://alice:secret123@proxy.corp.example:8080"),
            "http://***@proxy.corp.example:8080",
        )
        self.assertEqual(
            _redact_proxy("https://user:p%40ssword@10.0.0.1:3128"),
            "https://***@10.0.0.1:3128",
        )
        self.assertEqual(
            _redact_proxy("http://onlyuser@proxy.local"),
            "http://***@proxy.local",
        )

        # Unauthenticated proxies
        self.assertEqual(
            _redact_proxy("http://proxy.corp.example:8080"),
            "http://proxy.corp.example:8080",
        )
        self.assertEqual(
            _redact_proxy("http://127.0.0.1:8888"),
            "http://127.0.0.1:8888",
        )
        self.assertEqual(_redact_proxy(""), "")


if __name__ == "__main__":
    unittest.main()
