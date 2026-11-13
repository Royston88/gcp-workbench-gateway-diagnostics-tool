"""Tests for remediation accuracy (launch timeout, AM limit advice, Check 3 parsing, collector)."""

import unittest
from unittest.mock import MagicMock

from dataproc_gateway_diagnostics import checks
from dataproc_gateway_diagnostics.checks import (
    DATAPROC_KERNEL_LAUNCH_TIMEOUT_S,
    _am_limit_exceeded_diagnosis,
    check_am_capacity,
    check_launch_timeouts,
)
from dataproc_gateway_diagnostics.client import MASTER_COLLECTOR_SCRIPT

BOGUS_KEYS = ("GatewayProvisionerBase", "default_kernel_launch_timeout", "KERNEL_LAUNCH_TIMEOUT=600")


def _scheduler(max_am_pct, used_am=7000, am_limit=8000):
    return {
        "type": "capacityScheduler",
        "queues": {
            "queue": [
                {
                    "type": "capacitySchedulerLeafQueueInfo",
                    "queueName": "default",
                    "usedAMResource": {"memory": used_am},
                    "AMResourceLimit": {"memory": am_limit},
                    "capacities": {
                        "queueCapacitiesByPartition": [
                            {"partitionName": "", "maxAMLimitPercentage": max_am_pct}
                        ]
                    },
                    "numActiveApplications": 3,
                    "numPendingApplications": 1,
                    "users": {"user": []},
                }
            ]
        },
    }


def _accepted_app(diagnostics):
    return {
        "id": "application_1_0001",
        "user": "u",
        "name": "kernel",
        "state": "ACCEPTED",
        "queue": "default",
        "diagnostics": diagnostics,
    }


def _check2_text(max_am_pct, diagnostics):
    client = MagicMock()
    client.yarn_metrics.return_value = {"allocatedMB": 8000, "availableMB": 64000, "totalMB": 72000}
    client.yarn_scheduler.return_value = _scheduler(max_am_pct)
    client.yarn_apps.return_value = [_accepted_app(diagnostics)]
    res = check_am_capacity(client)
    return " ".join(str(v) for _, v in res.details)


class TestAmLimitAdvice(unittest.TestCase):
    def test_below_recommended_says_raise_with_current_value(self):
        text = _am_limit_exceeded_diagnosis(40.0)
        self.assertIn("40.0%", text)
        self.assertIn("raise", text)
        self.assertIn("maximum-am-resource-percent", text)

    def test_already_at_recommended_does_not_say_raise_to_08(self):
        text = _am_limit_exceeded_diagnosis(80.0)
        self.assertIn("already 80.0%", text)
        self.assertNotIn("raise capacity-scheduler", text)
        self.assertIn("orphaned", text)

    def test_unknown_limit(self):
        self.assertIn("unknown", _am_limit_exceeded_diagnosis(None))


class TestCheck2RootCauseText(unittest.TestCase):
    def test_register_with_rm_has_no_bogus_timeout_key(self):
        text = _check2_text(
            80.0, "AM container is launched, waiting for AM container to Register with RM"
        )
        self.assertIn(f"{DATAPROC_KERNEL_LAUNCH_TIMEOUT_S}s", text)
        self.assertIn("kernel.json", text)
        for bad in BOGUS_KEYS:
            self.assertNotIn(bad, text)
        self.assertNotIn("OpenTelemetry", text)

    def test_am_limit_exceeded_uses_current_limit(self):
        text = _check2_text(
            10.0,
            "Application is added to the scheduler and is not yet activated. "
            "Queue's AM resource limit exceeded.",
        )
        self.assertIn("10.0%", text)
        self.assertNotIn("increase maximum-am-resource-percent to 0.8", text)


class TestCheck3(unittest.TestCase):
    def _run(self, payloads):
        client = MagicMock()
        client.project_id = "p"
        client.cluster_name = "c"
        client.log_entries.return_value = [
            {"timestamp": "2026-11-13T00:00:00Z", "textPayload": p} for p in payloads
        ]
        return check_launch_timeouts(client)

    def test_float_timeout_is_parsed(self):
        res = self._run(
            [
                "YARN resources unavailable after 130.2 seconds for app application_1_0001, "
                "launch timeout: 96.4!  Check YARN configuration. KernelID: 'abc'"
            ]
        )
        self.assertEqual(res.metrics["effective_timeout_seconds"], 96)

    def test_integer_timeout_and_remediation(self):
        res = self._run(["... launch timeout: 120! KernelID: 'k1'"])
        self.assertEqual(res.metrics["effective_timeout_seconds"], 120)
        joined = " ".join(res.remediation)
        for bad in BOGUS_KEYS:
            self.assertNotIn(bad, joined)
        self.assertIn("custom image", joined)

    def test_no_events_mentions_dataproc_default(self):
        res = self._run([])
        text = " ".join(str(v) for _, v in res.details)
        self.assertIn(f"{DATAPROC_KERNEL_LAUNCH_TIMEOUT_S}s", text)


class TestCollectorScript(unittest.TestCase):
    def test_collector_only_keeps_launch_timeout_lines(self):
        self.assertIn('if "launch timeout" in line:', MASTER_COLLECTOR_SCRIPT)
        self.assertNotIn("500 post /api/kernels", MASTER_COLLECTOR_SCRIPT)

    def test_collector_script_compiles(self):
        compile(MASTER_COLLECTOR_SCRIPT, "<collector>", "exec")


class TestNoBogusKeysAnywhere(unittest.TestCase):
    def test_checks_module_has_no_bogus_keys(self):
        import inspect

        src = inspect.getsource(checks)
        for bad in BOGUS_KEYS:
            self.assertNotIn(bad, src)


if __name__ == "__main__":
    unittest.main()
