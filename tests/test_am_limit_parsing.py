"""Tests for CapacityScheduler AM limit percentage and ratio parsing."""

import unittest
from unittest.mock import MagicMock
from dataproc_gateway_diagnostics.checks import check_am_capacity


class TestAmLimitParsing(unittest.TestCase):
    def test_am_limit_percentage_one_percent(self):
        """Verify that maxAMLimitPercentage=1.0 is parsed as 1% (ratio 0.01), not 100%."""
        client = MagicMock()
        client.yarn_metrics.return_value = {
            "allocatedMB": 2048,
            "availableMB": 10240,
            "totalMB": 12288,
        }
        client.yarn_scheduler.return_value = {
            "type": "capacityScheduler",
            "queues": {
                "queue": [
                    {
                        "type": "capacitySchedulerLeafQueueInfo",
                        "queueName": "default",
                        "usedAMResource": {"memory": 1024, "vCores": 1},
                        "AMResourceLimit": {"memory": 2048, "vCores": 2},
                        "capacities": {
                            "queueCapacitiesByPartition": [
                                {
                                    "partitionName": "",
                                    "maxAMLimitPercentage": 1.0,
                                }
                            ]
                        },
                        "numActiveApplications": 1,
                        "numPendingApplications": 0,
                        "users": {"user": []},
                    }
                ]
            },
        }
        client.yarn_apps.return_value = []

        res = check_am_capacity(client)
        default_row = next(r for r in res.metrics["queues"] if r["queue"] == "default")
        self.assertEqual(default_row["max_am_percent"], 1.0)
        self.assertEqual(default_row["max_am_ratio"], 0.01)

    def test_am_limit_percentage_eighty_percent(self):
        """Verify standard 80% maximum-am-resource-percent parsing."""
        client = MagicMock()
        client.yarn_metrics.return_value = {"allocatedMB": 0, "availableMB": 10000, "totalMB": 10000}
        client.yarn_scheduler.return_value = {
            "type": "capacityScheduler",
            "queues": {
                "queue": [
                    {
                        "type": "capacitySchedulerLeafQueueInfo",
                        "queueName": "root.default",
                        "usedAMResource": {"memory": 0},
                        "AMResourceLimit": {"memory": 8000},
                        "capacities": {
                            "queueCapacitiesByPartition": [
                                {"partitionName": "gpu", "maxAMLimitPercentage": 10.0},
                                {"partitionName": "", "maxAMLimitPercentage": 80.0},
                            ]
                        },
                        "numActiveApplications": 0,
                        "numPendingApplications": 0,
                    }
                ]
            },
        }
        client.yarn_apps.return_value = []

        res = check_am_capacity(client)
        q_row = res.metrics["queues"][0]
        # Ensures empty partitionName "" was selected over "gpu"
        self.assertEqual(q_row["max_am_percent"], 80.0)
        self.assertEqual(q_row["max_am_ratio"], 0.8)

    def test_configured_max_am_ratio_fallback(self):
        """Verify configuredMaxAMResourceLimit=0.15 is parsed as ratio 0.15 and 15.0%."""
        client = MagicMock()
        client.yarn_metrics.return_value = {"allocatedMB": 0, "availableMB": 10000, "totalMB": 10000}
        client.yarn_scheduler.return_value = {
            "type": "capacityScheduler",
            "queues": {
                "queue": [
                    {
                        "type": "capacitySchedulerLeafQueueInfo",
                        "queueName": "default",
                        "usedAMResource": {"memory": 0},
                        "AMResourceLimit": {"memory": 1500},
                        "configuredMaxAMResourceLimit": 0.15,
                        "numActiveApplications": 0,
                        "numPendingApplications": 0,
                    }
                ]
            },
        }
        client.yarn_apps.return_value = []

        res = check_am_capacity(client)
        q_row = res.metrics["queues"][0]
        self.assertEqual(q_row["max_am_ratio"], 0.15)
        self.assertEqual(q_row["max_am_percent"], 15.0)


if __name__ == "__main__":
    unittest.main()
