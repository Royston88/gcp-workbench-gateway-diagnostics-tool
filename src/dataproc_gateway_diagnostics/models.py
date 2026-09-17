# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Data models for gateway diagnostics.

Uses stdlib dataclasses rather than pydantic so that the package remains
installable with ``pip install --no-deps`` on locked-down Workbench and
Dataproc environments.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


class Status:
    """Verdict values for an individual check."""

    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    SKIPPED = "SKIPPED"
    ERROR = "ERROR"

    # Rendered markers, matching the house style of the sibling
    # scheduler-jupyter-plugin-diagnostics tool.
    MARKERS = {
        PASS: "[\u2713] PASS",
        WARN: "[!] WARN",
        FAIL: "[\u2717] FAIL",
        SKIPPED: "[?] SKIPPED",
        ERROR: "[?] ERROR",
    }

    # Ordering used to compute the overall roll-up verdict.
    SEVERITY = {PASS: 0, SKIPPED: 1, ERROR: 2, WARN: 3, FAIL: 4}

    @classmethod
    def marker(cls, status: str) -> str:
        return cls.MARKERS.get(status, status)


@dataclass
class CheckResult:
    """Outcome of a single diagnostic check."""

    check_id: int
    name: str
    status: str = Status.SKIPPED
    summary: str = ""
    # Ordered (label, value) pairs rendered underneath the check heading.
    details: List[Tuple[str, str]] = field(default_factory=list)
    # Ordered remediation lines emitted in the summary block when not PASS.
    remediation: List[str] = field(default_factory=list)
    # Raw numeric/structured evidence, surfaced only via --json.
    metrics: Dict[str, Any] = field(default_factory=dict)

    def add(self, label: str, value: Any) -> None:
        self.details.append((label, str(value)))

    @property
    def is_actionable(self) -> bool:
        return self.status in (Status.FAIL, Status.WARN)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check_id": self.check_id,
            "name": self.name,
            "status": self.status,
            "summary": self.summary,
            "details": [{"label": k, "value": v} for k, v in self.details],
            "remediation": list(self.remediation),
            "metrics": self.metrics,
        }


@dataclass
class GatewayDiagnosticReport:
    """Aggregated result of every check performed against one cluster."""

    tool_version: str
    project_id: str
    region_id: str
    cluster_name: str
    active_account: str = ""
    cluster_uuid: str = ""
    cluster_state: str = ""
    image_version: str = ""
    generated_at: str = ""
    execution_context: Dict[str, Any] = field(default_factory=dict)
    iam_capabilities: Dict[str, Any] = field(default_factory=dict)
    my_sessions_only: bool = False
    scoped_user: Optional[str] = None
    total_cluster_kernels: int = 0
    total_cluster_yarn_apps: int = 0
    checks: List[CheckResult] = field(default_factory=list)

    @property
    def overall_status(self) -> str:
        if not self.checks:
            return Status.SKIPPED
        return max(
            (c.status for c in self.checks),
            key=lambda s: Status.SEVERITY.get(s, 0),
        )

    @property
    def primary_root_cause(self) -> Optional[CheckResult]:
        """The lowest-numbered failing check.

        Checks are ordered so that causes precede the symptoms they produce
        (AM starvation before launch timeouts), so the first FAIL is the one
        worth fixing first.
        """
        failures = [c for c in self.checks if c.status == Status.FAIL]
        return min(failures, key=lambda c: c.check_id) if failures else None

    def to_dict(self) -> Dict[str, Any]:
        primary = self.primary_root_cause
        return {
            "tool_version": self.tool_version,
            "generated_at": self.generated_at,
            "project_id": self.project_id,
            "region_id": self.region_id,
            "cluster_name": self.cluster_name,
            "cluster_uuid": self.cluster_uuid,
            "cluster_state": self.cluster_state,
            "image_version": self.image_version,
            "active_account": self.active_account,
            "execution_context": self.execution_context,
            "iam_capabilities": self.iam_capabilities,
            "my_sessions_only": self.my_sessions_only,
            "scoped_user": self.scoped_user,
            "total_cluster_kernels": self.total_cluster_kernels,
            "total_cluster_yarn_apps": self.total_cluster_yarn_apps,
            "overall_status": self.overall_status,
            "primary_root_cause": primary.name if primary else None,
            "checks": [c.to_dict() for c in self.checks],
        }
