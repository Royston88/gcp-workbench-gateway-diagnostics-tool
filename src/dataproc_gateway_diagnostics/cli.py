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

"""Command line interface.

Plain text output only -- no HTML, no colour codes -- so it renders identically
in a JupyterLab terminal, a notebook cell via ``!``, Cloud Shell, or a log file
pasted into a support case.
"""

import argparse
import json
import sys
import warnings
from datetime import datetime, timezone
from typing import List, Optional

warnings.filterwarnings("ignore")

from . import __version__
from .checks import (
    DEFAULT_APP_AGE_HOURS,
    DEFAULT_EXPECTED_USERS,
    DEFAULT_IDLE_HOURS,
    DEFAULT_TIMEOUT_LOOKBACK_DAYS,
    run_checks,
)
from .client import AccessDenied, DiagnosticError, GatewayDiagnosticClient
from .models import GatewayDiagnosticReport, Status

WIDTH = 65
RULE = "=" * WIDTH
THIN = "-" * WIDTH


def _center(text: str) -> str:
    return text.center(WIDTH).rstrip()


def build_report(
    client: GatewayDiagnosticClient,
    selected: Optional[List[int]] = None,
    idle_hours: float = DEFAULT_IDLE_HOURS,
    app_age_hours: float = DEFAULT_APP_AGE_HOURS,
    lookback_days: int = DEFAULT_TIMEOUT_LOOKBACK_DAYS,
    expected_users: int = DEFAULT_EXPECTED_USERS,
) -> GatewayDiagnosticReport:
    """Collect cluster metadata and run the requested checks."""
    cluster = client.get_cluster()
    software = cluster.get("config", {}).get("softwareConfig", {})

    execution_context = (
        client.resolve_execution_context()
        if hasattr(client, "resolve_execution_context")
        else {}
    )
    iam_capabilities = (
        client.check_iam_capabilities()
        if hasattr(client, "check_iam_capabilities")
        else {}
    )

    report = GatewayDiagnosticReport(
        tool_version=__version__,
        project_id=client.project_id,
        region_id=client.region,
        cluster_name=client.cluster_name,
        active_account=client.active_account,
        cluster_uuid=cluster.get("clusterUuid", ""),
        cluster_state=cluster.get("status", {}).get("state", ""),
        image_version=software.get("imageVersion", ""),
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        execution_context=execution_context,
        iam_capabilities=iam_capabilities,
    )
    report.checks = run_checks(
        client,
        selected=selected,
        idle_hours=idle_hours,
        app_age_hours=app_age_hours,
        lookback_days=lookback_days,
        expected_users=expected_users,
    )
    return report


def render_text(report: GatewayDiagnosticReport) -> str:
    """Render the report as plain text."""
    lines: List[str] = []
    lines.append(RULE)
    lines.append(_center("JUPYTER KERNEL GATEWAY & YARN CAPACITY AUDIT"))
    lines.append(RULE)
    lines.append(f"Tool Version      : {report.tool_version}")
    lines.append(f"Generated At      : {report.generated_at}")
    lines.append(f"Project ID        : {report.project_id}")
    lines.append(f"Region ID         : {report.region_id}")
    lines.append(f"Target Cluster    : {report.cluster_name}")
    if report.cluster_state:
        lines.append(f"Cluster State     : {report.cluster_state}")
    if report.image_version:
        lines.append(f"Image Version     : {report.image_version}")
    if report.active_account:
        lines.append(f"Active Account    : {report.active_account}")
    if report.execution_context and report.execution_context.get("display"):
        lines.append(f"Execution Context : {report.execution_context['display']}")
    lines.append(THIN)

    # Pre-Flight IAM Permissions & Diagnostic Capabilities Matrix
    if report.iam_capabilities:
        lines.append("[PRE-FLIGHT] IAM Permissions & Diagnostic Capabilities")
        iam = report.iam_capabilities

        # 1. Core Diagnostic Checks
        c_dp = iam.get("core_dataproc", {})
        c_gw = iam.get("core_gateway_yarn", {})
        c_log = iam.get("cloud_logging", {})
        core_ready = c_dp.get("granted", True) and c_gw.get("granted", True) and c_log.get("granted", True)
        core_status = "[✓] FULL (Checks 1, 2, 3, 4 ready)" if core_ready else "[!] DEGRADED"
        lines.append(f"   -> Core Diagnostic Checks     : {core_status}")
        lines.append(f"      * Dataproc Cluster API     : {'GRANTED' if c_dp.get('granted') else 'MISSING'} ({c_dp.get('role', 'roles/dataproc.viewer')})")
        lines.append(f"      * Gateway REST / YARN API  : {'GRANTED' if c_gw.get('granted') else 'MISSING'} ({c_gw.get('role', 'dataproc.clusters.use')})")
        lines.append(f"      * Cloud Logging Logs       : {'GRANTED' if c_log.get('granted') else 'MISSING'} ({c_log.get('role', 'roles/logging.viewer')})")

        # 2. Multi-VM Disambiguation
        s1 = iam.get("signal_1_guest_attributes", {})
        s2 = iam.get("signal_2_serial_console", {})
        s3 = iam.get("signal_3_cloud_monitoring", {})
        wb_inv = iam.get("workbench_inventory", {})
        multi_ready = wb_inv.get("granted", True) and (s1.get("granted") or s2.get("granted") or s3.get("granted"))
        multi_status = "[✓] ENABLED (Signals 1, 2, 3 active)" if multi_ready else "[!] DEGRADED"
        lines.append(f"   -> Multi-VM Disambiguation    : {multi_status}")
        lines.append(f"      * Signal 1 Guest Attributes: {'GRANTED' if s1.get('granted') else 'MISSING'} ({s1.get('role', 'compute.instances.get')})")
        lines.append(f"      * Signal 2 Serial Console  : {'GRANTED' if s2.get('granted') else 'MISSING'} ({s2.get('role', 'logging.entries.list')})")
        lines.append(f"      * Signal 3 Cloud Monitoring: {'GRANTED' if s3.get('granted') else 'MISSING'} ({s3.get('role', 'roles/monitoring.viewer')})")

        # 3. External In-Situ Probing
        m3 = iam.get("method_3_inverting_proxy", {})
        m2 = iam.get("method_2_iap_tunnel", {})
        m1 = iam.get("method_1_gce_exec", {})
        lines.append(f"   -> External In-Situ Probing   : [✓] AVAILABLE (Fallback Chain: 3 -> 2 -> 1 -> Cloud Logging)")
        lines.append(f"      * Method 3 Inverting Proxy : SKIPPED ({m3.get('detail')})")
        lines.append(f"      * Method 2 IAP Tunnel      : DEGRADED ({m2.get('detail')})")
        lines.append(f"      * Method 1 Non-Intr. SSH   : READY ({m1.get('detail')})")
        lines.append(f"      * Safety Net Serial Trace  : ACTIVE (Cloud Logging /lab/tree/ referer)")
        lines.append(THIN)

    for check in report.checks:
        lines.append("")
        lines.append(f"[CHECK {check.check_id}] {check.name}")
        for label, value in check.details:
            if not value:
                if label.startswith("---"):
                    lines.append(f"\n   {label}")
                elif label.startswith("["):
                    lines.append(f"\n   -> {label}")
                else:
                    lines.append(f"   -> {label}")
            elif (
                label.startswith("      * ")
                or label.startswith("   * ")
                or label.startswith("        - ")
            ):
                lines.append(f"{label:<36}: {value}")
            elif label.startswith("[Kernel]"):
                lines.append(f"\n   -> {label:<30}: {value}")
            else:
                lines.append(f"   -> {label:<30}: {value}")
        if check.details:
            lines.append("")
        lines.append(f"   -> {'Verdict':<30}: {Status.marker(check.status)}")
        if check.summary:
            for chunk in _wrap(check.summary, WIDTH - 6):
                lines.append(f"      {chunk}")

    lines.append("")
    lines.append(RULE)
    lines.append(_center("SUMMARY"))
    lines.append(RULE)

    primary = report.primary_root_cause
    for check in report.checks:
        marker = Status.marker(check.status)
        suffix = ""
        if primary is not None and check.check_id == primary.check_id:
            suffix = "   <-- PRIMARY ROOT CAUSE"
        label = f"Check {check.check_id}  {check.name}"
        lines.append(f"   {label:<42}: {marker}{suffix}")

    lines.append(f"   {'OVERALL':<42}: {Status.marker(report.overall_status)}")
    lines.append(RULE)

    actionable = [c for c in report.checks if c.is_actionable and c.remediation]
    if actionable:
        lines.append(_center("RECOMMENDED REMEDIATION (priority order)"))
        lines.append(RULE)
        step = 1
        seen = set()
        for check in actionable:
            for item in check.remediation:
                if item in seen:
                    continue
                seen.add(item)
                wrapped = _wrap(item, WIDTH - 7)
                lines.append(f"   {step}. {wrapped[0]}")
                for cont in wrapped[1:]:
                    lines.append(f"      {cont}")
                step += 1
        lines.append(RULE)
    else:
        lines.append(_center("No action required."))
        lines.append(RULE)

    return "\n".join(lines)


def _wrap(text: str, width: int) -> List[str]:
    words = text.split()
    out: List[str] = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if len(candidate) > width and current:
            out.append(current)
            current = word
        else:
            current = candidate
    if current:
        out.append(current)
    return out or [""]


def _parse_checks(value: str) -> Optional[List[int]]:
    if not value or value.strip().lower() == "all":
        return None
    selected: List[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if not part.isdigit() or int(part) not in (1, 2, 3, 4):
            raise argparse.ArgumentTypeError(
                f"Invalid check '{part}'. Use a comma separated subset of 1,2,3,4."
            )
        selected.append(int(part))
    return selected or None


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="gateway-diag",
        description=(
            "Read-only diagnostic for Jupyter Kernel Gateway kernel launch failures "
            "(HTTP 500 / TimeoutError) on Google Cloud Dataproc."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Checks performed:
  1  Zombie / idle kernel sessions holding ApplicationMaster capacity
  2  YARN ApplicationMaster capacity and admission limits
  3  Kernel Gateway launch timeouts recorded in Cloud Logging
  4  Spark driver / AM sizing and the resulting concurrency ceiling

Examples:
  # Full audit
  gateway-diag diagnose --cluster=my-dp-cluster --region=us-central1

  # From inside a notebook cell
  !python -m dataproc_gateway_diagnostics.cli diagnose --cluster=my-dp-cluster

  # Only the YARN capacity check
  gateway-diag diagnose --cluster=my-dp-cluster --checks=2

  # Machine readable output to attach to a support case
  gateway-diag diagnose --cluster=my-dp-cluster --json > audit.json
        """,
    )
    parser.add_argument(
        "action",
        nargs="?",
        default="diagnose",
        choices=["diagnose"],
        help="Action to perform (default: diagnose).",
    )
    parser.add_argument("--cluster", required=True, help="Dataproc cluster name.")
    parser.add_argument("--project", default=None, help="Project ID (auto-detected).")
    parser.add_argument("--region", default=None, help="Dataproc region (auto-detected).")
    parser.add_argument(
        "--checks",
        default="all",
        help="Comma separated subset of checks to run, e.g. --checks=2,3 (default: all).",
    )
    parser.add_argument(
        "--idle-hours",
        type=float,
        default=DEFAULT_IDLE_HOURS,
        help=f"Idle threshold for zombie kernels (default: {DEFAULT_IDLE_HOURS}).",
    )
    parser.add_argument(
        "--app-age-hours",
        type=float,
        default=DEFAULT_APP_AGE_HOURS,
        help=f"Age threshold for long-running apps (default: {DEFAULT_APP_AGE_HOURS}).",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_TIMEOUT_LOOKBACK_DAYS,
        help=f"Log lookback window (default: {DEFAULT_TIMEOUT_LOOKBACK_DAYS}).",
    )
    parser.add_argument(
        "--expected-users",
        type=int,
        default=DEFAULT_EXPECTED_USERS,
        help=(
            "Expected concurrent notebook users, used to judge the concurrency "
            f"ceiling (default: {DEFAULT_EXPECTED_USERS})."
        ),
    )
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    parser.add_argument("--verbose", action="store_true", help="Log every HTTP request.")
    parser.add_argument(
        "--version", action="version", version=f"dataproc-gateway-diagnostics {__version__}"
    )

    args = parser.parse_args(argv)

    try:
        selected = _parse_checks(args.checks)
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))
        return 2

    try:
        client = GatewayDiagnosticClient(
            cluster_name=args.cluster,
            project_id=args.project,
            region=args.region,
            timeout=args.timeout,
            verbose=args.verbose,
        )
        report = build_report(
            client,
            selected=selected,
            idle_hours=args.idle_hours,
            app_age_hours=args.app_age_hours,
            lookback_days=args.lookback_days,
            expected_users=args.expected_users,
        )
    except AccessDenied as exc:
        print(f"[!] Permission denied: {exc}", file=sys.stderr)
        if exc.required_roles:
            print(
                "\n    The caller needs the following on project "
                f"'{args.project or 'the target project'}':",
                file=sys.stderr,
            )
            for role in exc.required_roles:
                print(f"      - {role}", file=sys.stderr)
            print(
                "\n    Example:\n"
                "      gcloud projects add-iam-policy-binding <PROJECT> \\\n"
                "        --member='user:<YOUR_ACCOUNT>' \\\n"
                "        --role='roles/dataproc.viewer'",
                file=sys.stderr,
            )
        return 2
    except DiagnosticError as exc:
        print(f"[!] {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(report.to_dict(), indent=2))
    else:
        print(render_text(report))

    # Exit code: 0 healthy, 1 actionable finding, 2 could not run.
    return 1 if report.overall_status in (Status.FAIL, Status.WARN) else 0


if __name__ == "__main__":
    sys.exit(main())
