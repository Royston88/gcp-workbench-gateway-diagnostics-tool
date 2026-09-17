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

"""The four diagnostic checks.

Check ordering is deliberate: causes are numbered before the symptoms they
produce, so the lowest-numbered FAIL is the one worth fixing first.

  1. Zombie / idle kernel sessions   (why capacity leaks over days)
  2. YARN ApplicationMaster capacity (why new kernels are refused admission)
  3. Kernel Gateway launch timeouts  (the user-visible HTTP 500)
  4. Spark driver / AM sizing        (how many kernels the cluster can ever fit)
"""

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from .client import AccessDenied, DiagnosticError, GatewayDiagnosticClient, NotFound
from .models import CheckResult, Status

DEFAULT_IDLE_HOURS = 2.0
DEFAULT_APP_AGE_HOURS = 24.0
DEFAULT_TIMEOUT_LOOKBACK_DAYS = 7
DEFAULT_EXPECTED_USERS = 10

# A kernel AM is considered to be starving the queue at or above this ratio.
AM_SATURATION_FAIL = 0.90
AM_SATURATION_WARN = 0.70
# Free memory above this is "plenty available", which makes pending apps
# diagnostic of an admission-control limit rather than genuine exhaustion.
FREE_MEMORY_SIGNIFICANT_MB = 8 * 1024


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def parse_memory_mb(value: Any, default: Optional[int] = None) -> Optional[int]:
    """Parse a Spark/YARN memory string ('2g', '1024m', '2048') into MB."""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip().lower()
    if not text:
        return default
    match = re.match(r"^(\d+(?:\.\d+)?)\s*([kmgt]?)b?$", text)
    if not match:
        return default
    number = float(match.group(1))
    unit = match.group(2)
    factor = {"": 1.0, "k": 1.0 / 1024, "m": 1.0, "g": 1024.0, "t": 1024.0 * 1024}
    return int(number * factor.get(unit, 1.0))


def humanize_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes = rem // 60
    if days:
        return f"{days}d {hours}h {minutes}m"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def humanize_mb(mb: Optional[float]) -> str:
    if mb is None:
        return "unknown"
    if mb >= 1024:
        return f"{mb / 1024:.1f} GB"
    return f"{int(mb)} MB"


def parse_timestamp(value: str) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp, tolerating 'Z' and fractional seconds."""
    if not value:
        return None
    text = value.strip().replace("Z", "+00:00")
    # Python < 3.11 rejects more than 6 fractional digits.
    text = re.sub(r"(\.\d{6})\d+", r"\1", text)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


UUID_REGEX = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def format_epoch_ms(epoch_ms: Any) -> str:
    """Format epoch millisecond timestamp as 'YYYY-MM-DD HH:MM:SS UTC'."""
    try:
        sec = float(epoch_ms) / 1000.0
        dt = datetime.fromtimestamp(sec, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except (ValueError, TypeError, OSError):
        return "unknown"


def shorten_node_address(addr: str) -> str:
    """Shorten node FQDN to hostname:port or prefix...-w-N:port."""
    if not addr:
        return "unknown"
    host_part, _, port = addr.partition(":")
    short_host = host_part.split(".")[0]
    if len(short_host) > 28 and "-w-" in short_host:
        prefix, _, worker = short_host.rpartition("-w-")
        short_host = f"{prefix[:15]}...-w-{worker}"
    return f"{short_host}:{port}" if port else short_host


def iter_leaf_queues(node: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Depth-first collection of capacity-scheduler leaf queues."""
    leaves: List[Dict[str, Any]] = []
    if node.get("type") == "capacitySchedulerLeafQueueInfo":
        leaves.append(node)
    for child in (node.get("queues", {}) or {}).get("queue", []) or []:
        leaves.extend(iter_leaf_queues(child))
    return leaves


def _resource_mb(resource: Optional[Dict[str, Any]]) -> Optional[int]:
    if not isinstance(resource, dict):
        return None
    value = resource.get("memory")
    return int(value) if value is not None else None


def _skip_result(check_id: int, name: str, exc: Exception) -> CheckResult:
    """Convert an endpoint-level failure into a SKIPPED result."""
    result = CheckResult(check_id=check_id, name=name)
    if isinstance(exc, AccessDenied):
        result.status = Status.SKIPPED
        result.summary = "Insufficient permission to read this data source."
        result.remediation = [
            "Grant the caller roles/dataproc.viewer and dataproc.clusters.use "
            "(Component Gateway access), then re-run."
        ]
    elif isinstance(exc, NotFound):
        result.status = Status.SKIPPED
        result.summary = str(exc)
    else:
        result.status = Status.ERROR
        result.summary = f"{type(exc).__name__}: {exc}"
    result.add("Detail", str(exc)[:200])
    return result


# ----------------------------------------------------------------------
# Check 1 - zombie / idle kernel sessions
# ----------------------------------------------------------------------
def check_kernel_sessions(
    client: GatewayDiagnosticClient,
    idle_hours: float = DEFAULT_IDLE_HOURS,
    app_age_hours: float = DEFAULT_APP_AGE_HOURS,
) -> CheckResult:
    name = "Zombie / Idle Kernel Sessions"
    result = CheckResult(check_id=1, name=name)

    try:
        kernels = client.kernels()
    except (AccessDenied, NotFound, DiagnosticError) as exc:
        return _skip_result(1, name, exc)

    now = datetime.now(timezone.utc)
    idle_cutoff = timedelta(hours=idle_hours)

    idle_kernels: List[Tuple[str, float, str]] = []
    busy = 0
    max_idle = 0.0

    # Fetch running YARN applications (filtered for RUNNING state)
    running_apps: List[Dict[str, Any]] = []
    apps_error = None
    try:
        running_apps = client.yarn_apps(states="RUNNING")
    except (AccessDenied, NotFound, DiagnosticError) as exc:
        apps_error = str(exc)

    # Fetch cluster properties for culling configuration
    props: Dict[str, Any] = {}
    try:
        props = client.cluster_properties()
    except (AccessDenied, NotFound, DiagnosticError):
        pass

    # Detect YARN Application Lifetime monitor
    is_yarn_unlimited = True
    yarn_lifetime_val = "UNLIMITED (no automatic reaper)"
    yarn_prop_lifetime = props.get(
        "yarn:yarn.resourcemanager.app.max-lifetime"
    ) or props.get("yarn:yarn.resourcemanager.app.default-lifetime")
    if yarn_prop_lifetime and yarn_prop_lifetime != "-1":
        is_yarn_unlimited = False
        try:
            secs = int(yarn_prop_lifetime)
            yarn_lifetime_val = f"{secs}s ({humanize_duration(secs)} max lifetime)"
        except (ValueError, TypeError):
            yarn_lifetime_val = f"{yarn_prop_lifetime}s"
    else:
        for app in running_apps:
            timeouts = app.get("timeouts", {}).get("timeout", [])
            for t in timeouts:
                if t.get("type") == "LIFETIME":
                    exp = t.get("expiryTime")
                    if exp and exp != "UNLIMITED":
                        is_yarn_unlimited = False
                        yarn_lifetime_val = str(exp)

    # 1. In Situ Probe: Probe local Workbench JupyterLab instance if running on Workbench VM
    in_situ_wb: Dict[str, Any] = (
        client.local_workbench_sessions()
        if hasattr(client, "local_workbench_sessions")
        else {"is_workbench": False}
    )

    # 2. External Probe: Query Workbench instance inventory across project (if permitted)
    wb_inventory: Dict[str, Dict[str, Any]] = {}
    wb_inventory_error = False
    if hasattr(client, "workbench_instances"):
        try:
            wb_inventory = client.workbench_instances()
        except AccessDenied:
            wb_inventory_error = True
        except Exception:
            wb_inventory = {}

    # Cache for looked-up notebook files and remote sessions to avoid duplicate queries
    looked_up_notebooks: Dict[str, Optional[str]] = {}
    probed_remote_sessions: Dict[str, Tuple[List[Dict[str, Any]], Optional[str], Optional[str]]] = {}
    claimed_remote_sessions: Dict[str, Set[str]] = {}

    # Build active kernel IDs and kernels_detail
    active_kernel_ids = {k.get("id", "") for k in kernels if k.get("id")}
    kernels_detail: List[Dict[str, Any]] = []

    for kernel in kernels:
        k_id = kernel.get("id", "?")
        state = kernel.get("execution_state", "unknown")
        if state == "busy":
            busy += 1
        last = parse_timestamp(kernel.get("last_activity", ""))
        idle_seconds = (now - last).total_seconds() if last else 0.0
        max_idle = max(max_idle, idle_seconds)
        if last and (now - last) > idle_cutoff and state != "busy":
            idle_kernels.append((k_id, idle_seconds, state))

        # Check if any running YARN app corresponds to this kernel
        assoc_app_id: Optional[str] = None
        assoc_app: Optional[Dict[str, Any]] = None
        for app in running_apps:
            app_name = app.get("name", "")
            if k_id in app_name or (
                UUID_REGEX.match(app_name) and app_name.lower() == k_id.lower()
            ):
                assoc_app_id = app.get("id")
                assoc_app = app
                break

        # Workbench and Notebook correlation
        wb_vm: Optional[str] = None
        wb_state: Optional[str] = None
        wb_owner: Optional[str] = None
        wb_notebook: Optional[str] = None
        wb_candidates: List[str] = []
        matched_wb_id: Optional[str] = None
        confidence_str: str = ""

        last_str = kernel.get("last_activity", "")

        # Step 1: Local In Situ Match (Check if kernel belongs to active Jupyter session on this local VM)
        is_local_match = False
        if in_situ_wb.get("is_workbench"):
            sess_info = (
                in_situ_wb.get("sessions_by_kernel_id", {}).get(k_id)
                or in_situ_wb.get("sessions_by_last_activity", {}).get(last_str)
            )
            if sess_info:
                is_local_match = True
                wb_vm = in_situ_wb.get("vm_name")
                wb_state = "ACTIVE (Local VM)"
                wb_owner = in_situ_wb.get("owner")
                matched_wb_id = sess_info.get("session_id")
                wb_notebook = sess_info.get("notebook_path") or sess_info.get("notebook_name")
                confidence_str = " (100% confidence - in-situ local session)"

        # Step 2: Global / External Resolution (If not matched to local VM, resolve via inventory if available)
        if not is_local_match and assoc_app and wb_inventory:
            yarn_user = assoc_app.get("user", "")
            wb_inst = wb_inventory.get(yarn_user) or wb_inventory.get(yarn_user.lower())
            if wb_inst:
                cand_details = wb_inst.get("candidate_details", [])
                primary_cand = wb_inst
                best_score = 100
                confidence_str = ""

                # If multiple candidates exist for this identity, disambiguate with Signals 1, 2, 3
                if len(cand_details) > 1:
                    scored_candidates = []
                    for cand in cand_details:
                        c_name = cand.get("name", "")
                        c_zone = cand.get("zone", "")
                        c_id = cand.get("instance_id", "")
                        c_state = cand.get("state", "ACTIVE")

                        score = 0
                        signals_triggered = []

                        # Base score: state ACTIVE > STOPPED
                        if c_state == "ACTIVE":
                            score += 40

                        # Signal 1: GCE Guest Attributes last_activity
                        if c_name and c_zone and hasattr(client, "get_instance_guest_attributes"):
                            attrs = client.get_instance_guest_attributes(c_name, c_zone)
                            vm_last = attrs.get("last_activity")
                            if vm_last:
                                vm_dt = parse_timestamp(vm_last)
                                if vm_dt:
                                    delta = abs((now - vm_dt).total_seconds())
                                    if delta < 86400:  # active in last 24h
                                        score += 30
                                        signals_triggered.append("Signal 1 Guest Attributes")

                        # Signal 2: Cloud Logging Serial Activity around kernel start
                        if c_id and hasattr(client, "get_instance_serial_activity"):
                            serial_hits = client.get_instance_serial_activity(c_id)
                            if serial_hits > 0:
                                score += 20
                                signals_triggered.append("Signal 2 Serial Trace")

                        # Signal 3: Cloud Monitoring VM Network Egress
                        if c_id and hasattr(client, "get_instance_network_egress"):
                            egress_bytes = client.get_instance_network_egress(c_id, minutes=30)
                            if egress_bytes > 5000:
                                score += 10
                                signals_triggered.append("Signal 3 Cloud Monitoring")

                        scored_candidates.append((score, signals_triggered, cand))

                    scored_candidates.sort(key=lambda x: x[0], reverse=True)
                    best_score, best_signals, best_cand = scored_candidates[0]
                    primary_cand = best_cand
                    if best_signals:
                        confidence_str = f" ({best_score}% confidence via {', '.join(best_signals)})"
                    elif best_score == 0:
                        if all(c.get("state") == "STOPPED" for _, _, c in scored_candidates):
                            confidence_str = " (0% confidence - unranked among STOPPED instances)"
                        else:
                            confidence_str = " (0% confidence - no signals active)"
                else:
                    best_score = 100
                    confidence_str = " (100% confidence - unique associated instance)"

                wb_vm = primary_cand.get("name")
                wb_state = primary_cand.get("state")
                wb_owner = primary_cand.get("creator")
                inst_id = primary_cand.get("instance_id")
                proxy_uri = primary_cand.get("proxy_uri")
                zone = primary_cand.get("zone")

                all_cands = [c.get("name") for c in cand_details if c.get("name") != wb_vm]
                if all_cands and best_score < 100:
                    wb_candidates = all_cands

                # Try Remote Notebook Probing (Non-Intr. SSH via IAP)
                remote_sessions: List[Dict[str, Any]] = []
                method_used: Optional[str] = None
                remote_probe_err: Optional[str] = None

                if wb_vm and hasattr(client, "query_remote_workbench_sessions"):
                    if wb_vm not in probed_remote_sessions:
                        probed_remote_sessions[wb_vm] = client.query_remote_workbench_sessions(
                            wb_vm, proxy_uri=proxy_uri, zone=zone, instance_id=inst_id
                        )
                    remote_sessions, method_used, remote_probe_err = probed_remote_sessions[wb_vm]

                if remote_sessions:
                    vm_claimed = claimed_remote_sessions.setdefault(wb_vm or "", set())
                    # Match session by kernel ID
                    for sess in remote_sessions:
                        sk = sess.get("kernel") or {}
                        sid = sess.get("id")
                        if sk.get("id") == k_id:
                            matched_wb_id = sid
                            wb_notebook = sess.get("path") or sess.get("name")
                            if sid:
                                vm_claimed.add(sid)
                            break
                    if not wb_notebook:
                        # Fall back to first unclaimed session on this VM
                        for sess in remote_sessions:
                            sid = sess.get("id")
                            if sid and sid not in vm_claimed:
                                matched_wb_id = sid
                                wb_notebook = sess.get("path") or sess.get("name")
                                vm_claimed.add(sid)
                                break

                # If remote probing didn't resolve the notebook file, fall back to Cloud Logging Serial Trace
                if not wb_notebook and inst_id:
                    if inst_id not in looked_up_notebooks:
                        looked_up_notebooks[inst_id] = (
                            client.lookup_workbench_notebook_file(inst_id, wb_vm)
                            if hasattr(client, "lookup_workbench_notebook_file")
                            else None
                        )
                    wb_notebook = looked_up_notebooks[inst_id]

        # Resolve display and explanation strings for explicit rendering policy
        if wb_vm:
            state_str = f" ({wb_state or 'ACTIVE'})"
            cand_str = f" [Alternative: {', '.join(wb_candidates)}]" if wb_candidates else ""
            wb_vm_display = f"{wb_vm}{state_str}{confidence_str}{cand_str}"
            wb_vm_explanation = None
        else:
            if in_situ_wb.get("is_workbench"):
                yarn_u = assoc_app.get("user") if assoc_app else None
                if yarn_u:
                    wb_vm_display = f"[External to this VM (YARN user: {yarn_u})]"
                    wb_vm_explanation = f"Kernel belongs to external tenant '{yarn_u}' (no active session on {in_situ_wb.get('vm_name')})"
                else:
                    wb_vm_display = "[External to this VM]"
                    wb_vm_explanation = f"Kernel is not associated with any local session on {in_situ_wb.get('vm_name')}"
            else:
                if not assoc_app:
                    wb_vm_display = "[Unresolved] (No associated YARN application or tenant identity to correlate)"
                    wb_vm_explanation = "No associated YARN application or tenant identity to correlate"
                elif wb_inventory_error:
                    wb_vm_display = "[Unresolved] (Missing notebooks.instances.list permission)"
                    wb_vm_explanation = "Missing notebooks.instances.list permission"
                else:
                    yarn_u = assoc_app.get("user", "")
                    wb_vm_display = f"[Unresolved] (No matching Workbench VM found for identity '{yarn_u}')"
                    wb_vm_explanation = f"No matching Workbench VM found for identity '{yarn_u}'"

        if wb_owner:
            wb_owner_display = wb_owner
            wb_owner_explanation = None
        elif in_situ_wb.get("is_workbench") and assoc_app and assoc_app.get("user"):
            wb_owner_display = assoc_app.get("user")
            wb_owner_explanation = "Extracted from YARN application owner"
        elif not wb_vm:
            wb_owner_display = "[Unresolved] (Cannot determine owner because Workbench VM could not be identified)"
            wb_owner_explanation = "Cannot determine owner because Workbench VM could not be identified"
        else:
            wb_owner_display = "[Unresolved] (Workbench instance metadata unavailable)"
            wb_owner_explanation = "Workbench instance metadata unavailable"

        if wb_notebook:
            wb_notebook_display = wb_notebook
            wb_notebook_explanation = None
        elif in_situ_wb.get("is_workbench") and not is_local_match:
            wb_notebook_display = "[External / Headless Session]"
            wb_notebook_explanation = f"No active notebook session mapped on local instance {in_situ_wb.get('vm_name')}"
        else:
            wb_notebook_display = "[Unresolved] (No active /lab/tree/ referer found in recent GCE serial console logs)"
            wb_notebook_explanation = "No active /lab/tree/ referer found in recent GCE serial console logs"

        if matched_wb_id:
            wb_ui_id_display = f"{matched_wb_id[:8]} ({matched_wb_id})"
            wb_ui_id_explanation = None
        elif in_situ_wb.get("is_workbench") and not is_local_match:
            wb_ui_id_display = "[External to this VM]"
            wb_ui_id_explanation = "Local sidebar session UUID is only accessible within the originating VM"
        else:
            wb_ui_id_display = "[Unresolved] (Local sidebar session UUID; requires in-situ execution or Non-Intr. SSH)"
            wb_ui_id_explanation = "Local sidebar session UUID; requires in-situ execution or Non-Intr. SSH"

        kernels_detail.append(
            {
                "id": k_id,
                "name": kernel.get("name", "unknown"),
                "execution_state": state,
                "last_activity": kernel.get("last_activity", ""),
                "idle_seconds": idle_seconds,
                "connections": int(kernel.get("connections", 0) or 0),
                "associated_yarn_app": assoc_app_id,
                "workbench_vm": wb_vm,
                "workbench_vm_explanation": wb_vm_explanation,
                "workbench_state": wb_state,
                "workbench_owner": wb_owner,
                "workbench_owner_explanation": wb_owner_explanation,
                "notebook_file": wb_notebook,
                "notebook_file_explanation": wb_notebook_explanation,
                "workbench_candidates": wb_candidates,
                "workbench_ui_id": matched_wb_id,
                "workbench_ui_id_explanation": wb_ui_id_explanation,
                "workbench_vm_display": wb_vm_display,
                "workbench_owner_display": wb_owner_display,
                "workbench_notebook_display": wb_notebook_display,
                "workbench_ui_id_display": wb_ui_id_display,
            }
        )

    # Build YARN applications detail and classify orphans
    yarn_apps_detail: List[Dict[str, Any]] = []
    long_running: List[Tuple[str, float]] = []
    orphaned_count = 0

    for app in running_apps:
        app_id = app.get("id", "?")
        app_name = app.get("name", "")
        elapsed = float(app.get("elapsedTime", 0)) / 1000.0
        if elapsed > app_age_hours * 3600:
            long_running.append((app_id, elapsed))

        started_ms = app.get("startedTime")
        started_str = format_epoch_ms(started_ms)
        allocated_mb = int(app.get("allocatedMB", 0) or 0)
        vcores = int(app.get("allocatedVCores", 0) or 0)
        containers = int(app.get("runningContainers", 0) or 0)
        host_addr = (
            app.get("amHostHttpAddress", "")
            or app.get("host", "")
            or app.get("nodeHttpAddress", "")
        )
        if app.get("rpcPort") and ":" not in host_addr:
            host_addr = f"{host_addr}:{app.get('rpcPort')}"
        host_short = shorten_node_address(host_addr)

        # Classification
        matched_kernel_id: Optional[str] = None
        for k_id in active_kernel_ids:
            if k_id in app_name or (
                UUID_REGEX.match(app_name) and app_name.lower() == k_id.lower()
            ):
                matched_kernel_id = k_id
                break

        if matched_kernel_id:
            app_type = "Active Gateway Session"
            is_orphaned = False
        elif UUID_REGEX.match(app_name):
            app_type = "ORPHANED YARN APP"
            is_orphaned = True
            orphaned_count += 1
        else:
            app_type = "Standalone YARN App"
            is_orphaned = False

        yarn_apps_detail.append(
            {
                "id": app_id,
                "name": app_name,
                "user": app.get("user", "unknown"),
                "state": app.get("state", "RUNNING"),
                "started_time": started_str,
                "elapsed_seconds": elapsed,
                "allocated_mb": allocated_mb,
                "allocated_vcores": vcores,
                "running_containers": containers,
                "host": host_addr,
                "host_short": host_short,
                "app_type": app_type,
                "is_orphaned": is_orphaned,
                "associated_kernel_id": matched_kernel_id,
            }
        )

    # Populate result details
    result.add("Active kernels", len(kernels))
    result.add("Busy (executing)", busy)
    result.add(f"Idle > {idle_hours:g}h", len(idle_kernels))
    result.add("Longest idle", humanize_duration(max_idle) if kernels else "n/a")
    if apps_error:
        result.add("Running YARN applications", f"unavailable ({apps_error[:60]})")
    else:
        result.add(
            "Running YARN applications",
            f"{len(running_apps)} (older than {app_age_hours:g}h: {len(long_running)})",
        )

    # Configuration Status
    result.add("--- Configuration Status ---", "")
    result.add("YARN Application Lifetime", yarn_lifetime_val)

    # Active Kernel Gateway Sessions
    if kernels_detail:
        result.add("--- Active Kernel Gateway Sessions ---", "")
        for kd in kernels_detail:
            k_id_short = kd["id"][:8] if len(kd["id"]) > 8 else kd["id"]
            result.add(f"[Kernel] {k_id_short}...", kd["name"])
            result.add("      * Workbench VM", kd["workbench_vm_display"])
            result.add("        - Workbench Owner", kd["workbench_owner_display"])
            result.add("        - Notebook File", kd["workbench_notebook_display"])
            result.add("        - Workbench UI ID", kd["workbench_ui_id_display"])
            result.add(
                "      * State",
                f"{kd['execution_state']} (idle for {humanize_duration(kd['idle_seconds'])})",
            )
            result.add(
                "      * Active Connections",
                f"{kd['connections']} connected WebSocket client(s)",
            )
            assoc = kd.get("associated_yarn_app") or "None (launching or non-YARN)"
            result.add("      * Associated YARN App", assoc)

    # Running YARN Applications
    if yarn_apps_detail:
        result.add("--- Running YARN Applications ---", "")
        for ad in yarn_apps_detail:
            result.add(f"[{ad['app_type']}] {ad['id']}", "")
            result.add("      * Name", ad["name"])
            result.add("      * User", ad["user"])
            result.add(
                "      * Started",
                f"{ad['started_time']} ({humanize_duration(ad['elapsed_seconds'])} ago)",
            )
            result.add(
                "      * Allocation",
                f"{humanize_mb(ad['allocated_mb'])}, {ad['allocated_vcores']} vCores, {ad['running_containers']} container(s)",
            )
            result.add("      * Host Node", ad["host_short"])
            if ad["is_orphaned"]:
                result.add(
                    "      * Status", "No active gateway session; driver still alive"
                )

    result.metrics = {
        "active_kernels": len(kernels),
        "busy_kernels": busy,
        "idle_kernels": len(idle_kernels),
        "idle_threshold_hours": idle_hours,
        "max_idle_seconds": max_idle,
        "running_yarn_apps": len(running_apps),
        "long_running_apps": len(long_running),
        "orphaned_yarn_apps": orphaned_count,
        "app_age_threshold_hours": app_age_hours,
        "yarn_lifetime_config": {
            "expiry_time": yarn_lifetime_val,
            "is_unlimited": is_yarn_unlimited,
        },
        "kernels_detail": kernels_detail,
        "yarn_apps_detail": yarn_apps_detail,
    }

    remediation = []
    if is_yarn_unlimited:
        remediation.append(
            "Configure YARN Application Lifetime Reaper "
            "(yarn:yarn.resourcemanager.app-lifetime-monitor.enable=true, "
            "yarn:yarn.resourcemanager.app.max-lifetime=86400) to automatically terminate abandoned sessions."
        )
    if orphaned_count > 0:
        remediation.append(
            f"Kill {orphaned_count} orphaned YARN application(s): "
            "yarn application -kill <APP_ID> or via YARN ResourceManager Web UI."
        )
    remediation.append(
        "Shut down abandoned kernels via JupyterLab: 'Running Terminals and Kernels' tab in the left sidebar, or Kernel > Shut Down All Kernels."
    )

    if idle_kernels or long_running or orphaned_count > 0:
        result.status = Status.FAIL
        orphan_phrase = (
            f" (including {orphaned_count} orphaned app{'s' if orphaned_count > 1 else ''})"
            if orphaned_count
            else ""
        )
        result.summary = (
            f"{len(idle_kernels)} idle kernel(s) and {len(long_running)} long-running "
            f"YARN application(s){orphan_phrase} are holding ApplicationMaster capacity."
        )
        result.remediation = remediation
    elif not kernels:
        result.status = Status.PASS
        result.summary = "No active kernel sessions on the gateway."
    else:
        result.status = Status.PASS
        result.summary = f"{len(kernels)} active kernel(s), none idle beyond threshold."

    return result


# ----------------------------------------------------------------------
# Check 2 - YARN ApplicationMaster capacity  (primary hypothesis)
# ----------------------------------------------------------------------
def check_am_capacity(client: GatewayDiagnosticClient) -> CheckResult:
    name = "YARN ApplicationMaster Capacity"
    result = CheckResult(check_id=2, name=name)

    try:
        scheduler = client.yarn_scheduler()
        metrics = client.yarn_metrics()
    except (AccessDenied, NotFound, DiagnosticError) as exc:
        return _skip_result(2, name, exc)

    scheduler_type = scheduler.get("type", "unknown")
    if scheduler_type != "capacityScheduler":
        result.status = Status.SKIPPED
        result.summary = (
            f"Scheduler type '{scheduler_type}' is not the Capacity Scheduler; "
            "AM resource limits do not apply in the same way."
        )
        result.add("Scheduler type", scheduler_type)
        return result

    leaves = iter_leaf_queues(scheduler)
    if not leaves:
        result.status = Status.ERROR
        result.summary = "No leaf queues found in the scheduler response."
        return result

    available_mb = int(metrics.get("availableMB", 0) or 0)
    allocated_mb = int(metrics.get("allocatedMB", 0) or 0)
    total_mb = int(metrics.get("totalMB", 0) or 0)
    apps_pending = int(metrics.get("appsPending", 0) or 0)

    worst: Optional[Dict[str, Any]] = None
    queue_rows: List[Dict[str, Any]] = []

    for queue in leaves:
        used_am = _resource_mb(queue.get("usedAMResource")) or 0
        limit_am = _resource_mb(queue.get("AMResourceLimit")) or 0
        saturation = (used_am / limit_am) if limit_am else 0.0
        row = {
            "queue": queue.get("queueName", "?"),
            "used_am_mb": used_am,
            "am_limit_mb": limit_am,
            "saturation": saturation,
            "max_am_percent": queue.get("configuredMaxAMResourceLimit"),
            "num_pending": int(queue.get("numPendingApplications", 0) or 0),
            "num_active": int(queue.get("numActiveApplications", 0) or 0),
            "max_applications": queue.get("maxApplications"),
            "max_applications_per_user": queue.get("maxApplicationsPerUser"),
            "user_limit_factor": queue.get("userLimitFactor"),
            "user_am_limit_mb": _resource_mb(queue.get("userAMResourceLimit")),
            "raw_queue": queue,
        }
        queue_rows.append(row)
        if worst is None or (row["saturation"], row["num_pending"]) > (
            worst["saturation"],
            worst["num_pending"],
        ):
            worst = row

    assert worst is not None
    saturation = worst["saturation"]
    pending = worst["num_pending"]

    # Parse active users in the queue
    users_data = (worst.get("raw_queue", {}) or {}).get("users", {}).get("user", [])
    if isinstance(users_data, dict):
        users_data = [users_data]
    active_users: List[Dict[str, Any]] = []
    for u in users_data:
        num_act = int(u.get("numActiveApplications", 0) or 0)
        num_pend = int(u.get("numPendingApplications", 0) or 0)
        if num_act > 0 or num_pend > 0:
            active_users.append(
                {
                    "username": u.get("username", "?"),
                    "active_apps": num_act,
                    "pending_apps": num_pend,
                    "am_used_mb": _resource_mb(u.get("AMResourceUsed")) or 0,
                }
            )

    # The decisive signature: applications are queued while the cluster still
    # has free memory, which means admission control -- not genuine capacity --
    # is refusing them.
    starved_with_free_memory = (
        pending > 0 or apps_pending > 0
    ) and available_mb > FREE_MEMORY_SIGNIFICANT_MB

    result.add("Scheduler", scheduler_type)
    result.add("Queue examined", worst["queue"])
    max_am_pct = worst["max_am_percent"]
    result.add(
        "maximum-am-resource-percent",
        f"{max_am_pct} (recommended >= 0.8)" if max_am_pct is not None else "unknown",
    )
    result.add(
        "AM memory used / limit",
        f"{humanize_mb(worst['used_am_mb'])} / {humanize_mb(worst['am_limit_mb'])}"
        f"  ({saturation * 100:.1f}%)",
    )
    result.add("Applications ACTIVE", worst["num_active"])
    result.add("Applications PENDING (ACCEPTED)", pending)
    if active_users:
        user_summaries = [
            f"{u['username']} ({u['active_apps']} app(s), AM: {humanize_mb(u['am_used_mb'])})"
            for u in active_users
        ]
        result.add("Active queue user(s)", ", ".join(user_summaries))
    result.add(
        "Cluster memory",
        f"{humanize_mb(allocated_mb)} used / {humanize_mb(total_mb)} total"
        f"  ({humanize_mb(available_mb)} free)",
    )
    result.add(
        "maxApplications / per user",
        f"{worst['max_applications']} / {worst['max_applications_per_user']}",
    )
    result.add("userLimitFactor", worst["user_limit_factor"])

    result.metrics = {
        "scheduler_type": scheduler_type,
        "queues": [
            {k: v for k, v in q.items() if k != "raw_queue"} for q in queue_rows
        ],
        "worst_queue": worst["queue"],
        "am_saturation": saturation,
        "cluster_available_mb": available_mb,
        "cluster_allocated_mb": allocated_mb,
        "cluster_total_mb": total_mb,
        "cluster_apps_pending": apps_pending,
        "starved_with_free_memory": starved_with_free_memory,
        "active_users": active_users,
    }

    remediation = [
        "Recreate or reconfigure the cluster with "
        "--properties='capacity-scheduler:yarn.scheduler.capacity."
        "maximum-am-resource-percent=0.8'",
        "Release AM capacity now by shutting down idle kernels (see Check 1).",
    ]

    if starved_with_free_memory and saturation >= AM_SATURATION_FAIL:
        result.status = Status.FAIL
        result.summary = (
            "AM STARVATION CONFIRMED: applications are queued in ACCEPTED while "
            f"{humanize_mb(available_mb)} of cluster memory is still free. The "
            f"ApplicationMaster budget is {saturation * 100:.1f}% consumed."
        )
        result.remediation = remediation
    elif saturation >= AM_SATURATION_FAIL:
        result.status = Status.FAIL
        result.summary = (
            f"ApplicationMaster budget is {saturation * 100:.1f}% consumed; new "
            "kernels will be refused admission."
        )
        result.remediation = remediation
    elif starved_with_free_memory:
        result.status = Status.FAIL
        result.summary = (
            f"{max(pending, apps_pending)} application(s) are stuck in ACCEPTED while "
            f"{humanize_mb(available_mb)} is free -- an admission limit "
            "(AM percent, maxApplications or userLimitFactor) is blocking them."
        )
        result.remediation = remediation
    elif saturation >= AM_SATURATION_WARN:
        result.status = Status.WARN
        result.summary = (
            f"ApplicationMaster budget is {saturation * 100:.1f}% consumed; "
            "approaching the admission limit."
        )
        result.remediation = remediation
    else:
        result.status = Status.PASS
        result.summary = (
            f"ApplicationMaster budget {saturation * 100:.1f}% consumed, "
            f"{pending} application(s) pending. No starvation detected."
        )

    return result


# ----------------------------------------------------------------------
# Check 3 - Kernel Gateway launch timeouts
# ----------------------------------------------------------------------
def check_launch_timeouts(
    client: GatewayDiagnosticClient,
    lookback_days: int = DEFAULT_TIMEOUT_LOOKBACK_DAYS,
) -> CheckResult:
    name = "Kernel Gateway Launch Timeouts"
    result = CheckResult(check_id=3, name=name)

    since = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    log_filter = (
        'resource.type="cloud_dataproc_cluster"\n'
        f'resource.labels.cluster_name="{client.cluster_name}"\n'
        f'log_name="projects/{client.project_id}/logs/jupyter_kernel_gateway"\n'
        '"launch timeout"\n'
        f'timestamp>="{since.strftime("%Y-%m-%dT%H:%M:%SZ")}"'
    )

    try:
        entries = client.log_entries(log_filter)
    except (AccessDenied, NotFound, DiagnosticError) as exc:
        skipped = _skip_result(3, name, exc)
        if isinstance(exc, AccessDenied):
            skipped.remediation = [
                "Grant roles/logging.viewer to read Kernel Gateway logs, or inspect "
                "them manually in Cloud Logging with log_name="
                f'"projects/{client.project_id}/logs/jupyter_kernel_gateway".'
            ]
        return skipped

    timeout_values: List[int] = []
    kernel_ids: List[str] = []
    latest: Optional[str] = None
    matched = 0

    for entry in entries:
        text = entry.get("textPayload") or ""
        if not text:
            payload = entry.get("jsonPayload") or {}
            text = str(payload.get("message", "")) or str(payload)
        if "launch timeout" not in text:
            continue
        matched += 1
        if latest is None:
            latest = entry.get("timestamp", "")
        found = re.search(r"launch timeout:\s*(\d+)", text)
        if found:
            timeout_values.append(int(found.group(1)))
        kernel = re.search(r"KernelID:\s*'([^']+)'", text)
        if kernel:
            kernel_ids.append(kernel.group(1))

    effective_timeout = max(set(timeout_values), key=timeout_values.count) if timeout_values else None
    unique_kernels = sorted(set(kernel_ids))

    result.add("Lookback window", f"{lookback_days} day(s)")
    result.add("Timeout events found", matched)
    result.add(
        "Effective launch timeout",
        f"{effective_timeout}s" if effective_timeout else "not observed (default 120s)",
    )
    result.add("Distinct kernels affected", len(unique_kernels))
    if latest:
        result.add("Most recent event", latest)

    result.metrics = {
        "lookback_days": lookback_days,
        "timeout_events": matched,
        "effective_timeout_seconds": effective_timeout,
        "affected_kernel_ids": unique_kernels[:20],
        "latest_event": latest,
    }

    if matched:
        result.status = Status.FAIL
        result.summary = (
            f"{matched} kernel launch timeout(s) in the last {lookback_days} day(s). "
            "Each one surfaces to the user as HTTP 500 'Error Starting Kernel'."
        )
        result.remediation = [
            "Raise the provisioner timeout to 600s "
            "(GatewayProvisionerBase.default_kernel_launch_timeout=600, "
            "or export KERNEL_LAUNCH_TIMEOUT=600).",
            "Fix the underlying admission limit first -- a longer timeout only "
            "masks the queueing reported by Check 2.",
        ]
    else:
        result.status = Status.PASS
        result.summary = f"No launch timeouts logged in the last {lookback_days} day(s)."

    return result


# ----------------------------------------------------------------------
# Check 4 - Spark driver / AM sizing
# ----------------------------------------------------------------------
def check_driver_sizing(
    client: GatewayDiagnosticClient,
    expected_users: int = DEFAULT_EXPECTED_USERS,
) -> CheckResult:
    name = "Spark Driver / AM Sizing"
    result = CheckResult(check_id=4, name=name)

    try:
        props = client.cluster_properties()
    except (AccessDenied, NotFound, DiagnosticError) as exc:
        return _skip_result(4, name, exc)

    driver_mb = parse_memory_mb(props.get("spark:spark.driver.memory"), 1024) or 1024
    am_mb = parse_memory_mb(props.get("spark:spark.yarn.am.memory"))
    overhead_prop = parse_memory_mb(props.get("spark:spark.driver.memoryOverhead"))
    overhead_mb = overhead_prop if overhead_prop else max(384, int(driver_mb * 0.1))
    min_alloc = parse_memory_mb(
        props.get("yarn:yarn.scheduler.minimum-allocation-mb"), 1024
    ) or 1024

    # Kernel Gateway launches kernels in cluster mode, so the driver *is* the
    # ApplicationMaster; its container is rounded up to the YARN minimum
    # allocation granularity.
    raw_footprint = driver_mb + overhead_mb
    footprint_mb = ((raw_footprint + min_alloc - 1) // min_alloc) * min_alloc

    am_limit_mb: Optional[int] = None
    try:
        scheduler = client.yarn_scheduler()
        leaves = iter_leaf_queues(scheduler)
        limits = [
            _resource_mb(q.get("AMResourceLimit"))
            for q in leaves
            if _resource_mb(q.get("AMResourceLimit"))
        ]
        if limits:
            am_limit_mb = max(limits)
    except (AccessDenied, NotFound, DiagnosticError):
        am_limit_mb = None

    max_kernels = (am_limit_mb // footprint_mb) if (am_limit_mb and footprint_mb) else None

    result.add("spark.driver.memory", props.get("spark:spark.driver.memory", "default"))
    result.add(
        "spark.driver.memoryOverhead",
        f"{overhead_mb} MB" + ("" if overhead_prop else " (derived default)"),
    )
    if am_mb:
        result.add("spark.yarn.am.memory", props.get("spark:spark.yarn.am.memory"))
    result.add("yarn minimum-allocation-mb", min_alloc)
    result.add("AM footprint per kernel", humanize_mb(footprint_mb))
    result.add("Queue AM budget", humanize_mb(am_limit_mb))
    result.add(
        "Max concurrent kernels",
        max_kernels if max_kernels is not None else "unknown (scheduler unavailable)",
    )
    result.add("Expected concurrent users", expected_users)

    result.metrics = {
        "driver_memory_mb": driver_mb,
        "overhead_mb": overhead_mb,
        "min_allocation_mb": min_alloc,
        "am_footprint_mb": footprint_mb,
        "am_limit_mb": am_limit_mb,
        "max_concurrent_kernels": max_kernels,
        "expected_users": expected_users,
    }

    if max_kernels is None:
        result.status = Status.SKIPPED
        result.summary = (
            "Could not read the queue AM budget, so the concurrency ceiling is unknown."
        )
    elif max_kernels < expected_users:
        result.status = Status.WARN
        result.summary = (
            f"This cluster fits about {max_kernels} concurrent kernel(s); kernel number "
            f"{max_kernels + 1} will queue in ACCEPTED and then fail with HTTP 500. "
            f"Expected concurrency is {expected_users}."
        )
        result.remediation = [
            f"Reduce per-kernel footprint (spark.driver.memory is {driver_mb} MB), or",
            "raise maximum-am-resource-percent, or add worker nodes.",
        ]
    else:
        result.status = Status.PASS
        result.summary = (
            f"Headroom for about {max_kernels} concurrent kernel(s), at or above the "
            f"expected {expected_users}."
        )

    return result


ALL_CHECKS = {
    1: "kernels",
    2: "yarn",
    3: "timeouts",
    4: "sizing",
}


def run_checks(
    client: GatewayDiagnosticClient,
    selected: Optional[List[int]] = None,
    idle_hours: float = DEFAULT_IDLE_HOURS,
    app_age_hours: float = DEFAULT_APP_AGE_HOURS,
    lookback_days: int = DEFAULT_TIMEOUT_LOOKBACK_DAYS,
    expected_users: int = DEFAULT_EXPECTED_USERS,
) -> List[CheckResult]:
    """Run the requested checks, isolating failures so one bad check cannot
    abort the rest of the run."""
    wanted = set(selected or ALL_CHECKS.keys())
    runners = {
        1: lambda: check_kernel_sessions(client, idle_hours, app_age_hours),
        2: lambda: check_am_capacity(client),
        3: lambda: check_launch_timeouts(client, lookback_days),
        4: lambda: check_driver_sizing(client, expected_users),
    }
    names = {
        1: "Zombie / Idle Kernel Sessions",
        2: "YARN ApplicationMaster Capacity",
        3: "Kernel Gateway Launch Timeouts",
        4: "Spark Driver / AM Sizing",
    }

    results: List[CheckResult] = []
    for check_id in sorted(wanted):
        runner = runners.get(check_id)
        if not runner:
            continue
        try:
            results.append(runner())
        except Exception as exc:  # noqa: BLE001 - one check must not kill the run
            results.append(_skip_result(check_id, names[check_id], exc))
    return results
