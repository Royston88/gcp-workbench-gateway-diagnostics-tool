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

"""Authentication and read-only HTTP access to Dataproc, YARN and Cloud Logging.

Design constraints
------------------
* HTTP is performed with the standard library (``urllib.request``) so that the
  package installs cleanly with ``pip install --no-deps``.
* Credentials are resolved through a fallback chain so the tool works in a
  notebook kernel, a JupyterLab terminal, Cloud Shell or a plain VM.
* Every method is read-only. Nothing here mutates cluster or job state.
"""

import datetime
import json
import logging
import os
import platform
import re
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
DATAPROC_API = "https://dataproc.googleapis.com/v1"
LOGGING_API = "https://logging.googleapis.com/v2"
COMPUTE_API = "https://compute.googleapis.com/compute/v1"
MONITORING_API = "https://monitoring.googleapis.com/v3"
NOTEBOOKS_API = "https://notebooks.googleapis.com/v2"

logger = logging.getLogger("dataproc_gateway_diagnostics")


class DiagnosticError(RuntimeError):
    """Fatal error that prevents the diagnostic run from starting."""


class AccessDenied(DiagnosticError):
    """An endpoint was unreachable due to permissions (401/403).

    Subclasses DiagnosticError so that an escape at top level is still reported
    as a clean message rather than a traceback, while individual checks can
    catch it specifically and degrade to SKIPPED.
    """

    def __init__(self, message: str, required_roles: Optional[List[str]] = None):
        super().__init__(message)
        self.required_roles = required_roles or []


class NotFound(DiagnosticError):
    """An endpoint returned 404."""


def _run(cmd: List[str]) -> str:
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if out.returncode == 0:
            return out.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    return ""


def resolve_access_token() -> str:
    """Resolve an OAuth2 access token.

    Order of preference:
      1. ``GOOGLE_OAUTH_ACCESS_TOKEN`` environment variable (explicit override).
      2. Application Default Credentials via ``google.auth``.
      3. ``gcloud auth print-access-token``.
    """
    token = os.environ.get("GOOGLE_OAUTH_ACCESS_TOKEN", "").strip()
    if token:
        logger.info("Using token from GOOGLE_OAUTH_ACCESS_TOKEN.")
        return token

    try:
        import google.auth
        from google.auth.transport.requests import Request as AuthRequest

        creds, _ = google.auth.default(scopes=[CLOUD_PLATFORM_SCOPE])
        if not creds.valid:
            creds.refresh(AuthRequest())
        if creds.token:
            logger.info("Using Application Default Credentials.")
            return creds.token
    except Exception as exc:  # noqa: BLE001 - deliberate broad fallback
        logger.info("ADC unavailable (%s); falling back to gcloud.", exc)

    token = _run(["gcloud", "auth", "application-default", "print-access-token"])
    if token:
        logger.info("Using token from gcloud auth application-default.")
        return token

    token = _run(["gcloud", "auth", "print-access-token"])
    if token:
        logger.info("Using token from gcloud.")
        return token

    raise DiagnosticError(
        "Could not obtain Google Cloud credentials.\n"
        "Try one of the following:\n"
        "  gcloud auth application-default login\n"
        "  gcloud auth login\n"
        "  export GOOGLE_OAUTH_ACCESS_TOKEN=$(gcloud auth print-access-token)"
    )


def resolve_project_id() -> str:
    """Best-effort discovery of the active project id."""
    for env in ("GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "CLOUDSDK_CORE_PROJECT"):
        value = os.environ.get(env, "").strip()
        if value:
            return value

    try:
        import google.auth

        _, project = google.auth.default(scopes=[CLOUD_PLATFORM_SCOPE])
        if project:
            return project
    except Exception:  # noqa: BLE001
        pass

    value = _run(["gcloud", "config", "get-value", "project"])
    if value and value != "(unset)":
        return value

    # Workbench / GCE metadata server.
    try:
        req = urllib.request.Request(
            "http://metadata.google.internal/computeMetadata/v1/project/project-id",
            headers={"Metadata-Flavor": "Google"},
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            return resp.read().decode("utf-8").strip()
    except Exception:  # noqa: BLE001
        pass

    return ""


def resolve_region() -> str:
    """Best-effort discovery of the Dataproc region."""
    for env in ("CLOUDSDK_DATAPROC_REGION", "DATAPROC_REGION", "CLOUDSDK_COMPUTE_REGION"):
        value = os.environ.get(env, "").strip()
        if value:
            return value
    for prop in ("dataproc/region", "compute/region"):
        value = _run(["gcloud", "config", "get-value", prop])
        if value and value != "(unset)":
            return value
    return ""


def resolve_active_account() -> str:
    """Identity the diagnostic is running as (informational only)."""
    value = _run(["gcloud", "config", "get-value", "account"])
    if value and value != "(unset)":
        return value
    try:
        req = urllib.request.Request(
            "http://metadata.google.internal/computeMetadata/v1/"
            "instance/service-accounts/default/email",
            headers={"Metadata-Flavor": "Google"},
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            return resp.read().decode("utf-8").strip()
    except Exception:  # noqa: BLE001
        return ""


class GatewayDiagnosticClient:
    """Read-only client for Dataproc, YARN ResourceManager and Cloud Logging."""

    def __init__(
        self,
        cluster_name: str,
        project_id: Optional[str] = None,
        region: Optional[str] = None,
        timeout: int = 30,
        verbose: bool = False,
    ):
        if verbose:
            logging.basicConfig(level=logging.INFO)

        self.cluster_name = cluster_name
        self.project_id = project_id or resolve_project_id()
        self.region = region or resolve_region()
        self.timeout = timeout
        self.active_account = resolve_active_account()

        if not self.cluster_name:
            raise DiagnosticError("--cluster is required.")
        if not self.project_id:
            raise DiagnosticError(
                "Could not determine the project id. Pass --project explicitly."
            )
        if not self.region:
            raise DiagnosticError(
                "Could not determine the Dataproc region. Pass --region explicitly "
                "(for example --region=us-central1)."
            )

        self._token = resolve_access_token()
        self._cluster: Optional[Dict[str, Any]] = None
        self._endpoints: Optional[Dict[str, str]] = None

    # ------------------------------------------------------------------
    # Low-level HTTP
    # ------------------------------------------------------------------
    @staticmethod
    def _describe_http_error(exc: urllib.error.HTTPError) -> str:
        """Extract the human-readable message from a Google API error body."""
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            return f"HTTP {exc.code}"
        try:
            parsed = json.loads(body)
            message = parsed.get("error", {}).get("message")
            if message:
                return message
        except Exception:  # noqa: BLE001
            pass
        return body.strip()[:300] or f"HTTP {exc.code}"

    def _request(self, url: str, payload: Optional[Dict[str, Any]] = None) -> Any:
        if payload is None:
            req = urllib.request.Request(
                url, headers={"Authorization": f"Bearer {self._token}"}
            )
            logger.info("GET %s", url)
        else:
            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Authorization": f"Bearer {self._token}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            logger.info("POST %s", url)

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = self._describe_http_error(exc)
            if exc.code in (401, 403):
                roles = ["roles/dataproc.viewer"]
                if "dataproc.googleusercontent.com" in url:
                    roles = ["roles/dataproc.viewer", "dataproc.clusters.use"]
                elif "logging.googleapis.com" in url:
                    roles = ["roles/logging.viewer"]
                elif "compute.googleapis.com" in url:
                    roles = ["roles/compute.viewer"]
                elif "monitoring.googleapis.com" in url:
                    roles = ["roles/monitoring.viewer"]
                elif "notebooks.googleapis.com" in url:
                    roles = ["roles/notebooks.viewer"]
                raise AccessDenied(detail, required_roles=roles) from exc
            if exc.code == 404:
                raise NotFound(detail) from exc
            raise DiagnosticError(f"HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise DiagnosticError(f"Network error for {url}: {exc.reason}") from exc

    def _get_json(self, url: str) -> Any:
        return self._request(url)

    def _post_json(self, url: str, payload: Dict[str, Any]) -> Any:
        return self._request(url, payload)

    # ------------------------------------------------------------------
    # Dataproc
    # ------------------------------------------------------------------
    def get_cluster(self) -> Dict[str, Any]:
        """Fetch and cache the cluster resource."""
        if self._cluster is None:
            url = (
                f"{DATAPROC_API}/projects/{self.project_id}"
                f"/regions/{self.region}/clusters/{urllib.parse.quote(self.cluster_name)}"
            )
            try:
                self._cluster = self._get_json(url)
            except NotFound as exc:
                raise DiagnosticError(
                    f"Cluster '{self.cluster_name}' was not found in project "
                    f"'{self.project_id}' region '{self.region}'.\n"
                    "Verify the name and region:\n"
                    f"  gcloud dataproc clusters list --region={self.region}"
                ) from exc
        return self._cluster

    def cluster_properties(self) -> Dict[str, str]:
        return (
            self.get_cluster()
            .get("config", {})
            .get("softwareConfig", {})
            .get("properties", {})
        )

    def endpoints(self) -> Dict[str, str]:
        """Component Gateway HTTP endpoints keyed by component name."""
        if self._endpoints is None:
            self._endpoints = (
                self.get_cluster()
                .get("config", {})
                .get("endpointConfig", {})
                .get("httpPorts", {})
            )
        return self._endpoints

    def _endpoint(self, *name_fragments: str) -> str:
        """Find an endpoint URL whose key contains all given fragments."""
        for key, url in self.endpoints().items():
            lowered = key.lower()
            if all(frag.lower() in lowered for frag in name_fragments):
                return url.rstrip("/")
        return ""

    # ------------------------------------------------------------------
    # YARN ResourceManager (via Component Gateway)
    # ------------------------------------------------------------------
    def yarn(self, path: str) -> Any:
        base = self._endpoint("yarn")
        if not base:
            raise NotFound(
                "The YARN ResourceManager endpoint is not exposed on this cluster. "
                "Component Gateway must be enabled (--enable-component-gateway)."
            )
        return self._get_json(f"{base}/{path.lstrip('/')}")

    def yarn_metrics(self) -> Dict[str, Any]:
        return self.yarn("ws/v1/cluster/metrics").get("clusterMetrics", {})

    def yarn_scheduler(self) -> Dict[str, Any]:
        return self.yarn("ws/v1/cluster/scheduler").get("scheduler", {}).get(
            "schedulerInfo", {}
        )

    def yarn_apps(self, states: str = "RUNNING,ACCEPTED") -> List[Dict[str, Any]]:
        data = self.yarn(f"ws/v1/cluster/apps?states={urllib.parse.quote(states)}")
        apps = (data or {}).get("apps")
        # YARN returns {"apps": null} when nothing matches.
        if not apps:
            return []
        raw_apps = apps.get("app", []) or []
        if states:
            allowed = {s.strip().upper() for s in states.split(",") if s.strip()}
            return [a for a in raw_apps if a.get("state", "").upper() in allowed]
        return raw_apps

    # ------------------------------------------------------------------
    # Jupyter Kernel Gateway (via Component Gateway)
    # ------------------------------------------------------------------
    def kernel_gateway_base(self) -> str:
        return self._endpoint("jupyter", "gateway") or self._endpoint("kernel", "gateway")

    def kernels(self) -> List[Dict[str, Any]]:
        base = self.kernel_gateway_base()
        if not base:
            raise NotFound(
                "The Jupyter Kernel Gateway endpoint is not exposed on this cluster. "
                "Enable the JUPYTER_KERNEL_GATEWAY optional component."
            )
        data = self._get_json(f"{base}/api/kernels")
        return data if isinstance(data, list) else []

    def local_workbench_sessions(self, timeout: float = 2.0) -> Dict[str, Any]:
        """Probes local JupyterLab and GCE metadata if running inside a Workbench VM."""
        result: Dict[str, Any] = {
            "is_workbench": False,
            "vm_name": None,
            "owner": None,
            "sessions_by_kernel_id": {},
            "sessions_by_last_activity": {},
            "raw_kernels": [],
        }

        # 1. Probe local JupyterLab /api/sessions and /api/kernels FIRST.
        # Only if JupyterLab responds on 127.0.0.1:8080 do we consider this an in-situ environment.
        has_local_jupyter = False

        try:
            req_sess = urllib.request.Request(
                "http://127.0.0.1:8080/api/sessions",
                headers={"Accept": "application/json"},
            )
            with urllib.request.urlopen(req_sess, timeout=timeout) as resp:
                sessions_data = json.loads(resp.read().decode("utf-8"))
                if isinstance(sessions_data, list):
                    has_local_jupyter = True
                    for sess in sessions_data:
                        k = sess.get("kernel") or {}
                        k_id = k.get("id")
                        last_act = k.get("last_activity")
                        path = sess.get("path") or sess.get("name") or "unknown"
                        sess_id = sess.get("id")
                        info = {
                            "session_id": sess_id,
                            "notebook_path": path,
                            "notebook_name": sess.get("name"),
                            "kernel_id": k_id,
                            "last_activity": last_act,
                        }
                        if k_id:
                            if k_id not in result["sessions_by_kernel_id"]:
                                result["sessions_by_kernel_id"][k_id] = info
                                result["sessions_by_kernel_id"][k_id]["all_paths"] = [path]
                                result["sessions_by_kernel_id"][k_id]["all_sessions"] = [sess_id]
                            else:
                                existing = result["sessions_by_kernel_id"][k_id]
                                if path not in existing["all_paths"]:
                                    existing["all_paths"].append(path)
                                    existing["notebook_path"] = ", ".join(existing["all_paths"])
                                if sess_id not in existing["all_sessions"]:
                                    existing["all_sessions"].append(sess_id)
                        if last_act:
                            result["sessions_by_last_activity"][last_act] = info
        except Exception:
            pass

        try:
            req_k = urllib.request.Request(
                "http://127.0.0.1:8080/api/kernels",
                headers={"Accept": "application/json"},
            )
            with urllib.request.urlopen(req_k, timeout=timeout) as resp:
                kernels_data = json.loads(resp.read().decode("utf-8"))
                if isinstance(kernels_data, list):
                    has_local_jupyter = True
                    result["raw_kernels"] = kernels_data
        except Exception:
            pass

        if not has_local_jupyter:
            return result

        result["is_workbench"] = True

        # 2. If running alongside JupyterLab, read VM identity from GCE metadata
        try:
            req_name = urllib.request.Request(
                "http://metadata.google.internal/computeMetadata/v1/instance/name",
                headers={"Metadata-Flavor": "Google"},
            )
            with urllib.request.urlopen(req_name, timeout=1.0) as resp:
                result["vm_name"] = resp.read().decode("utf-8").strip()
        except Exception:
            pass

        try:
            req_user = urllib.request.Request(
                "http://metadata.google.internal/computeMetadata/v1/instance/attributes/proxy-user-mail",
                headers={"Metadata-Flavor": "Google"},
            )
            with urllib.request.urlopen(req_user, timeout=1.0) as resp:
                result["owner"] = resp.read().decode("utf-8").strip()
        except Exception:
            pass

        return result

    def local_workbench_kernels(
        self, url: str = "http://127.0.0.1:8080/api/kernels", timeout: float = 2.0
    ) -> List[Dict[str, Any]]:
        """Probes local JupyterLab server on 127.0.0.1:8080 if running inside a Workbench VM."""
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                if isinstance(data, list):
                    return data
        except Exception:
            pass
        return []

    def workbench_instances(self) -> Dict[str, Dict[str, Any]]:
        """Queries Vertex AI Workbench v2 API for instance inventory in the project.

        Returns a dictionary indexed by Service Account email and SA username prefix:
          sa_key -> {"name": instance_name, "creator": creator, "instance_id": id, "state": state, ...}
        """
        url = f"https://notebooks.googleapis.com/v2/projects/{self.project_id}/locations/-/instances"
        try:
            data = self._get_json(url)
        except (AccessDenied, NotFound, DiagnosticError) as exc:
            logger.debug(
                "Workbench API query unavailable (%s); skipping external instance mapping.",
                exc,
            )
            return {}

        instances = data.get("instances", []) if isinstance(data, dict) else []
        inventory: Dict[str, Dict[str, Any]] = {}

        # Prioritize ACTIVE instances over STOPPED ones, and sort by latest update/create time
        def _sort_key(inst: Dict[str, Any]) -> Tuple[int, str]:
            is_active = 1 if inst.get("state") == "ACTIVE" else 0
            update_time = inst.get("updateTime") or inst.get("createTime") or ""
            return (is_active, update_time)

        sorted_instances = sorted(instances, key=_sort_key, reverse=True)

        for inst in sorted_instances:
            full_name = inst.get("name", "")
            vm_name = full_name.split("/")[-1] if full_name else "unknown"
            creator = inst.get("creator", "")
            state = inst.get("state", "ACTIVE")
            proxy_uri = inst.get("proxyUri", "")

            zone = "us-central1-a"
            if "/locations/" in full_name:
                parts = full_name.split("/")
                idx = parts.index("locations")
                if idx + 1 < len(parts):
                    zone = parts[idx + 1]

            # Extract numeric GCE instance id and metadata if available
            gce_setup = inst.get("gceSetup", {})
            instance_id = gce_setup.get("instanceId", "")
            sa_list = gce_setup.get("serviceAccounts", [])
            inst_metadata = gce_setup.get("metadata", {})
            proxy_user = inst_metadata.get("proxy-user-mail", "").strip()
            owner = proxy_user or creator

            cand_detail = {
                "name": vm_name,
                "zone": zone,
                "creator": owner,
                "state": state,
                "proxy_uri": proxy_uri,
                "instance_id": str(instance_id),
            }

            for sa in sa_list:
                email = sa.get("email", "").strip()
                if not email:
                    continue
                info = {
                    "name": vm_name,
                    "zone": zone,
                    "creator": owner,
                    "state": state,
                    "proxy_uri": proxy_uri,
                    "instance_id": str(instance_id),
                    "sa_email": email,
                }
                # Register under email & prefix with multi-instance awareness
                for key in (email, email.lower(), email.split("@")[0], email.split("@")[0].lower()):
                    if key not in inventory:
                        inventory[key] = info.copy()
                        inventory[key]["active_candidates"] = [vm_name] if state == "ACTIVE" else []
                        inventory[key]["all_candidates"] = [vm_name]
                        inventory[key]["candidate_details"] = [cand_detail]
                    else:
                        target = inventory[key]
                        if vm_name not in target["all_candidates"]:
                            target["all_candidates"].append(vm_name)
                        if state == "ACTIVE" and vm_name not in target["active_candidates"]:
                            target["active_candidates"].append(vm_name)
                        if not any(c.get("name") == vm_name for c in target.get("candidate_details", [])):
                            target.setdefault("candidate_details", []).append(cand_detail)

            # Index by the true owner (proxy-user-mail, or creator fallback if unassigned).
            # NEVER index under creator if proxy-user-mail is assigned to a different user,
            # as that pollutes the creator/admin pool with VMs assigned to other data scientists.
            for user_id in filter(None, [owner]):
                u_clean = user_id.strip()
                c_info = {
                    "name": vm_name,
                    "zone": zone,
                    "creator": owner,
                    "state": state,
                    "proxy_uri": proxy_uri,
                    "instance_id": str(instance_id),
                    "sa_email": sa_list[0].get("email", "") if sa_list else "",
                }
                for c_key in (u_clean, u_clean.lower(), u_clean.split("@")[0], u_clean.split("@")[0].lower()):
                    if c_key not in inventory:
                        inventory[c_key] = c_info.copy()
                        inventory[c_key]["active_candidates"] = [vm_name] if state == "ACTIVE" else []
                        inventory[c_key]["all_candidates"] = [vm_name]
                        inventory[c_key]["candidate_details"] = [cand_detail]
                    else:
                        target = inventory[c_key]
                        if vm_name not in target["all_candidates"]:
                            target["all_candidates"].append(vm_name)
                        if state == "ACTIVE" and vm_name not in target["active_candidates"]:
                            target["active_candidates"].append(vm_name)
                        if not any(c.get("name") == vm_name for c in target.get("candidate_details", [])):
                            target.setdefault("candidate_details", []).append(cand_detail)

        return inventory

    def resolve_execution_context(self) -> Dict[str, Any]:
        """Detect whether the script is running inside or outside Workbench."""
        context: Dict[str, Any] = {
            "is_in_situ": False,
            "environment": "External Workstation",
            "location": "Outside Workbench",
            "host": "unknown",
            "zone": None,
            "vm_name": None,
            "display": "External Workstation (Outside Workbench)",
        }

        # Step A: Check GCE metadata server
        is_gce = False
        vm_name = None
        zone = None
        attributes: List[str] = []

        try:
            req_name = urllib.request.Request(
                "http://metadata.google.internal/computeMetadata/v1/instance/name",
                headers={"Metadata-Flavor": "Google"},
            )
            with urllib.request.urlopen(req_name, timeout=0.5) as resp:
                vm_name = resp.read().decode("utf-8").strip()
                is_gce = True
        except Exception:
            pass

        if is_gce and vm_name:
            try:
                req_zone = urllib.request.Request(
                    "http://metadata.google.internal/computeMetadata/v1/instance/zone",
                    headers={"Metadata-Flavor": "Google"},
                )
                with urllib.request.urlopen(req_zone, timeout=0.5) as resp:
                    raw_zone = resp.read().decode("utf-8").strip()
                    zone = raw_zone.split("/")[-1] if "/" in raw_zone else raw_zone
            except Exception:
                pass

            try:
                req_attrs = urllib.request.Request(
                    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/",
                    headers={"Metadata-Flavor": "Google"},
                )
                with urllib.request.urlopen(req_attrs, timeout=0.5) as resp:
                    attributes = resp.read().decode("utf-8").splitlines()
            except Exception:
                pass

            has_wb_attr = any(
                a.strip() in ("proxy-url", "proxy-backend-id", "notebooks-api", "framework")
                for a in attributes
            )

            has_local_jupyter = False
            try:
                req_jup = urllib.request.Request(
                    "http://127.0.0.1:8080/api/status",
                    headers={"Accept": "application/json"},
                )
                with urllib.request.urlopen(req_jup, timeout=0.5) as resp:
                    if resp.status == 200:
                        has_local_jupyter = True
            except Exception:
                pass

            if has_wb_attr or has_local_jupyter:
                context["is_in_situ"] = True
                context["environment"] = "Vertex AI Workbench Instance"
                context["location"] = "In Situ"
                context["host"] = vm_name
                context["vm_name"] = vm_name
                context["zone"] = zone
                zone_str = f", Zone: {zone}" if zone else ""
                context["display"] = f"Vertex AI Workbench Instance (In Situ: {vm_name}{zone_str})"
                return context
            else:
                context["is_in_situ"] = False
                context["environment"] = "External GCE VM"
                context["location"] = "Outside Workbench"
                context["host"] = vm_name
                context["vm_name"] = vm_name
                context["zone"] = zone
                zone_str = f", Zone: {zone}" if zone else ""
                context["display"] = f"External GCE VM (Outside Workbench: {vm_name}{zone_str})"
                return context

        # Step B: Fallback to workstation hostname
        try:
            fqdn = socket.getfqdn()
            hostname = fqdn or socket.gethostname() or platform.node()
        except Exception:
            hostname = "localhost"

        context["is_in_situ"] = False
        context["host"] = hostname
        if ".c.googlers.com" in hostname or "cloudtop" in hostname:
            context["environment"] = "Cloudtop Workstation"
            context["display"] = f"External Workstation (Outside Workbench: {hostname})"
        else:
            context["environment"] = "External Workstation"
            context["display"] = f"External Workstation (Outside Workbench: {hostname})"

        return context

    def check_iam_capabilities(self) -> Dict[str, Any]:
        """Probes caller capabilities and permission matrix for diagnostics."""
        matrix: Dict[str, Dict[str, Any]] = {
            "core_dataproc": {
                "name": "Dataproc Cluster API",
                "role": "roles/dataproc.viewer",
                "granted": True,
                "detail": "Granted",
            },
            "core_gateway_yarn": {
                "name": "Gateway REST / YARN API",
                "role": "dataproc.clusters.use",
                "granted": True,
                "detail": "Granted",
            },
            "cloud_logging": {
                "name": "Cloud Logging Logs",
                "role": "roles/logging.viewer",
                "granted": True,
                "detail": "Granted",
            },
            "workbench_inventory": {
                "name": "Workbench Inventory",
                "role": "roles/notebooks.viewer",
                "granted": True,
                "detail": "Granted",
            },
            "signal_1_guest_attributes": {
                "name": "Signal 1 Guest Attributes",
                "role": "compute.instances.get",
                "granted": True,
                "detail": "Granted",
            },
            "signal_2_serial_console": {
                "name": "Signal 2 Serial Console",
                "role": "logging.entries.list",
                "granted": True,
                "detail": "Granted",
            },
            "signal_3_cloud_monitoring": {
                "name": "Signal 3 Cloud Monitoring",
                "role": "roles/monitoring.viewer",
                "granted": True,
                "detail": "Granted",
            },
            "method_3_inverting_proxy": {
                "name": "Method 3 Inverting Proxy",
                "role": "Inverting Proxy Bearer",
                "granted": False,
                "detail": "Requires browser session cookie or direct SA token (HTTP 401)",
            },
            "method_2_iap_tunnel": {
                "name": "Method 2 IAP Tunnel",
                "role": "roles/iap.tunnelResourceAccessor",
                "granted": True,
                "detail": "Port 8080 bound to 127.0.0.1 inside VM (Connection Refused)",
            },
            "method_1_gce_exec": {
                "name": "Method 1 Non-Intr. SSH",
                "role": "roles/iap.tunnelResourceAccessor",
                "granted": True,
                "detail": "Supported via IAP tunnel (--tunnel-through-iap)",
            },
        }

        # Probe Dataproc cluster API
        try:
            self.get_cluster()
        except AccessDenied:
            matrix["core_dataproc"]["granted"] = False
            matrix["core_dataproc"]["detail"] = "Access Denied (401/403)"
        except Exception:
            pass

        # Probe Component Gateway / YARN
        try:
            base = self.kernel_gateway_base()
            if base:
                self._get_json(f"{base}/api/kernels")
        except AccessDenied:
            matrix["core_gateway_yarn"]["granted"] = False
            matrix["core_gateway_yarn"]["detail"] = "Access Denied (dataproc.clusters.use missing)"
        except Exception:
            pass

        # Probe Cloud Logging
        try:
            self.log_entries('resource.type="gce_instance"', page_size=1)
        except AccessDenied:
            matrix["cloud_logging"]["granted"] = False
            matrix["cloud_logging"]["detail"] = "Access Denied (roles/logging.viewer missing)"
            matrix["signal_2_serial_console"]["granted"] = False
            matrix["signal_2_serial_console"]["detail"] = "Access Denied (roles/logging.viewer missing)"
        except Exception:
            pass

        # Probe Workbench Inventory
        try:
            url = f"{NOTEBOOKS_API}/projects/{self.project_id}/locations/-/instances?pageSize=1"
            self._get_json(url)
        except AccessDenied:
            matrix["workbench_inventory"]["granted"] = False
            matrix["workbench_inventory"]["detail"] = "Access Denied (roles/notebooks.viewer missing)"
        except Exception:
            pass

        # Probe Cloud Monitoring
        try:
            now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
            metric_filter = urllib.parse.quote('metric.type="compute.googleapis.com/instance/network/sent_bytes_count"')
            mon_url = (
                f"{MONITORING_API}/projects/{self.project_id}/timeSeries"
                f"?filter={metric_filter}"
                f"&interval.startTime={urllib.parse.quote(now_iso)}"
                f"&interval.endTime={urllib.parse.quote(now_iso)}"
                f"&pageSize=1"
            )
            self._get_json(mon_url)
        except AccessDenied:
            matrix["signal_3_cloud_monitoring"]["granted"] = False
            matrix["signal_3_cloud_monitoring"]["detail"] = "Access Denied (roles/monitoring.viewer missing)"
        except Exception:
            pass

        return matrix

    # ------------------------------------------------------------------
    # Multi-Signal VM Disambiguation (Signals 1, 2, 3)
    # ------------------------------------------------------------------
    def get_instance_guest_attributes(
        self, instance_name: str, zone: str, query_path: str = "workbench-notebooks/"
    ) -> Dict[str, str]:
        """Signal 1: Reads GCE guest attributes reported by JupyterLab inside the instance."""
        if not instance_name or not zone:
            return {}
        url = (
            f"{COMPUTE_API}/projects/{self.project_id}/zones/{zone}/instances/"
            f"{urllib.parse.quote(instance_name)}/getGuestAttributes?queryPath={urllib.parse.quote(query_path)}"
        )
        try:
            data = self._get_json(url)
            items = data.get("queryValue", {}).get("items", [])
            return {item.get("key", ""): item.get("value", "") for item in items if item.get("key")}
        except Exception as exc:
            logger.debug("Guest attributes unavailable for %s (%s)", instance_name, exc)
            return {}

    def get_instance_serial_activity(
        self, instance_id: str, since: Optional[str] = None, until: Optional[str] = None
    ) -> int:
        """Signal 2: Counts Cloud Logging serial console HTTP entries around kernel launch."""
        if not instance_id:
            return 0
        filter_parts = [
            'resource.type="gce_instance"',
            f'resource.labels.instance_id="{instance_id}"',
            'textPayload:"/lab/tree/"',
        ]
        if since:
            filter_parts.append(f'timestamp >= "{since}"')
        if until:
            filter_parts.append(f'timestamp <= "{until}"')
        log_filter = "\n".join(filter_parts)
        try:
            entries = self.log_entries(log_filter, page_size=20)
            return len(entries)
        except Exception as exc:
            logger.debug("Serial activity trace unavailable for %s (%s)", instance_id, exc)
            return 0

    def get_instance_network_egress(self, instance_id: str, minutes: int = 30) -> int:
        """Signal 3: Queries Cloud Monitoring for bytes sent over the network."""
        if not instance_id:
            return 0
        now = datetime.datetime.now(datetime.timezone.utc)
        start = (now - datetime.timedelta(minutes=minutes)).isoformat()
        end = now.isoformat()
        filter_str = (
            f'metric.type="compute.googleapis.com/instance/network/sent_bytes_count" AND '
            f'resource.labels.instance_id="{instance_id}"'
        )
        url = (
            f"{MONITORING_API}/projects/{self.project_id}/timeSeries"
            f"?filter={urllib.parse.quote(filter_str)}"
            f"&interval.startTime={urllib.parse.quote(start)}"
            f"&interval.endTime={urllib.parse.quote(end)}"
            f"&pageSize=10"
        )
        try:
            data = self._get_json(url)
            series = data.get("timeSeries", [])
            total_bytes = 0
            for s in series:
                for pt in s.get("points", []):
                    val = pt.get("value", {}).get("int64Value")
                    if val:
                        total_bytes += int(val)
            return total_bytes
        except Exception as exc:
            logger.debug("Network egress telemetry unavailable for %s (%s)", instance_id, exc)
            return 0

    # ------------------------------------------------------------------
    # External In-Situ Probing Fallback Chain (Methods 3 -> 2 -> 1)
    # ------------------------------------------------------------------
    def query_remote_workbench_sessions(
        self,
        vm_name: str,
        proxy_uri: Optional[str] = None,
        zone: Optional[str] = None,
        instance_id: Optional[str] = None,
        timeout: float = 2.0,
    ) -> Tuple[List[Dict[str, Any]], Optional[str], Optional[str]]:
        """Queries remote Workbench /api/sessions via Method 3 -> Method 2 -> Method 1 fallback chain.

        Returns: (sessions_list, method_name, failure_explanation)
        """
        # Method 3: Direct Inverting Proxy REST API call (Primary)
        if proxy_uri:
            url = f"https://{proxy_uri.rstrip('/')}/api/sessions"
            try:
                req = urllib.request.Request(
                    url,
                    headers={
                        "Authorization": f"Bearer {self._token}",
                        "Accept": "application/json",
                    },
                )
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    if resp.status == 200:
                        data = json.loads(resp.read().decode("utf-8"))
                        if isinstance(data, list):
                            logger.info("Method 3 (Inverting Proxy REST) succeeded for %s", vm_name)
                            return data, "Method 3 (Inverting Proxy REST)", None
            except urllib.error.HTTPError as exc:
                logger.debug(
                    "Method 3 Inverting Proxy returned HTTP %d (%s); falling back to Method 2",
                    exc.code,
                    exc.reason,
                )
            except Exception as exc:
                logger.debug("Method 3 Inverting Proxy error (%s); falling back to Method 2", exc)

        # Method 2: Authenticated IAP TCP Tunnel (Fallback 1)
        if vm_name and zone:
            iap_proc = None
            try:
                cmd = [
                    "gcloud",
                    "compute",
                    "start-iap-tunnel",
                    vm_name,
                    "8080",
                    f"--zone={zone}",
                    "--local-host-port=localhost:0",
                    f"--project={self.project_id}",
                ]
                iap_proc = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
                )
                port = None
                start_wait = time.time()
                while time.time() - start_wait < 3.0:
                    line = iap_proc.stderr.readline() if iap_proc.stderr else ""
                    if not line and iap_proc.stdout:
                        line = iap_proc.stdout.readline()
                    m = re.search(r"\[(\d+)\]", line)
                    if m:
                        port = int(m.group(1))
                        break
                    if iap_proc.poll() is not None:
                        break

                if port:
                    tunnel_url = f"http://127.0.0.1:{port}/api/sessions"
                    req = urllib.request.Request(
                        tunnel_url, headers={"Accept": "application/json"}
                    )
                    with urllib.request.urlopen(req, timeout=timeout) as resp:
                        if resp.status == 200:
                            data = json.loads(resp.read().decode("utf-8"))
                            if isinstance(data, list):
                                logger.info("Method 2 (IAP Tunnel) succeeded for %s", vm_name)
                                return data, "Method 2 (IAP Tunnel)", None
            except Exception as exc:
                logger.debug("Method 2 IAP Tunnel failed (%s); falling back to Method 1", exc)
            finally:
                if iap_proc:
                    try:
                        iap_proc.terminate()
                        iap_proc.wait(timeout=1.0)
                    except Exception:
                        try:
                            iap_proc.kill()
                        except Exception:
                            pass

        # Method 1: Compute Engine Non-Interactive SSH (Fallback 2)
        if vm_name and zone:
            try:
                cmd = [
                    "gcloud",
                    "compute",
                    "ssh",
                    vm_name,
                    f"--zone={zone}",
                    f"--project={self.project_id}",
                    "--tunnel-through-iap",
                    '--command=curl -s http://127.0.0.1:8080/api/sessions',
                    "--ssh-flag=-o StrictHostKeyChecking=no",
                    "--ssh-flag=-o ConnectTimeout=5",
                    "--ssh-flag=-o BatchMode=yes",
                ]
                out = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=25.0
                )
                if out.returncode == 0 and out.stdout.strip():
                    data = json.loads(out.stdout.strip())
                    if isinstance(data, list):
                        logger.info("Method 1 (Non-Interactive SSH) succeeded for %s", vm_name)
                        return data, "Method 1 (Non-Interactive SSH)", None
            except Exception as exc:
                logger.debug("Method 1 SSH exec failed (%s)", exc)

        explanation = (
            "Method 3 HTTP 401 single-user cookie lock; "
            "Method 2 Port 8080 bound to localhost; "
            "Method 1 SSO/CorpSSH required"
        )
        return [], None, explanation

    def lookup_workbench_notebook_file(
        self, instance_id: str, instance_name: Optional[str] = None
    ) -> Optional[str]:
        """Best-effort discovery of the active notebook file from Cloud Logging serial console entries."""
        if not instance_id and not instance_name:
            return None

        filter_parts = ['resource.type="gce_instance"']
        if instance_id:
            filter_parts.append(f'resource.labels.instance_id="{instance_id}"')
        filter_parts.append('textPayload:"/lab/tree/"')

        log_filter = "\n".join(filter_parts)
        try:
            entries = self.log_entries(log_filter, page_size=50)
        except Exception as exc:
            logger.debug("Cloud Logging serial trace unavailable (%s)", exc)
            return None

        recent_files: List[str] = []
        for entry in entries:
            text = entry.get("textPayload") or ""
            match = re.search(r"/lab/tree/([^ \r\n\t?&\\\"']+)", text)
            if match:
                raw_path = match.group(1).strip().rstrip("\r\n\\\"'")
                notebook_file = urllib.parse.unquote(raw_path)
                if notebook_file and notebook_file not in recent_files:
                    recent_files.append(notebook_file)

        if not recent_files:
            return None
        if len(recent_files) == 1:
            return recent_files[0]
        # Multiple active notebook files seen in recent logs
        return f"{recent_files[0]} (most recent of {len(recent_files)} active: {', '.join(recent_files[:3])})"

    # ------------------------------------------------------------------
    # Cloud Logging
    # ------------------------------------------------------------------
    def log_entries(
        self, log_filter: str, page_size: int = 200
    ) -> List[Dict[str, Any]]:
        payload = {
            "resourceNames": [f"projects/{self.project_id}"],
            "filter": log_filter,
            "orderBy": "timestamp desc",
            "pageSize": page_size,
        }
        data = self._post_json(f"{LOGGING_API}/entries:list", payload)
        return data.get("entries", []) or []
