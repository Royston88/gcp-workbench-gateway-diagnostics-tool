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

import json
import logging
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
DATAPROC_API = "https://dataproc.googleapis.com/v1"
LOGGING_API = "https://logging.googleapis.com/v2"

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
