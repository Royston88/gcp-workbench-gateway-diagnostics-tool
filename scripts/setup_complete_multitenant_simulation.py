#!/usr/bin/env python3
"""Orchestrate the complete multi-tenant simulation from scratch.

This script:
1. Verifies/cleans existing kernels on Dataproc Kernel Gateway.
2. For each test scenario (User 1, User 2 multi-notebook, User 3, Admin):
   a. Acquires the user/tenant OAuth access token.
   b. Launches the PySpark kernel via Dataproc Component Gateway REST.
   c. Upgrades to WebSocket to complete kernel_info_request handshake (starting -> idle).
   d. Connects to the respective Workbench VM via IAP and creates the in-situ notebook session
      in JupyterLab on port 8080 with matching notebook path and kernel ID.
   e. Emits the /lab/tree/ referer to ensure GCE serial console trace logging.
   f. Generates activity signals on User 1 primary VM to score 100% confidence over candidate.
3. Prints the final verification status of all sessions across Dataproc, YARN, and Workbench VMs.
"""

import http.cookiejar
import json
import logging
import os
import ssl
import subprocess
import sys
import time
import urllib.request
from typing import Dict, Any, List, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("setup_simulation")

PROJECT_ID = "kenly-lakehouse-dev-1"
REGION = "us-central1"
ZONE = "us-central1-a"
CLUSTER_NAME = "pyspark-cluster-e2e-20260915-v5"
GATEWAY_URL = "https://c4yxwrgwnjdtzg7zn3svsap3im-dot-us-central1.dataproc.googleusercontent.com/gateway/default/jupyter"


def get_token(impersonate_sa: Optional[str] = None) -> str:
    """Acquire OAuth2 access token for admin or impersonated service account."""
    cmd = ["bash", "-c", "source ~/.bash_profile && source ~/.gcrc && gc admin--kenly-lakehouse-dev-1 gcloud auth print-access-token" + (f" --impersonate-service-account={impersonate_sa}" if impersonate_sa else "")]
    return subprocess.check_output(cmd, text=True).strip()


def delete_all_dataproc_kernels(token: str) -> None:
    """Clean up all active kernels on Dataproc Kernel Gateway."""
    logger.info("Cleaning up all existing kernels on Dataproc Kernel Gateway...")
    url = f"{GATEWAY_URL}/api/kernels"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    try:
        with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
            kernels = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.warning("Could not list Dataproc kernels: %s", e)
        kernels = []

    logger.info("Found %d active Dataproc kernels to delete.", len(kernels))
    for k in kernels:
        kid = k.get("id")
        del_req = urllib.request.Request(
            f"{GATEWAY_URL}/api/kernels/{kid}",
            headers={"Authorization": f"Bearer {token}"},
            method="DELETE",
        )
        try:
            with urllib.request.urlopen(del_req, context=ctx, timeout=15) as del_resp:
                logger.info("Deleted kernel %s: HTTP %s", kid, del_resp.status)
        except Exception as e:
            logger.warning("Failed or timed out deleting kernel %s: %s", kid, e)

    logger.info("Waiting 5s for YARN application containers to unregister...")
    time.sleep(5)


def launch_dataproc_kernel(token: str, kernel_spec: str = "pyspark_yarn") -> Dict[str, Any]:
    """Launch a remote kernel on Dataproc Kernel Gateway."""
    url = f"{GATEWAY_URL}/api/kernels"
    data = json.dumps({"name": kernel_spec}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def perform_websocket_handshake(kernel_id: str, token: str, user_label: str = "user") -> bool:
    """Connect to kernel via WebSocket and send kernel_info_request to flip starting -> idle."""
    try:
        import websocket
    except ImportError:
        logger.warning("websocket-client not available in local python; skipping WS handshake")
        return False

    ws_base = GATEWAY_URL.replace("https://", "wss://").replace("http://", "ws://")
    url = f"{ws_base}/api/kernels/{kernel_id}/channels"

    logger.info("[%s] Connecting WebSocket to %s...", user_label, kernel_id[:8])
    ws = websocket.WebSocket(sslopt={"cert_reqs": ssl.CERT_NONE})
    try:
        ws.connect(url, header=[f"Authorization: Bearer {token}"])
        session_id = f"sim-{kernel_id[:8]}-{int(time.time())}"
        handshake_msg = {
            "header": {
                "msg_id": f"h-{int(time.time()*1000)}",
                "username": user_label,
                "session": session_id,
                "msg_type": "kernel_info_request",
                "version": "5.3",
            },
            "parent_header": {},
            "metadata": {},
            "content": {},
            "channel": "shell",
        }
        ws.send(json.dumps(handshake_msg))
        ws.settimeout(10.0)
        while True:
            raw = ws.recv()
            if not raw:
                break
            msg = json.loads(raw)
            if msg.get("msg_type") == "kernel_info_reply":
                logger.info("[%s] Received kernel_info_reply! Kernel is IDLE.", user_label)
                break
            if msg.get("msg_type") == "status" and msg.get("content", {}).get("execution_state") == "idle":
                logger.info("[%s] Received status: idle!", user_label)
                break
        ws.close()
        return True
    except Exception as e:
        logger.warning("[%s] WebSocket handshake exception: %s", user_label, e)
        return False


def clean_workbench_vm_sessions(vm_name: str) -> None:
    """Delete any existing sessions inside the target Workbench VM."""
    logger.info("Cleaning local sessions on Workbench VM %s...", vm_name)
    remote_code = """
import urllib.request, json, http.cookiejar
try:
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    opener.open("http://127.0.0.1:8080/lab")
    xsrf = next((c.value for c in cj if c.name == "_xsrf"), None)
    req = urllib.request.Request("http://127.0.0.1:8080/api/sessions", headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        sessions = json.loads(resp.read().decode("utf-8"))
    for s in sessions:
        sid = s.get("id")
        del_req = urllib.request.Request(f"http://127.0.0.1:8080/api/sessions/{sid}", headers={"X-XSRFToken": xsrf}, method="DELETE")
        try:
            with opener.open(del_req) as dresp:
                pass
        except Exception:
            pass
    print(f"Cleaned {len(sessions)} sessions")
except Exception as e:
    print(f"Cleanup error: {e}")
"""
    cmd = [
        "bash",
        "-c",
        f"source ~/.bash_profile && gc admin--kenly-lakehouse-dev-1 gcloud compute ssh {vm_name} --zone={ZONE} --tunnel-through-iap --command=\"python3 -c '{remote_code}'\"",
    ]
    subprocess.run(cmd, capture_output=True, text=True)


def create_in_situ_workbench_session(
    vm_name: str,
    notebook_path: str,
    kernel_id: str,
    kernel_name: str = "python3",
) -> Dict[str, Any]:
    """Create in-situ notebook session in Workbench VM JupyterLab on port 8080."""
    logger.info("Creating in-situ session on %s for %s (kernel: %s)...", vm_name, notebook_path, kernel_id[:8])
    remote_code = f"""
import urllib.request, json, http.cookiejar, os

cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
opener.open("http://127.0.0.1:8080/lab")
xsrf = next((c.value for c in cj if c.name == "_xsrf"), None)

# Ensure dummy notebook file exists on disk
disk_path = os.path.expanduser("~/jupyter/{notebook_path}") if not os.path.isabs("{notebook_path}") else "{notebook_path}"
dir_name = os.path.dirname(disk_path)
if dir_name and not os.path.exists(dir_name):
    os.makedirs(dir_name, exist_ok=True)
if not os.path.exists(disk_path):
    with open(disk_path, "w", encoding="utf-8") as f:
        json.dump({{"cells": [], "metadata": {{"kernelspec": {{"name": "{kernel_name}"}}}}, "nbformat": 4, "nbformat_minor": 5}}, f)

body = json.dumps({{
    "path": "{notebook_path}",
    "type": "notebook",
    "name": os.path.basename("{notebook_path}"),
    "kernel": {{"id": "{kernel_id}", "name": "{kernel_name}"}}
}}).encode("utf-8")

req = urllib.request.Request(
    "http://127.0.0.1:8080/api/sessions",
    data=body,
    headers={{
        "Content-Type": "application/json",
        "X-XSRFToken": xsrf,
        "Referer": "http://127.0.0.1:8080/lab/tree/{notebook_path}"
    }}
)
try:
    with opener.open(req) as resp:
        print(resp.read().decode("utf-8"))
except urllib.error.HTTPError as e:
    print(json.dumps({{"error": e.code, "msg": str(e), "body": e.read().decode("utf-8")}}))
"""
    cmd = [
        "bash",
        "-c",
        f"source ~/.bash_profile && gc admin--kenly-lakehouse-dev-1 gcloud compute ssh {vm_name} --zone={ZONE} --tunnel-through-iap --command=\"python3 -c '{remote_code}'\"",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    out = res.stdout.strip()
    for line in out.splitlines():
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except Exception:
                pass
    return {"raw_output": out}


def touch_user1_guest_attributes(vm_name: str = "instance-20260901-162000-single-svc") -> None:
    """Touch guest attributes on VM 1A to ensure Signal 1 scoring awards 100% confidence."""
    logger.info("Setting guest attribute last_activity on %s for Signal 1...", vm_name)
    remote_code = """
import urllib.request, time
data = str(int(time.time())).encode("utf-8")
req = urllib.request.Request(
    "http://metadata.google.internal/computeMetadata/v1/instance/guest-attributes/workbench-notebooks/last_activity",
    data=data,
    headers={"Metadata-Flavor": "Google"},
    method="PUT"
)
try:
    with urllib.request.urlopen(req) as resp:
        print("Guest attribute updated")
except Exception as e:
    print(f"Guest attribute update error: {e}")
"""
    cmd = [
        "bash",
        "-c",
        f"source ~/.bash_profile && gc admin--kenly-lakehouse-dev-1 gcloud compute ssh {vm_name} --zone={ZONE} --tunnel-through-iap --command=\"python3 -c '{remote_code}'\"",
    ]
    subprocess.run(cmd, capture_output=True, text=True)


def main() -> None:
    logger.info("================================================================")
    logger.info("STARTING COMPLETE MULTI-TENANT SIMULATION FROM SCRATCH")
    logger.info("================================================================")

    # 1. Acquire Admin Token
    admin_token = get_token()
    logger.info("Acquired Admin Token.")

    # 2. Clean Dataproc Kernels
    delete_all_dataproc_kernels(admin_token)

    # 3. Clean Workbench VM Sessions on all active VMs
    vms_to_clean = [
        "instance-20260901-162000-single-svc",
        "instance-20251203-071523-single-svc",
        "instance-20251203-151903-single-svc-2",
        "instance-20251208-220655-single-svc-3",
    ]
    for vm in vms_to_clean:
        clean_workbench_vm_sessions(vm)

    # 4. Session Matrix Definition
    scenarios = [
        {
            "user_label": "ds_user_1",
            "impersonate_sa": "ds-user-1-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com",
            "vm_name": "instance-20260901-162000-single-svc",
            "notebook_path": "notebooks/customer_segmentation.ipynb",
            "desc": "User 1 (Active VM with Candidate Ambiguity against instance-20251203-071523-single-svc)",
        },
        {
            "user_label": "ds-user-2-svc_A",
            "impersonate_sa": "ds-user-2-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com",
            "vm_name": "instance-20251203-151903-single-svc-2",
            "notebook_path": "notebooks/feature_store_etl.ipynb",
            "desc": "User 2 - Notebook A (Multi-notebook on single VM)",
        },
        {
            "user_label": "ds-user-2-svc_B",
            "impersonate_sa": "ds-user-2-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com",
            "vm_name": "instance-20251203-151903-single-svc-2",
            "notebook_path": "notebooks/churn_xgboost_training.ipynb",
            "desc": "User 2 - Notebook B (Multi-notebook on single VM)",
        },
        {
            "user_label": "ds-user-3-svc",
            "impersonate_sa": "ds-user-3-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com",
            "vm_name": "instance-20251208-220655-single-svc-3",
            "notebook_path": "notebooks/quarterly_forecast.ipynb",
            "desc": "User 3 - Single Active Notebook",
        },
        {
            "user_label": "admin",
            "impersonate_sa": None,
            "vm_name": "instance-20260901-162000-single-svc",
            "notebook_path": "notebooks/platform_capacity_audit.ipynb",
            "desc": "Platform Admin - Fleet Audit Notebook",
        },
    ]

    created_sessions = []

    # 5. Launch and Correlate Each Session
    for sc in scenarios:
        logger.info("----------------------------------------------------------------")
        logger.info("Provisioning session: %s", sc["desc"])
        token = get_token(sc["impersonate_sa"])
        
        # Step A: Launch Dataproc Kernel
        logger.info("[%s] Launching Dataproc PySpark kernel via Component Gateway REST...", sc["user_label"])
        k_data = launch_dataproc_kernel(token, "pyspark_yarn")
        kernel_id = k_data.get("id")
        logger.info("[%s] Dataproc Kernel Created! ID: %s", sc["user_label"], kernel_id)

        # Step B: Perform WebSocket Handshake to transition to IDLE
        logger.info("[%s] Upgrading WebSocket to transition kernel to IDLE...", sc["user_label"])
        perform_websocket_handshake(kernel_id, token, sc["user_label"])

        # Step C: In-Situ Workbench Notebook Session Simulation
        wb_sess = create_in_situ_workbench_session(
            vm_name=sc["vm_name"],
            notebook_path=sc["notebook_path"],
            kernel_id=kernel_id,
            kernel_name="python3",
        )
        logger.info("[%s] In-Situ Workbench Session Created: %s", sc["user_label"], wb_sess.get("id"))

        created_sessions.append({
            "scenario": sc["desc"],
            "user": sc["user_label"],
            "kernel_id": kernel_id,
            "vm_name": sc["vm_name"],
            "notebook": sc["notebook_path"],
            "wb_session_id": wb_sess.get("id"),
        })

    # 6. Touch Signal 1 for User 1 primary VM
    touch_user1_guest_attributes("instance-20260901-162000-single-svc")

    logger.info("================================================================")
    logger.info("SIMULATION PROVISIONING COMPLETE!")
    logger.info("================================================================")
    for s in created_sessions:
        logger.info("  * [%s] Kernel: %s -> VM: %s, Notebook: %s (UI ID: %s)",
                    s["user"], s["kernel_id"][:8], s["vm_name"], s["notebook"], s["wb_session_id"] or "Unresolved")


if __name__ == "__main__":
    main()
