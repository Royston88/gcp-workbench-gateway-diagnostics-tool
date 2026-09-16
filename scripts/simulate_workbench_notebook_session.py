#!/usr/bin/env python3
"""Simulate an active JupyterLab notebook session inside a Vertex AI Workbench VM.

This script interacts with the local Jupyter Server REST API on the Workbench VM (http://127.0.0.1:8080):
1. Fetches the _xsrf token from /lab.
2. Creates an active session for a notebook (e.g. MultiTenant_E2E_Test.ipynb).
3. Generates the corresponding Referer header (http://127.0.0.1:8080/lab/tree/<notebook>) which emits
   into GCE serial logs (Signal 2).
4. Enables Method 1 (Non-Interactive SSH) and in-situ diagnostics to resolve Workbench UI ID and Notebook File.
"""

import argparse
import http.cookiejar
import json
import logging
import os
import subprocess
import sys
import urllib.request
from typing import Optional, Dict, Any, List

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("simulate_wb_session")


def run_remote_python(vm_name: str, zone: str, project: str, python_code: str) -> str:
    """Execute Python code inside the Workbench VM via gcloud compute ssh over IAP."""
    cmd = [
        "gcloud",
        "compute",
        "ssh",
        vm_name,
        f"--zone={zone}",
        f"--project={project}",
        "--tunnel-through-iap",
        f"--command=python3 -c {subprocess.list2cmdline([python_code])}",
        "--ssh-flag=-o StrictHostKeyChecking=no",
        "--ssh-flag=-o ConnectTimeout=10",
        "--ssh-flag=-o BatchMode=yes",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return res.stdout.strip()


def local_create_session(
    notebook_path: str = "MultiTenant_E2E_Test.ipynb",
    kernel_name: str = "python3",
    port: int = 8080,
) -> Dict[str, Any]:
    """Create or attach to a session directly against local Jupyter Server on port 8080."""
    base_url = f"http://127.0.0.1:{port}"
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

    # 1. GET /lab to get _xsrf cookie
    req = urllib.request.Request(f"{base_url}/lab")
    opener.open(req)

    xsrf = None
    for c in cj:
        if c.name == "_xsrf":
            xsrf = c.value
            break

    if not xsrf:
        raise RuntimeError("Failed to acquire _xsrf cookie from JupyterLab")

    # 2. Ensure dummy notebook file exists on disk if in /home/jupyter
    disk_path = os.path.expanduser(f"~/jupyter/{notebook_path}") if not os.path.isabs(notebook_path) else notebook_path
    if not os.path.exists(disk_path) and not os.path.isabs(notebook_path):
        home_path = os.path.expanduser(f"~/{notebook_path}")
        if not os.path.exists(home_path):
            try:
                with open(home_path, "w", encoding="utf-8") as f:
                    json.dump(
                        {
                            "cells": [],
                            "metadata": {"kernelspec": {"name": kernel_name, "display_name": kernel_name}},
                            "nbformat": 4,
                            "nbformat_minor": 5,
                        },
                        f,
                    )
            except Exception as e:
                logger.warning("Could not write dummy notebook file: %s", e)

    # 3. POST /api/sessions
    body = json.dumps(
        {
            "path": notebook_path,
            "type": "notebook",
            "name": os.path.basename(notebook_path),
            "kernel": {"name": kernel_name},
        }
    ).encode("utf-8")

    referer_url = f"{base_url}/lab/tree/{notebook_path}"
    post_req = urllib.request.Request(
        f"{base_url}/api/sessions",
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-XSRFToken": xsrf,
            "Referer": referer_url,
        },
    )
    with opener.open(post_req) as resp:
        return json.loads(resp.read().decode("utf-8"))


def local_list_sessions(port: int = 8080) -> List[Dict[str, Any]]:
    """List active sessions from local Jupyter Server."""
    url = f"http://127.0.0.1:{port}/api/sessions"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=5.0) as resp:
        return json.loads(resp.read().decode("utf-8"))


def local_delete_session(session_id: str, port: int = 8080) -> bool:
    """Delete a session by ID."""
    base_url = f"http://127.0.0.1:{port}"
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    opener.open(f"{base_url}/lab")
    xsrf = next((c.value for c in cj if c.name == "_xsrf"), None)
    if not xsrf:
        return False

    req = urllib.request.Request(
        f"{base_url}/api/sessions/{session_id}",
        headers={"X-XSRFToken": xsrf},
        method="DELETE",
    )
    with opener.open(req) as resp:
        return resp.status in (204, 200)


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate active Workbench notebook session")
    parser.add_argument(
        "--action",
        choices=["create", "list", "delete"],
        default="create",
        help="Action to perform (default: create)",
    )
    parser.add_argument("--vm-name", default="instance-20260901-162000-single-svc", help="Workbench VM name")
    parser.add_argument("--zone", default="us-central1-a", help="GCE Zone of Workbench VM")
    parser.add_argument("--project", default="kenly-lakehouse-dev-1", help="GCP Project ID")
    parser.add_argument(
        "--notebook-path",
        default="MultiTenant_E2E_Test.ipynb",
        help="Relative or absolute notebook path",
    )
    parser.add_argument("--kernel-name", default="python3", help="Kernel spec name (default: python3)")
    parser.add_argument("--session-id", help="Session ID to delete (for action=delete)")
    parser.add_argument(
        "--in-situ",
        action="store_true",
        help="Run directly on current machine instead of SSHing into --vm-name",
    )

    args = parser.parse_args()

    if args.in_situ:
        if args.action == "create":
            sess = local_create_session(args.notebook_path, args.kernel_name)
            print(json.dumps(sess, indent=2))
        elif args.action == "list":
            sessions = local_list_sessions()
            print(json.dumps(sessions, indent=2))
        elif args.action == "delete":
            if not args.session_id:
                print("Error: --session-id required for action=delete", file=sys.stderr)
                sys.exit(1)
            ok = local_delete_session(args.session_id)
            print(f"Session {args.session_id} deleted: {ok}")
        return

    # Remote execution via gcloud compute ssh
    remote_code = f"""
import http.cookiejar, json, urllib.request, os

action = {args.action!r}
port = 8080
base_url = f"http://127.0.0.1:{{port}}"

if action == "list":
    req = urllib.request.Request(f"{{base_url}}/api/sessions", headers={{"Accept": "application/json"}})
    with urllib.request.urlopen(req, timeout=5.0) as resp:
        print(resp.read().decode("utf-8"))
elif action == "create":
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    opener.open(f"{{base_url}}/lab")
    xsrf = next((c.value for c in cj if c.name == "_xsrf"), None)
    
    body = json.dumps({{
        "path": {args.notebook_path!r},
        "type": "notebook",
        "name": os.path.basename({args.notebook_path!r}),
        "kernel": {{"name": {args.kernel_name!r}}}
    }}).encode("utf-8")
    
    req = urllib.request.Request(
        f"{{base_url}}/api/sessions",
        data=body,
        headers={{
            "Content-Type": "application/json",
            "X-XSRFToken": xsrf,
            "Referer": f"{{base_url}}/lab/tree/{args.notebook_path}"
        }}
    )
    with opener.open(req) as resp:
        print(resp.read().decode("utf-8"))
"""
    logger.info("Executing remote %s on %s...", args.action, args.vm_name)
    out = run_remote_python(args.vm_name, args.zone, args.project, remote_code)
    try:
        parsed = json.loads(out)
        print(json.dumps(parsed, indent=2))
    except Exception:
        print(out)


if __name__ == "__main__":
    main()
