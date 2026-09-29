#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Standalone Dataproc In-Cluster Diagnostic Probe (Method 2B - Direct gcloud Submission)
#
# Executes on the Dataproc master node via spark.master=local[1], bypassing
# Component Gateway (*.dataproc.googleusercontent.com) and VPC-SC Cloud Logging
# restrictions by reading local loopback endpoints (127.0.0.1:8888, localhost:8090)
# and /var/log/jupyter_kernel_gateway.log directly on the master VM.
#
# Usage:
#   gcloud dataproc jobs submit pyspark scripts/probe_yarn_metrics.py \
#       --cluster=<CLUSTER_NAME> \
#       --region=<REGION> \
#       --properties=spark.master=local[1]

import json
import os
import re
import ssl
import urllib.request

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


def fetch_yarn(path: str):
    last_err = None
    for base in (
        "https://localhost:8090/ws/v1/cluster",
        "http://localhost:8088/ws/v1/cluster",
    ):
        url = f"{base}/{path.lstrip('/')}"
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            if base.startswith("https"):
                with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            else:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            last_err = exc
    raise RuntimeError(f"Failed to query YARN {path}: {last_err}")


def humanize_mb(mb) -> str:
    if mb is None:
        return "unknown"
    mb_val = float(mb)
    if mb_val >= 1024:
        return f"{mb_val / 1024:.1f} GB ({int(mb_val)} MB)"
    return f"{int(mb_val)} MB"


# ----------------------------------------------------------------------
# 1. Check 1 Equivalent: Local Jupyter Kernel Gateway & Running YARN Apps
# ----------------------------------------------------------------------
print("=================== 1. ACTIVE KERNELS & RUNNING YARN APPS ===================")
try:
    req = urllib.request.Request(
        "http://127.0.0.1:8888/gateway/default/jupyter/api/kernels",
        headers={"Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        kernels = json.loads(resp.read().decode("utf-8"))
    if not isinstance(kernels, list):
        kernels = []
    print(f"Active Gateway Kernels       : {len(kernels)}")
    for k in kernels:
        print(
            f"  * Kernel {k.get('id')} | Name: {k.get('name')} | "
            f"State: {k.get('execution_state')} | Last Activity: {k.get('last_activity')}"
        )
except Exception as exc:
    print(f"Failed to query local Kernel Gateway (127.0.0.1:8888): {exc}")

try:
    apps_resp = fetch_yarn("apps")
    all_apps = ((apps_resp or {}).get("apps") or {}).get("app", []) or []
    if isinstance(all_apps, dict):
        all_apps = [all_apps]
    running_apps = [a for a in all_apps if a.get("state") == "RUNNING"]
    accepted_apps = [a for a in all_apps if a.get("state") == "ACCEPTED"]
    print(f"Running YARN Applications    : {len(running_apps)}")
    for a in running_apps:
        print(
            f"  * {a.get('id')} | User: {a.get('user')} | Name: {a.get('name')} | "
            f"Allocated MB: {a.get('allocatedMB')} | Driver Host: {a.get('amHostHttpAddress')}"
        )
except Exception as exc:
    running_apps = []
    accepted_apps = []
    print(f"Failed to query YARN applications: {exc}")

# ----------------------------------------------------------------------
# 2. Check 2 Equivalent: Queue Capacity, AM Limits & Cluster Memory
# ----------------------------------------------------------------------
print("\n=================== 2. QUEUE CAPACITY & AM METRICS ===================")
try:
    metrics = (fetch_yarn("metrics") or {}).get("clusterMetrics", {})
    total_mb = metrics.get("totalMB", 0)
    alloc_mb = metrics.get("allocatedMB", 0)
    avail_mb = metrics.get("availableMB", 0)

    sched = fetch_yarn("scheduler")
    s_info = (sched or {}).get("scheduler", {}).get("schedulerInfo", {})
    q = ((s_info.get("queues") or {}).get("queue") or [{}])[0]

    partition_caps = (
        (q.get("capacities") or {}).get("queueCapacitiesByPartition") or [{}]
    )[0]
    max_am_pct = partition_caps.get("maxAMLimitPercentage") or q.get(
        "configuredMaxAMResourceLimit"
    )
    am_limit_mb = (q.get("AMResourceLimit") or {}).get("memory", 0)
    am_used_mb = (q.get("usedAMResource") or {}).get("memory", 0)
    user_am_limit_mb = (q.get("userAMResourceLimit") or {}).get("memory", 0)
    sat_pct = (am_used_mb / am_limit_mb * 100.0) if am_limit_mb else 0.0

    print("Scheduler Type               :", s_info.get("type", "capacityScheduler"))
    print(
        f"Queue Name                   : {q.get('queueName')} "
        f"(capacity: {q.get('capacity')}%, absolute: {q.get('absoluteCapacity')}%)"
    )
    print("maxAMLimitPercentage         :", f"{max_am_pct}% (recommended >= 80.0%)")
    print(
        "AMResourceLimit (Queue Limit):",
        humanize_mb(am_limit_mb),
    )
    print(
        "usedAMResource (Queue Used)  :",
        f"{humanize_mb(am_used_mb)} ({sat_pct:.1f}% of AM limit)",
    )
    print(
        "userAMResourceLimit (User Cap):",
        humanize_mb(user_am_limit_mb),
    )
    print("userLimitFactor              :", q.get("userLimitFactor"))
    print("maxApplications              :", q.get("maxApplications"))
    print("maxApplicationsPerUser       :", q.get("maxApplicationsPerUser"))
    print("Applications ACTIVE          :", q.get("numActiveApplications", 0))
    print("Applications PENDING         :", q.get("numPendingApplications", 0))
    print(
        "Cluster Memory               :",
        f"{humanize_mb(alloc_mb)} used / {humanize_mb(total_mb)} total ({humanize_mb(avail_mb)} free)",
    )

    print("\n=================== 3. ACTIVE USERS BREAKDOWN ===================")
    users = (q.get("users") or {}).get("user", [])
    if isinstance(users, dict):
        users = [users]
    if not users:
        print("No active users in queue.")
    for u in users:
        u_am = (u.get("AMResourceUsed") or {}).get("memory", 0)
        print(
            f"User: {u.get('username')} | Active Apps: {u.get('numActiveApplications')} | "
            f"Pending Apps: {u.get('numPendingApplications')} | AM Used: {humanize_mb(u_am)}"
        )
except Exception as exc:
    print(f"Failed to query scheduler/metrics: {exc}")

# ----------------------------------------------------------------------
# 3. Queued (ACCEPTED) Application Diagnostics
# ----------------------------------------------------------------------
print("\n=================== 4. QUEUED APPLICATION DIAGNOSTICS ===================")
if not accepted_apps:
    print("No applications currently queued in ACCEPTED.")
for a in accepted_apps[:5]:
    print(f"App ID      : {a.get('id')}")
    print(f"User        : {a.get('user')}")
    print(f"Name        : {a.get('name')}")
    print(f"State       : {a.get('state')}")
    print(f"Diagnostics : {(a.get('diagnostics') or '').strip()}")
    print("-" * 50)

# ----------------------------------------------------------------------
# 4. Check 3 Equivalent: Local /var/log/jupyter_kernel_gateway.log Timeouts
# ----------------------------------------------------------------------
print("\n=================== 5. LOCAL GATEWAY LAUNCH TIMEOUT LOGS ===================")
log_path = "/var/log/jupyter_kernel_gateway.log"
try:
    if os.path.exists(log_path):
        size = os.stat(log_path).st_size
        with open(log_path, "r", errors="replace") as f:
            if size > 20 * 1024 * 1024:
                f.seek(size - 20 * 1024 * 1024)
                f.readline()
            timeout_lines = [line.strip() for line in f if "launch timeout" in line]
        print(f"Timeout Events Found (tail)  : {len(timeout_lines)}")
        for tl in timeout_lines[-5:]:
            print(f"  * {tl}")
    else:
        print(f"Log file {log_path} not present on master node.")
except Exception as exc:
    print(f"Failed to read {log_path}: {exc}")
