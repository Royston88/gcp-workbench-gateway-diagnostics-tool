# GCP Dataproc Gateway Diagnostics Tool

**Tool:** `dataproc-gateway-diagnostics` v0.2.0  
**Repository:** `https://github.com/Royston88/gcp-workbench-gateway-diagnostics-tool`  
**Audience:** Anyone investigating Jupyter Kernel Gateway `HTTP 500` or `TimeoutError` failures on Dataproc.  
**Execution:** CLI / Terminal (Cloud Shell, Cloudtop, Local Workstation, CI/CD) or Vertex AI Workbench (Notebook Cell / Terminal). Read-only. No cluster changes.

---

## What this tool answers

When a kernel fails to launch, YARN shows applications sitting in `ACCEPTED` and the notebook shows:

```
TimeoutError: Timeout waiting for kernel_id ... launch timeout: 120
```

There are four plausible causes, and they have **conflicting remediations**. Adding workers fixes one and wastes money on another. This tool determines which one you actually have.

| # | Check | Cause it tests |
|---|---|---|
| 1 | Zombie / Idle Kernel Sessions | Abandoned kernels & orphaned YARN applications holding AM slots; audits YARN application lifetime reaper settings & session activity |
| 2 | YARN ApplicationMaster Capacity | AM budget exhausted while memory is free ← **most common**; attributes queue memory to active users |
| 3 | Kernel Gateway Launch Timeouts | Launch timeout too short for cold starts |
| 4 | Spark Driver / AM Sizing | Per-kernel footprint too large for expected concurrency |

> [!TIP]
> Checks are numbered so that **causes appear before symptoms**. When several fail, the lowest-numbered failure is flagged `PRIMARY ROOT CAUSE` — fix that one first.

---

## Step 1 — Confirm prerequisites

**The cluster must have Component Gateway enabled.** Checks 1 and 2 read YARN and Jupyter Kernel Gateway through it.

```bash
gcloud dataproc clusters describe <CLUSTER> --region=<REGION> \
    --format="value(config.endpointConfig.enableHttpPortAccess)"
```

Expect `True`. If empty or `False`, Checks 1 and 2 will report `SKIPPED`.

### User Personas & Access Models

The diagnostic tool is designed for two distinct operational personas with different visibility requirements and permission boundaries:

| Persona | Primary Goal | Recommended Execution Mode | Required IAM Permissions | Diagnostic Visibility |
|---|---|---|---|---|
| **Platform Administrator / Cluster Operator / SRE** | Full fleet audit, cluster capacity planning, triaging multi-tenant AM starvation, identifying rogue/abandoned tenant kernels and orphaned YARN apps across all users. | **Full Cluster Audit (Default)**<br>`gateway-diag diagnose --cluster=<CLUSTER>` | **Comprehensive Diagnostic Suite:**<br>• `dataproc.clusters.get`<br>• `dataproc.clusters.use`<br>• `logging.entries.list`<br>• *Optional:* `notebooks.instances.list`, `compute.instances.get`, `monitoring.timeSeries.list` | **Full Visibility:**<br>• All active kernels across all tenants<br>• All running YARN apps across the cluster<br>• Project-wide Workbench VM inventory & creator identities<br>• Multi-VM candidate disambiguation (Signals 1–3)<br>• Full unmasked multi-tenant YARN user breakdown |
| **Regular Data Scientist / Notebook User** | Self-service troubleshooting when personal notebook kernels fail to launch (`HTTP 500` / `TimeoutError`), checking personal AM allocation without seeing peer tenant workloads. | **Personal Scoped Audit**<br>`gateway-diag diagnose --cluster=<CLUSTER> --my-sessions-only` | **Irreducible Minimum (Standard Notebook Access):**<br>• `dataproc.clusters.get`<br>• `dataproc.clusters.use`<br>*(Zero extra admin, logging, or compute permissions needed)* | **Zero-Leakage Personal View:**<br>• Strictly caller's active notebook session(s) and YARN app(s)<br>• 100% fidelity in-situ session correlation (local notebook file & sidebar UI ID)<br>• Personal AM allocation (`My AM allocation: X GB`)<br>• Peer tenant sessions/apps omitted; peer usernames masked |

---

### Required IAM Permissions & Technical Rationale

The required permissions must be granted to the **identity executing the script**:
* **Inside Vertex AI Workbench (notebook cell or terminal):** Grant roles to the **Workbench Instance Service Account** (e.g. `ds-user-1-svc@<PROJECT>.iam.gserviceaccount.com`), or to the end-user identity if user credential delegation is enabled.
* **Outside Workbench (Cloud Shell, Cloudtop, local developer machine):** Grant roles to the authenticating user account (`gcloud auth login`) or service account (`GOOGLE_APPLICATION_CREDENTIALS`).

#### Permissions Matrix by Persona Tier

| Tier | Predefined Role | Minimum IAM Permission | Target API / Endpoint Called | Technical Rationale & Failure Mode |
|---|---|---|---|---|
| **Tier 1: Irreducible Minimum (Data Scientist)** | `roles/dataproc.viewer` | `dataproc.clusters.get` | `GET https://dataproc.googleapis.com/v1/projects/{project}/regions/{region}/clusters/{cluster}` | **Cluster Discovery & Component Gateway Resolution:** Discovers cluster state, hardware capacity, and dynamic reverse-proxy Component Gateway URLs.<br><br>*Failure Mode:* If missing, the tool exits immediately with `AccessDenied` (`HTTP 403`). |
| **Tier 1: Irreducible Minimum (Data Scientist)** | `roles/dataproc.editor` *(or custom role)* | `dataproc.clusters.use` | HTTP requests routed via `https://<hash>.dataproc.googleusercontent.com/gateway/default/...` | **Component Gateway Ingress:** Authorizes HTTP requests through Component Gateway to access:<br>1. **Jupyter Kernel Gateway REST API** (`/api/kernels`) for Check 1.<br>2. **YARN ResourceManager REST API** (`/ws/v1/cluster/scheduler`, `/metrics`, `/apps`) for Checks 1, 2, and 4.<br><br>*Key Gotcha:* `roles/dataproc.viewer` **does not** include `dataproc.clusters.use`. Both permissions are required for any notebook user to connect to Dataproc kernels. |
| **Tier 2: Full Audit & Logs (Platform Admin)** | `roles/logging.viewer` | `logging.entries.list` | `POST https://logging.googleapis.com/v2/entries:list` | **Launch Timeout Analysis (Check 3):** Queries Cloud Logging for `log_name=.../jupyter_kernel_gateway` to detect kernel launch timeout exceptions and cold-start latency.<br><br>*Failure Mode:* For Data Scientists without this role, Check 3 degrades gracefully to `[!] DEGRADED / [?] SKIPPED` without affecting Checks 1, 2, or 4. |
| **Tier 2: Multi-VM Disambiguation (Platform Admin)** | `roles/notebooks.viewer` + `roles/compute.viewer` | `notebooks.instances.list`<br>`compute.instances.get` | Vertex AI Workbench v2 API & Compute Engine REST API | **Cross-Tenant VM Resolution:** Discovers project-wide Workbench instances and reads Guest Attributes (Signal 1) to disambiguate which external VM owns which YARN application.<br><br>*Failure Mode:* Data Scientists running in-situ resolve their own VM locally via `127.0.0.1:8080` (100% confidence) without needing these APIs. External peer VMs are simply marked `[External to this VM]`. |
| **Tier 2: Metrics & Probing (Platform Admin)** | `roles/monitoring.viewer`<br>`roles/iap.tunnelResourceAccessor` | `monitoring.timeSeries.list` | Cloud Monitoring & IAP Tunnel SSH | **Signal 3 & Remote Probing:** Reads VM network egress metrics (Signal 3) and allows Non-Intrusive SSH via IAP tunnel to inspect remote JupyterLab sessions on peer VMs.<br><br>*Failure Mode:* Automatically skipped if unpermitted. |

---

### IAM Setup Recipes

#### Recipe 1: Regular Data Scientist (Least-Privilege / In-Situ Workbench)

A data scientist only needs the permissions already required to run notebooks against the Dataproc cluster. No administrative, logging, or compute permissions are needed:

```bash
# 1. Allow reading Dataproc cluster metadata
gcloud projects add-iam-policy-binding <PROJECT> \
    --member="serviceAccount:<DATA_SCIENTIST_WORKBENCH_SA>" \
    --role="roles/dataproc.viewer"

# 2. Allow connecting through Component Gateway (least-privilege custom role)
gcloud iam roles create DataprocGatewayUser \
    --project=<PROJECT_ID> \
    --title="Dataproc Gateway User" \
    --description="Minimal permissions for a data scientist to connect to Component Gateway and diagnose personal sessions" \
    --permissions="dataproc.clusters.get,dataproc.clusters.use" \
    --stage="GA"

gcloud projects add-iam-policy-binding <PROJECT_ID> \
    --member="serviceAccount:<DATA_SCIENTIST_WORKBENCH_SA>" \
    --role="projects/<PROJECT_ID>/roles/DataprocGatewayUser"
```

#### Recipe 2: Platform Administrator / Cluster Operator (Full Fleet Diagnostics)

Deploy the comprehensive read-only `DataprocGatewayDiagnosticsAuditor` custom role for cluster administrators:

```bash
gcloud iam roles create DataprocGatewayDiagnosticsAuditor \
    --project=<PROJECT_ID> \
    --title="Dataproc Gateway Diagnostics Auditor" \
    --description="Comprehensive read-only permissions for diagnosing Kernel Gateway, YARN capacity, Cloud Logging, and Workbench correlation" \
    --permissions="dataproc.clusters.get,dataproc.clusters.use,logging.entries.list,notebooks.instances.list,compute.instances.get,monitoring.timeSeries.list" \
    --stage="GA"

gcloud projects add-iam-policy-binding <PROJECT_ID> \
    --member="user:<ADMIN_USER_EMAIL>" \
    --role="projects/<PROJECT_ID>/roles/DataprocGatewayDiagnosticsAuditor"
```

> [!NOTE]
> Missing an optional role is not fatal. The pre-flight matrix in the CLI header will report `[!] DEGRADED` for that specific capability, and the affected check reports `[?] SKIPPED` with the exact remediation command while the remaining core checks continue to run.

---

## Step 2 — Install

The tool can be installed either in an external terminal (Cloud Shell, Cloudtop, local workstation, CI/CD) or directly within a Vertex AI Workbench environment.

### Option A: External Terminal / Cloud Shell / Local Workstation

```bash
git clone https://github.com/Royston88/gcp-workbench-gateway-diagnostics-tool.git
cd gcp-workbench-gateway-diagnostics-tool
pip install --no-deps -e .
```

### Option B: Vertex AI Workbench (Notebook Cell or Terminal)

Run this directly **in a notebook cell**:

```python
import sys
!git clone https://github.com/Royston88/gcp-workbench-gateway-diagnostics-tool.git ~/dataproc-gateway-diagnostics
!{sys.executable} -m pip install --no-deps -e ~/dataproc-gateway-diagnostics
```

> [!IMPORTANT]
> When installing inside Workbench notebooks, use `{sys.executable}`, not a bare `pip`. On Vertex AI Workbench the JupyterLab **server** runs in `/opt/micromamba/envs/jupyterlab` while the notebook **kernel** runs `/opt/micromamba/bin/python3`. A bare `pip install` frequently targets the server environment, and the tool then fails inside cells with `FileNotFoundError: 'gateway-diag'`. `{sys.executable}` always resolves to the interpreter actually executing your cell.

`--no-deps` guarantees pip cannot upgrade, downgrade, or overwrite any existing Google Cloud library. The package has a single dependency, `google-auth`, already present in modern Google Cloud environments.

---

## Step 3 — Run

### Option A: Platform Admin / Cluster Operator (Full Fleet Audit)

Run from an external terminal, Cloud Shell, Cloudtop, or CI/CD to inspect all cluster sessions, cross-VM disambiguation signals, and multi-tenant AM allocation:

```bash
# Using the console script directly:
gateway-diag diagnose --project=<PROJECT> --region=<REGION> --cluster=<CLUSTER>

# Or via Python module:
python3 -m dataproc_gateway_diagnostics diagnose --project=<PROJECT> --region=<REGION> --cluster=<CLUSTER>
```

### Option B: Regular Data Scientist (Personal Scoped Audit)

Run directly inside a **notebook cell** or terminal in Vertex AI Workbench. Using `--my-sessions-only` isolates diagnostics strictly to your own active notebook sessions and personal YARN capacity while omitting peer tenant details:

```python
import sys
PY = sys.executable

# Personal scoped diagnosis (zero peer noise, zero permission errors):
!{PY} -m dataproc_gateway_diagnostics diagnose \
    --project=<PROJECT> --region=<REGION> --cluster=<CLUSTER> \
    --my-sessions-only
```

> [!TIP]
> Run it **while the problem is occurring**. Checks 1 and 2 read live YARN state; on an idle cluster they will legitimately pass even if the cluster fails under load.

---

## Expected output — Case A: healthy cluster

```
=================================================================
           JUPYTER KERNEL GATEWAY & YARN CAPACITY AUDIT
=================================================================
Tool Version   : 0.2.0
Target Cluster : pyspark-cluster-dev-multitenant
Cluster State  : RUNNING
Image Version  : 2.3.36-debian12
-----------------------------------------------------------------

[CHECK 2] YARN ApplicationMaster Capacity
   -> maximum-am-resource-percent   : 0.8 (recommended >= 0.8)
   -> AM memory used / limit        : 0 MB / 41.3 GB  (0.0%)
   -> Applications ACTIVE           : 0
   -> Applications PENDING (ACCEPTED): 0
   -> Cluster memory                : 0 MB used / 51.7 GB total  (51.7 GB free)
   -> Verdict                       : [✓] PASS

=================================================================
                             SUMMARY
=================================================================
   Check 1  Zombie / Idle Kernel Sessions    : [✓] PASS
   Check 2  YARN ApplicationMaster Capacity  : [✓] PASS
   Check 3  Kernel Gateway Launch Timeouts   : [✓] PASS
   Check 4  Spark Driver / AM Sizing         : [✓] PASS
   OVERALL                                   : [✓] PASS
=================================================================
                       No action required.
=================================================================
```

Exit code `0`. All four causes ruled out — look elsewhere (networking, image, gateway process health).

---

## Expected output — Case B: AM starvation

```
[CHECK 2] YARN ApplicationMaster Capacity
   -> Scheduler                     : capacityScheduler
   -> Queue examined                : default
   -> maximum-am-resource-percent   : 0.1 (recommended >= 0.8)
   -> AM memory used / limit        : 2.4 GB / 2.5 GB  (96.3%)
   -> Applications ACTIVE           : 1
   -> Applications PENDING (ACCEPTED): 5
   -> Active queue user(s)          : ds-user-1-svc (1 app(s), AM: 2.4 GB)
   -> Cluster memory                : 8.5 GB used / 24.7 GB total  (16.1 GB free)
   -> Verdict                       : [✗] FAIL
      AM STARVATION CONFIRMED: applications are queued in
      ACCEPTED while 16.1 GB of cluster memory is still free. The
      ApplicationMaster budget is 96.3% consumed.

=================================================================
   Check 2  YARN ApplicationMaster Capacity  : [✗] FAIL   <-- PRIMARY ROOT CAUSE
   Check 4  Spark Driver / AM Sizing         : [!] WARN
   OVERALL                                   : [✗] FAIL
=================================================================
             RECOMMENDED REMEDIATION (priority order)
=================================================================
   1. Recreate or reconfigure the cluster with
      --properties='capacity-scheduler:yarn.scheduler.capacity.maximum-am-resource-percent=0.8'
   2. Release AM capacity now by shutting down idle kernels (see Check 1).
=================================================================
```

Exit code `1`. Reading it line by line:

| Line | Why it matters |
|---|---|
| `maximum-am-resource-percent : 0.1` | Only 10% of the queue may hold ApplicationMasters. This is the constraint. |
| `AM memory used / limit : 2.4 GB / 2.5 GB (96.3%)` | The AM budget is effectively full. |
| `Applications PENDING (ACCEPTED): 5` | Five workloads are admitted but not started. |
| `Cluster memory : 16.1 GB free` | **The decisive signal.** Queuing with abundant free memory means an *admission limit*, not a capacity shortage. |
| `<-- PRIMARY ROOT CAUSE` | Fix this before anything else. |

---

## Expected output — Case C: Zombie / idle kernels & orphaned YARN apps

```
=================================================================
           JUPYTER KERNEL GATEWAY & YARN CAPACITY AUDIT
=================================================================
Tool Version      : 0.2.0
Generated At      : 2026-09-16 05:22:07 UTC
Project ID        : kenly-lakehouse-dev-1
Region ID         : us-central1
Target Cluster    : pyspark-cluster-e2e-20260915-v5
Cluster State     : RUNNING
Image Version     : 2.3.36-debian12
Active Account    : admin@kenly.altostrat.com
Execution Context : External GCE VM (Outside Workbench: kenly, Zone: asia-southeast1-b)
-----------------------------------------------------------------
[PRE-FLIGHT] IAM Permissions & Diagnostic Capabilities
   -> Core Diagnostic Checks     : [✓] FULL (Checks 1, 2, 3, 4 ready)
      * Dataproc Cluster API     : GRANTED (roles/dataproc.viewer)
      * Gateway REST / YARN API  : GRANTED (dataproc.clusters.use)
      * Cloud Logging Logs       : GRANTED (roles/logging.viewer)
   -> Multi-VM Disambiguation    : [✓] ENABLED (Signals 1, 2, 3 active)
      * Signal 1 Guest Attributes: GRANTED (compute.instances.get)
      * Signal 2 Serial Console  : GRANTED (logging.entries.list)
      * Signal 3 Cloud Monitoring: GRANTED (roles/monitoring.viewer)
   -> External In-Situ Probing   : [✓] AVAILABLE (Fallback Chain: 3 -> 2 -> 1 -> Cloud Logging)
      * Method 3 Inverting Proxy : SKIPPED (Requires browser session cookie or direct SA token (HTTP 401))
      * Method 2 IAP Tunnel      : DEGRADED (Port 8080 bound to 127.0.0.1 inside VM (Connection Refused))
      * Method 1 Non-Intr. SSH   : BLOCKED (Requires CorpSSH/SSO or instance SSH keys)
      * Safety Net Serial Trace  : ACTIVE (Cloud Logging /lab/tree/ referer)
-----------------------------------------------------------------

[CHECK 1] Zombie / Idle Kernel Sessions
   -> Active kernels                : 1
   -> Busy (executing)              : 0
   -> Idle > 2h                     : 0
   -> Longest idle                  : 1h 36m
   -> Running YARN applications     : 1 (older than 24h: 0)

   --- Configuration Status ---
   -> YARN Application Lifetime     : 86400s (1d 0h 0m max lifetime)

   --- Active Kernel Gateway Sessions ---

   -> [Kernel] 05a855ac...          : pyspark_yarn
      * Workbench VM                : instance-20260901-162000-single-svc (ACTIVE) (100% confidence via Signal 1 Guest Attributes, Signal 2 Serial Trace, Signal 3 Cloud Monitoring) [Alternative: instance-20251203-071523-single-svc, instance-20251120-225315-single-svc]
      * Workbench Owner             : ds_user_1@kenly.altostrat.com
      * Notebook File               : Dataproc_Gateway_Diagnostics.ipynb
      * Workbench UI ID             : [Unresolved] (Local sidebar session UUID; requires in-situ execution or Method 1/2 remote exec)
      * State                       : idle (idle for 1h 36m)
      * Active Connections          : 0 connected WebSocket client(s)
      * Associated YARN App         : application_1789471462247_0006

   --- Running YARN Applications ---

   -> [Active Gateway Session] application_1789471462247_0006
      * Name                        : 05a855ac-68d2-43df-9013-38a819b085a7
      * User                        : ds-user-1-svc
      * Started                     : 2026-09-16 01:31:18 UTC (3h 50m ago)
      * Allocation                  : 4.8 GB, 3 vCores, 2 container(s)
      * Host Node                   : pyspark-cluster...-w-0:8044
   -> Verdict                       : [✓] PASS
      1 active kernel(s), none idle beyond threshold.

=================================================================
                             SUMMARY
=================================================================
   Check 1  Zombie / Idle Kernel Sessions    : [✓] PASS
   Check 2  YARN ApplicationMaster Capacity  : [✓] PASS
   Check 3  Kernel Gateway Launch Timeouts   : [✓] PASS
   Check 4  Spark Driver / AM Sizing         : [✓] PASS
   OVERALL                                   : [✓] PASS
=================================================================
                       No action required.
=================================================================
```

Reading it line by line:

| Line | Why it matters |
|---|---|
| `Execution Context` | Automatically distinguishes whether the tool is running *in-situ* inside a Workbench VM or *outside* on a Cloudtop/workstation. |
| `[PRE-FLIGHT] IAM Permissions` | Upfront capability audit indicating caller permissions and active diagnostic signals/methods. |
| `* Workbench VM` | Identifies the Vertex AI Workbench Compute Engine instance that initiated the session. When multiple VMs share the SA, disambiguates via Signals 1, 2, and 3. |
| `* Workbench Owner` | Identifies the human creator / owner of the notebook instance (`creator` or `proxy-user-mail`). |
| `* Notebook File` | Discovered active `.ipynb` notebook file path (via remote probing or Cloud Logging serial console referer trace). |
| `* Workbench UI ID` | The local session UUID shown in JupyterLab's *Running Terminals and Kernels* left sidebar (populated when executing inside the Workbench VM or via remote probe). Explicitly marked `[Unresolved]` when blocked. |
| `YARN Application Lifetime : 86400s` | YARN lifetime monitor active to reap long-abandoned interactive drivers after 24 hours. |
| `[Active Gateway Session]` | PySpark session initiated from Workbench, currently idle with WebSocket connections holding the AM slot. |
| `[ORPHANED YARN APP]` | A Spark driver running on YARN with **no active kernel** on the gateway. Left behind after a gateway crash or ungraceful shutdown. |

---

## How to read your verdict

| Marker | Meaning | Action |
|---|---|---|
| `[✓] PASS` | Cause ruled out | None |
| `[!] WARN` | Not breaking now, will break under load | Plan a fix |
| `[✗] FAIL` | Actively causing launch failures | Fix now |
| `[?] SKIPPED` | Data source unreachable (usually IAM) | Grant the printed role and re-run |

### Finding → remediation

| Finding | Fix |
|---|---|
| **Check 1 FAIL** — idle kernels holding AMs | • **On running clusters:** Shut down idle kernels via JupyterLab (*Running Terminals and Kernels*) or kill orphaned YARN applications via master-local job (see below).<br>• **At cluster creation:** Enforce YARN application lifetime reaping (`yarn:yarn.resourcemanager.app.max-lifetime=86400`). *(Note: Dataproc multi-tenant clusters do not enforce OS daemon culling via cluster properties or init actions; see [Appendix B: Multi-Tenant Architectural Constraints](#appendix-b-multi-tenant-architectural-constraints) below).* |
| **Check 2 FAIL** — AM starvation | Raise the AM budget: `--properties='capacity-scheduler:yarn.scheduler.capacity.maximum-am-resource-percent=0.8'` *(Required at creation time; cannot be modified on running clusters).* |
| **Check 3 FAIL** — launch timeouts | • **On running clusters:** Resolve Check 2 (AM capacity) first; launch timeouts are almost always downstream symptoms of AM queuing rather than slow container starts.<br>• **At cluster creation:** Configure higher launch timeout if custom container environments require longer cold starts. *(Cannot be modified on running clusters).* |
| **Check 4 WARN** — concurrency ceiling too low | Lower `spark.driver.memory`, raise `maximum-am-resource-percent`, or add workers. |

### How to Deal with Idle Sessions on Running Clusters

Depending on whether a session is still active on Jupyter Kernel Gateway or abandoned in YARN, use the appropriate method below:

#### Approach 1: Clean Kernel Deletion via Component Gateway REST API (Recommended Programmatic Clean Way)

When `gateway-diag` reports an active kernel session that is idle (`[Kernel] <ID>`), the cleanest and most reliable way to terminate it is via the Jupyter Kernel Gateway REST API over Component Gateway:

```bash
# 1. Resolve Component Gateway URL for Jupyter and access token:
GATEWAY_URL=$(gcloud dataproc clusters describe <CLUSTER_NAME> \
  --region=<REGION> \
  --format="value(config.endpointConfig.httpPorts['Jupyter Kernel Gateway'])")

TOKEN=$(gcloud auth print-access-token)

# 2. Issue DELETE request to terminate the kernel:
curl -i -X DELETE "${GATEWAY_URL}api/kernels/<KERNEL_ID>" \
  -H "Authorization: Bearer ${TOKEN}"
```

**Why this is superior to killing YARN directly:**
* Kernel Gateway signals the local kernel launcher process to exit.
* The Spark driver JVM catches the process signal and executes its Java shutdown hook (`SparkContext.stop()`).
* The Spark driver cleanly invokes `unregisterApplicationMaster(SUCCEEDED)` with YARN ResourceManager.
* YARN **immediately destroys the AM container and reclaims the ~4.8 GB RAM within 3 seconds**.
* **Both** Kernel Gateway and YARN are completely clean — avoiding zombie sessions or orphaned drivers.

**Required IAM Permissions:**
* `dataproc.clusters.use`: Authorizes ingress traffic through Component Gateway.
* `dataproc.clusters.get`: Resolves the cluster's gateway URL.
* *(Contained in predefined roles `roles/dataproc.editor`, `roles/dataproc.admin`, or a minimal custom role `DataprocKernelTerminator`).*

---

#### Approach 2: End-User Interactive Self-Service (Inside Vertex AI Workbench)

End-users can terminate their own idle sessions directly from the JupyterLab notebook interface:
1. Open the left sidebar in JupyterLab.
2. Click the **Running Terminals and Kernels** tab (circle icon with a square stop button inside).
3. Under **Notebook Kernels**, click **Shut Down** next to any idle PySpark notebook.
*(Alternatively, in an open notebook, select **Kernel → Shut Down Kernel...** from the top menu).*

JupyterLab sends the `DELETE /api/kernels/<kernel-id>` call behind the scenes, triggering the Spark shutdown hook and releasing the YARN AM capacity immediately.

---

#### Approach 3: Killing Orphaned YARN Applications (Safety Fallback)

When `gateway-diag` identifies an `[ORPHANED YARN APP]` (a Spark driver running in YARN with **no active gateway kernel session** due to a past gateway crash or hard disconnect), it cannot be closed through Jupyter.

Because SSH is disabled on hermetic multi-tenant clusters, administrators can terminate orphaned YARN applications by submitting a master-local PySpark job. Because `spark.master=local[1]` executes directly inside the master VM driver without requesting a YARN container, it bypasses YARN AM admission limits:

```bash
gcloud dataproc jobs submit pyspark \
  --cluster=<CLUSTER_NAME> \
  --region=<REGION> \
  --properties="spark.master=local[1]" \
  -e '
import subprocess, sys
app_id = "<APPLICATION_ID>"  # e.g. application_1789456205936_0005
print(f"Terminating orphaned YARN application: {app_id}")
res = subprocess.run(["yarn", "application", "-kill", app_id], capture_output=True, text=True)
print("STDOUT:", res.stdout)
print("STDERR:", res.stderr)
sys.exit(res.returncode)
'
```

---

## Useful variations

```bash
# Terminal (replace with '!{PY} -m dataproc_gateway_diagnostics' if running inside a notebook cell):

# Only the YARN AM capacity check — fast iteration while applying a fix
gateway-diag diagnose --cluster=<CLUSTER> --checks=2

# Treat kernels idle beyond 30 minutes as zombies
gateway-diag diagnose --cluster=<CLUSTER> --idle-hours=0.5

# Judge the concurrency ceiling against 25 expected users
gateway-diag diagnose --cluster=<CLUSTER> --expected-users=25

# Machine-readable output for a support case
gateway-diag diagnose --cluster=<CLUSTER> --json > gateway_audit.json
```

### Full option reference

| Flag | Default | Description |
|---|---|---|
| `--cluster` | *(required)* | Dataproc cluster name |
| `--project` | auto-detected | Project ID |
| `--region` | auto-detected | Dataproc region |
| `--checks` | `all` | Subset, e.g. `--checks=2,3` |
| `--idle-hours` | `2` | Idle threshold for zombie kernels |
| `--app-age-hours` | `24` | Age threshold for long-running YARN apps |
| `--lookback-days` | `7` | Cloud Logging lookback window |
| `--expected-users` | `10` | Expected concurrent users (Check 4) |
| `--timeout` | `30` | HTTP timeout in seconds for Dataproc / YARN API calls |
| `--json` | off | Emit JSON instead of text |
| `--verbose` | off | Log every HTTP request |

### Exit codes

| Code | Meaning |
|---|---|
| `0` | All checks passed |
| `1` | At least one `WARN` or `FAIL` |
| `2` | Could not run (credentials, cluster not found) |

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `FileNotFoundError: 'gateway-diag'` | Installed into a different environment than the kernel | Reinstall with `{sys.executable}` (Step 2) |
| `[?] SKIPPED` on Checks 1/2 | Component Gateway disabled, or missing `dataproc.clusters.use` | Verify Step 1; grant the role the tool prints |
| `[?] SKIPPED` on Check 3 | Missing `roles/logging.viewer` | Grant it, or ignore — Check 3 is corroborating only |
| Everything passes but kernels still fail | Run happened while the cluster was idle | Re-run **during** the failure |

---

## Attaching evidence to a support case

```bash
# Terminal / Cloud Shell:
gateway-diag diagnose --cluster=<CLUSTER> --json > gateway_audit.json

# Inside Vertex AI Workbench (notebook cell):
!{PY} -m dataproc_gateway_diagnostics diagnose --cluster=<CLUSTER> --json > gateway_audit.json
```

The JSON output provides complete, machine-readable telemetry across all checks, including active sessions, orphaned YARN applications, idle culling parameters, YARN application lifetime reaper settings, and queue user attribution:

```json
{
  "tool_version": "0.2.0",
  "project_id": "<PROJECT_ID>",
  "region_id": "us-central1",
  "cluster_name": "pyspark-cluster-dev-multitenant",
  "overall_status": "FAIL",
  "primary_root_cause": 1,
  "checks": [
    {
      "check_id": 1,
      "name": "Zombie / Idle Kernel Sessions",
      "status": "FAIL",
      "metrics": {
        "active_kernels": 1,
        "busy_kernels": 0,
        "idle_kernels": 1,
        "running_yarn_apps": 2,
        "orphaned_yarn_apps": 1,
        "yarn_lifetime_config": {
          "expiry_time": "UNLIMITED",
          "is_unlimited": true
        },
        "kernels_detail": [
          {
            "id": "64fa53be-9cd7-4886-b382-08aac85d4eb2",
            "name": "pyspark_yarn",
            "execution_state": "idle",
            "connections": 4,
            "associated_app_id": "application_1779383468488_0011"
          }
        ],
        "yarn_apps_detail": [
          {
            "id": "application_1779383468488_0005",
            "name": "67913c09-b89b-4f8b-9d48-e44954a67643",
            "user": "ds-user-1-svc",
            "allocated_mb": 4800,
            "is_orphaned": true
          }
        ]
      }
    },
    {
      "check_id": 2,
      "name": "YARN ApplicationMaster Capacity",
      "status": "PASS",
      "metrics": {
        "am_saturation": 0.057,
        "cluster_available_mb": 48020,
        "cluster_apps_pending": 0,
        "starved_with_free_memory": false,
        "active_users": [
          {
            "username": "ds-user-1-svc",
            "active_apps": 1,
            "pending_apps": 0,
            "am_used_mb": 4800
          }
        ]
      }
    }
  ]
}
```

`starved_with_free_memory: true` is the signature of an admission limit rather than memory exhaustion — the single most useful field for a support engineer. Additionally, `orphaned_yarn_apps > 0` directly exposes driver leakage detached from active notebook kernels.

---

## Appendix A: Sample Cluster Provisioning Reference (`scripts/`)

To prevent kernel exhaustion, orphaned YARN drivers, and AM starvation from day one, provision Dataproc multi-tenant clusters with declarative properties baked in.

> [!NOTE]
> For the underlying technical constraints explaining why these settings must be configured at cluster creation time and cannot be modified on running clusters, see [Appendix B: Multi-Tenant Architectural Constraints](#appendix-b-multi-tenant-architectural-constraints).

A ready-to-use provisioning script is included in the repository at [`scripts/create_multitenant_cluster.sh`](scripts/create_multitenant_cluster.sh):

```bash
# 1. Quick start with environment file:
# Copy the generic demo template to scripts/.env.local (gitignored):
cp scripts/.env.example scripts/.env.local

# Edit scripts/.env.local with your project details, then run:
chmod +x scripts/create_multitenant_cluster.sh
./scripts/create_multitenant_cluster.sh

# Or pass parameters directly via positional arguments:
./scripts/create_multitenant_cluster.sh [CLUSTER_NAME] [PROJECT_ID] [REGION] [USER_MAPPING]
```

### Declarative `gcloud` Creation Template (Grouped Properties)

You can copy and paste the entire block below directly into your terminal. The configuration properties are cleanly categorized into subsystem groups and joined with `gcloud`'s custom delimiter syntax (`^|^...`) to prevent embedded commas in jar URLs from breaking argument parsing:

```bash
# -----------------------------------------------------------------------------
# 1. Define Cluster Configuration Properties by Subsystem Group
# -----------------------------------------------------------------------------
CLUSTER_PROPERTIES=(
  # === Group 1: YARN Capacity Scheduler & AM Admission Limits ===
  # Critical: Allows ApplicationMasters to consume up to 80% of total queue memory.
  # Prevents kernel launch HTTP 500 / TimeoutError when free cluster memory is abundant.
  "capacity-scheduler:yarn.scheduler.capacity.maximum-am-resource-percent=0.8"

  # === Group 2: Spark Driver, Executor & AM Compute Sizing ===
  # Driver (2g), AM container overhead (640m), 2 Executors (2 cores, 2g each)
  "spark:spark.driver.memory=2g"
  "spark:spark.driver.maxResultSize=1920m"
  "spark:spark.executor.memory=2g"
  "spark:spark.executor.cores=2"
  "spark:spark.executor.instances=2"
  "spark:spark.yarn.am.memory=640m"
  "spark:spark.scheduler.mode=FAIR"
  "spark:spark.executorEnv.OPENBLAS_NUM_THREADS=1"

  # === Group 3: Spark SQL Query Optimization ===
  # Enables cost-based optimizer and runtime bloom filter joins
  "spark:spark.sql.cbo.enabled=true"
  "spark:spark.sql.optimizer.runtime.bloomFilter.join.pattern.enabled=true"

  # === Group 4: Dataproc Multi-Tenancy Engine ===
  # Mandatory when Jupyter Kernel Gateway is installed on Dataproc
  "dataproc:dataproc.dynamic.multi.tenancy.enabled=true"

  # === Group 5: YARN Application Lifetime Reaper (Safety Net) ===
  # Automatically terminate any YARN application running longer than 24 hours (86400s).
  # Prevents abandoned interactive sessions from permanently occupying AM slots.
  "yarn:yarn.resourcemanager.app-lifetime-monitor.enable=true"
  "yarn:yarn.resourcemanager.app.max-lifetime=86400"
  "yarn:yarn.resourcemanager.app.default-lifetime=86400"

  # === Group 6: (Optional) Apache Iceberg Runtime & BigQuery Metastore Catalog ===
  # Uncomment to enable Spark Iceberg runtime with BigQuery Metastore Catalog integration
  # "spark:spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  # "spark:spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
  # "spark:spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog"
  # "spark:spark.sql.catalog.my_catalog.catalog-impl=org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog"
  # "spark:spark.sql.catalog.my_catalog.gcp_location=<REGION>"
  # "spark:spark.sql.catalog.my_catalog.gcp_project=<PROJECT_ID>"
  # "spark:spark.sql.catalog.my_catalog.warehouse=gs://<PROJECT_ID>-iceberg"
)

# -----------------------------------------------------------------------------
# 2. Execute Cluster Creation (Single Copy-Pasteable Invocation)
# -----------------------------------------------------------------------------
gcloud dataproc clusters create <CLUSTER_NAME> \
  --project=<PROJECT_ID> \
  --region=<REGION> \
  --zone=<REGION>-a \
  --image-version=2.3-debian12 \
  --master-machine-type=n1-standard-4 \
  --master-boot-disk-type=pd-standard \
  --master-boot-disk-size=1000GB \
  --num-workers=2 \
  --worker-machine-type=n1-standard-8 \
  --worker-boot-disk-type=pd-standard \
  --worker-boot-disk-size=1000GB \
  --optional-components=JUPYTER_KERNEL_GATEWAY \
  --enable-component-gateway \
  --tags=dataproc-internal \
  --secure-multi-tenancy-user-mapping="<USER_EMAIL>:<EXECUTION_SERVICE_ACCOUNT>" \
  --properties="^|^$(IFS='|'; echo "${CLUSTER_PROPERTIES[*]}")"
```

---

## Appendix B: Multi-Tenant Architectural Constraints

This appendix documents critical Google Cloud Dataproc architectural constraints that dictate why cluster configuration and automated culling must be handled declaratively at creation time (as implemented in [`scripts/create_multitenant_cluster.sh`](scripts/create_multitenant_cluster.sh)).

### 1. Configuration Parameters Cannot Be Modified on Running Multi-Tenant Clusters

> [!WARNING]
> **Hermetic VM Isolation Prevents Live In-Place Mutation:**  
> Multi-tenant Dataproc clusters automatically enforce **Hermetic VM Isolation** (`hermetic-vm: 'true'` and `block-project-ssh-keys: 'true'`), which disables the SSH daemon (`Connection refused` on port 22). In addition, Dataproc Component Gateway reverse-proxy blocks HTTP `PUT` requests (`405 Method Not Allowed`) to the YARN ResourceManager REST API, and Dataproc jobs execute as unprivileged user `admin` without passwordless `sudo` privileges.  
>  
> **What this means in practice:**  
> * **Jupyter Kernel Launch Timeouts** (`default_kernel_launch_timeout`) **CANNOT** be edited on a running cluster.  
> * **YARN Application Lifetime Reaper** (`yarn:yarn.resourcemanager.app-lifetime-monitor.*`) **CANNOT** be enabled or modified on a running cluster.  
> * **YARN AM Resource Limits** (`maximum-am-resource-percent`) **CANNOT** be changed on a running cluster.  
>  
> **Recommended Actions:**  
> 1. **Immediate remediation on existing clusters:** Terminate orphaned YARN applications using the master-local PySpark job command (see [Approach 3](#approach-3-killing-orphaned-yarn-applications-safety-fallback)), and shut down idle sessions via the JupyterLab UI.  
> 2. **Permanent solution:** Recreate or provision new clusters using declarative creation-time properties baked in (see [Sample Cluster Provisioning Reference](#appendix-a-sample-cluster-provisioning-reference-scripts), or use the reference script in [`scripts/create_multitenant_cluster.sh`](scripts/create_multitenant_cluster.sh)).

### 2. Why Jupyter Gateway Idle Culling Cannot Be Enforced via Cluster Properties or Initialization Actions

> [!IMPORTANT]
> **Architectural Constraint on Multi-Tenant Dataproc:**  
> You may notice that `dataproc:jupyter.cull.*` properties are omitted from the cluster provisioning template. This is due to a fundamental Google Cloud Dataproc architectural constraint:  
> 1. **No Native Property Mapping:** Dataproc does not recognize `dataproc:jupyter.cull.*` as an internal configuration prefix (gcloud emits `WARNING: Property 'dataproc:jupyter.cull.idle.timeout' is not a supported property`). Dataproc records the value in GCE cluster metadata, but **never writes it to `/etc/jupyter/jupyter_kernel_gateway_config.py`** on the VM. The VM daemon remains hardcoded to Dataproc's base image default of **12 hours** (`cull_idle_timeout = 43200`).  
> 2. **Initialization Actions Blocked:** Dataproc explicitly rejects `--initialization-actions` on secure multi-tenant clusters (`INVALID_ARGUMENT: Initialization actions are not supported for secure multi-tenant clusters`).  
> 3. **Kernel Gateway Requires Multi-Tenancy:** Dataproc's component activation scripts enforce that `JUPYTER_KERNEL_GATEWAY` is **only** supported when multi-tenancy is active (`if ! is_multi_tenant_enabled; then log_and_fail "Jupyter Kernel Gateway is only supported in multi-tenant clusters"`).  
>  
> **How to enforce automated daemon culling:** To enforce kernel culling at the OS daemon level on Dataproc multi-tenant clusters, organizations must bake the configuration into a **Dataproc Custom Image** using `generate_custom_image.py`.  
> **The Active Cluster Safety Net:** On standard multi-tenant clusters without custom images, the **YARN Application Lifetime Reaper** (`yarn:yarn.resourcemanager.app-lifetime-monitor.enable=true` and `yarn.resourcemanager.app.max-lifetime=86400`) **IS** natively translated into `/etc/hadoop/conf/yarn-site.xml` and enforced by Hadoop YARN to terminate runaway or abandoned applications after 24 hours. For daytime idle session hygiene, `gateway-diag` provides the vital observability to detect and terminate idle sessions before they exhaust AM capacity.

---

## Safety

Strictly read-only. Every call is a `GET`, except YARN's scheduler-info `POST` and Cloud Logging's `entries:list` (a read operation despite the verb). The tool never kills kernels, submits jobs, or changes configuration. Safe against production.

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).
