# GCP Dataproc Gateway Diagnostics Tool

**Tool:** `dataproc-gateway-diagnostics` v0.1.0  
**Repository:** `https://github.com/Royston88/gcp-workbench-gateway-diagnostics-tool`  
**Audience:** Anyone investigating Jupyter Kernel Gateway `HTTP 500` or `TimeoutError` failures on Dataproc.  
**Runtime:** One notebook cell. Read-only. No cluster changes.

---

## What this tool answers

When a kernel fails to launch, YARN shows applications sitting in `ACCEPTED` and the notebook shows:

```
TimeoutError: Timeout waiting for kernel_id ... launch timeout: 120
```

There are four plausible causes, and they have **conflicting remediations**. Adding workers fixes one and wastes money on another. This tool determines which one you actually have.

| # | Check | Cause it tests |
|---|---|---|
| 1 | Zombie / Idle Kernel Sessions | Abandoned kernels holding ApplicationMaster slots |
| 2 | YARN ApplicationMaster Capacity | AM budget exhausted while memory is free ← **most common** |
| 3 | Kernel Gateway Launch Timeouts | Launch timeout too short for cold starts |
| 4 | Spark Driver / AM Sizing | Per-kernel footprint too large for expected concurrency |

> [!TIP]
> Checks are numbered so that **causes appear before symptoms**. When several fail, the lowest-numbered failure is flagged `PRIMARY ROOT CAUSE` — fix that one first.

---

## Step 1 — Confirm prerequisites

**The cluster must have Component Gateway enabled.** Checks 1 and 2 read YARN through it.

```bash
gcloud dataproc clusters describe <CLUSTER> --region=<REGION> \
    --format="value(config.endpointConfig.enableHttpPortAccess)"
```

Expect `True`. If empty or `False`, Checks 1 and 2 will report `SKIPPED`.

### Required IAM Permissions & Technical Rationale

The required permissions must be granted to the **identity executing the script**:
* **Inside Vertex AI Workbench (notebook cell or terminal):** Grant the roles to the **Workbench Instance Service Account** (e.g. `ds-user-1-svc@<PROJECT>.iam.gserviceaccount.com`), or to the end-user identity if user credential delegation is enabled.
* **Outside Workbench (Cloud Shell, Cloudtop, local developer machine):** Grant the roles to the authenticating user account (`gcloud auth login`) or service account (`GOOGLE_APPLICATION_CREDENTIALS`).

#### Permissions Matrix

| Predefined Role | Minimum IAM Permission | Target API / Endpoint Called | Technical Rationale & Failure Mode |
|---|---|---|---|
| `roles/dataproc.viewer` | `dataproc.clusters.get` | `GET https://dataproc.googleapis.com/v1/projects/{project}/regions/{region}/clusters/{cluster}` | **Cluster Metadata & Endpoint Resolution:** Discovers cluster state, hardware capacity, cluster software properties (`softwareConfig.properties` for YARN and Jupyter settings), and reads `config.endpointConfig.httpPorts` to discover dynamic reverse-proxy Component Gateway URLs.<br><br>*Failure Mode:* If missing, the script halts immediately with `AccessDenied` (`HTTP 403`). |
| `roles/dataproc.editor` *(or custom role)* | `dataproc.clusters.use` | HTTP requests routed via `https://<hash>.dataproc.googleusercontent.com/gateway/default/...` | **Component Gateway Ingress:** Authorizes HTTP requests routed through Google Cloud Component Gateway to access cluster-internal Web UIs without VPN or SSH tunnels.<br>Specifically accesses:<br>1. **YARN ResourceManager REST API** (`/ws/v1/cluster/metrics`, `/ws/v1/cluster/scheduler`, `/ws/v1/cluster/apps`) for Checks 1, 2, and 4.<br>2. **Jupyter Kernel Gateway REST API** (`/api/kernels`) for Check 1.<br><br>*Key Gotcha:* `roles/dataproc.viewer` **does not** include `dataproc.clusters.use`. Accessing Component Gateway endpoints with only `dataproc.viewer` results in `HTTP 403 Forbidden`. |
| `roles/logging.viewer` | `logging.entries.list` | `POST https://logging.googleapis.com/v2/entries:list` | **Log Inspection:** Queries Cloud Logging for `resource.type="cloud_dataproc_cluster"` and `log_name=.../jupyter_kernel_gateway` to detect kernel launch timeout exceptions, cold-start latency, and stack traces (Check 3).<br><br>*Failure Mode:* If missing, Check 3 degrades gracefully to `[?] SKIPPED`. |

#### Quick Grant (Predefined Roles)

```bash
# For Vertex AI Workbench instance service account:
gcloud projects add-iam-policy-binding <PROJECT> \
    --member="serviceAccount:<WORKBENCH_SA_EMAIL>" \
    --role="roles/dataproc.viewer"

gcloud projects add-iam-policy-binding <PROJECT> \
    --member="serviceAccount:<WORKBENCH_SA_EMAIL>" \
    --role="roles/dataproc.editor"

gcloud projects add-iam-policy-binding <PROJECT> \
    --member="serviceAccount:<WORKBENCH_SA_EMAIL>" \
    --role="roles/logging.viewer"
```

#### Least-Privilege Custom Role (Enterprise Standard)

In security-conscious enterprise environments, granting `roles/dataproc.editor` may violate least-privilege compliance, as `dataproc.editor` permits cluster mutation and job submission. To provide strictly read-only diagnostic access, deploy this minimal Custom IAM Role:

```bash
gcloud iam roles create DataprocGatewayDiagnosticsAuditor \
    --project=<PROJECT_ID> \
    --title="Dataproc Gateway Diagnostics Auditor" \
    --description="Read-only permissions for diagnosing Jupyter Kernel Gateway and YARN via Component Gateway" \
    --permissions="dataproc.clusters.get,dataproc.clusters.use,logging.entries.list" \
    --stage="GA"
```

And bind it to the Workbench Service Account or user:

```bash
gcloud projects add-iam-policy-binding <PROJECT_ID> \
    --member="serviceAccount:<WORKBENCH_SA_EMAIL>" \
    --role="projects/<PROJECT_ID>/roles/DataprocGatewayDiagnosticsAuditor"
```

> [!NOTE]
> Missing a role is not fatal. The affected check reports `[?] SKIPPED` with the exact role required, and the remaining checks still run.

---

## Step 2 — Install

Run this **in a notebook cell**:

```python
import sys
!git clone https://github.com/Royston88/gcp-workbench-gateway-diagnostics-tool.git ~/dataproc-gateway-diagnostics
!{sys.executable} -m pip install --no-deps -e ~/dataproc-gateway-diagnostics
```

> [!IMPORTANT]
> Use `{sys.executable}`, not a bare `pip`. On Vertex AI Workbench the JupyterLab **server** runs in `/opt/micromamba/envs/jupyterlab` while the notebook **kernel** runs `/opt/micromamba/bin/python3`. A bare `pip install` frequently targets the server environment, and the tool then fails inside cells with `FileNotFoundError: 'gateway-diag'`. `{sys.executable}` always resolves to the interpreter actually executing your cell.

`--no-deps` guarantees pip cannot upgrade, downgrade, or overwrite any existing Google Cloud library. The package has a single dependency, `google-auth`, already present on Workbench.

---

## Step 3 — Run

```python
import sys
PY = sys.executable

!{PY} -m dataproc_gateway_diagnostics diagnose \
    --project=<PROJECT> --region=<REGION> --cluster=<CLUSTER>
```

Run it **while the problem is occurring**. Checks 1 and 2 read live YARN state; on an idle cluster they will legitimately pass even if the cluster fails under load.

---

## Expected output — Case A: healthy cluster

```
=================================================================
           JUPYTER KERNEL GATEWAY & YARN CAPACITY AUDIT
=================================================================
Tool Version   : 0.1.0
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
[CHECK 1] Zombie / Idle Kernel Sessions
   -> Active kernels                : 1
   -> Busy (executing)              : 0
   -> Idle > 2h                     : 1
   -> Longest idle                  : 12d 11h 59m
   -> Running YARN applications     : 2 (older than 24h: 2)

   --- Configuration Status ---
   -> Gateway cull_idle_timeout     : Not configured (disabled)
   -> Gateway cull_connected        : False (open browser tabs block culling)
   -> YARN Application Lifetime     : UNLIMITED (no automatic reaper)

   --- Active Kernel Gateway Sessions ---

   -> [Kernel] 64fa53be...          : pyspark_yarn
      * State                       : idle (idle for 12d 11h 59m)
      * Active Connections          : 4 connected WebSocket client(s)
      * Associated YARN App         : application_1779383468488_0011

   --- Running YARN Applications ---

   -> [Active Gateway Session] application_1779383468488_0011
      * Name                        : 64fa53be-9cd7-4886-b382-08aac85d4eb2
      * User                        : ds-user-1-svc
      * Started                     : 2026-09-01 10:18:53 UTC (13d 3h 20m ago)
      * Allocation                  : 4.8 GB, 3 vCores, 2 container(s)
      * Host Node                   : pyspark-cluster...-w-1:8044

   -> [ORPHANED YARN APP] application_1779383468488_0005
      * Name                        : 67913c09-b89b-4f8b-9d48-e44954a67643
      * User                        : ds-user-1-svc
      * Started                     : 2026-09-01 09:31:27 UTC (13d 4h 8m ago)
      * Allocation                  : 4.8 GB, 3 vCores, 2 container(s)
      * Host Node                   : pyspark-cluster...-w-0:8044
      * Status                      : No active gateway session; driver still alive
   -> Verdict                       : [✗] FAIL
      1 idle kernel(s) and 2 long-running YARN application(s)
      (including 1 orphaned app) are holding ApplicationMaster
      capacity.

=================================================================
   Check 1  Zombie / Idle Kernel Sessions    : [✗] FAIL   <-- PRIMARY ROOT CAUSE
   Check 2  YARN ApplicationMaster Capacity  : [✓] PASS
   OVERALL                                   : [✗] FAIL
=================================================================
             RECOMMENDED REMEDIATION (priority order)
=================================================================
   1. Enable idle kernel culling on the Kernel Gateway
      (cull_idle_timeout=7200, cull_interval=300,
      cull_connected=True).
   2. Kill 1 orphaned YARN application(s): yarn application
      -kill <APP_ID> or via YARN ResourceManager Web UI.
   3. Shut down abandoned kernels: JupyterLab > Running
      Terminals and Kernels.
=================================================================
```

Exit code `1`. Reading it line by line:

| Line | Why it matters |
|---|---|
| `Gateway cull_connected : False` | Open browser tabs block culling even if `cull_idle_timeout` is configured. |
| `YARN Application Lifetime : UNLIMITED` | YARN has no lifetime monitor active to reap long-abandoned interactive drivers. |
| `[Active Gateway Session]` | PySpark session initiated from Workbench, currently idle with WebSocket connections holding the AM slot. |
| `[ORPHANED YARN APP]` | A Spark driver running on YARN with **no active kernel** on the gateway. Left behind after a gateway crash or ungraceful shutdown. |
| `Kill orphaned YARN application` | Orphaned apps cannot be culled through Jupyter; they must be terminated via `yarn application -kill <APP_ID>`. |

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
| **Check 1 FAIL** — idle kernels holding AMs | Enable culling: `c.MappingKernelManager.cull_idle_timeout = 7200`, `cull_interval = 300`, `cull_connected = True`, `cull_busy = False`; Kill orphaned YARN applications: `yarn application -kill <APP_ID>` |
| **Check 2 FAIL** — AM starvation | Raise the AM budget: `--properties='capacity-scheduler:yarn.scheduler.capacity.maximum-am-resource-percent=0.8'` |
| **Check 3 FAIL** — launch timeouts | Raise the timeout: `c.GatewayProvisionerBase.default_kernel_launch_timeout = 600`. A mitigation, not a cure — resolve Check 2 first |
| **Check 4 WARN** — concurrency ceiling too low | Lower `spark.driver.memory`, raise `maximum-am-resource-percent`, or add workers |

> [!WARNING]
> `yarn.scheduler.capacity.maximum-am-resource-percent` **cannot be changed on a running cluster.** Either set it at creation time, or edit `/etc/hadoop/conf/capacity-scheduler.xml` on the master and run `yarn rmadmin -refreshQueues`.

---

## Useful variations

```python
# Only the YARN AM capacity check — fast iteration while applying a fix
!{PY} -m dataproc_gateway_diagnostics diagnose --cluster=<CLUSTER> --checks=2

# Treat kernels idle beyond 30 minutes as zombies
!{PY} -m dataproc_gateway_diagnostics diagnose --cluster=<CLUSTER> --idle-hours=0.5

# Judge the concurrency ceiling against 25 expected users
!{PY} -m dataproc_gateway_diagnostics diagnose --cluster=<CLUSTER> --expected-users=25

# Machine-readable output for a support case
!{PY} -m dataproc_gateway_diagnostics diagnose --cluster=<CLUSTER> --json > gateway_audit.json
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

```python
!{PY} -m dataproc_gateway_diagnostics diagnose --cluster=<CLUSTER> --json > gateway_audit.json
```

The JSON includes the decision inputs, notably:

```json
{
  "am_saturation": 0.963,
  "cluster_available_mb": 16504,
  "cluster_apps_pending": 5,
  "starved_with_free_memory": true
}
```

`starved_with_free_memory: true` is the signature of an admission limit rather than memory exhaustion — the single most useful field for a support engineer.

---

## Safety

Strictly read-only. Every call is a `GET`, except YARN's scheduler-info `POST` and Cloud Logging's `entries:list` (a read operation despite the verb). The tool never kills kernels, submits jobs, or changes configuration. Safe against production.

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).
