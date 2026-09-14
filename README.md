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

**Grant the identity running the notebook these roles:**

| Role | Needed for |
|---|---|
| `roles/dataproc.viewer` | Read cluster configuration (all checks) |
| `roles/dataproc.editor` | Reach YARN via Component Gateway — supplies `dataproc.clusters.use` (Checks 1, 2, 4) |
| `roles/logging.viewer` | Read gateway logs (Check 3) |

On Vertex AI Workbench the relevant identity is the **instance service account**, not your user account:

```bash
gcloud projects add-iam-policy-binding <PROJECT> \
    --member="serviceAccount:<SA_EMAIL>" \
    --role="roles/dataproc.viewer"
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

> [!CAUTION]
> The instinctive response — add workers — **will not help**. More memory does not raise the AM budget if the percentage stays at `0.1`. You would pay for nodes and remain blocked.

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
| **Check 1 FAIL** — idle kernels holding AMs | Enable culling: `c.MappingKernelManager.cull_idle_timeout = 7200`, `cull_interval = 300`, `cull_connected = True`, `cull_busy = False` |
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
