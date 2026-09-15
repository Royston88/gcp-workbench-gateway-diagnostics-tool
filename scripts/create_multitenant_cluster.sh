#!/usr/bin/env bash
# =============================================================================
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
# =============================================================================
#
# Script: create_multitenant_cluster.sh
# Description: Provisions a hardened, enterprise multi-tenant Dataproc cluster
#              optimized for Vertex AI Workbench with Jupyter Kernel Gateway,
#              automated idle kernel culling, YARN ApplicationMaster capacity
#              headroom, YARN application lifetime reaping, and Apache Iceberg
#              with BigQuery Metastore Catalog integration.
#
# Usage:
#   ./scripts/create_multitenant_cluster.sh [CLUSTER_NAME] [PROJECT_ID] [REGION]
#
# Examples:
#   ./scripts/create_multitenant_cluster.sh
#   ./scripts/create_multitenant_cluster.sh my-pyspark-cluster
#   ./scripts/create_multitenant_cluster.sh my-pyspark-cluster my-project us-central1
# =============================================================================

set -euo pipefail

# -----------------------------------------------------------------------------
# Configuration & Defaults
# -----------------------------------------------------------------------------
TIMESTAMP="$(date +%Y%m%d%H%M%S)"
CLUSTER_NAME="${1:-"pyspark-cluster-multitenant-${TIMESTAMP}"}"
PROJECT_ID="${2:-$(gcloud config get-value project 2>/dev/null || echo "kenly-lakehouse-dev-1")}"
REGION="${3:-$(gcloud config get-value dataproc/region 2>/dev/null || echo "us-central1")}"
ZONE="${ZONE:-"${REGION}-a"}"

IMAGE_VERSION="2.3-debian12"
MASTER_MACHINE_TYPE="n1-standard-4"
MASTER_BOOT_DISK_SIZE="1000GB"
NUM_WORKERS=2
WORKER_MACHINE_TYPE="n1-standard-8"
WORKER_BOOT_DISK_SIZE="1000GB"
NETWORK_TAGS="dataproc-internal"

# Multi-tenancy user mapping: maps Dataproc system users and human identities to proxy service accounts
DEFAULT_USER_MAPPING="admin@kenly.altostrat.com:admin-svc@${PROJECT_ID}.iam.gserviceaccount.com"
DEFAULT_USER_MAPPING="${DEFAULT_USER_MAPPING},ds_user_1@kenly.altostrat.com:ds-user-1-svc@${PROJECT_ID}.iam.gserviceaccount.com"
DEFAULT_USER_MAPPING="${DEFAULT_USER_MAPPING},ds-user-1-svc@${PROJECT_ID}.iam.gserviceaccount.com:ds-user-1-svc@${PROJECT_ID}.iam.gserviceaccount.com"
DEFAULT_USER_MAPPING="${DEFAULT_USER_MAPPING},ds-user-2-svc@${PROJECT_ID}.iam.gserviceaccount.com:ds-user-2-svc@${PROJECT_ID}.iam.gserviceaccount.com"
DEFAULT_USER_MAPPING="${DEFAULT_USER_MAPPING},ds-user-3-svc@${PROJECT_ID}.iam.gserviceaccount.com:ds-user-3-svc@${PROJECT_ID}.iam.gserviceaccount.com"
USER_MAPPING="${USER_MAPPING:-"${DEFAULT_USER_MAPPING}"}"

ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-"gs://${PROJECT_ID}-iceberg-1"}"
ICEBERG_TMP_BUCKET="${ICEBERG_TMP_BUCKET:-"https://storage.googleapis.com/${PROJECT_ID}-tmp"}"

# Culling configuration (audited by gateway-diag)
CULL_IDLE_TIMEOUT="${CULL_IDLE_TIMEOUT:-"7200"}"
CULL_CONNECTED="${CULL_CONNECTED:-"true"}"
CULL_INTERVAL="${CULL_INTERVAL:-"300"}"
# Note: Dataproc rejects --initialization-actions on secure multi-tenant clusters
INIT_ACTIONS="${INIT_ACTIONS:-""}"

echo "============================================================================="
echo "Dataproc Multi-Tenant Cluster Provisioning"
echo "============================================================================="
echo "Cluster Name:  ${CLUSTER_NAME}"
echo "Project ID:    ${PROJECT_ID}"
echo "Region / Zone: ${REGION} / ${ZONE}"
echo "Image Version: ${IMAGE_VERSION}"
echo "Master Node:   1x ${MASTER_MACHINE_TYPE} (${MASTER_BOOT_DISK_SIZE})"
echo "Worker Nodes:  ${NUM_WORKERS}x ${WORKER_MACHINE_TYPE} (${WORKER_BOOT_DISK_SIZE})"
echo "User Mapping:  ${USER_MAPPING}"
echo "Culling Conf:  timeout=${CULL_IDLE_TIMEOUT}s, interval=${CULL_INTERVAL}s, connected=${CULL_CONNECTED}"
echo "Init Actions:  ${INIT_ACTIONS:-"None"}"
echo "============================================================================="

# -----------------------------------------------------------------------------
# Define Cluster Configuration Properties by Subsystem Group
# -----------------------------------------------------------------------------
CLUSTER_PROPERTIES=(
  # === Group 1: YARN Capacity Scheduler & AM Admission Limits ===
  # Critical: Set to 0.8 so ApplicationMasters can utilize up to 80% of queue memory.
  # Prevents kernel launch HTTP 500 / TimeoutError when free memory is abundant.
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
  # Enables cost-based optimizer and runtime bloom filter joins for high-throughput SQL
  "spark:spark.sql.cbo.enabled=true"
  "spark:spark.sql.optimizer.runtime.bloomFilter.join.pattern.enabled=true"

  # === Group 4: Apache Iceberg Runtime & BigQuery Metastore Catalog ===
  # Configures Spark Iceberg 1.6.1 runtime and BigQuery Metastore Catalog integration
  "spark:spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  "spark:spark.jars=https://storage-download.googleapis.com/maven-central/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar,${ICEBERG_TMP_BUCKET}/iceberg-bigquery-catalog-1.6.1-1.0.1-beta.jar"
  "spark:spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
  "spark:spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog"
  "spark:spark.sql.catalog.my_catalog.catalog-impl=org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog"
  "spark:spark.sql.catalog.my_catalog.gcp_location=${REGION}"
  "spark:spark.sql.catalog.my_catalog.gcp_project=${PROJECT_ID}"
  "spark:spark.sql.catalog.my_catalog.warehouse=${ICEBERG_WAREHOUSE}"

  # === Group 5: Dataproc Multi-Tenancy Engine ===
  # Mandatory when Jupyter Kernel Gateway is installed on Dataproc
  "dataproc:dataproc.dynamic.multi.tenancy.enabled=true"

  # === Group 6: Jupyter Kernel Gateway Idle Culling ===
  # Automatically terminate idle kernels after timeout (${CULL_IDLE_TIMEOUT}s), even with open browser tabs.
  # Polls every ${CULL_INTERVAL}s.
  "dataproc:jupyter.cull.idle.timeout=${CULL_IDLE_TIMEOUT}"
  "dataproc:jupyter.cull.connected=${CULL_CONNECTED}"
  "dataproc:jupyter.cull.interval=${CULL_INTERVAL}"

  # === Group 7: YARN Application Lifetime Reaper (Safety Net) ===
  # Automatically terminate any YARN application running longer than 24 hours (86400s).
  # Prevents abandoned interactive sessions from permanently occupying AM slots.
  "yarn:yarn.resourcemanager.app-lifetime-monitor.enable=true"
  "yarn:yarn.resourcemanager.app.max-lifetime=86400"
  "yarn:yarn.resourcemanager.app.default-lifetime=86400"
)

# -----------------------------------------------------------------------------
# Join Properties with gcloud Custom Delimiter Syntax (^|^...)
# -----------------------------------------------------------------------------
# The ^|^ prefix tells gcloud to split key-value pairs by '|' rather than ',',
# preventing embedded commas in jar URLs from breaking dictionary argument parsing.
PROPERTIES_ARG="^|^$(IFS='|'; echo "${CLUSTER_PROPERTIES[*]}")"

# -----------------------------------------------------------------------------
# Execute Cluster Creation Command
# -----------------------------------------------------------------------------
# Build extra flags
EXTRA_FLAGS=()
if [[ -n "${INIT_ACTIONS}" ]]; then
  EXTRA_FLAGS+=("--initialization-actions=${INIT_ACTIONS}")
fi

echo "Provisioning cluster '${CLUSTER_NAME}' in project '${PROJECT_ID}'..."
gcloud dataproc clusters create "${CLUSTER_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --zone="${ZONE}" \
  --image-version="${IMAGE_VERSION}" \
  --master-machine-type="${MASTER_MACHINE_TYPE}" \
  --master-boot-disk-type="pd-standard" \
  --master-boot-disk-size="${MASTER_BOOT_DISK_SIZE}" \
  --num-workers="${NUM_WORKERS}" \
  --worker-machine-type="${WORKER_MACHINE_TYPE}" \
  --worker-boot-disk-type="pd-standard" \
  --worker-boot-disk-size="${WORKER_BOOT_DISK_SIZE}" \
  --optional-components=JUPYTER_KERNEL_GATEWAY \
  --enable-component-gateway \
  --tags="${NETWORK_TAGS}" \
  --secure-multi-tenancy-user-mapping="${USER_MAPPING}" \
  --properties="${PROPERTIES_ARG}" \
  "${EXTRA_FLAGS[@]}"

echo "============================================================================="
echo "Cluster '${CLUSTER_NAME}' provisioned successfully!"
echo "Verify configuration with:"
echo "  gateway-diag diagnose --cluster=${CLUSTER_NAME} --project=${PROJECT_ID} --region=${REGION}"
echo "============================================================================="
