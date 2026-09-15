#!/bin/bash
# =============================================================================
# Customization Script: enable_fast_culling.sh
# Purpose: Configures automated idle kernel culling on Jupyter Kernel Gateway
#          (180s timeout, 60s poll interval, cull_connected=True).
# Note: On Secure Multi-Tenant Dataproc clusters, initialization actions are
#       blocked by the Dataproc API at creation time. Use this script as a
#       customization script when building a Dataproc Custom Image via:
#         python generate_custom_image.py --customization-script=...
# =============================================================================

set -euo pipefail

JUPYTERGW_CONFIG_FILE="/etc/jupyter/jupyter_kernel_gateway_config.py"
ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role 2>/dev/null || echo "")"

# Only configure on the master node
if [[ "${ROLE}" == "Master" ]]; then
  echo "Applying fast culling configuration to ${JUPYTERGW_CONFIG_FILE}..."
  mkdir -p /etc/jupyter
  cat >> "${JUPYTERGW_CONFIG_FILE}" << 'EOF'

# --- Fast Idle Culling Configuration (Testing) ---
c.MappingKernelManager.cull_idle_timeout = 180
c.MappingKernelManager.cull_interval = 60
c.MappingKernelManager.cull_connected = True
c.MappingKernelManager.cull_busy = False
EOF

  # If the service is already running, restart it to pick up the updated settings
  if systemctl is-active --quiet jupyterkernelgateway; then
    echo "Restarting jupyterkernelgateway.service..."
    systemctl restart jupyterkernelgateway
  fi
  echo "Fast culling configuration applied successfully."
fi

