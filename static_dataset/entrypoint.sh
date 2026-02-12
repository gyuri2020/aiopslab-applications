#!/bin/bash
# Static Dataset Container
# Stays alive so host can trigger processing via "docker exec"
# and agent can read telemetry via "docker exec".

set -e

echo "=== Static Dataset Container Ready ==="
echo "Namespace: ${NAMESPACE:-static-data}"

# Directory layout:
#   /root/raw/        → raw dataset (bind-mount, read-only, root-only)
#   /root/config.json → processing config (bind-mount, read-only, root-only)
#   /app/             → scripts (root-only, remove world access)
#   /agent/           → agent workspace (agent user only)
#     └── telemetry/  → processed data written by process_telemetry.py
mkdir -p /agent/telemetry
chown -R agent:agent /agent
chmod o-rwx /app

# Keep container alive for docker exec access
tail -f /dev/null
