#!/usr/bin/env bash
set -euo pipefail

# Deploy MPFS offchain service to preprod (umpfs.plutimus.com)
#
# Usage: ./scripts/deploy-preprod.sh
#
# Prerequisites:
#   - nix build .#docker-image must have been run
#   - ssh production must be configured

VERSION=$(nix eval .#version --raw)
IMAGE="ghcr.io/lambdasistemi/cardano-mpfs-offchain/mpfs-serve:${VERSION}"

echo "=== Deploying mpfs-serve:${VERSION} to preprod ==="

# 1. Transfer docker image to server
echo "--- Transferring docker image..."
docker load < result
docker save "${IMAGE}" | ssh production 'docker load'

# 2. Update compose file with new image tag
echo "--- Updating docker-compose on server..."
ssh production "sed -i 's|image: ghcr.io/lambdasistemi/cardano-mpfs-offchain/mpfs-serve:.*|image: ${IMAGE}|' ~/services/umpfs/docker-compose.yaml"

# 3. Restart service
echo "--- Restarting umpfs-preprod..."
ssh production 'cd ~/services/umpfs && docker compose up -d'

# 4. Wait for service to be ready
echo "--- Waiting for service..."
for i in $(seq 1 30); do
    if curl -sf https://umpfs.plutimus.com/status >/dev/null 2>&1; then
        echo "--- Service is up!"
        curl -s https://umpfs.plutimus.com/status | jq .
        exit 0
    fi
    sleep 2
done

echo "ERROR: Service did not come up in 60 seconds"
ssh production 'docker logs --tail 50 umpfs-preprod'
exit 1
