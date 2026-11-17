#!/usr/bin/env bash
set -euo pipefail

echo "=== Installing Docker Engine, Docker Compose v2, and Python tools in WSL Ubuntu ==="

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y ca-certificates curl python3 python3-pip python3-venv git

install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc

ARCH="$(dpkg --print-architecture)"
CODENAME="$(. /etc/os-release && echo "$VERSION_CODENAME")"
echo "deb [arch=${ARCH} signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${CODENAME} stable" > /etc/apt/sources.list.d/docker.list

apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Add invoking non-root user to docker group in local WSL dev environment
TARGET_USER="${SUDO_USER:-$USER}"
if [[ -n "$TARGET_USER" && "$TARGET_USER" != "root" ]] && id "$TARGET_USER" >/dev/null 2>&1; then
  usermod -aG docker "$TARGET_USER"
fi

service docker start || dockerd >/tmp/dockerd.log 2>&1 &
sleep 2

echo "=== Verification ==="
docker --version
docker compose version
echo "=== DONE: Docker & Compose are ready in WSL Ubuntu ==="
