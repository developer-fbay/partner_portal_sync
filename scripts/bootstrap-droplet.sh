#!/usr/bin/env bash
# First-time install on a DigitalOcean droplet.
# Usage (on the droplet as root):
#   curl -fsSL https://raw.githubusercontent.com/developer-fbay/partner_portal_sync/main/scripts/bootstrap-droplet.sh | bash
# Or after cloning:
#   bash scripts/bootstrap-droplet.sh

set -euo pipefail

APP_DIR="/opt/close-sync-worker"
REPO_URL="https://github.com/developer-fbay/partner_portal_sync.git"
IMAGE_NAME="close-sync-worker"

echo "==> Installing docker + git if needed..."
apt-get update -qq
apt-get install -y docker.io git
systemctl enable --now docker

echo "==> Cloning repo to ${APP_DIR}..."
mkdir -p "${APP_DIR}"
if [[ -d "${APP_DIR}/.git" ]]; then
  cd "${APP_DIR}" && git pull
else
  git clone "${REPO_URL}" "${APP_DIR}"
  cd "${APP_DIR}"
fi

if [[ ! -f "${APP_DIR}/.env" ]]; then
  echo ""
  echo "ERROR: Create ${APP_DIR}/.env before continuing."
  echo "Copy from your local machine:"
  echo "  scp .env root@YOUR_DROPLET_IP:${APP_DIR}/.env"
  echo "  chmod 600 ${APP_DIR}/.env"
  exit 1
fi

chmod 600 "${APP_DIR}/.env"

echo "==> Building Docker image..."
docker build -t "${IMAGE_NAME}" .

touch /var/log/close-sync.log

echo "==> Installing cron jobs..."
(
  crontab -l 2>/dev/null | grep -v close-sync-worker | grep -v close-sync.log || true
  cat <<'CRON'
# Incremental sync every 30 minutes
*/30 * * * * docker run --rm --env-file /opt/close-sync-worker/.env close-sync-worker >> /var/log/close-sync.log 2>&1

# Full re-sync daily at 6:00 AM UTC
0 6 * * * docker run --rm --env-file /opt/close-sync-worker/.env close-sync-worker --mode full >> /var/log/close-sync.log 2>&1

# Lead details enrichment daily at midnight UTC (full Close fetch per lead)
0 0 * * * docker run --rm --env-file /opt/close-sync-worker/.env close-sync-worker --phase lead_details >> /var/log/close-sync.log 2>&1

# Partner activate/deactivate weekly (Mondays 7:00 AM UTC)
0 7 * * 1 docker run --rm --env-file /opt/close-sync-worker/.env close-sync-worker --phase partners >> /var/log/close-sync.log 2>&1
CRON
) | crontab -

echo "==> Done. Test with:"
echo "  docker run --rm --env-file ${APP_DIR}/.env ${IMAGE_NAME} --phase dealsheet --debug --skip-lock"
