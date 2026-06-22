#!/usr/bin/env bash
# Run on the DigitalOcean droplet to pull latest code and rebuild the worker image.
# See README.md — section "Updating after code changes".

set -euo pipefail

APP_DIR="/opt/close-sync-worker"
IMAGE_NAME="close-sync-worker"
ENV_FILE="${APP_DIR}/.env"

cd "${APP_DIR}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing ${ENV_FILE}. Create it first (see README section 4)." >&2
  exit 1
fi

echo "==> Pulling latest code from GitHub..."
git pull

echo "==> Building Docker image: ${IMAGE_NAME}..."
docker build -t "${IMAGE_NAME}" .

echo "==> Deploy complete."
echo "    Cron will use the new image on the next scheduled run."
echo ""
echo "Optional manual test:"
echo "  docker run --rm --env-file ${ENV_FILE} ${IMAGE_NAME}"
echo "  docker run --rm --env-file ${ENV_FILE} ${IMAGE_NAME} --phase partners --debug"
