#!/usr/bin/env bash
# Wrapper for docker run with dealsheet Google key file mounted.
set -euo pipefail

APP_DIR="/opt/close-sync-worker"
ENV_FILE="${APP_DIR}/.env"
KEY_FILE="${APP_DIR}/secrets/google-sa.pem"
IMAGE_NAME="close-sync-worker"

DOCKER_ARGS=(--rm --env-file "${ENV_FILE}")
if [[ -f "${KEY_FILE}" ]]; then
  DOCKER_ARGS+=(-v "${KEY_FILE}:/run/secrets/google-sa.pem:ro")
fi

exec docker run "${DOCKER_ARGS[@]}" "${IMAGE_NAME}" "$@"
