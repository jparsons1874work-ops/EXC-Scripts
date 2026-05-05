#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/betfair-scripts"
SERVICE_NAME="betfair-scripts.service"
SERVICE_SOURCE="${APP_DIR}/deploy/betfair-scripts.service.example"
SERVICE_TARGET="/etc/systemd/system/${SERVICE_NAME}"

if [[ ! -f "${SERVICE_SOURCE}" ]]; then
  echo "Missing service example: ${SERVICE_SOURCE}" >&2
  exit 1
fi

if [[ ! -f "${APP_DIR}/.env" ]]; then
  echo "Missing ${APP_DIR}/.env. Create it from .env.example before enabling the service." >&2
  exit 1
fi

cp "${SERVICE_SOURCE}" "${SERVICE_TARGET}"
systemctl daemon-reload
systemctl enable "${SERVICE_NAME}"
systemctl restart "${SERVICE_NAME}"
systemctl status "${SERVICE_NAME}" --no-pager
