#!/usr/bin/env bash
# Deploy do bluesky-trends na VPS: sincroniza o codigo e sobe a stack.
#
# Uso local:
#   deploy/deploy.sh deploy@203.0.113.10 /srv/bluesky
#
# Pela CI, o deploy.yml chama este script no runner com a chave do secret
# VPS_SSH_KEY. A chave local `bluesky` (~/.ssh/bluesky) serve para rodar na mao.
set -euo pipefail

HOST="${1:?uso: deploy.sh <usuario@host> [caminho-remoto]}"
REMOTE_PATH="${2:-/srv/bluesky}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bluesky}"
SSH_OPTS=(-o StrictHostKeyChecking=accept-new -o IdentitiesOnly=yes -i "$SSH_KEY")
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "==> rsync $REPO_ROOT -> $HOST:$REMOTE_PATH"
rsync -az --delete \
  --exclude '.git' \
  --exclude 'data' \
  --exclude 'docs/data.json' \
  --exclude '.env' \
  --exclude '__pycache__' \
  --exclude '.pytest_cache' \
  --exclude '.ruff_cache' \
  -e "ssh ${SSH_OPTS[*]}" \
  "$REPO_ROOT/" "$HOST:$REMOTE_PATH/"

echo "==> publicando o dashboard estatico"
ssh "${SSH_OPTS[@]}" "$HOST" bash -s -- "$REMOTE_PATH" <<'REMOTE'
set -euo pipefail
REMOTE_PATH="$1"
sudo install -d -o deploy -g deploy -m 755 "$REMOTE_PATH/public"
sudo install -o deploy -g deploy -m 644 "$REMOTE_PATH/docs/index.html" "$REMOTE_PATH/public/index.html"
REMOTE

echo "==> subindo a stack"
ssh "${SSH_OPTS[@]}" "$HOST" bash -s -- "$REMOTE_PATH" <<'REMOTE'
set -euo pipefail
REMOTE_PATH="$1"
cd "$REMOTE_PATH"
docker compose up -d --build
sudo systemctl restart bluesky-dashboard.service || true
sudo systemctl list-timers bluesky-dashboard.timer --no-pager || true
REMOTE

echo "==> pronto: https://<seu-dominio>/trends/"
