#!/usr/bin/env bash
# Deploy do bluesky-trends na VPS compartilhada: sincroniza o codigo e sobe a
# stack no Docker rootless do usuario deploy. Roda sem privilegio de root.
#
# Uso local:
#   deploy/deploy.sh deploy@203.0.113.10 /srv/bluesky
#   SSH_PORT=2222 deploy/deploy.sh deploy@203.0.113.10
#
# Variaveis de ambiente:
#   SSH_PORT   porta do ssh na VPS (padrao 22)
#   SSH_KEY    chave privada (padrao ~/.ssh/bluesky)
set -euo pipefail

HOST="${1:?uso: deploy.sh <usuario@host> [caminho-remoto]}"
REMOTE_PATH="${2:-/srv/bluesky}"
SSH_PORT="${SSH_PORT:-22}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bluesky}"
SSH_OPTS=(-p "$SSH_PORT" -o StrictHostKeyChecking=accept-new -o IdentitiesOnly=yes -i "$SSH_KEY")
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "==> rsync $REPO_ROOT -> $HOST:$REMOTE_PATH (porta $SSH_PORT)"
rsync -az --delete \
  --exclude '.git' \
  --exclude 'data' \
  --exclude 'public' \
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
mkdir -p "$REMOTE_PATH/public"
install -m 644 "$REMOTE_PATH/docs/index.html" "$REMOTE_PATH/public/index.html"
REMOTE

echo "==> conferindo o Docker rootless"
ssh "${SSH_OPTS[@]}" "$HOST" bash -s <<'REMOTE'
set -euo pipefail
if ! docker info >/dev/null 2>&1; then
  echo "docker indisponivel para este usuario. Rode como deploy:"
  echo "  systemctl --user start docker && export DOCKER_HOST=unix://\$XDG_RUNTIME_DIR/docker.sock"
  exit 1
fi
if docker info --format '{{.CgroupDriver}}' | grep -qv '^systemd$'; then
  echo "cgroup driver nao e systemd: os limites de memoria/CPU do Compose nao valem."
  echo "habilite a delegacao de cgroup v2 (ver README, passo 4)."
  exit 1
fi
REMOTE

echo "==> subindo a stack"
ssh "${SSH_OPTS[@]}" "$HOST" bash -s -- "$REMOTE_PATH" <<'REMOTE'
set -euo pipefail
REMOTE_PATH="$1"
cd "$REMOTE_PATH"
docker compose up -d --build --remove-orphans
docker compose ps
REMOTE

echo "==> pronto: https://trends.jvcamacho.dev/"

