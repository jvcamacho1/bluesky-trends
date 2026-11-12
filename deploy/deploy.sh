#!/usr/bin/env bash
# Deploy do bluesky-trends na VPS compartilhada: sincroniza o codigo e sobe a
# stack no Docker rootless do usuario bluesky. Roda sem privilegio de root.
#
# Uso local:
#   deploy/deploy.sh bluesky@203.0.113.10 /srv/bluesky
#   SSH_PORT=2222 deploy/deploy.sh bluesky@203.0.113.10
#
# O script so sobe a stack depois de publicar as paginas: a pagina publica em
# public/index.html e o painel interno em private/index.html.
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

echo "==> publicando as paginas (publica e interna)"
ssh "${SSH_OPTS[@]}" "$HOST" bash -s -- "$REMOTE_PATH" <<'REMOTE'
set -euo pipefail
REMOTE_PATH="$1"
mkdir -p "$REMOTE_PATH/public" "$REMOTE_PATH/private"
install -m 644 "$REMOTE_PATH/docs/public.html" "$REMOTE_PATH/public/index.html"
install -m 644 "$REMOTE_PATH/docs/index.html" "$REMOTE_PATH/private/index.html"
REMOTE

# Uma sessao ssh nao carrega o ~/.bashrc, entao o PATH do Docker rootless
# (~/bin) e o DOCKER_HOST precisam ser montados aqui. O socket fica em
# /run/user/<uid>/docker.sock, que so existe com o daemon do usuario ativo.
REMOTE_ENV='export PATH="$HOME/bin:$PATH"; export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"; export DOCKER_HOST="unix://$XDG_RUNTIME_DIR/docker.sock"'

echo "==> conferindo o Docker rootless"
ssh "${SSH_OPTS[@]}" "$HOST" bash -s <<REMOTE
set -euo pipefail
$REMOTE_ENV
if ! command -v docker >/dev/null 2>&1; then
  echo "docker nao encontrado em \$HOME/bin. Instale o Docker rootless (README, passo 3)."
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "daemon rootless parado; tentando iniciar o servico do usuario"
  systemctl --user start docker || true
  sleep 5
fi
if ! docker info >/dev/null 2>&1; then
  echo "docker indisponivel para este usuario mesmo com DOCKER_HOST=\$DOCKER_HOST"
  echo "verifique: systemctl --user status docker e loginctl show-user \$(id -un) -p Linger"
  exit 1
fi
if [ "\$(docker info --format '{{.CgroupDriver}}')" != "systemd" ]; then
  echo "cgroup driver nao e systemd: os limites de memoria/CPU do Compose nao valem."
  echo "habilite a delegacao de cgroup v2 (ver README, passo 4)."
  exit 1
fi
echo "docker rootless ok: \$(docker info --format '{{.ServerVersion}}')"
REMOTE

echo "==> subindo a stack"
ssh "${SSH_OPTS[@]}" "$HOST" bash -s -- "$REMOTE_PATH" <<REMOTE
set -euo pipefail
$REMOTE_ENV
REMOTE_PATH="\$1"
cd "\$REMOTE_PATH"
docker compose up -d --build --remove-orphans
docker compose ps
REMOTE

echo "==> pronto: https://blueskytrends.jvcamacho.dev/"

