#!/usr/bin/env bash
# Vigia dos jobs: roda dentro do container do jobmanager e chama o submit.sh a
# cada FLINK_WATCH_INTERVAL_S segundos. O cluster Flink sobe vazio; sem este
# laco, um restart do jobmanager (OOM, reboot da VPS) ou um job que esgotou as
# tentativas deixava o pipeline parado ate o proximo deploy.
set -uo pipefail

INTERVAL=${FLINK_WATCH_INTERVAL_S:-120}

# da tempo do jobmanager escrever a configuracao e abrir a API REST
sleep "${FLINK_WATCH_DELAY_S:-20}"

while true; do
  /opt/submit.sh 2>&1 | sed 's/^/[watch] /' || echo "[watch] submit falhou; nova tentativa em ${INTERVAL}s"
  sleep "$INTERVAL"
done
