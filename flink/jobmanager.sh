#!/usr/bin/env bash
# Entrada do container do jobmanager: liga o vigia dos jobs em segundo plano e
# entrega o processo principal para o entrypoint oficial da imagem do Flink.
# Quando o container reinicia, o vigia volta junto e resubmete os jobs.
set -euo pipefail

# o vigia roda como o usuario flink, igual ao jobmanager depois do entrypoint
if [ "$(id -u)" = "0" ]; then
  gosu flink /opt/watch.sh &
else
  /opt/watch.sh &
fi

exec /docker-entrypoint.sh jobmanager
