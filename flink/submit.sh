#!/usr/bin/env bash
# Submete os jobs de trending no cluster Flink.
#
# Roda uma vez, depois que o jobmanager responde. Os SQLs sao enviados na ordem:
# fontes, sinks, trending e volume. Cada INSERT vira um job em execucao.
set -euo pipefail

SQL_CLIENT=/opt/flink/bin/sql-client.sh
SQL_DIR=/opt/sql
JM=${FLINK_JOBMANAGER:-flink-jobmanager:8081}

echo "==> esperando o jobmanager em $JM"
for _ in $(seq 1 60); do
  if /opt/flink/bin/flink list -m "$JM" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

run_sql() {
  local file="$1"
  echo "==> $file"
  "$SQL_CLIENT" -f "$SQL_DIR/$file"
}

# fontes e sinks precisam estar no mesmo catalogo de cada sessao de INSERT,
# entao cada job envia os arquivos base junto
BASE=("01_sources.sql" "02_sinks.sql")

submit() {
  local file="$1"
  echo "==> $file"
  cat "${BASE[@]/#/$SQL_DIR/}" "$SQL_DIR/$file" > /tmp/job.sql
  "$SQL_CLIENT" -f /tmp/job.sql
}

submit 03_jobs.sql
submit 04_volume.sql
submit 05_terms.sql

echo "==> jobs submetidos"
/opt/flink/bin/flink list -m "$JM" || true
