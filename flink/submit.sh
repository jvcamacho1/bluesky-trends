#!/usr/bin/env bash
# Submete os jobs de trending no cluster Flink.
#
# Cada arquivo de job vira um job com nome fixo (pipeline.name = bluesky-<arquivo>).
# O script olha a lista de jobs vivos e submete so os que faltam, entao pode
# rodar quantas vezes for preciso: o watch.sh chama a cada poucos minutos e
# repoe o job que caiu ou que sumiu quando o jobmanager reiniciou.
#
# Os jobs leem o Kafka com latest-offset e sem checkpoint, entao resubmeter nao
# reprocessa janela antiga: o job novo continua a partir de agora.
#
# FORCE_SUBMIT=1 submete todos, mesmo os que ja estao rodando (duplica janela;
# so para depuracao).
set -euo pipefail

FLINK_BIN=${FLINK_BIN:-/opt/flink/bin}
SQL_CLIENT="$FLINK_BIN/sql-client.sh"
FLINK="$FLINK_BIN/flink"
SQL_DIR=${SQL_DIR:-/opt/sql}
WORK_DIR=${SUBMIT_WORK_DIR:-/tmp}
JM=${FLINK_JOBMANAGER:-flink-jobmanager:8081}

# fontes e sinks precisam estar no mesmo catalogo de cada sessao de INSERT,
# entao cada job envia os arquivos base junto
BASE=("01_sources.sql" "02_sinks.sql")
JOBS=("03_jobs.sql" "04_volume.sql" "05_terms.sql")

# uma submissao por vez: o watch.sh e uma chamada manual (make flink-submit)
# nao podem ver a mesma lacuna e submeter o mesmo job duas vezes
exec 9>"$WORK_DIR/bluesky-submit.lock"
flock 9

echo "==> esperando o jobmanager em $JM"
pronto=0
for _ in $(seq 1 60); do
  if "$FLINK" list -m "$JM" >/dev/null 2>&1; then
    pronto=1
    break
  fi
  sleep 2
done
if [ "$pronto" != "1" ]; then
  echo "jobmanager nao respondeu em $JM"
  exit 1
fi

# -r lista os jobs vivos (RUNNING, RESTARTING, CREATED...). Um job que o Flink
# esta reiniciando sozinho conta como vivo, para nao ganhar um gemeo.
vivos=$("$FLINK" list -r -m "$JM" 2>/dev/null || true)

job_name() {
  echo "bluesky-${1%.sql}"
}

submit() {
  local file="$1"
  local name
  name=$(job_name "$file")
  echo "==> submetendo $name"
  {
    cat "${BASE[@]/#/$SQL_DIR/}"
    echo "SET 'pipeline.name' = '$name';"
    cat "$SQL_DIR/$file"
  } > "$WORK_DIR/job.sql"
  "$SQL_CLIENT" -f "$WORK_DIR/job.sql"
}

faltando=0
for file in "${JOBS[@]}"; do
  name=$(job_name "$file")
  if [ "${FORCE_SUBMIT:-0}" != "1" ] && grep -qF " : $name (" <<<"$vivos"; then
    continue
  fi
  faltando=$((faltando + 1))
  submit "$file"
done

if [ "$faltando" = "0" ]; then
  echo "==> ${#JOBS[@]} jobs em execucao; nada a submeter"
else
  echo "==> $faltando job(s) submetido(s)"
  "$FLINK" list -r -m "$JM" || true
fi
