-- Fontes do job de trending (Fase 2).
--
-- O topico carrega Avro com Schema Registry, entao a leitura usa o formato
-- avro-confluent. `event_time` e o createdAt do post: e o tempo de evento, e
-- as janelas andam nele. `ingest_time` fica para medir latencia.
--
-- Nao ha views intermediarias de proposito: no Flink 1.20 uma view com UNNEST
-- antes da janela quebra o planner ("belongs to a different planner"). As duas
-- agregacoes leem a tabela posts direto (03_jobs.sql e 04_volume.sql).

SET 'table.local-time-zone' = 'UTC';

CREATE TABLE posts (
  uri STRING,
  did STRING,
  rkey STRING,
  event_time STRING,
  ingest_time STRING,
  seq BIGINT,
  text STRING,
  langs ARRAY<STRING>,
  self_labels ARRAY<STRING>,
  hashtag_entities ARRAY<ROW<tag STRING, `start` INT, `end` INT>>,
  -- createdAt chega em dois formatos: "2026-11-12T03:08:51.123Z" e
  -- "2026-11-12T03:08:51+00:00". O offset e cortado antes do CAST; um valor
  -- que nao converte vira NULL e o filtro de cada job descarta.
  ts AS CAST(
    SUBSTRING(
      REPLACE(REPLACE(event_time, 'T', ' '), 'Z', ''),
      1, 19
    ) AS TIMESTAMP(3)
  ),
  -- 2 minutos de tolerancia: o Jetstream entrega quase em ordem, mas um post
  -- com relogio adiantado nao pode travar a janela para sempre
  WATERMARK FOR ts AS ts - INTERVAL '2' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = 'bluesky.posts.pt.v1',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-trending',
  'scan.startup.mode' = 'latest-offset',
  'properties.auto.offset.reset' = 'latest',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://schema-registry:8081'
);
