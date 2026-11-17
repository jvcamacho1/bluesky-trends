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
  --
  -- createdAt vem do cliente, e ha cliente com relogio adiantado (ja chegou
  -- post 12 h no futuro). O watermark segue o MAIOR tempo de evento visto,
  -- entao um unico post desses empurrava o watermark para o futuro: todo
  -- minuto real fechava antes de os posts chegarem, que eram descartados
  -- como atrasados, e o volume_minutes ficava com buracos de 20 a 50 minutos.
  -- Por isso o tempo de evento tem teto: se o createdAt passa do horario em
  -- que o Jetstream recebeu o post (ingest_time) por mais de 1 minuto, vale
  -- o ingest_time. Computed column nao pode citar outra, dai a repeticao.
  ts AS CASE
    WHEN CAST(SUBSTRING(REPLACE(REPLACE(event_time, 'T', ' '), 'Z', ''), 1, 19) AS TIMESTAMP(3))
         > CAST(SUBSTRING(REPLACE(REPLACE(ingest_time, 'T', ' '), 'Z', ''), 1, 19) AS TIMESTAMP(3))
           + INTERVAL '1' MINUTE
    THEN CAST(SUBSTRING(REPLACE(REPLACE(ingest_time, 'T', ' '), 'Z', ''), 1, 19) AS TIMESTAMP(3))
    ELSE CAST(SUBSTRING(REPLACE(REPLACE(event_time, 'T', ' '), 'Z', ''), 1, 19) AS TIMESTAMP(3))
  END,
  -- 2 minutos de tolerancia para o Jetstream entregar fora de ordem
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
