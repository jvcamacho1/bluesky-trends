-- Sinks do job: um topico Kafka para trending e outro para volume.
-- Fase 3 troca estes sinks pelo ClickHouse; por enquanto o Kafka deixa o
-- resultado observavel com kafka-console-consumer, sem inventar dado nenhum.

CREATE TABLE trending_sink (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  term STRING,
  kind STRING,
  authors BIGINT,
  posts BIGINT,
  PRIMARY KEY (window_start, term, kind) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'bluesky.trending.v1',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE volume_sink (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  posts BIGINT,
  PRIMARY KEY (window_start) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'bluesky.volume.v1',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);
