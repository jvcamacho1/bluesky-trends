-- Banco do bluesky-trends no ClickHouse (fase 3).
--
-- Duas tabelas, as duas com TTL para o disco nao crescer sem fim:
--   trending_terms: uma linha por (janela, termo, tipo) com autores e posts
--   volume_minutes: uma linha por minuto com o total de posts
--
-- A deduplicacao e pelo ReplacingMergeTree com chave de ordenacao que inclui a
-- janela e o termo: se o job reenviar a mesma janela (reprocessamento, restart),
-- a linha nova substitui a antiga na compactacao em vez de duplicar.

CREATE DATABASE IF NOT EXISTS bluesky;

CREATE TABLE IF NOT EXISTS bluesky.trending_terms
(
    window_start DateTime('UTC'),
    window_end   DateTime('UTC'),
    term         String,
    kind         LowCardinality(String),
    authors      UInt32,
    posts        UInt32,
    ingested_at  DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, kind, term)
TTL window_start + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS bluesky.volume_minutes
(
    window_start DateTime('UTC'),
    window_end   DateTime('UTC'),
    posts        UInt32,
    ingested_at  DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start)
TTL window_start + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;
