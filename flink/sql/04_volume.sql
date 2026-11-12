-- Volume por minuto (TUMBLE de 1 minuto), no tempo de evento.
-- Post com rotulo proprio adulto sai de todos os agregados publicos (ADR-012),
-- inclusive deste.
INSERT INTO volume_sink
SELECT
  window_start,
  window_end,
  COUNT(*) AS posts
FROM TABLE(
  TUMBLE(TABLE posts, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
)
WHERE ts IS NOT NULL
  AND ts > TIMESTAMP '2020-01-01 00:00:00'
  AND ts < CURRENT_TIMESTAMP + INTERVAL '1' DAY
  AND (self_labels IS NULL OR NOT (
    ARRAY_CONTAINS(self_labels, 'porn') OR ARRAY_CONTAINS(self_labels, 'sexual')
    OR ARRAY_CONTAINS(self_labels, 'nudity') OR ARRAY_CONTAINS(self_labels, 'graphic-media')
  ))
GROUP BY window_start, window_end;
