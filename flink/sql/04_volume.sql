-- Volume por minuto (TUMBLE de 1 minuto), no tempo de evento.
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
GROUP BY window_start, window_end;
