-- Trending de hashtags: HOP de 5 minutos deslizando 1 minuto.
-- `authors` conta did distintos, que e o que tira robo do topo; `posts` fica
-- ao lado para dar contexto.
INSERT INTO trending_sink
SELECT
  window_start,
  window_end,
  LOWER(TRIM(t.tag)) AS term,
  'hashtag' AS kind,
  COUNT(DISTINCT p.did) AS authors,
  COUNT(*) AS posts
FROM TABLE(
  HOP(TABLE posts, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)
) AS p
CROSS JOIN UNNEST(p.hashtag_entities) AS t(tag, tag_start, tag_end)
WHERE t.tag IS NOT NULL AND TRIM(t.tag) <> ''
  AND p.ts IS NOT NULL
  AND p.ts > TIMESTAMP '2020-01-01 00:00:00'
  AND p.ts < CURRENT_TIMESTAMP + INTERVAL '1' DAY
  AND (p.self_labels IS NULL OR NOT (
    ARRAY_CONTAINS(p.self_labels, 'porn') OR ARRAY_CONTAINS(p.self_labels, 'sexual')
    OR ARRAY_CONTAINS(p.self_labels, 'nudity') OR ARRAY_CONTAINS(p.self_labels, 'graphic-media')
  ))
GROUP BY window_start, window_end, LOWER(TRIM(t.tag));
