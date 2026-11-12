-- Trending de termos do texto: HOP de 5 minutos deslizando 1 minuto.
--
-- O Flink nao tem REGEXP_EXTRACT_ALL no SQL, entao a tokenizacao usa
-- SPLIT_INDEX sobre as primeiras 50 palavras do post. A palavra passa por
-- normalizacao (minuscula, sem pontuacao, sem acento) para a lista de palavras
-- vazias casar: "não" e "nao" viram o mesmo termo. E o caminho previsto na
-- ADR-009: o producer entrega texto cru, a agregacao decide o que e termo.
INSERT INTO trending_sink
SELECT
  window_start,
  window_end,
  term,
  'term' AS kind,
  COUNT(DISTINCT did) AS authors,
  COUNT(*) AS posts
FROM (
  SELECT
    window_start,
    window_end,
    p.did,
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            LOWER(SPLIT_INDEX(p.text, ' ', n)),
            '[^a-zà-ÿ0-9]', ''
          ),
          '[àáâãä]', 'a'
        ),
        '[éêë]', 'e'
      ),
      '[íîï]', 'i'
    ) AS term
  FROM TABLE(
    HOP(TABLE posts, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)
  ) AS p
  CROSS JOIN UNNEST(ARRAY[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
                          21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,
                          39,40,41,42,43,44,45,46,47,48,49]) AS i(n)
  WHERE SPLIT_INDEX(p.text, ' ', n) IS NOT NULL
    AND p.ts IS NOT NULL
    AND p.ts > TIMESTAMP '2020-01-01 00:00:00'
    AND p.ts < CURRENT_TIMESTAMP + INTERVAL '1' DAY
    AND (p.self_labels IS NULL OR NOT (
      ARRAY_CONTAINS(p.self_labels, 'porn') OR ARRAY_CONTAINS(p.self_labels, 'sexual')
      OR ARRAY_CONTAINS(p.self_labels, 'nudity') OR ARRAY_CONTAINS(p.self_labels, 'graphic-media')
    ))
) AS words
WHERE CHAR_LENGTH(term) >= 3
  AND term NOT IN (
    'que','com','para','por','dos','das','uma','nao','mais','como','mas','seu',
    'sua','seus','suas','ele','ela','eles','elas','isso','isto','aqui','ali',
    'muito','tambem','quando','onde','porque','entao','sobre','entre','depois',
    'antes','agora','ainda','sempre','nunca','todo','toda','todos','todas',
    'outro','outra','pelo','pela','pelos','pelas','esse','essa','esses','essas',
    'este','esta','estes','estas','aquele','aquela','vai','vou','vamos','sou',
    'estou','ser','ter','tem','tinha','hoje','fazer','gente','coisa','cara',
    'nada','anos','ano','mim','pode','off','quem','estou','quero','queria',
    'apenas','pro','pra','grupo','casa','lugar','sendo','menos','dois','amo'
  )
GROUP BY window_start, window_end, term;
