# bluesky-trends

Pipeline de streaming em tempo real que consome o **Jetstream do Bluesky**, filtra posts em português e calcula **trending de hashtags/termos**, **volume de posts por minuto** e **detecção de picos**, com painel ao vivo.

Projeto de portfólio de engenharia de dados: **Kafka (KRaft) + Schema Registry (Avro) + Flink SQL + ClickHouse + Grafana + Prometheus**, tudo em Docker Compose com limites de recursos, testes automatizados e CI/CD.

## Estado atual — Fase 1 (de 5)

- [x] **Fase 1** — Producer Python (Jetstream v2 → Kafka) + Kafka KRaft + Schema Registry, Avro com evolução de schema, tópico de dead letters, testes unitários e de integração, CI no GitHub Actions
- [x] **Fase 2** — Job Flink com janelas de trending (HOP 5 min / slide 1 min) e volume por minuto
- [x] **Fase 3** — ClickHouse com TTL e dedupe, painel `/historico/` com crescimento, ritmo da semana e volume
- [ ] **Fase 4** — Detecção de picos + métricas Prometheus + alertas
- [ ] **Fase 5** — Deploy na VPS (nginx + autenticação, health checks, CI/CD de deploy) — o dashboard e o CI/CD de deploy já estão prontos em `deploy/`, falta executar a instalação da VPS

## Arquitetura (alvo completo)

```mermaid
flowchart LR
    subgraph Bluesky
        JS[Jetstream v2\nwss: subscribeEvents\ncollections=app.bsky.feed.post\nkinds=commit]
    end

    subgraph Docker Compose
        P[Producer Python\nasyncio + websockets\nbackoff + cursor + filtro langs pt\nAvro + Schema Registry]
        K[(Kafka KRaft\n1 broker\ntópico posts 3 partições\nchave = did)]
        DLQ[(Kafka DLQ\nbluesky.posts.pt.dlq.v1)]
        SR[Schema Registry\nAvro, compatibilidade BACKWARD]
        F[Flink 1.20\nFlink SQL\nHOP windows + watermarks]
        CH[(ClickHouse\nReplacingMergeTree\nsink idempotente)]
        G[Grafana\npainel trending]
        PR[Prometheus\nlag, eventos/s, saúde]
    end

    JS -->|websocket| P
    P -->|serialize Avro| SR
    SR -->|schema id| P
    P -->|posts pt| K
    P -->|inválidas| DLQ
    K --> F
    F --> CH
    CH --> G
    PR --> G
```

## Como rodar local

Pré-requisitos: Docker com Compose v2, Python 3.12 (para os testes).

```bash
cp .env.example .env
make up          # Kafka + Schema Registry + producer (conecta no Jetstream real)
make logs-producer
make consume-check   # decodifica e imprime 10 mensagens Avro do tópico
make down
```

Testes e lint:

```bash
make install          # dependências de dev do producer
make test-unit        # parsing, filtro, Avro, cursor, pipeline
make test-integration # sobe Kafka+SR no Compose e valida o fluxo ponta a ponta
make lint
```

O teste de integração não depende do Bluesky: o producer tem um modo `INPUT_MODE=file` que lê um fixture JSONL (`producer/tests/fixtures/jetstream_frames.jsonl`) com os mesmos frames do formato v2. É esse modo que a CI usa.

### Painel público

A raiz de https://blueskytrends.jvcamacho.dev/ é pública, sem senha: mostra o que o Bluesky em português está falando agora, o volume de 24 horas e como o pipeline funciona. O painel de validação com posts brutos saiu da raiz e vive em `/interno/`, atrás de basic auth.

| Rota | Conteúdo | Acesso |
|---|---|---|
| `/` | `public/index.html` | aberto, `Cache-Control: public, max-age=300` |
| `/public.json` | agregados | aberto, `Cache-Control: public, max-age=30` |
| `/flink/` | `public/flink/index.html`: janelas do Flink ao vivo | aberto, `max-age=300` |
| `/flink.json` | saída dos jobs Flink | aberto, `max-age=30` |
| `/interno/` | painel de validação e o `data.json` com posts brutos | senha |

- O `data.json` fica em `/srv/bluesky/private/`, nunca em `public/`.
- `/trends/` responde 301 para `/`.
- O navegador só lê arquivos estáticos: não existe consulta ao banco exposta.

#### Contrato do `public.json`

```json
{
  "generated_at": "2026-11-11T19:48:27Z",
  "kpis": {"posts_per_minute": 27, "posts_24h": 41380, "authors_1h": 1204, "latency_p95_ms": 1800},
  "trending": {"ranking": "authors", "items": [
    {"term": "exemplo", "kind": "hashtag", "authors": 812, "posts": 1930, "ratio": null, "spark": [2, 2, 3, 3, 4, 6, 9, 14, 22, 30, 28, 25]}
  ]},
  "volume_24h": [{"t": "2026-11-10T19:50:00Z", "posts_per_minute": 24}],
  "spikes": [],
  "week_heatmap": null,
  "pipeline": {"uptime_s": null, "events_per_s": null, "lag": null, "dlq_ratio": null}
}
```

- `volume_24h` tem 288 pontos (um por balde de 5 minutos); `trending.items` tem até 10 itens com `spark` de 12 baldes.
- `ranking` é `authors` enquanto não há histórico no ClickHouse; com 3 dias ou mais, vira `growth` e `ratio` passa a ser `(autores na última hora + 5) / (média da mesma hora nos últimos 7 dias + 5)`.
- `spikes` e `week_heatmap` só existem nas fases 3 e 4. Campo sem dado vale `null` ou lista vazia, e a página esconde o bloco.
- Todos os horários são UTC; a página converte para o horário de Brasília.

#### Regras de conteúdo

1. **Só agregados.** O arquivo público nunca contém texto de post, `did`, `uri` nem `rkey`. O `did` só é usado em memória, para contar autores distintos.
2. **Conteúdo adulto fica fora.** Um post é descartado de todos os agregados se o autor o marcou com rótulo próprio (`porn`, `sexual`, `nudity`, `graphic-media`) ou se alguma hashtag ou palavra dele está em `producer/src/producer/blocklist_pt.txt`.
3. **Mínimo de autores.** Um termo só aparece com pelo menos 10 autores distintos na janela (`PUBLIC_MIN_AUTHORS`).
4. **Contar pessoas, não posts.** Cada autor conta uma vez por termo por janela, o que tira robôs do topo.

O serviço `public-export` do Compose mantém as contagens em memória, em baldes de 5 minutos das últimas 24 horas. Ao iniciar, relê o tópico uma vez para se aquecer e depois só consome o que chega; a cada 60 segundos grava `public.json.tmp` e troca por `mv`.

### Dashboard de validação (servido pela VPS)

Um dashboard estático e minimalista (`docs/index.html`) lê `data.json` e mostra volume de posts por minuto, hashtags mais frequentes e os últimos posts ingeridos. Ele é servido pelo nginx do host em **https://blueskytrends.jvcamacho.dev/** (raiz do subdomínio, com basic auth). O `data.json` é escrito a cada 5 minutos pelo serviço `dashboard-export` do Compose, que lê o Kafka da própria stack e troca o arquivo de forma atômica (`data.json.tmp` → `mv`).

Arquivos em `deploy/`:

| Arquivo | O que faz |
|---|---|
| `nginx-bluesky-trends.conf` | `blueskytrends.jvcamacho.dev` na raiz, basic auth, `no-store` no `data.json`, 301 de `/trends/` para `/` |
| `nginx-grafana.conf` | `grafana.jvcamacho.dev` com `proxy_pass` para `127.0.0.1:3000` e cabeçalhos de WebSocket |
| `deploy.sh` | rsync do repo para a VPS por ssh (porta em `SSH_PORT`, chave `bluesky`), publica o HTML e sobe a stack no Docker rootless |

O deploy não usa root: o usuário `bluesky` opera o Docker rootless e é dono de `/srv/bluesky`. O workflow `.github/workflows/deploy.yml` roda o mesmo `deploy.sh` em push para `main`.

## Instalação na VPS (uma vez, pelo dono da máquina, como root)

A VPS é compartilhada: já hospeda outro site em produção na porta 8000, com nginx e certificados no host. Nada abaixo toca nesse site. O projeto ainda **não** está em produção; estes passos são o que falta.

1. **Usuário de deploy, sem sudo.** A conta é `bluesky`, sem grupo `sudo`, com a chave pública `bluesky` em `authorized_keys`. Se ainda não existir:

   ```bash
   adduser --disabled-password --gecos "" bluesky
   install -d -m 700 -o bluesky -g bluesky /home/bluesky/.ssh
   # cole a linha de ~/.ssh/bluesky.pub:
   install -m 600 -o bluesky -g bluesky /dev/stdin /home/bluesky/.ssh/authorized_keys
   ```

2. **Diretório do projeto.** `/srv/bluesky` pertence a `bluesky`:

   ```bash
   install -d -m 755 -o bluesky -g bluesky /srv/bluesky
   install -d -m 755 -o bluesky -g bluesky /srv/bluesky/public
   ```

3. **Docker rootless.** Instale o Docker Engine com o extra rootless e configure pelo usuário `bluesky`:

   ```bash
   apt-get install -y uidmap slirp4netns fuse-overlayfs dbus-user-session
   sudo -iu bluesky sh -c 'curl -fsSL https://get.docker.com/rootless | sh'
   sudo -iu bluesky dockerd-rootless-setuptool.sh install
   sudo loginctl enable-linger bluesky
   ```

   No `~/.bashrc` do `bluesky`, exporte `DOCKER_HOST=unix://$XDG_RUNTIME_DIR/docker.sock` e `PATH=$HOME/bin:$PATH`.

4. **Delegação de cgroup v2.** Sem isso os limites de memória e CPU do Compose não valem. Confirme `systemd.unified_cgroup_hierarchy=1` e delegue os controladores:

   ```bash
   mkdir -p /etc/systemd/system/user@.service.d
   cat >/etc/systemd/system/user@.service.d/delegate.conf <<'EOF'
   [Service]
   Delegate=cpu cpuset io memory pids
   EOF
   systemctl daemon-reload
   ```

   O `deploy.sh` confere `docker info --format '{{.CgroupDriver}}'` e falha se não for `systemd`.

5. **Cloudflare.** Na zona `jvcamacho.dev`, crie os registros `trends` e `grafana` apontando para a VPS, com o proxy ligado, e deixe o modo de criptografia em **Full (strict)**. A origem só aceita 80 e 443, vindos da Cloudflare.

6. **nginx e certificados.** O certificado de origem vem da Cloudflare Origin CA (Full strict), salvo em `/etc/ssl/cloudflare/jvcamacho.dev.pem` e `.key`, cobrindo `*.jvcamacho.dev`. Não use certbot: o navegador vê o certificado da borda da Cloudflare. Instale os arquivos do projeto, crie o htpasswd e recarregue:

   ```bash
   install -m 644 deploy/nginx-bluesky-trends.conf /etc/nginx/sites-available/bluesky-trends
   install -m 644 deploy/nginx-grafana.conf        /etc/nginx/sites-available/bluesky-grafana
   ln -sf /etc/nginx/sites-available/bluesky-trends /etc/nginx/sites-enabled/bluesky-trends
   ln -sf /etc/nginx/sites-available/bluesky-grafana /etc/nginx/sites-enabled/bluesky-grafana
   htpasswd -c /etc/nginx/.htpasswd-trends <usuario>
   nginx -t && systemctl reload nginx
   ```

   Só esses dois arquivos são do projeto; não altere a configuração do outro site.

7. **Secrets no GitHub.** No ambiente `vps` do repositório: `VPS_HOST`, `VPS_USER`, `VPS_PORT`, `VPS_SSH_KEY` (conteúdo de `~/.ssh/bluesky`) e `VPS_KNOWN_HOSTS` (`ssh-keyscan -p <porta> -H <host>`). `VPS_PATH` é opcional e vale `/srv/bluesky`.

8. **Primeiro deploy.** Da sua máquina, com a chave `bluesky`:

   ```bash
   SSH_PORT=<porta> make deploy HOST=bluesky@SEU_IP
   ```

   Em até 6 minutos o `dashboard-export` escreve `/srv/bluesky/public/data.json` e o subdomínio passa a mostrar dados.

### Serviços e orçamento de memória

| Serviço | Imagem | Limite | Heap |
|---|---|---|---|
| Kafka (KRaft combined, 1 broker) | `confluentinc/cp-kafka:7.7.1` | 1 GB / 0.5 CPU | 512 MB |
| Schema Registry | `confluentinc/cp-schema-registry:7.7.1` | 512 MB / 0.3 CPU | 256 MB |
| Producer | build local | 256 MB / 0.5 CPU | — |
| Dashboard export | build local (mesma imagem) | 256 MB / 0.2 CPU | — |

Total da stack hoje: **2,5 GB de memória e 1,8 CPU**, dentro do orçamento de 7 GB e 3 CPUs que a VPS compartilhada permite. Todo serviço novo (Flink, ClickHouse, Grafana, Prometheus) nasce com limite de memória e CPU, e heap explícito quando for JVM. O ClickHouse entra com `max_server_memory_usage` abaixo do limite do container e TTL nas tabelas.

Porta publicada pelo Docker ignora o firewall do host, então todo `ports:` do Compose começa com `127.0.0.1:`. O teste `producer/tests/unit/test_compose_ports.py` roda na CI e falha se alguma porta escapar do loopback, se algum serviço ficar sem limite ou se a soma passar do orçamento.

Kafka e Schema Registry publicam apenas em `127.0.0.1` (nada exposto à internet); o Kafka expõe um listener `HOST` em `127.0.0.1:29092` para ferramentas rodando no host (o listener `PLAINTEXT` interno anuncia `kafka:9092` e só é resolvível dentro do Compose). Na VPS (Fase 5) essas portas saem do Compose e o Grafana entra atrás do nginx existente com autenticação.

## Fase 3: histórico no ClickHouse

O ClickHouse guarda as janelas que o Flink fecha, para a página `/historico/` olhar além da última hora.

| Rota | Conteúdo | Acesso |
|---|---|---|
| `/historico/` | crescimento por termo, ritmo da semana, volume por hora | aberto |
| `/historico.json` | os mesmos dados, `Cache-Control: public, max-age=30` | aberto |

- **Duas tabelas**, as duas com `ReplacingMergeTree` e TTL: `bluesky.trending_terms` (7 dias) e `bluesky.volume_minutes` (30 dias). A chave de ordenação inclui a janela e o termo, então reenviar a mesma janela substitui a linha em vez de duplicar.
- **Quem grava:** o serviço `ch-sink`, que lê os tópicos do Flink e insere em lote pelo HTTP do ClickHouse. O conector JDBC do Flink 3.4 não tem factory para ClickHouse, e forçar a URL de outro banco faz o driver errado ser carregado; um consumidor Python resolve sem gambiarra.
- **Quem lê:** o serviço `ch-export`, que roda `SELECT` a cada minuto e regrava `historico.json`. Nenhuma consulta ao banco sai do servidor.
- **Crescimento** só aparece com pelo menos 3 horas de histórico: `ratio = (autores na última hora + 5) / (média da mesma hora nas últimas 24h + 5)`. Antes disso a lista ordena por autores e a página diz isso.
- **Memória:** o container tem 1 GB e o `max_server_memory_usage` fica em 700 MB. Os logs de sistema do ClickHouse (`metric_log`, `query_log`, `trace_log`) ficam desligados: eram eles que estouravam o limite.

## Fase 2: janelas no Flink

O cluster Flink 1.20 LTS roda em `docker-compose.flink.yml`, incluído pelo compose principal. São três jobs SQL, todos lendo o tópico Avro pelo conector `avro-confluent`:

| Job | Janela | Saída |
|---|---|---|
| trending de hashtags | `HOP` 5 min, slide 1 min, `COUNT(DISTINCT did)` | `bluesky.trending.v1` (`kind=hashtag`) |
| trending de termos | `HOP` 5 min, slide 1 min, tokenização por `SPLIT_INDEX` | `bluesky.trending.v1` (`kind=term`) |
| volume por minuto | `TUMBLE` 1 min | `bluesky.volume.v1` |

- **Watermark** de 2 minutos sobre `event_time` (o `createdAt` do post). `ingest_time` não entra em janela: ele existe para medir latência.
- **Tempo de evento inválido é descartado**, não derruba o job. O Jetstream às vezes entrega `createdAt` com offset (`+00:00`) ou lixo; o `ts` é cortado nos 19 primeiros caracteres e cada job filtra `ts IS NOT NULL`.
- **Autores, não posts.** `COUNT(DISTINCT did)` é o que tira robô do topo; `posts` fica ao lado para dar contexto.
- **Termos do texto** são extraídos no Flink, como diz a ADR-009: 50 primeiras palavras por post, sem pontuação, sem acento, com lista de palavras vazias.
- **Sem views intermediárias** antes da janela: no Flink 1.20 uma view com `UNNEST` ou filtro sobre a tabela com watermark quebra o planner (`belongs to a different planner`). As agregações leem a tabela `posts` direto.
- **Recursos:** jobmanager 1,75 GB / 0,4 CPU e taskmanager 1,75 GB / 0,5 CPU, com heap explícito. A stack inteira fica em 6,25 GB e 3,0 CPUs, dentro do orçamento de 7 GB e 3.

Como usar:

```bash
make up            # sobe tudo, incluindo o cluster Flink
make flink-submit  # submete os tres jobs
make flink-jobs    # lista os jobs em execucao
make trending      # ultimas janelas de trending
make volume        # ultimas janelas de volume
```

A interface do Flink fica em `http://127.0.0.1:8082` (só loopback). A Fase 3 troca os tópicos de saída pelo ClickHouse.

### Página `/flink/`

A página `docs/flink.html` (publicada em `/flink/`) mostra o que os três jobs escrevem, com a identidade visual do portfólio: o caminho nascente → Flink → termos em alta, a janela HOP mais recente com minigráfico das 12 anteriores e o volume por minuto das últimas 3 horas. Cada ponto que corre nos canos é um post, no ritmo do último minuto fechado; nada é simulado.

O serviço `flink-export` do Compose (128 MB / 0,1 CPU, sem porta) lê `bluesky.trending.v1` e `bluesky.volume.v1`, começa pelos últimos minutos com `offsets_for_times` e grava `flink.json` a cada 30 segundos, com troca atômica.

- **Só janela assentada.** O Flink regrava a mesma chave enquanto chegam posts daquela janela (upsert). O exportador guarda o último valor de cada chave e só usa janelas cujo fim passou de agora − 3 min (watermark de 2 min + folga).
- **Mesmas regras do painel público.** Os três jobs descartam posts com rótulo próprio adulto (ADR-012); o exportador tira termos da lista de bloqueio, palavras vazias e pedaços de link ou de menção (o Flink tokeniza sem removê-los), e exige `FLINK_MIN_AUTHORS` autores distintos na janela (padrão 10).
- **Lacuna é lacuna.** Minuto ou janela ausente no tópico vira `null` e a página deixa em branco.
- **Jobs no deploy.** O `deploy.sh` chama `flink/submit.sh` depois do `docker compose up`; o script não faz nada se já há jobs rodando e resubmete quando o jobmanager foi recriado.

Contrato do `flink.json`:

```json
{
  "generated_at": "2026-11-12T04:14:48Z",
  "min_authors": 10,
  "window": {"start": "2026-11-12T04:06:00Z", "end": "2026-11-12T04:11:00Z"},
  "kpis": {"posts_last_minute": 26, "posts_last_hour": 1902,
           "peak_minute": {"t": "2026-11-12T03:30:00Z", "posts": 63}, "event_delay_s": 228},
  "hashtags": [{"term": "exemplo", "authors": 12, "posts": 20, "spark": [null, 3, 5, 8, 0, 9, 10, 12, 11, 12, 12, 12]}],
  "terms": [],
  "volume": [{"t": "2026-11-12T01:12:00Z", "posts": 41}]
}
```

## Decisões (ADRs curtos)

### ADR-001 — Jetstream v2 em vez da v1
O endpoint v2 (`/xrpc/network.bsky.jetstream.subscribeEvents`) é o recomendado para projetos novos: filtro server-side por `collections` (não mais `wantedCollections`) e `kinds`, campo `cursor` em cada evento e envelope `{\"$type\": \"message\", \"payload\": {...}}` com subprotocolo `xrpc.v1.json`. A v1 continua de pé, mas é legada. **Trade-off:** v2 é mais novo e pode mudar mais; mitigamos com parsing defensivo e DLQ.

### ADR-002 — Kafka em modo KRaft, 1 broker
Desde o Kafka 4.0 o ZooKeeper não existe mais; KRaft é o único modo. Um único broker em modo *combined* (broker+controller) é suficiente: o volume de posts em português é de poucos milhares de eventos/s no máximo. **Trade-off:** sem replicação (`replication.factor=1`) não há tolerância a falha do broker — aceitável porque os dados são recomputáveis a partir do Jetstream e a agregação final vive no ClickHouse.

### ADR-003 — Chave de partição = `did` do autor
Mensagens do mesmo autor caem sempre na mesma partição, o que preserva ordenação por autor e equilibra bem a carga (muitos autores distintos). Alternativas descartadas: chave pela URI do post (ordenação inútil, distribuição quase aleatória) e chave nula (round-robin, sem nenhuma ordenação). Como as agregações do Flink são por janela de tempo e juntam todas as partições, o `did` dá o melhor trade-off. O paralelismo do consumer fica em 3, igual às partições.

### ADR-004 — Avro + Schema Registry, compatibilidade BACKWARD
O schema (`schemas/post_event_v1.avsc`) é versionado no repositório e registrado no startup com `auto.register` implícito controlado: o producer registra o schema do arquivo e falha se o registro rejeitar. Estratégia de evolução: novos campos sempre opcionais (`[\"null\", ...]` + `default`); nunca remover/renomear campo em uso; mudanças incompatíveis → novo sufixo de versão no tópico (`bluesky.posts.pt.v2`). **Trade-off:** Avro exige o Registry como dependência a mais, mas dá contrato explícito, evolução segura e binário compacto — é o que se quer demonstrar num portfólio.

### ADR-005 — Tempo de evento vs tempo de processamento
O schema carrega **dois timestamps**: `event_time` (o `createdAt` do post, quando o autor escreveu) e `ingest_time` (o `time` do payload Jetstream, quando a rede emitiu o evento). O Flink (Fase 2) usará `event_time` para as janelas, com watermark de desordem limitada; `ingest_time` serve para medir latência ponta a ponta. A diferença entre os dois é o que explica janelas "reabrindo" quando um post antigo chega atrasado.

### ADR-006 — Entrega at-least-once e deduplicação downstream
O Jetstream garante at-least-once (o cursor é inclusivo e eventos podem repetir após reconexão) e o producer Kafka é idempotente (`enable.idempotence=true`, `acks=all`), mas a duplicata *lógica* (mesmo post reemitido) é aceita no tópico. A deduplicação real será pelo `uri` no ClickHouse (`ReplacingMergeTree`, Fase 3) — idempotência no sink, não na frente. Isso mantém o producer simples e honesto com a semântica da fonte.

### ADR-007 — Dead letters em vez de crash
Frames que falham no parsing ou na validação vão para `bluesky.posts.pt.dlq.v1` com headers `error.stage` e `error.reason`; o job continua. Eventos `update`/`delete` de post são **descartados com contador** (não são erro: post editado/deletado não tem `record` e não há o que agregar). **Trade-off:** descartar perde o sinal de edição; se um dia for relevante, o contador vira métrica e os eventos vão para um tópico próprio.

### ADR-008 — Filtro de idioma no producer, com subtag primária `pt`
O Jetstream não filtra por idioma server-side; o campo `langs` do record é filtrado no producer. Aceitamos qualquer código com subtag primária `pt` (pt-BR, pt-PT), incluindo posts multilíngues (`[\"en\", \"pt-BR\"]`). **Trade-off:** `langs` é declarado pelo cliente do usuário e pode estar errado/ausente; post sem `pt` declarado é descartado mesmo se escrito em português. É o custo padrão desse tipo de filtro.

### ADR-009 — Hashtags extraídas no producer, termos no Flink
As hashtags vêm prontas nos `facets` do record (`app.bsky.richtext.facet#tag`) e são serializadas no schema como `hashtag_entities`. Termos livres (n-gramas do texto) ficam para o Flink (Fase 2), onde a tokenização faz parte da lógica de agregação. Isso mantém o schema rico e o job SQL simples.

### ADR-010 — Flink 1.20 LTS
Escolhemos a linha 1.20 (LTS) em vez da 2.x: os conectores Kafka/Avro/Schema Registry e o material de estudo da 1.20 são maduros, e a API Flink SQL usada aqui é estável nas duas linhas. **Trade-off:** 2.x é o futuro do projeto Flink; quando os conectores da linha 2.x estabilizarem, a migração é baixo risco porque o job é SQL.

### ADR-011 — Fases com orçamento de memória desde o dia 1
Todo container nasce com `mem_limit`/`cpus` e heaps explícitos (Kafka 512 MB, Registry 256 MB), mesmo em desenvolvimento. O orçamento total do pipeline é ~7 GB numa VPS compartilhada com outro site em produção; os limites garantem que o pipeline nunca dispute recursos com ele.

### ADR-012 — Rótulo próprio no schema, para excluir conteúdo adulto

O Jetstream entrega os rótulos que o próprio autor aplicou ao post em `record.labels.values[].val`. Acrescentamos o campo opcional `self_labels` (`["null", {"type": "array", "items": "string"}]`, `default: null`) ao `post_event_v1.avsc`: é uma evolução **BACKWARD**, exatamente o caminho previsto na ADR-004, e serve de demonstração prática de que o contrato evolui sem quebrar consumidores. Post com `porn`, `sexual`, `nudity` ou `graphic-media` sai de todos os agregados públicos. **Trade-off:** dependemos do autor marcar o próprio post; o que ele não marca entra, e por isso a lista de bloqueio de termos existe como segunda barreira.

## Estrutura

```
├── docker-compose.yml        # Kafka KRaft, Schema Registry, kafka-init, producer
├── clickhouse/                # fase 3: schema com TTL e limites de memoria
├── flink/                     # fase 2: cluster e jobs SQL
│   ├── Dockerfile             # Flink 1.20 + conectores versionados em lib/
│   ├── docker-compose.flink.yml
│   ├── submit.sh              # submete os tres jobs
│   └── sql/                   # fontes, sinks, trending, volume, termos
├── docs/public.html           # painel publico (raiz do subdominio)
├── docs/index.html            # painel de validacao (/interno/)
├── schemas/post_event_v1.avsc
├── producer/
│   ├── Dockerfile
│   ├── pyproject.toml
│   ├── src/producer/
│   │   ├── main.py            # loop asyncio: frames → filtro → Avro → Kafka
│   │   ├── jetstream_client.py# URL v2, websocket com subprotocolo e stall timeout
│   │   ├── parsing.py         # envelope v2 → PostEvent (validação)
│   │   ├── filters.py         # langs com subtag primária pt
│   │   ├── avro_serde.py      # registro no Schema Registry + wire format Avro
│   │   ├── kafka_producer.py  # producer idempotente + DLQ
│   │   ├── cursor_store.py    # persistência atômica do cursor (seq)
│   │   ├── consume_check.py   # CLI p/ consumir e decodificar (debug)
│   │   ├── public_export.py   # servico public-export: agregados -> public.json
│   │   ├── public_aggregator.py# baldes de 5 min, 24 h, minimo de autores
│   │   ├── public_terms.py    # tokenizacao e filtro de conteudo
│   │   ├── blocklist_pt.txt   # termos que excluem o post dos agregados
│   │   ├── stopwords_pt.txt   # palavras vazias do portugues
│   │   └── settings.py        # config via env
│   └── tests/
│       ├── fixtures/jetstream_frames.jsonl
│       ├── unit/
│       └── integration/test_flow.py
├── deploy/                    # nginx (trends e grafana) e script de deploy
├── .github/workflows/ci.yml   # ruff + pytest unit + integração com Compose
├── .github/workflows/deploy.yml # rsync + docker compose na VPS (chave bluesky)
└── Makefile
```

## Fluxo de branches

A `main` é sempre o código mais atual e sempre verde. O fluxo imita o do orbita (feature → develop → release → main), adaptado para quem trabalha com branches de trabalho `vibe/**` e sem `develop`:

1. Todo trabalho acontece numa branch `vibe/**`. O push abre sozinho um PR para a `main` (`.github/workflows/promote-to-main.yml`).
2. Quando o CI daquele commit fica verde, o PR recebe o label `auto-merge`.
3. O merge só acontece se o CI passou no mesmo commit que está no topo da branch e se não há PR aberto mais novo esperando a vez. Conflito ou CI vermelho deixam o PR aberto para revisão humana.
4. Depois do merge, a branch promovida é apagada e as outras `vibe/**` avançam para a `main` sem `force`, para não sobrescrever trabalho que ainda não entrou.

O `deploy.yml` só roda depois que o CI passa no mesmo commit, e o job `verify` confere a produção antes de dar o deploy como concluído.

## CI/CD

O workflow [`.github/workflows/ci.yml`](.github/workflows/ci.yml) roda em todo push/PR:
1. **lint-unit**: `ruff check` + `ruff format --check` + `pytest` unitário (sem Docker).
2. **integration**: sobe Kafka + Schema Registry no Compose, produz o fixture em modo arquivo e valida ponta a ponta (posts Avro decodificáveis no tópico, inválidos na DLQ).

Nenhum segredo é necessário: o Jetstream é público e gratuito e não há credenciais no repositório.

## Roadmap detalhado

- **Fase 2 (Flink):** job Flink SQL consumindo o tópico Avro via `avro-confluent`, janelas `HOP` de 5 min deslizando 1 min para hashtags/termos, `TUMBLE` de 1 min para volume, watermarks sobre `event_time` e política para eventos atrasados documentada.
- **Fase 3 (ClickHouse + Grafana):** MergeTree/ReplacingMergeTree dedup por `uri`, dashboards de trending e volume.
- **Fase 4 (picos + observabilidade):** z-score sobre janelas, exportadores de métricas (lag do consumer, eventos/s, DLQ/s), alertas.
- **Fase 5 (VPS):** deploy atrás do nginx existente com autenticação no Grafana, health checks, CI/CD de deploy via Actions.
