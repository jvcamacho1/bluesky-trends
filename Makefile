COMPOSE ?= docker compose
PY ?= python3
PIP_INSTALL ?= pip install -e "./producer[dev]"
FIXTURE ?= producer/tests/fixtures/jetstream_frames.jsonl

.PHONY: help install up down restart ps logs logs-producer topics consume consume-dlq consume-check export-dashboard test test-unit test-integration lint fmt clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## Instala dependencias de dev do producer
	$(PIP_INSTALL)

up: ## Sobe Kafka + Schema Registry + Producer
	$(COMPOSE) up -d --build

down: ## Derruba tudo (sem apagar volumes)
	$(COMPOSE) down

ps: ## Estado dos servicos
	$(COMPOSE) ps

logs: ## Logs de todos os servicos
	$(COMPOSE) logs -f --tail=100

logs-producer: ## Logs do producer
	$(COMPOSE) logs -f --tail=100 producer

topics: ## Lista topicos Kafka
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:9092 --list

consume: ## Console consumer (bytes brutos) do topico de posts
	$(COMPOSE) exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic bluesky.posts.pt.v1 --from-beginning --max-messages 10

consume-dlq: ## Console consumer do topico de dead letters
	$(COMPOSE) exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic bluesky.posts.pt.dlq.v1 --from-beginning --max-messages 10

consume-check: ## Decodifica e imprime mensagens Avro do topico de posts
	cd producer && $(PY) -m producer.consume_check localhost:9092 http://localhost:8081 bluesky.posts.pt.v1 10

export-dashboard: ## Exporta dados do topico para docs/data.json (dashboard)
	cd producer && $(PY) -m producer.export_dashboard --output ../docs/data.json

test: test-unit ## Roda lint + unit + integracao
	$(MAKE) test-integration

test-unit: ## Somente testes unitarios
	cd producer && $(PY) -m pytest -m "not integration" -v

test-integration: ## Teste de integracao via Docker Compose (requer Docker)
	cd producer && $(PY) -m pytest -m integration -v

lint: ## Ruff (lint + format check)
	cd producer && $(PY) -m ruff check src tests && $(PY) -m ruff format --check src tests

fmt: ## Ruff format + fix
	cd producer && $(PY) -m ruff check --fix src tests && $(PY) -m ruff format src tests

clean: ## Remove artefatos locais
	rm -rf producer/.pytest_cache producer/.ruff_cache producer/src/*.egg-info
