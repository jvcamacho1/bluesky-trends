# Conectores do Flink

Jars baixados do Maven Central, versionados para o build ser reproduzível e não
depender de download em tempo de deploy. O `Dockerfile` copia tudo daqui para
`/opt/flink/lib/`.

| Jar | Versão | Por quê |
|---|---|---|
| `flink-sql-connector-kafka` | 3.4.0-1.20 | conector Kafka do Flink 1.20 (fonte e sink) |
| `flink-sql-avro-confluent-registry` | 1.20.5 | formato `avro-confluent`, que lê o Schema Registry |

Para atualizar, troque o jar e a versão aqui e no `Dockerfile`:

```bash
curl -sSLO https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar
curl -sSLO https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.20.5/flink-sql-avro-confluent-registry-1.20.5.jar
```
