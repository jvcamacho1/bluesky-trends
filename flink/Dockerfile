# Flink 1.20 LTS (ADR-010) com os conectores que o job SQL precisa.
# Os jars ficam versionados em lib/ para a imagem ser reproduzivel e para o
# build nao depender de download em tempo de deploy.
FROM flink:1.20.1-scala_2.12-java17

COPY lib/*.jar /opt/flink/lib/
COPY sql /opt/sql
COPY submit.sh /opt/submit.sh
RUN chmod +x /opt/submit.sh
