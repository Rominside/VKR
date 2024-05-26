# Dockerfile для Apache Kafka
FROM openjdk:8-jre-alpine

ARG KAFKA_VERSION=2.7.1
ARG SCALA_VERSION=2.13

WORKDIR /kafka

# Скачиваем и устанавливаем Apache Kafka
RUN wget https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    tar -xzf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz --strip 1 && \
    rm kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz

COPY server.properties config/

CMD ["bin/kafka-server-start.sh", "config/server.properties"]
