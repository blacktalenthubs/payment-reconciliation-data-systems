# Dockerfile for Kafka CLI in the same VPC as MSK

FROM openjdk:8-jdk-alpine
RUN apk add --no-cache bash
RUN wget https://archive.apache.org/dist/kafka/3.4.0/kafka_2.13-3.4.0.tgz -O /tmp/kafka.tgz \
    && tar xzf /tmp/kafka.tgz -C /opt \
    && mv /opt/kafka_2.13-3.4.0 /opt/kafka
WORKDIR /opt/kafka
ENTRYPOINT ["/bin/bash"]