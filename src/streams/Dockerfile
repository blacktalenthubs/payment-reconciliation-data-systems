# file: Dockerfile.streaming
FROM bitnami/spark:3.3.1

USER root
RUN pip install --no-cache-dir kafka-python

COPY streaming_index.py /opt/bitnami/spark/
ENV KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
ENV KAFKA_TOPIC="fraud_signals_topic"

CMD ["/opt/bitnami/spark/bin/spark-submit", "/opt/bitnami/spark/streaming_index.py"]