# file: Dockerfile.batch
FROM bitnami/spark:3.3.1

USER root
RUN pip install --no-cache-dir kafka-python

COPY batch_index.py /opt/bitnami/spark/

CMD ["/opt/bitnami/spark/bin/spark-submit", "/opt/bitnami/spark/batch_index.py"]