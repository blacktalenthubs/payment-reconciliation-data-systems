FROM apache/airflow:2.5.1

USER root
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==3.0.0 \
    pyspark==3.3.2 \
    faker==18.11.2 \
    requests==2.28.2 \
    pyarrow==10.0.1
