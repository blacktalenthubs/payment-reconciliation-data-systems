#!/usr/bin/env python3
"""
streaming_pipeline.py

Reads transactions from a Kafka topic in near real-time,
enriches/filters as needed, and writes a "Partial Streaming Index"
as Parquet to local storage.

Usage:
  python streaming_pipeline.py

Prereqs:
- Kafka running at localhost:9092
- 'transactions_topic' exists

Author: Your Name
"""

import os
import json
import time
import logging

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType
)

# ------------------------------ #
#  CONFIGURATIONS & CONSTANTS    #
# ------------------------------ #
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "transactions_topic"

# Where we'll write the partial streaming index
STREAMING_INDEX_OUTPUT = "/opt/airflow/data/partial_stream_index"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def main():
    logging.info("Starting Streaming Pipeline...")

    spark = (
        SparkSession.builder
        .appName("StreamingPipeline")
       # .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4")
        .getOrCreate()
    )

    # 1) Read transactions from Kafka
    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # 2) Parse the JSON from the 'value' field
    df_raw = df_kafka.selectExpr("CAST(value AS STRING) as json_str")

    # Define a schema for our transactions
    transaction_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("amount", FloatType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("event_time", StringType(), True)
    ])

    df_parsed = df_raw.select(
        F.from_json(F.col("json_str"), transaction_schema).alias("data")
    ).select("data.*")

    # 3) (Optional) Enrichment or filtering
    # For now, just add a processing timestamp
    df_enriched = df_parsed.withColumn("processed_at", F.current_timestamp())

    # 4) Write partial streaming index to Parquet (append mode)
    os.makedirs(STREAMING_INDEX_OUTPUT, exist_ok=True)

    query = (
        df_enriched
        .writeStream
        .format("parquet")
        .option("checkpointLocation", STREAMING_INDEX_OUTPUT + "/_checkpoints")
        .option("path", STREAMING_INDEX_OUTPUT)
        .trigger(processingTime="5 seconds")  # micro-batch every 5s
        .outputMode("append")
        .start()
    )

    logging.info(f"Streaming from topic '{KAFKA_TOPIC}' to '{STREAMING_INDEX_OUTPUT}'")

    query.awaitTermination()

if __name__ == "__main__":
    main()
