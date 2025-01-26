#!/usr/bin/env python3
"""
final_index_pipeline.py

Demonstrates how one might merge the Batch Index with other data
(e.g., the partial streaming index or ML signals, policy data)
to produce a "Final Index."

Usage:
  python final_index_pipeline.py

Author: Your Name
"""

import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

# ------------------------------ #
#  CONFIGURATIONS & CONSTANTS    #
# ------------------------------ #
BATCH_INDEX_OUTPUT = "/opt/airflow/data/batch_index"
STREAMING_INDEX_OUTPUT = "/opt/airflow/data/partial_stream_index"
ML_SIGNALS_PATH = "/opt/airflow/data/ml_fraud_signals.parquet" # optional
FINAL_INDEX_OUTPUT = "opt/airflow/data/final_index"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def main():
    logging.info("Starting Final Index Pipeline...")

    spark = (
        SparkSession.builder
        .appName("FinalIndexPipeline")
        .getOrCreate()
    )

    # 1) Read the Batch Index, selecting columns we need
    logging.info(f"Reading Batch Index from {BATCH_INDEX_OUTPUT}")
    df_batch_index = spark.read.parquet(BATCH_INDEX_OUTPUT).select(
        "transaction_id",
        "merchant_id",
        "user_id",
        "amount",
        "currency",
        "status",
        "discrepancy",
        "reconciliation_status"
    )

    # 2) Read partial streaming index (select columns we need)
    logging.info(f"Reading partial streaming index from {STREAMING_INDEX_OUTPUT}")
    df_partial = spark.read.parquet(STREAMING_INDEX_OUTPUT).select(
        "transaction_id",
        "processed_at"
    )

    # 3) Read ML signals (select columns we need)
    logging.info(f"Reading ML signals from {ML_SIGNALS_PATH}")
    df_ml_signals = spark.read.parquet(ML_SIGNALS_PATH).select(
        "transaction_id",
        "fraud_signal_id",
        "fraud_score",
        "inference_time"
    )

    # 4) Join Batch Index with ML signals on `transaction_id`
    df_joined_ml = df_batch_index.join(
        df_ml_signals,
        "transaction_id",  # auto-equijoin on transaction_id
        how="left"
    )

    # 5) Join the result with the partial streaming index
    df_joined_partial = df_joined_ml.join(
        df_partial,
        "transaction_id",  # again equijoin
        how="left"
    )

    # 6) Add final timestamp
    run_ts = datetime.utcnow().isoformat()
    df_final = df_joined_partial.withColumn("final_index_ts", F.lit(run_ts))

    # 7) Select columns for the "Final Index"
    #    (In this approach, there's now a single `transaction_id` column, no ambiguity.)
    df_final_index = df_final.select(
        "transaction_id",
        "merchant_id",
        "user_id",
        "amount",
        "currency",
        "status",
        "discrepancy",
        "reconciliation_status",
        "fraud_signal_id",
        "fraud_score",
        "inference_time",
        "processed_at",
        "final_index_ts"
    )

    # 8) Write out the final index
    os.makedirs(FINAL_INDEX_OUTPUT, exist_ok=True)
    logging.info(f"Writing Final Index to {FINAL_INDEX_OUTPUT}")
    df_final_index.write.mode("overwrite").parquet(FINAL_INDEX_OUTPUT)
    count_out = df_final_index.count()

    logging.info(f"Wrote {count_out} records to Final Index.")
    spark.stop()

if __name__ == "__main__":
    main()
