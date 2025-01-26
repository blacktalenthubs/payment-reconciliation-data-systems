#!/usr/bin/env python3
"""
batch.py

Reads:
  - Aggregator Statements (Parquet)
  - Partial Streaming Index (Parquet)

Joins them on transaction_id, applies basic reconciliation logic (e.g., discrepancy),
and writes a "Batch Index" for further analysis.

Usage:
  python batch.py

Author: Your Name
"""

import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession, functions as F, types as T

# ------------------------------ #
#  CONFIGURATIONS & CONSTANTS    #
# ------------------------------ #
AGGREGATOR_STATEMENTS_PATH = "/opt/airflow/data/aggregator_statements.parquet"
STREAMING_INDEX_OUTPUT = "/opt/airflow/data/partial_stream_index"
BATCH_INDEX_OUTPUT = "/opt/airflow/data/batch_index"

# Basic UDF for calculating discrepancy
def calc_discrepancy_py(amount, amount_posted):
    if amount is None or amount_posted is None:
        return 0.0
    return round(amount_posted - amount, 2)

# We'll register it as a Spark UDF
calc_discrepancy = F.udf(calc_discrepancy_py, T.FloatType())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def main():
    logging.info("Starting Batch Reconciliation...")

    spark = (
        SparkSession.builder
        .appName("BatchReconciliation")
       # .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )

    # 1) Read aggregator statements
    logging.info(f"Reading aggregator statements from {AGGREGATOR_STATEMENTS_PATH}")
    df_statements = spark.read.parquet(AGGREGATOR_STATEMENTS_PATH)

    # 2) Read partial streaming index
    logging.info(f"Reading partial streaming index from {PARTIAL_STREAM_INDEX_PATH}")
    df_partial = spark.read.parquet(PARTIAL_STREAM_INDEX_PATH)

    # 3) Join on transaction_id
    #    We'll keep all partial stream records, left-joining aggregator
    df_joined = df_partial.alias("st").join(
        df_statements.alias("ag"),
        F.col("st.transaction_id") == F.col("ag.transaction_id"),
        how="left"
    )

    # 4) Calculate discrepancy
    #    partial stream's "amount" vs aggregator's "amount_posted"
    df_enriched = df_joined.withColumn(
        "discrepancy",
        calc_discrepancy(F.col("st.amount"), F.col("ag.amount_posted"))
    )

    # 5) Optionally define a "reconciliation_status"
    #    If aggregator is missing => "UNMATCHED", else "MATCHED"
    df_enriched = df_enriched.withColumn(
        "reconciliation_status",
        F.when(F.col("ag.statement_id").isNull(), F.lit("UNMATCHED"))
         .otherwise(F.lit("MATCHED"))
    )

    # 6) Add a batch_run_timestamp
    current_ts = datetime.utcnow().isoformat()
    df_final = df_enriched.withColumn("batch_run_timestamp", F.lit(current_ts))

    # 7) Select columns for the "Batch Index"
    df_batch_index = df_final.select(
        "st.transaction_id",
        "st.merchant_id",
        "st.user_id",
        "st.amount",
        "st.currency",
        "st.status",  # streaming pipeline's real-time status
        "ag.statement_id",
        "ag.posting_date",
        "ag.amount_posted",
        "ag.fee_charged",
        "discrepancy",
        "reconciliation_status",
        "batch_run_timestamp"
    )

    # 8) Write Batch Index
    os.makedirs(BATCH_INDEX_OUTPUT, exist_ok=True)
    logging.info(f"Writing Batch Index to {BATCH_INDEX_OUTPUT}")

    df_batch_index.write.mode("overwrite").parquet(BATCH_INDEX_OUTPUT)
    count_out = df_batch_index.count()

    logging.info(f"Wrote {count_out} records to the Batch Index.")
    spark.stop()

if __name__ == "__main__":
    main()
