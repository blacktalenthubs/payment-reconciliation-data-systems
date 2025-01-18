# file: final_aggregate.py

import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


STREAMING_INDEX_PATH = os.getenv("STREAMING_INDEX_PATH", "streaming_index")
BATCH_INDEX_PATH     = os.getenv("BATCH_INDEX_PATH", "batch_index")
FINAL_INDEX_OUTPUT   = os.getenv("FINAL_INDEX_OUTPUT", "final_index")

def get_spark_session(app_name: str = "FinalAggregateJob"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )

def merge_indexes(df_stream, df_batch):
    """
    Simple union and deduplicate on transaction_id
    by picking the row with the latest timestamp.
    """
    union_df = df_stream.unionByName(df_batch)

    union_df = union_df.withColumn(
        "timestamp_parsed",
        F.to_timestamp("timestamp")
    )

    w = Window.partitionBy("transaction_id").orderBy(F.col("timestamp_parsed").desc())
    deduped = (
        union_df
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn", "timestamp_parsed")
    )

    return deduped

def main():
    spark = get_spark_session()

    df_stream = spark.read.parquet(STREAMING_INDEX_PATH)
    df_batch  = spark.read.parquet(BATCH_INDEX_PATH)

    final_df = merge_indexes(df_stream, df_batch)
    final_df.write.mode("overwrite").parquet(FINAL_INDEX_OUTPUT)

    spark.stop()

if __name__ == "__main__":
    main()