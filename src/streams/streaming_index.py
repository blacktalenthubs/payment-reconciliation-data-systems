# file: streaming_index.py
import os

import pyspark
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "fraud_signals_topic")
S3_OUTPUT = os.getenv("S3_OUTPUT", "s3a://my-stream-output/streaming_index")
def get_spark_session(app_name: str = "StreamingIndexJob"):
    return (
        SparkSession.builder
        .appName("StreamingIndexJob")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4")
        .getOrCreate()
    )

def main():
    spark = get_spark_session()

    # 1) Define schema for fraud signals
    fraud_schema = (
        StructType()
        .add("fraud_id", StringType())
        .add("transaction_id", StringType())
        .add("model_version", StringType())
        .add("fraud_score", DoubleType())
        .add("inference_time", StringType())
        .add("reason_code", StringType())
    )

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "fraud_signals_topic")
        .option("startingOffsets", "latest")  # or "earliest"
        .load()
    )

    parsed_fraud_df = (
        kafka_df
        .select(
            F.from_json(F.col("value").cast("string"), fraud_schema).alias("fraud_json")
        )
        .select("fraud_json.*")
    )


    transactions_df = spark.read.parquet("data/transaction")

    transactions_df.show()

    # 5) Join on transaction_id
    # This adds user_id, merchant_id, etc. from your transactions table
    joined_stream = parsed_fraud_df.join(transactions_df, on="transaction_id", how="left")
    joined_stream.show()

    # 6) Write to Parquet in streaming mode
    query = (
        joined_stream
        .writeStream
        .format("parquet")
        .option("path", "streaming_index")
        .option("checkpointLocation", "streaming_index_checkpoint")
        .outputMode("append")
        .start()
    )

    query.awaitTermination()
    spark.stop()

if __name__ == "__main__":
    main()


