from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType
import random
from faker import Faker
from datetime import datetime
import sys

if __name__ == "__main__":

    num_records = int(sys.argv[1]) if len(sys.argv) > 1 else 10000

    S3_OUTPUT_PATH = "s3://mentorhub-training-emr-scripts-bucket/transactions/"

    spark = SparkSession.builder.appName("GenerateTransactionData").getOrCreate()

    fake = Faker()
    schema = StructType([
        StructField("transaction_id", LongType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("amount", DoubleType(), True)
    ])

    transactions = []
    for i in range(num_records):
        transaction_id = i + 1
        user_id = fake.uuid4()
        product_id = fake.random_element(elements=["prod_apple", "prod_samsung", "prod_sony", "prod_dell", "prod_nokia"])
        timestamp = fake.date_time_between(start_date='-1y', end_date='now')
        amount = round(random.uniform(10, 500), 2)
        transactions.append((transaction_id, user_id, product_id, timestamp, amount))
    df = spark.createDataFrame(transactions, schema=schema)
    df.write.mode("overwrite").parquet(S3_OUTPUT_PATH)
    print("Number of records:", df.count())
    print("Sample record:")
    df.show(5, truncate=False)

    spark.stop()

