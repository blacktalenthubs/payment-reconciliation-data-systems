# file: batch_index.py

import pyspark
from pyspark.sql import SparkSession, DataFrame, functions as F

def get_spark_session(app_name: str = "BatchIndexJob"):
    return (
        SparkSession.builder
            .appName(app_name)
            .config("spark.driver.bindAddress", "127.0.0.1")
            # Common S3 configs
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
            #
            # .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY")
            # .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY")
            .getOrCreate()
    )


def main():
    spark = get_spark_session()

    users = spark.read.parquet("s3://mentorhub-upstreams/batch/users")
    merchants = spark.read.parquet("s3://mentorhub-upstreams/batch/merchants")
    policies = spark.read.parquet("s3://mentorhub-upstreams/batch/policies")
    transactions = spark.read.parquet("s3://mentorhub-upstreams/batch/transactions")

    # users.show()
    # merchants.show()
    # policies.show()
    # transactions.show()


    join_tx_policy = transactions.join(policies,on="policy_id",how="left")


    joined_tx_merchant = join_tx_policy.join(merchants, on="merchant_id", how="left")
    joined_tx_merchant.show()
    final_batch_df = joined_tx_merchant.join(users, on="user_id", how="left")

    final_batch_df.show()
    final_batch_df.write.mode("overwrite").parquet("data/batch_index")

    # david to add the enrichment of fraud signal to the final batch
    """
    master - working code 
    PR- enrichment 
    
    create a PR - 
    
    send PR to review - other engineers 
    
    run automated tests on your PR 
    add your automated test 
    MERGE TO MASTER
    
    Jenkins 
    Drone CI - 
    CICD - 
    
    """

    spark.stop()

if __name__ == "__main__":
    main()