
"""

AWS cli installed   google how to install AWS cli

aws configure

aws s3 ls

run with
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.1026 \
  --driver-memory 4g \
  complex_schema.py


"""
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType, MapType,
    TimestampType, IntegerType
)
from pyspark.sql.functions import to_timestamp, col
from faker import Faker
import random

fake = Faker()
# Optional: for deterministic results, you can set a seed
# Faker.seed(42)
# random.seed(42)

def generate_transactions_data(n):
    """Generate a list of Rows for the 'Transactions' table."""
    possible_dts = ["2025-01-01", "2025-01-02", "2025-01-03"]
    regions = ["US", "EU", "APAC"]

    data = []
    for _ in range(n):
        tx_id = "tx-" + str(fake.random_number(digits=6))
        user_id = "user-" + str(fake.random_number(digits=3))
        merchant_id = "mch-" + str(fake.random_number(digits=3))
        dt_val = random.choice(possible_dts)
        region_val = random.choice(regions)

        # items array
        item_list = []
        for _ in range(random.randint(1, 3)):
            sku = "SKU-" + fake.bothify(text="??##")
            price = round(random.uniform(1.0, 500.0), 2)
            quantity = random.randint(1, 5)
            # attributes map
            attr_map = {
                "color": fake.color_name(),
                "size": random.choice(["S", "M", "L", "XL"]),
            }
            item_list.append(Row(sku=sku, price=price, quantity=quantity, attributes=attr_map))

        # location_info struct
        # Cast lat/lon to float to avoid Decimal issues
        location_info = Row(
            country=fake.country_code(),
            city=fake.city(),
            coords=Row(
                lat=float(round(fake.latitude(), 5)),
                lon=float(round(fake.longitude(), 5))
            )
        )

        extra_info = {
            "payment_method": random.choice(["CARD", "PAYPAL", "CRYPTO"]),
            "promo_code": fake.bothify(text="PROMO-##??")
        }

        # Use Faker to generate a random timestamp (convert to string, parse later)
        transaction_time = fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S")

        data.append(
            Row(
                transaction_id=tx_id,
                user_id=user_id,
                merchant_id=merchant_id,
                items=item_list,
                transaction_time=transaction_time,
                location_info=location_info,
                extra_info=extra_info,
                dt=dt_val,
                region=region_val
            )
        )
    return data

def generate_users_data(n):
    """Generate a list of Rows for the 'Users' table."""
    possible_dts = ["2025-01-01", "2025-01-02", "2025-01-03"]
    environments = ["DEV", "PROD"]

    data = []
    for _ in range(n):
        user_id = "user-" + str(fake.random_number(digits=4))
        first_name = fake.first_name()
        last_name = fake.last_name()
        emails = [fake.email() for _ in range(random.randint(1, 2))]
        preferences = {
            "language": fake.language_code(),
            "theme": random.choice(["light", "dark"])
        }
        sign_up_timestamp = fake.date_time_this_decade().strftime("%Y-%m-%d %H:%M:%S")
        status = random.choice(["ACTIVE", "INACTIVE", "BANNED"])
        dt_val = random.choice(possible_dts)
        env_val = random.choice(environments)

        data.append(
            Row(
                user_id=user_id,
                name=Row(first_name=first_name, last_name=last_name),
                emails=emails,
                preferences=preferences,
                sign_up_timestamp=sign_up_timestamp,
                status=status,
                dt=dt_val,
                environment=env_val
            )
        )
    return data

def generate_user_events_data(n):
    """Generate a list of Rows for the 'user_events' table."""
    possible_dts = ["2025-01-01", "2025-01-02", "2025-01-03"]
    environments = ["DEV", "PROD"]
    event_types = ["LOGIN", "PURCHASE", "LOGOUT", "BROWSE"]

    data = []
    for _ in range(n):
        event_id = "evt-" + str(fake.random_number(digits=6))
        user_id = "user-" + str(fake.random_number(digits=3))
        event_time_str = fake.date_time_between(start_date="-10d", end_date="now").strftime("%Y-%m-%d %H:%M:%S")
        dt_val = random.choice(possible_dts)
        env_val = random.choice(environments)

        event_info = Row(
            type=random.choice(event_types),
            device=random.choice(["iPhone", "Android", "Web", "Tablet"]),
            location=fake.city(),
            metadata={
                "user_agent": fake.user_agent(),
                "referrer": fake.uri()
            }
        )
        data.append(
            Row(
                event_id=event_id,
                user_id=user_id,
                event_time=event_time_str,
                event_info=event_info,
                dt=dt_val,
                environment=env_val
            )
        )
    return data

def generate_merchants_data(n):
    """Generate a list of Rows for the 'Merchants' table."""
    possible_dts = ["2025-01-01", "2025-01-02", "2025-01-03"]
    regions = ["US", "EU", "APAC"]

    data = []
    for _ in range(n):
        merchant_id = "mch-" + str(fake.random_number(digits=5))
        name = fake.company()
        cat_count = random.randint(1, 3)
        categories = random.sample(["Electronics", "Fashion", "Food", "Travel", "Sports"], cat_count)
        contact_struct = Row(
            phone=fake.phone_number(),
            email=fake.company_email(),
            address={
                "street": fake.street_address(),
                "city": fake.city(),
                "postal_code": fake.postcode()
            }
        )
        rating = round(random.uniform(1.0, 5.0), 1)
        dt_val = random.choice(possible_dts)
        region_val = random.choice(regions)

        data.append(
            Row(
                merchant_id=merchant_id,
                name=name,
                categories=categories,
                contact=contact_struct,
                rating=rating,
                dt=dt_val,
                region=region_val
            )
        )
    return data

def generate_fraud_signals_data(n):
    """Generate a list of Rows for the 'fraudSignals' table."""
    possible_dts = ["2025-01-01", "2025-01-02", "2025-01-03"]
    environments = ["DEV", "PROD"]
    risk_levels = ["LOW", "MEDIUM", "HIGH"]

    data = []
    for _ in range(n):
        signal_id = "sig-" + str(fake.random_number(digits=6))
        tx_id = "tx-" + str(fake.random_number(digits=6))
        signal_time_str = fake.date_time_between(start_date="-10d", end_date="now").strftime("%Y-%m-%d %H:%M:%S")
        risk_level = random.choice(risk_levels)
        reasons = random.sample(["StolenCard", "GeoMismatch", "HighValue", "MultipleAttempts"], random.randint(1, 3))
        details_map = {
            "analyst_note": fake.sentence(nb_words=5),
            "ip_address": fake.ipv4()
        }
        dt_val = random.choice(possible_dts)
        env_val = random.choice(environments)

        data.append(
            Row(
                signal_id=signal_id,
                transaction_id=tx_id,
                signal_time=signal_time_str,
                risk_level=risk_level,
                reasons=reasons,
                details=details_map,
                dt=dt_val,
                environment=env_val
            )
        )
    return data

def generate_locations_data(n):
    """Generate a list of Rows for the 'Locations' table."""
    possible_dts = ["2025-01-01", "2025-01-02", "2025-01-03"]
    environments = ["DEV", "PROD"]

    data = []
    for _ in range(n):
        loc_id = "loc-" + str(fake.random_number(digits=4))
        region = random.choice(["US", "EU", "APAC"])
        name = fake.city()
        # Cast lat/lon to float to avoid Decimal issues
        coords_struct = Row(
            lat=float(round(fake.latitude(), 5)),
            lon=float(round(fake.longitude(), 5))
        )
        attributes_map = {
            "timezone": fake.timezone(),
            "local_language": fake.language_code()
        }
        dt_val = random.choice(possible_dts)
        env_val = random.choice(environments)

        data.append(
            Row(
                location_id=loc_id,
                region=region,
                name=name,
                coords=coords_struct,
                attributes=attributes_map,
                dt=dt_val,
                environment=env_val
            )
        )
    return data


def main():
    spark = (
        SparkSession.builder
        .appName("WriteToS3Example")
        .master("local[2]")
        # S3 credentials (not recommended to hardcode in production)
        .config("spark.hadoop.fs.s3a.access.key", "")
        .config("spark.hadoop.fs.s3a.secret.key", "")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.driver.bindAddress", "127.0.0.1")  # or "0.0.0.0"
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Adjust these to generate more or fewer records
    num_transactions = 20
    num_users = 20
    num_user_events = 20
    num_merchants = 20
    num_fraud_signals = 20
    num_locations = 20

    # 1) Transactions: partition by dt, region
    transactions_schema = StructType([
        StructField("transaction_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("merchant_id", StringType(), nullable=False),
        StructField("items", ArrayType(
            StructType([
                StructField("sku", StringType(), nullable=True),
                StructField("price", DoubleType(), nullable=True),
                StructField("quantity", IntegerType(), nullable=True),
                StructField("attributes", MapType(StringType(), StringType()), nullable=True)
            ])
        ), nullable=True),
        StructField("transaction_time", StringType(), nullable=False),  # will convert to Timestamp
        StructField("location_info",
            StructType([
                StructField("country", StringType(), nullable=True),
                StructField("city", StringType(), nullable=True),
                StructField("coords", StructType([
                    StructField("lat", DoubleType(), nullable=True),
                    StructField("lon", DoubleType(), nullable=True),
                ]), nullable=True)
            ])
        , nullable=True),
        StructField("extra_info", MapType(StringType(), StringType()), nullable=True),
        StructField("dt", StringType(), nullable=False),
        StructField("region", StringType(), nullable=False)
    ])

    tx_rdd = spark.sparkContext.parallelize(generate_transactions_data(num_transactions))
    transactions_temp_df = spark.createDataFrame(tx_rdd, transactions_schema)
    transactions_df = transactions_temp_df.withColumn("transaction_time", to_timestamp(col("transaction_time")))

    # 2) Users: partition by dt, environment
    users_schema = StructType([
        StructField("user_id", StringType(), nullable=False),
        StructField("name", StructType([
            StructField("first_name", StringType(), nullable=True),
            StructField("last_name", StringType(), nullable=True),
        ]), nullable=True),
        StructField("emails", ArrayType(StringType()), nullable=True),
        StructField("preferences", MapType(StringType(), StringType()), nullable=True),
        StructField("sign_up_timestamp", StringType(), nullable=False),  # will convert to Timestamp
        StructField("status", StringType(), nullable=True),
        StructField("dt", StringType(), nullable=False),
        StructField("environment", StringType(), nullable=False)
    ])

    users_rdd = spark.sparkContext.parallelize(generate_users_data(num_users))
    users_temp_df = spark.createDataFrame(users_rdd, users_schema)
    users_df = users_temp_df.withColumn("sign_up_timestamp", to_timestamp(col("sign_up_timestamp")))

    # 3) user_events: partition by dt, environment
    user_events_schema = StructType([
        StructField("event_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("event_time", StringType(), nullable=False),  # will convert to Timestamp
        StructField("event_info", StructType([
            StructField("type", StringType(), nullable=True),
            StructField("device", StringType(), nullable=True),
            StructField("location", StringType(), nullable=True),
            StructField("metadata", MapType(StringType(), StringType()), nullable=True),
        ]), nullable=True),
        StructField("dt", StringType(), nullable=False),
        StructField("environment", StringType(), nullable=False)
    ])

    user_events_rdd = spark.sparkContext.parallelize(generate_user_events_data(num_user_events))
    user_events_temp_df = spark.createDataFrame(user_events_rdd, user_events_schema)
    user_events_df = user_events_temp_df.withColumn("event_time", to_timestamp(col("event_time")))

    # 4) Merchants: partition by dt, region
    merchants_schema = StructType([
        StructField("merchant_id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("categories", ArrayType(StringType()), nullable=True),
        StructField("contact", StructType([
            StructField("phone", StringType(), nullable=True),
            StructField("email", StringType(), nullable=True),
            StructField("address", MapType(StringType(), StringType()), nullable=True),
        ]), nullable=True),
        StructField("rating", DoubleType(), nullable=True),
        StructField("dt", StringType(), nullable=False),
        StructField("region", StringType(), nullable=False)
    ])

    merchants_rdd = spark.sparkContext.parallelize(generate_merchants_data(num_merchants))
    merchants_df = spark.createDataFrame(merchants_rdd, merchants_schema)

    # 5) fraudSignals: partition by dt, environment
    fraud_signals_schema = StructType([
        StructField("signal_id", StringType(), nullable=False),
        StructField("transaction_id", StringType(), nullable=False),
        StructField("signal_time", StringType(), nullable=False), # will convert to Timestamp
        StructField("risk_level", StringType(), nullable=True),
        StructField("reasons", ArrayType(StringType()), nullable=True),
        StructField("details", MapType(StringType(), StringType()), nullable=True),
        StructField("dt", StringType(), nullable=False),
        StructField("environment", StringType(), nullable=False)
    ])

    fraud_signals_rdd = spark.sparkContext.parallelize(generate_fraud_signals_data(num_fraud_signals))
    fraud_signals_temp_df = spark.createDataFrame(fraud_signals_rdd, fraud_signals_schema)
    fraud_signals_df = fraud_signals_temp_df.withColumn("signal_time", to_timestamp(col("signal_time")))

    # 6) Locations: partition by dt, environment
    locations_schema = StructType([
        StructField("location_id", StringType(), nullable=False),
        StructField("region", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("coords", StructType([
            StructField("lat", DoubleType(), nullable=True),
            StructField("lon", DoubleType(), nullable=True),
        ]), nullable=True),
        StructField("attributes", MapType(StringType(), StringType()), nullable=True),
        StructField("dt", StringType(), nullable=False),
        StructField("environment", StringType(), nullable=False)
    ])

    locations_rdd = spark.sparkContext.parallelize(generate_locations_data(num_locations))
    locations_df = spark.createDataFrame(locations_rdd, locations_schema)

    locations_df.show()

    # -------------------------------------------------------------------------
    # Write the data to S3 as Parquet with multiple partition columns
    # -------------------------------------------------------------------------
    s3_base = "s3a://mentorhub-data-complex"

    # Transactions -> partition by dt, region
    transactions_df.write.partitionBy("dt", "region") \
        .mode("overwrite") \
        .parquet(f"{s3_base}/transactions")

    # Users -> partition by dt, environment
    users_df.write.partitionBy("dt", "environment") \
        .mode("overwrite") \
        .parquet(f"{s3_base}/users")

    # user_events -> partition by dt, environment
    user_events_df.write.partitionBy("dt", "environment") \
        .mode("overwrite") \
        .parquet(f"{s3_base}/user_events")

    # Merchants -> partition by dt, region
    merchants_df.write.partitionBy("dt", "region") \
        .mode("overwrite") \
        .parquet(f"{s3_base}/merchants")

    # fraudSignals -> partition by dt, environment
    fraud_signals_df.write.partitionBy("dt", "environment") \
        .mode("overwrite") \
        .parquet(f"{s3_base}/fraudSignals")

    # Locations -> partition by dt, environment
    locations_df.write.partitionBy("dt", "environment") \
        .mode("overwrite") \
        .parquet(f"{s3_base}/locations")

    spark.stop()

if __name__ == "__main__":
    main()
