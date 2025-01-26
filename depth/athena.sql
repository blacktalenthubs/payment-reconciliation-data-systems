CREATE DATABASE IF NOT EXISTS mentorhub_db;


CREATE EXTERNAL TABLE IF NOT EXISTS transactions (
  transaction_id     STRING,
  user_id            STRING,
  merchant_id        STRING,
  items ARRAY<
    STRUCT<
      sku:STRING,
      price:DOUBLE,
      quantity:INT,
      attributes:MAP<STRING,STRING>
    >
  >,
  transaction_time   TIMESTAMP,
  location_info STRUCT<
    country:STRING,
    city:STRING,
    coords:STRUCT<
      lat:DOUBLE,
      lon:DOUBLE
    >
  >,
  extra_info         MAP<STRING,STRING>
)
PARTITIONED BY (
  dt     STRING,
  region STRING
)
STORED AS PARQUET
LOCATION 's3://mentorhub-data-complex/transactions'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Populate metadata about new partitions
MSCK REPAIR TABLE transactions;




CREATE EXTERNAL TABLE IF NOT EXISTS users (
  user_id           STRING,
  name STRUCT<
    first_name:STRING,
    last_name:STRING
  >,
  emails            ARRAY<STRING>,
  preferences       MAP<STRING,STRING>,
  sign_up_timestamp TIMESTAMP,
  status            STRING
)
PARTITIONED BY (
  dt           STRING,
  environment  STRING
)
STORED AS PARQUET
LOCATION 's3://mentorhub-data-complex/users'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

MSCK REPAIR TABLE users;



CREATE EXTERNAL TABLE IF NOT EXISTS user_events (
  event_id    STRING,
  user_id     STRING,
  event_time  TIMESTAMP,
  event_info  STRUCT<
    type:STRING,
    device:STRING,
    location:STRING,
    metadata:MAP<STRING,STRING>
  >
)
PARTITIONED BY (
  dt           STRING,
  environment  STRING
)
STORED AS PARQUET
LOCATION 's3://mentorhub-data-complex/user_events'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

MSCK REPAIR TABLE user_events;


CREATE EXTERNAL TABLE IF NOT EXISTS merchants (
  merchant_id  STRING,
  name         STRING,
  categories   ARRAY<STRING>,
  contact      STRUCT<
    phone:STRING,
    email:STRING,
    address:MAP<STRING,STRING>
  >,
  rating       DOUBLE
)
PARTITIONED BY (
  dt     STRING,
  region STRING
)
STORED AS PARQUET
LOCATION 's3://mentorhub-data-complex/merchants'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

MSCK REPAIR TABLE merchants;

CREATE EXTERNAL TABLE IF NOT EXISTS fraudSignals (
  signal_id       STRING,
  transaction_id  STRING,
  signal_time     TIMESTAMP,
  risk_level      STRING,
  reasons         ARRAY<STRING>,
  details         MAP<STRING,STRING>
)
PARTITIONED BY (
  dt           STRING,
  environment  STRING
)
STORED AS PARQUET
LOCATION 's3://mentorhub-data-complex/fraudSignals'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

MSCK REPAIR TABLE fraudSignals;


CREATE EXTERNAL TABLE IF NOT EXISTS locations (
  location_id  STRING,
  region       STRING,
  name         STRING,
  coords       STRUCT<lat:DOUBLE, lon:DOUBLE>,
  attributes   MAP<STRING,STRING>
)
PARTITIONED BY (
  dt           STRING,
  environment  STRING
)
STORED AS PARQUET
LOCATION 's3://mentorhub-data-complex/locations'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

MSCK REPAIR TABLE locations;