
---
**Tables setup using the Athena Data Generation file**
- **transactions** partitioned by `(dt, region)`
- **users** partitioned by `(dt, environment)`
- **user_events** partitioned by `(dt, environment)`
- **merchants** partitioned by `(dt, region)`
- **fraudSignals** partitioned by `(dt, environment)`
- **locations** partitioned by `(dt, environment)`


```sql
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

```

---

### Query 1: **Flatten an Array of Structs**  
 Unnesting an array to get each item row-by-row.

```sql
SELECT
  transactions.transaction_id,
  items.sku,
  items.price,
  items.quantity
FROM transactions
CROSS JOIN UNNEST(transactions.items) AS items
WHERE transactions.dt = '2025-01-01'
  AND transactions.region = 'US';
```
- `CROSS JOIN UNNEST(...)` expands `transactions.items` (array of structs) so each row in the result has one item.

---

### Query 2: **Flatten Arrays & Access Nested Maps**  
 Reading map values inside a flattened array.

```sql
SELECT
  transactions.transaction_id,
  items.sku,
  items.price,
  items.attributes['color'] AS color_used
FROM transactions
CROSS JOIN UNNEST(transactions.items) AS items
WHERE transactions.dt = '2025-01-02'
  AND transactions.region = 'EU';
```
- We reference the map key `'color'` from `items.attributes`.  

---

### Query 3: **Aggregate Array Fields**  
 Summing over flattened data.

```sql
SELECT
  transactions.transaction_id,
  SUM(items.price * items.quantity) AS total_order_value
FROM transactions
CROSS JOIN UNNEST(transactions.items) AS items
WHERE transactions.dt = '2025-01-01'
  AND transactions.region = 'APAC'
GROUP BY transactions.transaction_id
ORDER BY total_order_value DESC
LIMIT 5;
```
- After unnesting, we calculate total value (`price * quantity`) and group by the parent transaction.

---

### Query 4: **Reading Nested Struct Fields**  
 Referencing nested `location_info`.

```sql
SELECT
  transactions.transaction_id,
  transactions.location_info.country AS country_code,
  transactions.location_info.coords.lat AS latitude,
  transactions.location_info.coords.lon AS longitude
FROM transactions
WHERE transactions.dt = '2025-01-02'
  AND transactions.region = 'US'
ORDER BY transactions.transaction_id
LIMIT 10;
```
- We use the dot notation to read `location_info.country` and the nested `coords.lat`.

---

### Query 5: **Filter by Map Values**  
 Checking a condition within a map column.

```sql
SELECT
  transactions.transaction_id,
  transactions.extra_info['payment_method'] AS payment_method
FROM transactions
WHERE transactions.dt = '2025-01-01'
  AND transactions.region = 'EU'
  AND transactions.extra_info['payment_method'] = 'CRYPTO';
```
- We only return transactions using `'CRYPTO'` from the `extra_info` map.

---

### Query 6: **Get All Promo Codes**  
 Displaying map values in a separate column.

```sql
SELECT
  transactions.transaction_id,
  transactions.extra_info['promo_code'] AS promo_code
FROM transactions
WHERE transactions.dt = '2025-01-03'
  AND transactions.region = 'US';
```
- This extracts `'promo_code'` from the `extra_info` map for each transaction.

---

### Query 7: **Check Distinct Colors in Items**  
 Unnest array, read map within struct, deduplicate.

```sql
SELECT DISTINCT items.attributes['color'] AS unique_color
FROM transactions
CROSS JOIN UNNEST(transactions.items) AS items
WHERE transactions.dt = '2025-01-02'
  AND transactions.region = 'APAC'
  AND items.attributes['color'] IS NOT NULL;
```
- `DISTINCT` returns unique color values. We also ensure it’s not null.

---

### Query 8: **Show Basic User Info**  
 Access struct fields `name.first_name` & `name.last_name`.

```sql
SELECT
  users.user_id,
  users.name.first_name AS first_name,
  users.name.last_name  AS last_name
FROM users
WHERE users.dt = '2025-01-01'
  AND users.environment = 'DEV';
```
- Dot notation to retrieve nested fields from `users.name`.

---

### Query 9: **Unnest an Array of Emails**  
 Flatten user emails (array of strings).

```sql
SELECT
  users.user_id,
  email
FROM users
CROSS JOIN UNNEST(users.emails) AS email
WHERE users.dt = '2025-01-02'
  AND users.environment = 'PROD';
```
- Each email from `emails` becomes a separate row.

---

### Query 10: **Filter Users by a Specific Preference**  
 Checking a map for a given key-value pair.

```sql
SELECT
  users.user_id,
  users.preferences['theme'] AS theme_selected
FROM users
WHERE users.dt = '2025-01-03'
  AND users.environment = 'DEV'
  AND users.preferences['theme'] = 'dark';
```
- Searching for `'theme' = 'dark'` in the `preferences` map.

---

### Query 11: **Count Users by Status**  
 Simple aggregation over a normal string column.

```sql
SELECT
  users.status,
  COUNT(*) AS total_users
FROM users
WHERE users.dt = '2025-01-02'
  AND users.environment = 'PROD'
GROUP BY users.status
ORDER BY total_users DESC;
```
- Summarizes how many users per `status` exist in that partition.

---

### Query 12: **Identify PURCHASE Events Only**  
 Filtering nested struct fields in `user_events`.

```sql
SELECT
  user_events.event_id,
  user_events.user_id,
  user_events.event_info.type AS event_type,
  user_events.event_time
FROM user_events
WHERE user_events.dt = '2025-01-01'
  AND user_events.environment = 'DEV'
  AND user_events.event_info.type = 'PURCHASE';
```
- We want only `'PURCHASE'` from `event_info.type`.

---

### Query 13: **Read Map in user_events.event_info.metadata**  
 Accessing a sub-map inside a struct.

```sql
SELECT
  user_events.event_id,
  user_events.event_info.metadata['user_agent'] AS user_agent
FROM user_events
WHERE user_events.dt = '2025-01-01'
  AND user_events.environment = 'PROD';
```
- Demonstrates referencing `event_info.metadata['user_agent']`.

---

### Query 14: **Count user_events per Device**  
 Group by a nested field inside a struct.

```sql
SELECT
  user_events.event_info.device AS device_type,
  COUNT(*) AS total_events
FROM user_events
WHERE user_events.dt = '2025-01-03'
  AND user_events.environment = 'DEV'
GROUP BY user_events.event_info.device
ORDER BY total_events DESC;
```
- Summarizes how many events per device type appear.

---

### Query 15: **Join user_events with users**  
 Basic join across tables, referencing tables fully.

```sql
SELECT
  user_events.event_id,
  user_events.event_info.type AS event_type,
  users.name.first_name AS user_first_name,
  users.name.last_name  AS user_last_name
FROM user_events
JOIN users
  ON user_events.user_id = users.user_id
  AND user_events.dt = users.dt
  AND user_events.environment = users.environment
WHERE user_events.dt = '2025-01-02'
  AND user_events.environment = 'DEV';
```
- We align partitions (`dt` and `environment`) plus match `user_id`.

---

### Query 16: **Filter Merchants by Category**  
 Searching an array with `ARRAY_CONTAINS`.

```sql
SELECT
  merchants.merchant_id,
  merchants.name,
  merchants.categories
FROM merchants
WHERE merchants.dt = '2025-01-01'
  AND merchants.region = 'US'
  AND ARRAY_CONTAINS(merchants.categories, 'Fashion');
```
- We only want rows where the array `categories` has `'Fashion'`.

---

### Query 17: **Flatten Merchant Categories**  
 Unnest an array of strings to see one category per row.

```sql
SELECT
  merchants.merchant_id,
  category
FROM merchants
CROSS JOIN UNNEST(merchants.categories) AS category
WHERE merchants.dt = '2025-01-02'
  AND merchants.region = 'EU';
```
- Each row returned has one category from the `categories` array.

---

### Query 18: **Explore Merchant Contact Info**  
 Reading nested struct + map inside `contact`.

```sql
SELECT
  merchants.merchant_id,
  merchants.contact.phone AS phone_number,
  merchants.contact.address['city'] AS city_in_address,
  merchants.contact.address['postal_code'] AS zip_code
FROM merchants
WHERE merchants.dt = '2025-01-03'
  AND merchants.region = 'APAC';
```
- `contact` is a struct; `contact.address` is a map.

---

### Query 19: **Top 5 Merchants by Rating**  
 Ordering results by a numeric field (descending).

```sql
SELECT
  merchants.merchant_id,
  merchants.name,
  merchants.rating
FROM merchants
WHERE merchants.dt = '2025-01-02'
  AND merchants.region = 'US'
ORDER BY merchants.rating DESC
LIMIT 5;
```
- Standard ordering on a `DOUBLE` rating field.

---

### Query 20: **Find Fraud Signals with 'GeoMismatch'**  
 Searching arrays in `fraudSignals`.

```sql
SELECT
  fraudSignals.signal_id,
  fraudSignals.transaction_id,
  fraudSignals.risk_level,
  fraudSignals.reasons
FROM fraudSignals
WHERE fraudSignals.dt = '2025-01-01'
  AND fraudSignals.environment = 'PROD'
  AND ARRAY_CONTAINS(fraudSignals.reasons, 'GeoMismatch');
```
- We look for `'GeoMismatch'` within the array `fraudSignals.reasons`.

---

### Query 21: **Count Fraud Signals by Risk**  
 Simple grouping.

```sql
SELECT
  fraudSignals.risk_level,
  COUNT(*) AS total_signals
FROM fraudSignals
WHERE fraudSignals.dt = '2025-01-02'
  AND fraudSignals.environment = 'DEV'
GROUP BY fraudSignals.risk_level
ORDER BY total_signals DESC;
```
- Tally how many signals for each `risk_level`.

---

### Query 22: **View IP Address from details Map**  
 Accessing a key in the `details` map.

```sql
SELECT
  fraudSignals.signal_id,
  fraudSignals.details['ip_address'] AS ip_address
FROM fraudSignals
WHERE fraudSignals.dt = '2025-01-02'
  AND fraudSignals.environment = 'PROD'
  AND fraudSignals.risk_level = 'HIGH';
```
- Map access with `fraudSignals.details['ip_address']`.

---

### Query 23: **Locations Basic Select**  
 Reading a struct inside `coords`.

```sql
SELECT
  locations.location_id,
  locations.name AS location_name,
  locations.coords.lat AS latitude,
  locations.coords.lon AS longitude
FROM locations
WHERE locations.dt = '2025-01-01'
  AND locations.environment = 'DEV';
```
- Dot notation to get nested `lat` and `lon`.

---

### Query 24: **Check Map in Locations Table**  
 Reading keys from `attributes`.

```sql
SELECT
  locations.location_id,
  locations.attributes['timezone'] AS timezone_attr,
  locations.attributes['local_language'] AS local_lang
FROM locations
WHERE locations.dt = '2025-01-02'
  AND locations.environment = 'PROD';
```
- We get `'timezone'` and `'local_language'` from the map.

---

### Query 25: **Join transactions with fraudSignals**  
 Combine two partitioned tables by transaction ID.

```sql
SELECT
  fraudSignals.signal_id,
  fraudSignals.risk_level,
  transactions.transaction_id,
  transactions.extra_info['payment_method'] AS pay_method
FROM fraudSignals
JOIN transactions
  ON fraudSignals.transaction_id = transactions.transaction_id
  -- if dt or region align, you can also match them, e.g. fraudSignals.dt = transactions.dt
WHERE fraudSignals.dt = '2025-01-03'
  AND fraudSignals.environment = 'DEV'
  AND transactions.dt = '2025-01-03'
  AND transactions.region = 'US';
```
- We match by `transaction_id`. This is a typical cross-table join for analysis.

---

### Query 26: **Check Total Amount for Fraudulent Transactions**  
 Flatten items, join to fraudSignals, compute total.

```sql
SELECT
  fraudSignals.signal_id,
  fraudSignals.risk_level,
  transactions.transaction_id,
  SUM(items.price * items.quantity) AS total_value
FROM fraudSignals
JOIN transactions
  ON fraudSignals.transaction_id = transactions.transaction_id
CROSS JOIN UNNEST(transactions.items) AS items
WHERE fraudSignals.dt = '2025-01-01'
  AND fraudSignals.environment = 'PROD'
GROUP BY
  fraudSignals.signal_id,
  fraudSignals.risk_level,
  transactions.transaction_id
HAVING SUM(items.price * items.quantity) > 100
ORDER BY total_value DESC;
```
- Demonstrates **join + unnest** + **aggregate** for suspicious transactions over $100.

---

### Query 27: **Compare user_events to users**  
 Another join example matching environment & dt, showing nested fields.

```sql
SELECT
  user_events.event_id,
  user_events.event_time,
  users.name.first_name,
  user_events.event_info.device,
  user_events.event_info.metadata['referrer'] AS ref_url
FROM user_events
JOIN users
  ON user_events.user_id = users.user_id
  AND user_events.dt = users.dt
  AND user_events.environment = users.environment
WHERE user_events.dt = '2025-01-02'
  AND user_events.environment = 'DEV';
```
- Illustrates combining data from `user_events` and `users`.

---

### Query 28: **Show All Coordinates from transactions & locations**  
 Reading struct fields from two tables in a single query (union or separate, if we had logic).

**(Option 1)** If you just want them separate, do two queries or a union. Here’s a demonstration with union (assuming same column list):
```sql
SELECT
  transactions.location_info.coords.lat AS lat,
  transactions.location_info.coords.lon AS lon,
  'transactions' AS source
FROM transactions
WHERE transactions.dt = '2025-01-02'
  AND transactions.region = 'US'

UNION ALL

SELECT
  locations.coords.lat AS lat,
  locations.coords.lon AS lon,
  'locations' AS source
FROM locations
WHERE locations.dt = '2025-01-02'
  AND locations.environment = 'DEV';
```
- `UNION ALL` merges two result sets. The schema must match.

---

### Query 29: **Window Function Example**  
 Assign row numbers by time for each user in `user_events`.

```sql
WITH numbered AS (
  SELECT
    user_events.user_id,
    user_events.event_time,
    ROW_NUMBER() OVER (
      PARTITION BY user_events.user_id
      ORDER BY user_events.event_time
    ) AS row_num
  FROM user_events
  WHERE user_events.dt = '2025-01-03'
    AND user_events.environment = 'PROD'
)
SELECT *
FROM numbered
ORDER BY user_id, row_num;
```
- `ROW_NUMBER()` is a **window function** that orders events for each user.

---

### Query 30: **Check Missing Promo Codes**  
 Testing `IS NULL` for a map key that doesn’t exist.

```sql
SELECT
  transactions.transaction_id,
  transactions.extra_info,
  transactions.extra_info['promo_code'] AS maybe_promo
FROM transactions
WHERE transactions.dt = '2025-01-03'
  AND transactions.region = 'EU'
  AND transactions.extra_info['promo_code'] IS NULL;
```
- We look for transactions that lack a `promo_code` key or it’s null.

---


---

## 1) **Viewing All Partitions Metadata**

**Problem**: How do we see the complete list of partition values for a given table without scanning data?  
 Using the special `table$partitions` to inspect partition metadata.

```sql
SELECT *
FROM "transactions$partitions"
ORDER BY dt, region;
```


  - This query returns each partition folder path known to Athena (e.g., `dt=2025-01-01/region=US`).  
  - **No data** is scanned from the underlying files.

---

## 2) **Finding Latest Partition per Region**

**Problem**: You need to figure out, for each region, the most recent `dt` partition in `transactions`.  
 Aggregating partition metadata.

```sql
SELECT 
  region, 
  MAX(dt) AS latest_dt
FROM "transactions$partitions"
GROUP BY region;
```


  - This query scans **only** partition metadata, not data files.  
  - Great for quickly identifying the newest partition per region.

---

## 3) **Minimizing Scan by Partitioning on Both `dt` and `region`**

**Problem**: Show how restricting **both** partition columns can massively reduce scanned data.  
 Partition pruning with multiple columns.

```sql
SELECT
  transaction_id,
  user_id
FROM transactions
WHERE dt = '2025-01-02'
  AND region = 'APAC';
```


  - Only the S3 folders `dt=2025-01-02/region=APAC` are read.  
  - Partition elimination drastically lowers the cost and runtime vs. scanning all dt or all region values.

---

## 4) **Filter on Non-Partition Column vs. Partition Column**

**Problem**: Demonstrate the difference in performance if you forget to filter on partition columns.  
 Partition vs. non-partition filtering.

```sql
-- LESS EFFICIENT query: ignoring dt, region
SELECT transaction_id
FROM transactions
WHERE extra_info['payment_method'] = 'CARD';

-- MORE EFFICIENT query: 
SELECT transaction_id
FROM transactions
WHERE dt = '2025-01-01'
  AND region = 'US'
  AND extra_info['payment_method'] = 'CARD';
```


  - The first query scans **all partitions**.  
  - The second query scans **only** `dt=2025-01-01, region=US` partition, saving cost.

---

## 5) **Using `SHOW PARTITIONS`**

**Problem**: Another way to list partitions for a table.  
 `SHOW PARTITIONS` (older approach, but can still be used).

```sql
SHOW PARTITIONS transactions;
```


  - Returns a list of partition values known to Glue (similar to `table$partitions`).  
  - For large numbers of partitions, `table$partitions` can be more flexible (e.g., you can `SELECT` or `GROUP BY` them).

---

## 6) **MSCK REPAIR TABLE** to Load New Partitions

**Problem**: How to synchronize newly added partitions in S3 with Athena’s metadata.  
 Using `MSCK REPAIR TABLE`.

```sql
MSCK REPAIR TABLE transactions;
```


  - This crawls the S3 path structure to find any newly created partition folders.  
  - Important after you do new writes that create partition subfolders.

---

## 7) **Partition Elimination with `locations` Table** (Two Columns)

**Problem**: Demonstrate filtering on `(dt, environment)`.  
 Specifying both partition columns for efficient queries.

```sql
SELECT
  location_id,
  coords.lat,
  coords.lon
FROM locations
WHERE dt = '2025-01-01'
  AND environment = 'DEV';
```


  - Athena will scan only that `dt=2025-01-01/environment=DEV` partition directory.  
  - Minimizes cost.

---

## 8) **Partition-level Grouping / Summaries**

**Problem**: Summarize how many users exist per day, ignoring environment.  
 Grouping by partition column to see daily roll-ups.

```sql
SELECT 
  dt,
  COUNT(*) AS total_users
FROM users
GROUP BY dt
ORDER BY dt;
```


  - Aggregates by the partition column `dt`.  
  - Scans all partitions but quickly groups them by date.

---

## 9) **Compare Partition-based Joins vs. Non-Partition Joins**

**Problem**: Show how matching partition columns in a join can reduce scanned data.  
 If tables share partition columns, you can join on them for extra pruning.

```sql
SELECT
  transactions.transaction_id,
  merchants.merchant_id,
  merchants.name
FROM transactions
JOIN merchants
  ON transactions.merchant_id = merchants.merchant_id
  AND transactions.dt = merchants.dt
  AND transactions.region = merchants.region
WHERE transactions.dt = '2025-01-02'
  AND transactions.region = 'US';
```


  - Both sides filter by the same `(dt, region)` partition, so Athena only scans relevant subsets.  
  - If you omit partition columns in the join condition, Athena might scan more data.

---

## 10) **EXPLAIN Statement** (Analyzing Query Plan)

**Problem**: Check the query plan and see partition filter usage.  
 `EXPLAIN` helps you confirm if Athena is applying partition pruning.

```sql
EXPLAIN
SELECT
  transaction_id
FROM transactions
WHERE dt = '2025-01-02'
  AND region = 'EU';
```


  - `EXPLAIN` prints the plan, showing that only partition `dt=2025-01-02, region=EU` will be scanned.  
  - good way to confirm performance optimizations.

---

- **Use `MSCK REPAIR TABLE`** or **`ALTER TABLE ADD PARTITION`** to keep metadata in sync.  
- **`table$partitions`** or **`SHOW PARTITIONS`** queries are an efficient way to see partition metadata—**no** data scanning is involved.  
- **EXPLAIN** statements can help diagnose if Athena properly prunes partitions or if you’re scanning more data than needed.
1. **Partitioning**: Always filter by partition columns (`dt`, `region`, `environment`) to minimize scan.  
2. **Structs**: Use dot notation (`transactions.location_info.city`).  
3. **Arrays**: `CROSS JOIN UNNEST(...)` flattens them. Functions like `ARRAY_CONTAINS` let you check membership.  
4. **Maps**: Access with `map_column['key']`. Check missing keys via `IS NULL`.  
5. **Joins**: Combine data across tables by matching keys. Partition columns might also be used in the join condition if relevant.  
6. **Window Functions**: Over partitions (e.g., `ROW_NUMBER() OVER (...)`) let you do advanced analytics (ranking, time-based ordering).  
