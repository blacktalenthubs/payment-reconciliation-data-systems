## **Architectural Design for Payment Recon indexing systems**
```
                                            +-------------------------------------------+
                                            |    Data Producers (APIs, Partner APIs)    |
                                            |  (Flask endpoints, external systems)       |
                                            +-------------------------+------------------+
                                                                      |
(1) Ingestion calls or data from APIs                                  |  
                                                                      v
                                           +---------------------------------------------------+
                                           |   Ingestion Scripts (Python, etc.)                |
                                           |   - batch: ingest_to_s3.py                        |
                                           |   - stream: ingest_to_kafka.py                    |
                                           +----------------------------+-----------------------+
                                                                        | (2) writes files/JSON
                                                                        |     or Parquet to S3
                                                                        v
                             +----------------------------------------------------+
                             |                 S3 (Raw Data Bucket)               |
                             |     (historical transactions, dimension tables)    |
                             +----------------------------------------------------+
                                                                        ^
                                                                        | (3) publishes messages
                                                                        |     to Kafka topics
                             +------------------------------------------+------------------------+
                             |  Kafka / MSK  (streaming input for Spark)              |
                             |  - transactions_topic, fraud_signals_topic, etc.       |
                             +------------------------------------------+------------------------+

                                                
                           +--------------------------------------------------------------+
                           |     Spark Streaming Pipeline (streaming_index_advanced.py)   |
                           |     - reads from Kafka (transactions & fraud)               |
                           |     - merges dimension data (User, Merchant, etc.)          |
                           |     - handles dedup, watermark, salting for skew            |
                           |     - writes "Streaming Index" to S3                        |
                           +-----------------------------^--------------------------------+
                                                         | (4) micro-batch Parquet output
                                                         v
          +------------------------------------------------------------------------------------+
          |                 S3 (Processed Bucket)                                              |
          |     "streaming_index/"  (continuously updated in micro-batches)                    |
          +------------------------------------------------------------------------------------+
                                                         ^
                                                         | (5) overwrites/append from next job
                                                         |
                           +-------------------------------------------------+-----------------+
                           |   Spark Batch Pipeline (batch_index_refactored.py)               |
                           |   - reads raw historical data (transactions, fraud signals) from |
                           |     S3 plus dimension tables                                     |
                           |   - merges, cleans, transforms                                   |
                           |   - writes "Batch Index" in Parquet                              |
                           +-------------------------------------------------+-----------------+
                                                         |
                                                         v
          +------------------------------------------------------------------------------------+
          |                S3 (Processed Bucket)                                               |
          |     "batch_index/" (partitioned, compressed Parquet)                               |
          +------------------------------------------------------------------------------------+
                                                         ^
                                                         | (6) reads both streaming & batch indexes
                                                         v
                           +----------------------------------------------------------+
                           |       Final Aggregator (final_aggregate.py)             |
                           |   - merges "Streaming Index" & "Batch Index"            |
                           |   - deduplicates, picks latest transaction states        |
                           |   - writes "Final Aggregate" in Parquet                 |
                           +---------------------------+------------------------------+
                                                       | (7) final snapshot
                                                       v
          +------------------------------------------------------------------------------------+
          |                      S3 (Processed Bucket)                                         |
          |   "final_aggregate/"  (comprehensive, up-to-date dataset)                         |
          +------------------------------------------------------------------------------------+
                                                       ^
                                                       |
         +---------------------------------------------------------------------------------------------------+
         |                                         Downstream Consumers                                     |
         +---------------------------------------------------------------------------------------------------+
         | (8) Athena / BI Tools:                                                                             |
         |     - We define external tables on "final_aggregate/"                                             |
         |     - Analysts query with SQL via Athena or build dashboards in Tableau / QuickSight / etc.       |
         |                                                                                                   |
         | (9) Aggregation Service:                                                                           |
         |     - Periodic job reading from "streaming_index/" or "final_aggregate/"                          |
         |     - Summarizes data (e.g. merchant totals), pushes to Redis / DynamoDB                          |
         |     - Powers real-time dashboards for ops or risk teams                                          |
         |                                                                                                   |
         | (10) ML Pipeline (offline training):                                                              |
         |     - ml_training_pipeline.py loads "final_aggregate/"                                            |
         |     - creates advanced features, trains new fraud detection model                                 |
         |     - saves model artifacts to S3 or ML registry                                                  |
         +---------------------------------------------------------------------------------------------------+
```

### Diagram Overview

1. **Data Producers**: Various APIs (Flask or partner systems) feed data.  
2. **Ingestion Scripts**: 
   - **Batch** writes directly to S3.  
   - **Stream** publishes to Kafka.  
3. **Kafka**: Holds streaming topics for real-time events.  
4. **Spark Streaming** pipeline:  
   - Reads Kafka events.  
   - Enriches with dimension data from S3.  
   - Writes “Streaming Index.”  
5. **Spark Batch** pipeline:  
   - Processes historical data from S3.  
   - Joins dimension data.  
   - Writes “Batch Index.”  
6. **Final Aggregator**: Merges both indexes to produce a “Final Aggregate.”  
7. **Downstream**:  
   - **Athena / BI** for analytics queries.  
   - **Aggregation Service** for quick rollups in a key-value store.  
   - **ML Pipeline** for building advanced fraud models.
