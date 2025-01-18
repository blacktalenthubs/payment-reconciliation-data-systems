**Low-Level Data Systems Design**

**Overview**  
This section dives deeper into the core engineering decisions and data architecture patterns, discussing whether to adopt a Lambda or Kappa approach, how we plan to store and process data, the trade-offs in our design, and strategies for performance, testing, and scaling. It covers common data engineering pitfalls and how we plan to mitigate them, ultimately setting the stage for the actual coding and implementation in Python, Spark Structured Streaming, and related AWS services.

---

**Architectural Pattern**  
- **Kappa vs. Lambda**  
  - **Kappa Architecture**: Treats all data as real-time streams, replaying event logs for both real-time and batch views. This approach simplifies code paths by having one pipeline handle streaming data, with the possibility of reprocessing from the source stream if needed.  
  - **Lambda Architecture**: Splits data into real-time (speed) and batch (batch) layers, then merges the results. It can be easier to reason about but often duplicates logic.  

We lean toward a **Kappa-like approach** where all incoming data is treated as streams (from MSK). For historical or late-arriving data, we can replay from Kafka or ingest it in “batch mode” through the same pipeline logic (Spark Structured Streaming micro-batches). This unifies code paths, minimizes duplication, and simplifies maintenance. However, a small “batch ETL” portion remains for large historical reprocessing and final compactions—so it’s a near-Kappa design, not purely Kappa.

---

**Storage & Compute Choices**  
- **Event Streaming & Storage**  
  - **AWS MSK (Kafka)** for real-time ingestion, ensures a scalable, fault-tolerant event log that can handle bursts of transactions.  
  - **Amazon S3** for any batch/archival data. Also used for longer-term retention and replay if required.

- **Compute Engines**  
  - **Spark Structured Streaming** on EMR containers or a dedicated Spark cluster. This allows micro-batch or continuous processing of Kafka streams.  
  - **Apache Spark on Amazon EMR** for batch jobs that do large-scale historical reprocessing or compaction steps.  
  - Using the same framework (Spark) across streaming and batch reduces complexity and improves developer velocity.

- **Index/Serving Layer**  
  - **DynamoDB or Amazon OpenSearch Service** for sub-second lookups. DynamoDB provides straightforward key-based queries, while OpenSearch offers full-text or more complex queries. The choice depends on required query patterns:
    - If primarily key-value or range queries by `transaction_id` or `user_id`, DynamoDB is simpler.  
    - If searching across multiple fields or advanced filtering, OpenSearch might be better.  
  - Both support high throughput and flexible scaling in AWS.

---

**Trade-Offs**  
- **MSK vs. Kinesis**  
  - MSK (Kafka) offers broader community tooling and advanced partition controls, but can be more complex to manage. Kinesis is fully managed by AWS but has certain limits on partition throughput and consumer patterns. We choose MSK for its open-source ecosystem and flexibility.  
- **DynamoDB vs. OpenSearch**  
  - DynamoDB excels at consistent, low-latency key-value queries with predictable scaling. However, it lacks advanced search.  
  - OpenSearch offers flexible search and aggregation but can be more costly and complex to scale for heavy write scenarios.  
  - We can also run a hybrid model if needed (DynamoDB for real-time transactions, OpenSearch for advanced search queries).  
- **EMR vs. Serverless**  
  - EMR provides full Spark cluster control and fine-grained scaling. Serverless frameworks (e.g., AWS Glue, EMR Serverless) simplify ops but offer less control.  
  - For large-scale transformations or specialized configurations, we prefer full EMR.

---

**Performance Engineering & Testing**  
- **Performance Targets**  
  - Sub-second to low-seconds latency for real-time transactions.  
  - Batch pipelines completing within defined SLA (e.g., daily by 2 AM) for large datasets.

- **Performance Tuning**  
  - **Spark Structured Streaming**:  
    - Optimize micro-batch intervals (e.g., small intervals to reduce latency, but large enough to handle overhead).  
    - Use checkpointing and caching to minimize repeated computations.  
    - Leverage adaptive execution to handle skew or varying data volumes.  
  - **Partitions & Parallelism**:  
    - Tune the number of Kafka partitions to spread out load.  
    - Configure Spark shuffle partitions carefully to avoid bottlenecks or excessive overhead.  
  - **Indexing Strategy**:  
    - For DynamoDB, use carefully chosen partition keys to avoid “hot partitions.”  
    - For OpenSearch, use appropriate sharding and indexing for write-heavy ingestion.

- **Load & Stress Testing**  
  - Generate synthetic transaction data at peak expected rates (e.g., holiday spikes).  
  - Validate end-to-end latency from ingestion to indexing under stress.  
  - Monitor Spark job throughput, MSK consumer lag, DynamoDB write/read capacity usage.  
  - Identify bottlenecks (e.g., shuffle saturation in Spark, partition hotspots in MSK).

---

**Scaling & Latency**  
- **Horizontal Scaling**  
  - MSK can add partitions for more throughput.  
  - EMR clusters can add worker nodes on demand.  
  - DynamoDB auto-scaling can adjust read/write capacity based on traffic.  
- **Latency Considerations**  
  - End-to-end pipeline latency is primarily shaped by micro-batch intervals in Spark and the write latency to the serving layer.  
  - For strict real-time constraints, consider continuous mode in Spark Structured Streaming or use small micro-batch intervals (like 1-2 seconds).  
  - If latencies still exceed requirements, a specialized low-latency pipeline or caching layer (e.g., Redis) might be introduced.

---

**Key Data Engineering Problems & Mitigation**  
- **Data Skew**  
  - Certain merchants or user segments may produce disproportionately large volumes of transactions.  
  - Use partitioning keys that distribute load (e.g., hashing `user_id` + time).  
  - Leverage adaptive partitioning in Spark to handle skewed partitions.

- **Late-Arriving or Partial Data**  
  - Transactions sometimes arrive after the real-time window has passed or are corrected days later.  
  - Implement reprocessing logic: maintain durable event logs in MSK or store raw data in S3 for replays.  
  - Use the same Spark Structured Streaming code to re-consume from Kafka offsets if needed.

- **Schema Evolution**  
  - Payment data frequently changes with new fields or formats.  
  - Integrate a schema registry (e.g., Confluent for Kafka or Glue Data Catalog) for versioning.  
  - Write transformations that can handle missing or extra fields gracefully.

- **Fault Tolerance**  
  - Spark’s checkpointing ensures exactly-once or at-least-once processing semantics.  
  - MSK retains events for a defined retention period, enabling replay on failure.  
  - DynamoDB or OpenSearch replication across availability zones reduces data loss risk.

- **Duplicate Transactions**  
  - Payment systems may generate the same transaction multiple times or with partial updates.  
  - Deduplicate using a unique key (`transaction_id`) in the index, combined with a version/timestamp approach.

- **Data Quality**  
  - Validate numeric fields (e.g., `amount` > 0).  
  - Log or quarantine malformed records.  
  - Enforce consistency checks before committing final changes to the index.

---

**Conclusion & Next Steps**  
This low-level design phase clarifies the core engineering choices—adopting a near-Kappa architecture with MSK, Spark Structured Streaming, and AWS EMR. It also details how we plan to store, process, and serve data at scale while addressing typical data engineering challenges like data skew, late arrivals, and schema evolution.  
Moving forward, these decisions guide the actual coding and infrastructure setup, including Python-based Spark transformations, CI/CD pipelines, and detailed test suites (integration, performance, and stress). This implementation will yield a robust, highly scalable Payment & Fraud Signals Data Platform ready for real-world production workloads.




**Low-Level Data Systems Design**

**Overview**  
We have decided to follow a near-Kappa architecture for our Payment & Fraud Signals Data Platform. Our goal is to maintain a single code path for both real-time and batch-style data processing, reducing duplication and complexity. That said, we still expect to run certain batch-oriented jobs (like large-scale historical reprocessing or compactions) on a schedule. Below, we outline our data sources, discuss why we chose Kappa, and detail how we will handle performance, scalability, and key engineering challenges.

---

**Data Sources**  
- **Policy Data**  
  - Our policy data will arrive via a partner API that we call periodically.  
  - We will ingest these JSON payloads into Amazon S3 so we have a reliable, versioned record of policy changes (e.g., velocity limits, region blocks).  
  - We plan to keep this ingestion relatively simple (e.g., a scheduled AWS Lambda or small Python script) that fetches updates every few hours, stores them in S3, and publishes a message to Kafka indicating new policy data is ready.

- **Transaction Data**  
  - Our transaction data originates from a payment API. We have a microservice that calls this API in real time and pushes the resulting events into Kafka topics hosted on AWS MSK (Managed Streaming for Apache Kafka).  
  - Each transaction event includes fields like `transaction_id`, `user_id`, `amount`, `timestamp`, and a minimal set of contextual attributes.  
  - We enrich these transactions in real time by merging them with policy information (from S3 or a fast lookup store) and by calling fraud detection services or reading ML signals from Kafka topics.

- **Merchant Data**  
  - We also receive merchant or partner details via another API. These details might include merchant categories, risk levels, or contract statuses.  
  - Similar to policy data, we will store merchant information in S3 and occasionally push updates to MSK if we need them in real-time streams.  
  - We will rely on merchant metadata for advanced risk logic or policy-based decisions.

---

**Why We Chose a Near-Kappa Architecture**  
- **Unified Code Path**  
  - We want to write our enrichment logic once and apply it to both real-time and batch data. By treating incoming data as streams from MSK, we can replay events for historical reprocessing without maintaining completely separate code.  
- **Event-Centric Thinking**  
  - Our platform is event-driven, with transactions arriving continuously. Kappa aligns well with continuous ingestion. We can load older or corrected transactions from S3 into the same pipeline if needed.  
- **Reduced Duplication**  
  - Traditional Lambda architecture splits logic across two parallel pipelines (batch and speed). We prefer a single Spark Structured Streaming approach that can handle micro-batch intervals for real-time data, while still supporting large-scale batch runs for historical data.  
- **Flexibility for Future Growth**  
  - As we add more event sources or ML outputs, we can simply publish new events to Kafka and rely on our existing Spark pipeline to process them.

---

**Storage and Compute Choices**  
- **MSK (Kafka) for Real-Time Feeds**  
  - We will store transaction and policy update events in Kafka topics. Each event can be re-played if we need to recompute or re-run part of our pipeline.  
- **Amazon S3 for Batch / Archival**  
  - We will keep versioned JSON or Parquet files in S3 for policy data, merchant updates, and any other large-scale data sets that don’t arrive continuously.  
  - Spark on EMR will handle batch ingestion and transformations of these files.  
- **Spark Structured Streaming**  
  - We plan to run Spark Structured Streaming on AWS EMR (or potentially on Kubernetes) to process live Kafka topics.  
  - We will handle micro-batches of transaction data, apply fraud signals, incorporate merchant and policy info, and produce an enriched output.  
- **Serving Index**  
  - For sub-second queries, we expect to use DynamoDB if our queries are primarily key-based lookups (for example, get details by `transaction_id`).  
  - If we need search-based queries (e.g., partial matches, advanced filtering), we will use Amazon OpenSearch. For now, we lean toward DynamoDB for simpler key-value retrieval since that covers many payment use cases.

---

**Performance, Scale, and Latency**  
- **Real-Time Target**  
  - We aim for under two seconds of end-to-end latency from the moment a transaction hits Kafka to when the enriched record is available in DynamoDB.  
  - We will tune our Spark micro-batch interval (e.g., one-second or two-second intervals) to balance throughput and latency.  
- **Batch SLA**  
  - Our large-scale historical or daily runs should complete within defined business SLAs (e.g., by 2 AM for settlement tasks).  
  - Autoscaling EMR clusters will help us handle peak loads, such as end-of-month spikes in transaction volume.  
- **Partitions & Parallelism**  
  - We plan to configure Kafka partitions carefully to avoid hot partitions (e.g., hashing on `transaction_id` or `user_id`).  
  - We will adjust Spark’s shuffle partition counts to match data volumes and rely on adaptive execution to handle skew.  
- **Horizontal Scalability**  
  - Kafka can scale horizontally by adding more partitions or brokers, while EMR can scale by adding nodes in real time.  
  - DynamoDB scales automatically based on our read/write capacity settings.

---

**Key Data Engineering Problems and Mitigation**  
- **Data Skew**  
  - We will avoid hashing on timestamps or a single high-traffic merchant. Instead, we plan to combine multiple fields (e.g., `transaction_id + user_id`) to distribute load.  
  - Adaptive Spark partitioning can help if one merchant or user triggers disproportionate traffic.  
- **Late-Arriving Data**  
  - We will rely on Kafka’s retention policies and our S3 archives to replay or reprocess late or corrected events. Spark Structured Streaming can re-seek Kafka offsets for replays, ensuring consistent logic.  
- **Schema Evolution**  
  - We will store schemas in a Glue Data Catalog (or Confluent Schema Registry if we need advanced schema evolution for Kafka). Our code will gracefully handle new fields or missing older fields.  
- **Duplicates**  
  - We will deduplicate at the aggregator level (DynamoDB or Spark) using a unique transaction_id. If a transaction arrives twice, only the most recent or highest version is preserved.  
- **Data Quality**  
  - We will validate numerical fields like amounts (>0) and reject or quarantine malformed records in S3 for follow-up.  
  - Additional checks for invalid currency codes or policy references will trigger alerts in CloudWatch.

---

**Conclusion and Next Steps**  
We are building a near-Kappa data platform that ingests policy information, transactions, and merchant details primarily via MSK (Kafka) and secondarily from S3 for batch processing. We chose this design to unify our streaming and batch pipelines under Spark Structured Streaming. By doing so, we reduce complexity, allow for easy replay of events, and simplify future enhancements. We will now proceed with coding the Spark jobs in Python, defining our CI/CD pipeline, and configuring AWS infrastructure so we can bring this platform to life in production.