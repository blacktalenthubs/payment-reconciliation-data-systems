Below is a suggested approach for organizing the **entire codebase** into two clear phases, making it straightforward to teach your students:

1. **Phase 1: Baseline Implementation** (simple, direct, minimal optimizations)  
2. **Phase 2: Performance-Optimized Implementation** (addressing data skew, partitioning, compression, etc.)

This structure allows you to easily **demo** the differences and illustrate how performance engineering refactors can improve latency, scalability, and cost management.

---

# Overall Folder Layout

A possible folder structure showing both baseline and optimized code side by side:

```
payment_fraud_demo/
├── phase1_baseline/
│   ├── schemas.py
│   ├── generate_initial_data.py
│   ├── streaming_index.py
│   ├── batch_index.py
│   ├── final_aggregate.py
│   ├── README.md
│   └── (Optional) scripts for ingestion to Kafka, etc.
├── phase2_optimized/
│   ├── schemas.py
│   ├── generate_initial_data.py
│   ├── streaming_index_optimized.py
│   ├── batch_index_optimized.py
│   ├── final_aggregate_optimized.py
│   ├── README.md
│   └── ...
└── docs/
    ├── architecture_diagrams.md
    ├── performance_engineering.md
    └── teaching_notes.md
```

- **phase1_baseline**: The simplest code that implements the functionality (two indexes + final aggregator) **without** advanced partitioning, compression, or data skew mitigation.  
- **phase2_optimized**: The “refactored” version demonstrating solutions for skew, shuffle optimizations, compression, partitioning, and so on.

Both phases can use the **same** or very similar `schemas.py` and `generate_initial_data.py`, ensuring consistent input data but different pipeline logic. This lets you run side-by-side comparisons.

---

# Phase 1 (Baseline) — Example Summaries

Below is a **very high-level summary** (not the full code) of each script in **phase1_baseline**. You already have most of these from previous discussions, but here’s how you’d keep them minimal and direct.

## 1. generate_initial_data.py  
- Creates synthetic data for User, Merchant, Transaction, Policy, FraudSignal, RiskRule.  
- Writes everything to local or S3 as unpartitioned **Parquet** with no special compression.

## 2. streaming_index.py  
- Reads Transaction & FraudSignal from Kafka.  
- Joins dimension tables.  
- Writes **Streaming Index** to Parquet (no partitioning, no compression config, no shuffle tuning).

## 3. batch_index.py  
- Reads historical Transaction & FraudSignal from S3.  
- Joins dimension tables.  
- Writes **Batch Index** to Parquet (again, no advanced partitioning or compression).

## 4. final_aggregate.py  
- Reads Streaming Index + Batch Index.  
- Union them, picks the latest record, writes **Final Aggregate**.  
- No attempt at coalesce or broadcast. Minimal, direct approach.

## 5. README.md (Phase 1)  
- Explains how to run each job (Spark commands), plus data requirements and usage.

---

# Phase 2 (Optimized) — Example Summaries

Now, in **phase2_optimized**, you re-use the same data generation script (since it’s still valid) but apply the solutions you want to demo: partitioning, compression, controlling shuffle partitions, etc.

## 1. generate_initial_data.py (same or slightly enhanced)  
- Possibly add a few more records or bigger volumes.  
- The rest can remain identical to phase1.

## 2. streaming_index_optimized.py  
- Enables `spark.sql.shuffle.partitions=200`.  
- Possibly sets `spark.sql.adaptive.enabled=true`.  
- Joins dimension tables in a single chain.  
- Writes out partitioned by a relevant column (e.g., `status`) or sets a compression codec (`snappy`).  
- Or, if you prefer, keep streaming index unpartitioned but do some basic coalesce at the end.

## 3. batch_index_optimized.py  
- Use `.partitionBy("status")` or `trans_date`.  
- Sets `spark.sql.parquet.compression.codec=snappy`.  
- Coalesces or repartitions final output to a manageable number of files.  
- Possibly does a `salting` trick or includes a small example of data skew mitigation.

## 4. final_aggregate_optimized.py  
- Same union and dedup logic, but you might add coalesce or write with partitioning if relevant.  
- Could demonstrate storing the final index in a separate location to keep incremental snapshots.

## 5. README.md (Phase 2)  
- Explains the improvements added.  
- Summarizes differences from Phase 1 (like partitioning, compression, shuffle config, data skew handling).

---

# Teaching Workflow

Below is a suggested flow for your **teaching demo**:

1. **Explain Phase 1**  
   - Show how the baseline code reads data, writes data, and note potential performance bottlenecks (like no partitioning, large or small files, possible skew).

2. **Run Phase 1**  
   - Execute the `generate_initial_data.py`.  
   - Demonstrate the streaming & batch pipeline.  
   - Possibly measure how many Parquet files get created or how the Spark UI looks during a shuffle.

3. **Explain Key Performance Problems**  
   - Data skew, uncompressed storage, no partition pruning, too many small files.  
   - Show how a big join might cause slow partitions or how queries scan everything.

4. **Move to Phase 2**  
   - Show the specific code changes: partitioning in `.write.partitionBy("...")`, setting Spark configs, using `coalesce(20)`, enabling `snappy`.  
   - Remind students how these solve the problems from Step 3.

5. **Run Phase 2**  
   - Repeat the pipeline.  
   - Observe fewer, bigger Parquet files, partitioned directories, compressed sizes.  
   - Possibly measure job durations or file size differences: see lower I/O, faster queries.

6. **Q&A**  
   - Encourage students to think about potential trade-offs: using partitionBy columns that might produce many partitions, or selecting an appropriate compression codec, etc.

---

# Conclusion

By **splitting** your entire solution into these two phases and maintaining consistent code organization, you’ll be able to **visually and practically** show your students how:

- Initial straightforward data engineering code might work but has pitfalls in large-scale or cost-sensitive environments.  
- Applying **performance engineering** transforms it into a more robust, efficient pipeline without requiring a total rewrite—just careful changes to partitioning, compression, shuffle, and join logic.

This approach keeps your teaching structured, letting students follow from a **baseline** to an **optimized** pipeline in a step-by-step manner.