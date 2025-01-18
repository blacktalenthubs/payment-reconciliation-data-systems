```mermaid

flowchart TB
    A1((Kafka<br>Transactions & FraudSignals)) -->|Spark Reads| SIndexJob[Streaming Index Job]
    SIndexJob -->|Writes| SIndex[S3: Streaming Index]

    B1((S3: Historical<br>Transactions,<br>Policies,<br>FraudSignals)) -->|Spark Reads| BIndexJob[Batch Index Job]
    BIndexJob -->|Writes| BIndex[S3: Batch Index]

    D1((S3: Dimension Tables<br>(User, Merchant, RiskRule, etc.))) -- Broadcast Joins --> SIndexJob
    D1 -- Broadcast Joins --> BIndexJob

    SIndex --> FinalJob[Final Aggregate Job]
    BIndex --> FinalJob
    FinalJob --> FIndex[S3: Final Index]


```