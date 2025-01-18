# schemas.py

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
)

# 1) User
UserSchema = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("user_name", StringType(), nullable=True),
    StructField("signup_date", StringType(), nullable=True),
])

# 2) Merchant
MerchantSchema = StructType([
    StructField("merchant_id", StringType(), nullable=False),
    StructField("merchant_name", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("risk_level", StringType(), nullable=True)
])

# 3) Transaction
TransactionSchema = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=True),
    StructField("merchant_id", StringType(), nullable=True),
    StructField("timestamp", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("currency", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("policy_id", StringType(), nullable=True)  # optional
])

# 4) Policy
PolicySchema = StructType([
    StructField("policy_id", StringType(), nullable=False),
    StructField("policy_name", StringType(), nullable=True),
    StructField("effective_date", StringType(), nullable=True),
    StructField("policy_details", StringType(), nullable=True)
])

# 5) FraudSignal
FraudSignalSchema = StructType([
    StructField("fraud_id", StringType(), nullable=False),
    StructField("transaction_id", StringType(), nullable=False),
    StructField("model_version", StringType(), nullable=True),
    StructField("fraud_score", DoubleType(), nullable=True),
    StructField("inference_time", StringType(), nullable=True),
    StructField("reason_code", StringType(), nullable=True)
])

# 6) RiskRule
RiskRuleSchema = StructType([
    StructField("rule_id", StringType(), nullable=False),
    StructField("rule_name", StringType(), nullable=True),
    StructField("severity", StringType(), nullable=True),
    StructField("created_at", StringType(), nullable=True)
])
