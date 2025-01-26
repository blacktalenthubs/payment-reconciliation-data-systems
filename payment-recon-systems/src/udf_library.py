#!/usr/bin/env python3
"""
udf_library.py

PySpark UDF definitions for Payment Reconciliation Index System:
1) udf_calc_discrepancy(amount, amount_posted)
2) udf_flag_compliance(fraud_score, policy_flags)
3) udf_mask_sensitive(user_id)
4) udf_derive_status(cur_status, bank_status)
5) udf_kpi_calculation(merchant_id, daily_index)

Author: Your Name
Date: 2025-01-19
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, BooleanType, ArrayType, IntegerType


# ----------------------------------------------------------------------
# 1. udf_calc_discrepancy
# ----------------------------------------------------------------------

def calc_discrepancy_py(amount: float, amount_posted: float) -> float:
    """
    Calculates the difference between what we expected (amount)
    and what the bank posted (amount_posted).
    Example return: amount_posted - amount
    """
    if amount is None or amount_posted is None:
        return 0.0  # or handle None logic differently
    return round(amount_posted - amount, 2)


udf_calc_discrepancy = F.udf(calc_discrepancy_py, DoubleType())


# ----------------------------------------------------------------------
# 2. udf_flag_compliance
# ----------------------------------------------------------------------

def flag_compliance_py(fraud_score: float, policy_flags: str) -> str:
    """
    Determines a compliance/risk label based on fraud_score + policy flags.
    This is a simple example:
      - If fraud_score > 0.8 or if "BLACKLIST" is in policy_flags,
        return 'HIGH_RISK'
      - Else if "KYC" in policy_flags, return 'REQUIRES_KYC'
      - Otherwise, return 'OK'

    Adjust logic as needed, especially if policy_flags is stored as an array.
    """
    if fraud_score is None:
        fraud_score = 0.0
    # Example: policy_flags might be a comma-separated string or a JSON array
    # For simplicity, let's just treat it as a string search
    flags = policy_flags.upper() if policy_flags else ""

    if fraud_score > 0.8 or "BLACKLIST" in flags:
        return "HIGH_RISK"
    elif "KYC" in flags:
        return "REQUIRES_KYC"
    else:
        return "OK"


udf_flag_compliance = F.udf(flag_compliance_py, StringType())


# ----------------------------------------------------------------------
# 3. udf_mask_sensitive
# ----------------------------------------------------------------------

def mask_sensitive_py(user_id: str) -> str:
    """
    Masks or tokenizes a user_id to protect PII.
    Example: keep only first 2 characters, then mask the rest.
    If user_id = 'USR-1234', result might be 'US********'
    """
    if not user_id:
        return ""
    # Example: just a simple mask after first 2 characters
    return user_id[:2] + "*" * (len(user_id) - 2)


udf_mask_sensitive = F.udf(mask_sensitive_py, StringType())


# ----------------------------------------------------------------------
# 4. udf_derive_status
# ----------------------------------------------------------------------

def derive_status_py(cur_status: str, bank_status: str) -> str:
    """
    Resolves the final status of a transaction by blending the real-time status
    (cur_status) with aggregator or bank statements (bank_status).
    This is a placeholder for your business logic:
      - If bank_status == "MATCHED" and cur_status in ("APPROVED", "REFUNDED"), keep cur_status
      - If bank_status == "UNMATCHED", return "DISCREPANCY"
      - etc.

    Adjust as needed.
    """
    # Example approach:
    if not bank_status:
        # fallback to current
        return cur_status if cur_status else "UNKNOWN"

    bank_status_upper = bank_status.upper()

    if bank_status_upper == "MATCHED":
        # keep the current status if it exists
        return cur_status if cur_status else "MATCHED"
    elif bank_status_upper == "UNMATCHED":
        return "DISCREPANCY"
    else:
        # fallback
        return cur_status if cur_status else bank_status


udf_derive_status = F.udf(derive_status_py, StringType())


# ----------------------------------------------------------------------
# 5. udf_kpi_calculation
# ----------------------------------------------------------------------

def kpi_calculation_py(merchant_id: str, daily_value: float) -> float:
    """
    Computes a KPI for each merchant. Example: multiply daily_value by a factor
    or apply some logic to produce a KPI. In real usage, you might
    aggregate at a merchant/day level, sum amounts, etc.

    This is a placeholder example returning daily_value * a random multiplier.
    """
    if daily_value is None:
        daily_value = 0.0

    # For demonstration, let's just add 10% if merchant_id starts with 'MCH-A'
    if merchant_id and merchant_id.startswith("MCH-A"):
        return round(daily_value * 1.1, 2)
    else:
        return round(daily_value, 2)


udf_kpi_calculation = F.udf(kpi_calculation_py, DoubleType())

# ----------------------------------------------------------------------
# EXAMPLE USAGE in Spark:
# df = df.withColumn("discrepancy", udf_calc_discrepancy(F.col("amount"), F.col("amount_posted")))
# df = df.withColumn("risk_label", udf_flag_compliance(F.col("fraud_score"), F.col("policy_flags")))
# df = df.withColumn("masked_user", udf_mask_sensitive(F.col("user_id")))
# df = df.withColumn("final_status", udf_derive_status(F.col("cur_status"), F.col("bank_status")))
# df = df.withColumn("merchant_kpi", udf_kpi_calculation(F.col("merchant_id"), F.col("daily_value")))
# ----------------------------------------------------------------------
