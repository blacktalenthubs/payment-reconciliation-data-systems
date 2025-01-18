# udfs.py

import json
from datetime import datetime

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType


def classify_fraud_score(score):
    """
    Classify a numeric fraud_score into a label:
      >= 90 => HIGH
      >= 70 => MEDIUM
      else => LOW
    If score is None, return 'UNKNOWN'.
    """
    if score is None:
        return "UNKNOWN"
    elif score >= 90:
        return "HIGH"
    elif score >= 70:
        return "MEDIUM"
    else:
        return "LOW"


classify_fraud_score_udf = udf(classify_fraud_score, StringType())


def mask_user_name(user_name):
    """
    Simple mask: keep first letter, replace the rest with '*'.
    For demonstration. If user_name is None or empty, return as is.
    """
    if not user_name:
        return user_name
    return user_name[0] + ("*" * (len(user_name) - 1))


mask_user_name_udf = udf(mask_user_name, StringType())

# If you needed a Policy JSON parse, you could do something like:
import json


def extract_region_from_policy(policy_json):
    """
    If policy_details is something like:
      {
        "region": "US_ONLY",
        "velocity_limit": 3
      }
    We'll return the 'region' field or 'UNKNOWN' if missing.
    """
    if not policy_json:
        return "UNKNOWN"
    try:
        data = json.loads(policy_json)
        return data.get("region", "UNKNOWN")
    except:
        return "INVALID_JSON"


extract_region_udf = udf(extract_region_from_policy, StringType())


def classify_fraud_score(score):
    """
    Classify a numeric fraud_score into a label:
      >= 90 => HIGH
      >= 70 => MEDIUM
      else => LOW
    If score is None, return 'UNKNOWN'.
    """
    if score is None:
        return "UNKNOWN"
    elif score >= 90:
        return "HIGH"
    elif score >= 70:
        return "MEDIUM"
    else:
        return "LOW"


classify_fraud_score_udf = udf(classify_fraud_score, StringType())


def mask_user_name(user_name):
    """
    Simple mask: keep first letter, replace the rest with '*'.
    If user_name is None or empty, return as is.
    """
    if not user_name:
        return user_name
    return user_name[0] + ("*" * (len(user_name) - 1))


mask_user_name_udf = udf(mask_user_name, StringType())


def extract_region_from_policy(policy_json):
    """
    If policy_details is JSON, e.g.:
      {
        "region": "US_ONLY",
        "velocity_limit": 3
      }
    We'll return the 'region' field or 'UNKNOWN' if missing.
    """
    if not policy_json:
        return "UNKNOWN"
    try:
        data = json.loads(policy_json)
        return data.get("region", "UNKNOWN")
    except:
        return "INVALID_JSON"


extract_region_udf = udf(extract_region_from_policy, StringType())

#
# 2) New UDFs for Amount, Currency, and Signup Date
#

# 2A) Convert (amount, currency) => USD
exchange_rates = {
    "USD": 1.0,
    "EUR": 1.07,
    "GBP": 1.24,
    "CAD": 0.80,
    "JPY": 0.0075
    # ... add more if needed
}


def convert_currency_to_usd(amount, currency):
    """
    Convert the given amount from its currency to USD using the static rates above.
    If currency not recognized or amount is None, return the original amount or 0.0.
    """
    if amount is None:
        return 0.0
    if currency not in exchange_rates:
        return float(amount)  # fallback, keep same amount
    rate = exchange_rates[currency]
    return round(float(amount) * rate, 2)


convert_currency_to_usd_udf = udf(convert_currency_to_usd, FloatType())


# 2B) Round amount to N decimal places - for demonstration we'll fix it to 2 decimals
def round_amount(amount):
    """
    Round the amount to 2 decimals. If None, return 0.0.
    """
    if amount is None:
        return 0.0
    return round(float(amount), 2)


round_amount_udf = udf(round_amount, FloatType())


# 2C) Normalize signup date to "YYYY-MM-DD" format
def normalize_signup_date(date_str):
    """
    Takes various date string formats (like "Jan 15, 2025", "2025-01-15", etc.)
    and returns a standardized YYYY-MM-DD string.
    If parsing fails, return 'INVALID_DATE'.
    """
    if not date_str:
        return "UNKNOWN"

    # Try multiple formats in a best-effort approach
    possible_formats = [
        "%Y-%m-%d",  # e.g. "2025-01-15"
        "%b %d, %Y",  # e.g. "Jan 15, 2025"
        "%B %d, %Y",  # e.g. "January 15, 2025"
        "%m/%d/%Y",  # e.g. "01/15/2025"
    ]

    for fmt in possible_formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")  # normalized
        except ValueError:
            pass

    # If all fails:
    return "INVALID_DATE"


normalize_signup_date_udf = udf(normalize_signup_date, StringType())
