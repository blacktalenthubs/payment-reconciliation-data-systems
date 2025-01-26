
---

## What Are UDFs?

- A User-Defined Function (UDF) is a custom function you create and execute within Spark’s DataFrame or SQL contexts.  
- Spark provides many built-in functions, but sometimes your unique business logic or data transformations aren’t covered by these. UDFs let you plug in your own logic and apply it row by row in a DataFrame.  
-  Because UDFs run Python code for each record, they can be slower than using Spark’s native functions. Only use them when built-ins can’t accomplish your goal.

---

## UDF Life Cycle

1. **Write** a Python function that does the custom logic.  
2. **Register** it as a Spark UDF, specifying the return type (e.g., `DoubleType`).  
3. **Apply** it on columns in a DataFrame, often using `df.withColumn("new_col", my_udf(F.col("existing_col")))`.  
4. **Optimize** if you run into performance bottlenecks, or consider using built-in Spark functions or SQL expressions if possible.

---

## Example UDFs in Our Payment Reconciliation Context

**UDF examples used to enrich data in a Payment Reconciliation Index pipeline. Each addresses a distinct business scenario**

### `udf_calc_discrepancy`
- Calculate the difference between expected `amount` and `amount_posted`.
  ```python
  def calc_discrepancy_py(amount, amount_posted):
      if amount is None or amount_posted is None:
          return 0.0
      return round(amount_posted - amount, 2)
  ```
- **Usage**:
  ```python
  df = df.withColumn(
      "discrepancy",
      udf_calc_discrepancy(F.col("amount"), F.col("amount_posted"))
  )
  ```
- **Business Impact**: Easily identifies mismatches for reconciliation.

### `udf_flag_compliance`
- **Purpose**: Assign a risk/compliance label based on `fraud_score` and any policy flags (like “BLACKLIST”).  
- **Logic**:
  ```python
  def flag_compliance_py(fraud_score, policy_flags):
      if fraud_score > 0.8 or "BLACKLIST" in (policy_flags or "").upper():
          return "HIGH_RISK"
      elif "KYC" in (policy_flags or "").upper():
          return "REQUIRES_KYC"
      else:
          return "OK"
  ```
- **Usage**:
  ```python
  df = df.withColumn(
      "risk_label",
      udf_flag_compliance(F.col("fraud_score"), F.col("policy_flags"))
  )
  ```
- **Business Impact**: Automates compliance checks, escalating suspicious transactions.

### `udf_mask_sensitive`
- **Purpose**: Mask or tokenize personally identifiable information (`user_id`).  
- **Logic**:
  ```python
  def mask_sensitive_py(user_id):
      if not user_id:
          return ""
      return user_id[:2] + "*" * (len(user_id) - 2)
  ```
- **Usage**:
  ```python
  df = df.withColumn("masked_user", udf_mask_sensitive(F.col("user_id")))
  ```
- **Business Impact**: Maintains privacy standards by hiding sensitive data.

### `udf_derive_status`
- **Purpose**: Combine a real-time transaction status with bank status (e.g., “MATCHED” vs. “UNMATCHED”) into a final status.  
- **Logic**:
  ```python
  def derive_status_py(cur_status, bank_status):
      if not bank_status:
          return cur_status if cur_status else "UNKNOWN"
      if bank_status.upper() == "MATCHED":
          return cur_status if cur_status else "MATCHED"
      elif bank_status.upper() == "UNMATCHED":
          return "DISCREPANCY"
      else:
          return cur_status if cur_status else bank_status
  ```
- **Usage**:
  ```python
  df = df.withColumn(
      "final_status",
      udf_derive_status(F.col("cur_status"), F.col("bank_status"))
  )
  ```
- **Business Impact**: Maintains consistency in transaction states across real-time and aggregator data.

### `udf_kpi_calculation`
- **Purpose**: Compute a custom KPI based on merchant IDs and daily values.  
- **Logic**:
  ```python
  def kpi_calculation_py(merchant_id, daily_value):
      if daily_value is None:
          daily_value = 0.0
      if merchant_id and merchant_id.startswith("MCH-A"):
          return round(daily_value * 1.1, 2)
      else:
          return round(daily_value, 2)
  ```
- **Usage**:
  ```python
  df = df.withColumn(
      "merchant_kpi",
      udf_kpi_calculation(F.col("merchant_id"), F.col("daily_value"))
  )
  ```
- **Business Impact**: Tracks custom performance metrics (e.g., daily revenue with surcharges or bonuses) per merchant.

---

## Registering & Using These UDFs

Each Python function is **wrapped** by `F.udf(..., returnType)` to create a Spark UDF. You then **invoke** them inside a DataFrame transformation:
```python
df = df.withColumn("some_column", my_udf(F.col("existing_col")))
```
**Important**: Because each row calls your Python code, large DataFrames can experience overhead. Use UDFs sparingly and only when native Spark functions don’t suffice.
---


## Summary

- **Use Built-Ins**: Check if Spark SQL or built-in DataFrame functions can achieve the same result.  
- **Test Thoroughly**: Write unit tests for your Python functions (`calc_discrepancy_py`, etc.) before turning them into UDFs.  
- **Return Types**: Specify the correct Spark data type (`StringType`, `DoubleType`, etc.) in `F.udf(..., <TYPE>)`.  
- **Security**: For something like `mask_sensitive_py`, ensure the masking logic meets privacy or compliance rules.  
- **Pandas UDFs**: For more efficient vectorized operations, consider using **Pandas UDFs** if your logic can be batch-based.
- PySpark UDFs let you **extend** Spark’s native functionality with **custom Python code**.  
- Each example (`udf_calc_discrepancy`, `udf_flag_compliance`, etc.) addresses a **business logic gap** not covered by built-ins.  
- Ensure you **optimize** or **use** built-in Spark functions first, resorting to UDFs only where truly necessary.  
- With proper design, UDFs can **enhance** your data pipeline’s flexibility and deliver key insights for financial reconciliation, compliance, and more.
- - UDFs integrate easily into a standard Spark pipeline, letting you **enrich** data with custom columns—like **`discrepancy`** or **`risk_label`**—in one step.  
- They enable **domain-specific** transformations crucial for Payment Reconciliation, Fraud Detection, and Compliance workflows.  
- Always be mindful of **performance** and **maintainability** when introducing row-level logic as UDFs.
