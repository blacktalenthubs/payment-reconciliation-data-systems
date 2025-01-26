#!/usr/bin/env python3
"""
app.py

A minimal Flask application exposing endpoints that serve data and insights
from the Final Reconciliation Index (and related datasets).

Features demonstrated:
1) Real-Time Fraud Visibility (mocked data from final_index for demonstration)
2) Daily Transaction Reconciliation (reads final_index or batch_index)
3) Merchant Performance Dashboards (basic summary by merchant)
4) User Spend Profiling
5) Compliance & Auditing
6) Settlement & Payout Timelines
7) Operational Monitoring (simple placeholder)

Usage:
  1) Ensure you have Python 3.8+ installed.
  2) pip install -r requirements.txt
  3) python app.py
  4) Access endpoints at http://127.0.0.1:5000/api/...

Author: Your Name
Date: 2025-01-19
"""

import os
import logging
import pandas as pd
from flask import Flask, request, jsonify

app = Flask(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------
DATA_PATH = os.getenv("DATA_PATH", "payment-recon-systems/src/output/final_index")  # local folder or "s3://my-bucket/folder"
FINAL_INDEX_FILE = os.path.join(DATA_PATH, "final_index.parquet")

# (Optional) other data sets if you'd like direct endpoints to them:
BATCH_INDEX_FILE = os.path.join(DATA_PATH, "batch_index.parquet")
STREAMING_INDEX_FILE = os.path.join(DATA_PATH, "streaming_index.parquet")

# For demonstration, we'll load the final index into memory at startup
try:
    df_final = pd.read_parquet(FINAL_INDEX_FILE)
    logging.info(f"Loaded final index: {FINAL_INDEX_FILE}, {len(df_final)} records.")
except Exception as e:
    logging.error(f"Failed to load final index. Error: {e}")
    df_final = pd.DataFrame()  # fallback if file not found

# Helper function to reload data (if you prefer dynamic reload)
def reload_final_index():
    global df_final
    try:
        df_final = pd.read_parquet(FINAL_INDEX_FILE)
        logging.info(f"Reloaded final index from {FINAL_INDEX_FILE}, {len(df_final)} records.")
    except Exception as e:
        logging.error(f"Failed reloading final index: {e}")

# -------------------------------------------------------------------------
# 1) Real-Time Fraud Visibility
#    (Endpoint: GET /api/fraud_alerts)
#    In a real system, you'd query a real-time store or read from partial streaming index.
# -------------------------------------------------------------------------
@app.route("/api/fraud_alerts", methods=["GET"])
def get_fraud_alerts():
    """
    Returns a list of suspicious transactions (example: fraud_score > 0.8).
    In practice, you'd read from a real-time store or a specialized 'alerts' table.
    """
    threshold = float(request.args.get("threshold", 0.8))
    if "fraud_score" in df_final.columns:
        df_suspicious = df_final[df_final["fraud_score"] > threshold].copy()
    else:
        df_suspicious = pd.DataFrame()

    # Convert to JSON
    alerts = df_suspicious.to_dict(orient="records")
    return jsonify({
        "count": len(alerts),
        "threshold": threshold,
        "alerts": alerts
    })

# -------------------------------------------------------------------------
# 2) Daily Transaction Reconciliation
#    (Endpoint: GET /api/reconcile)
#    Show matched/unmatched or discrepancy info from final index or batch index.
# -------------------------------------------------------------------------
@app.route("/api/reconcile", methods=["GET"])
def get_reconciliation():
    """
    Simple example: returns any transaction with a discrepancy != 0 from the final index.
    """
    if "discrepancy" not in df_final.columns:
        return jsonify({"error": "No discrepancy column found in final index"}), 400

    df_disc = df_final[df_final["discrepancy"] != 0].copy()
    records = df_disc.to_dict(orient="records")
    return jsonify({
        "count": len(records),
        "discrepancies": records
    })

# -------------------------------------------------------------------------
# 3) Merchant Performance Dashboards
#    (Endpoint: GET /api/merchant/<merchant_id>/performance)
#    Summaries: total volume, average transaction, chargeback/refund count, etc.
# -------------------------------------------------------------------------
@app.route("/api/merchant/<merchant_id>/performance", methods=["GET"])
def merchant_performance(merchant_id):
    """
    Aggregates data for a given merchant_id.
    Example: sum of amounts, count of transactions, average, refund count, etc.
    """
    df_merch = df_final[df_final["merchant_id"] == merchant_id]

    if df_merch.empty:
        return jsonify({"merchant_id": merchant_id, "message": "No data found"}), 404

    total_volume = df_merch["amount"].sum() if "amount" in df_merch.columns else 0
    transaction_count = len(df_merch)
    refund_count = 0
    if "status" in df_merch.columns:
        refund_count = df_merch[df_merch["status"] == "REFUNDED"].shape[0]

    response = {
        "merchant_id": merchant_id,
        "total_volume": total_volume,
        "transaction_count": transaction_count,
        "refund_count": refund_count,
        "average_tx_amount": (total_volume / transaction_count) if transaction_count > 0 else 0
    }
    return jsonify(response)

# -------------------------------------------------------------------------
# 4) User Spend Profiling
#    (Endpoint: GET /api/user/<user_id>/spend_profile)
#    Summaries: total spending, merchant diversity, last transaction, etc.
# -------------------------------------------------------------------------
@app.route("/api/user/<user_id>/spend_profile", methods=["GET"])
def user_spend_profile(user_id):
    """
    Returns aggregated data for a specific user.
    """
    df_user = df_final[df_final["user_id"] == user_id]
    if df_user.empty:
        return jsonify({"user_id": user_id, "message": "No data"}), 404

    total_spend = df_user["amount"].sum() if "amount" in df_user.columns else 0
    unique_merchants = df_user["merchant_id"].nunique() if "merchant_id" in df_user.columns else 0
    last_tx_time = None
    if "timestamp" in df_user.columns:
        last_tx_time = df_user["timestamp"].max()

    return jsonify({
        "user_id": user_id,
        "total_spend": total_spend,
        "unique_merchants": unique_merchants,
        "last_transaction_time": str(last_tx_time)
    })

# -------------------------------------------------------------------------
# 5) Compliance & Auditing
#    (Endpoint: GET /api/compliance)
#    E.g., returns all transactions flagged for KYC/AML or blacklisted merchants, etc.
# -------------------------------------------------------------------------
@app.route("/api/compliance", methods=["GET"])
def compliance_checks():
    """
    Returns a list of transactions with compliance or AML flags.
    Example: If there's a column "compliance_flag" or "risk_label".
    We'll filter for 'HIGH_RISK' or 'REQUIRES_KYC', etc.
    """
    risk_col = "risk_label"  # or 'compliance_flag' etc.
    if risk_col not in df_final.columns:
        return jsonify({"warning": f"No '{risk_col}' column found in final index"}), 200

    df_flags = df_final[df_final[risk_col].isin(["HIGH_RISK", "REQUIRES_KYC", "BLACKLISTED"])]
    results = df_flags.to_dict(orient="records")
    return jsonify({
        "count": len(results),
        "compliance_cases": results
    })

# -------------------------------------------------------------------------
# 6) Settlement & Payout Timelines
#    (Endpoint: GET /api/settlements)
#    Suppose the final index has 'settlement_date' or 'payout_due' column.
# -------------------------------------------------------------------------
@app.route("/api/settlements", methods=["GET"])
def get_settlements():
    """
    Returns upcoming or past-due settlement records. Example:
    If final_index has a column 'payout_due_date' or 'settlement_status'.
    We'll do a simple filter for demonstration.
    """
    payout_col = "payout_due_date"
    if payout_col not in df_final.columns:
        return jsonify({"message": f"No column '{payout_col}' found"}), 200

    # In real usage, parse dates properly and filter by current date/time
    # For demo, just return the entire set
    df_settlements = df_final[~df_final[payout_col].isnull()].copy()
    records = df_settlements.to_dict(orient="records")
    return jsonify({
        "count": len(records),
        "settlements": records
    })

# -------------------------------------------------------------------------
# 7) Operational Monitoring
#    (Endpoint: GET /api/health)
#    A simple endpoint to check system status.
# -------------------------------------------------------------------------
@app.route("/api/health", methods=["GET"])
def health_check():
    """
    Basic health-check endpoint.
    In a real production system, you'd also check connection to DB,
    Kafka lags, or pipeline statuses.
    """
    return jsonify({
        "status": "ok",
        "records_in_final_index": len(df_final)
    }), 200

# -------------------------------------------------------------------------
# Additional Admin or Utility Endpoints
# -------------------------------------------------------------------------
@app.route("/api/reload", methods=["POST"])
def reload_data():
    """
    Example admin endpoint to reload the final index from disk (or S3).
    """
    reload_final_index()
    return jsonify({"status": "reloaded", "records": len(df_final)})


# -------------------------------------------------------------------------
# MAIN ENTRY POINT
# -------------------------------------------------------------------------
if __name__ == "__main__":
    # In production, you'd use something like gunicorn or uwsgi
    # e.g.: gunicorn -w 4 -b 0.0.0.0:5000 app:app
    app.run(host="127.0.0.1", port=5000, debug=True)
