#!/usr/bin/env python3
"""
ml_signal_api.py

Minimal Flask API for storing and retrieving ML Fraud Signals.
This file runs as a standalone service.

Usage:
  python ml_signal_api.py
"""

import uuid
import logging
from flask import Flask, request, jsonify

# In a production system, you'd replace this in-memory list with a real DB.
ML_SIGNALS_DB = []

# Flask app initialization
app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

@app.route("/ml_signals", methods=["POST"])
def add_ml_signal():
    """
    POST /ml_signals
    JSON example:
    {
      "fraud_signal_id": "...",
      "transaction_id": "...",
      "fraud_score": 0.85,
      "model_version": "model_v2",
      "inference_time": "2025-01-19T10:25:00"
    }
    """
    data = request.get_json()
    required_fields = [
        "fraud_signal_id",
        "transaction_id",
        "fraud_score",
        "model_version",
        "inference_time"
    ]
    for field in required_fields:
        if field not in data:
            return jsonify({"error": f"Missing field: {field}"}), 400

    ML_SIGNALS_DB.append(data)
    logging.info(f"Added fraud signal: {data['fraud_signal_id']}")
    return jsonify({"status": "success", "recorded_id": data["fraud_signal_id"]}), 201


@app.route("/ml_signals", methods=["GET"])
def get_ml_signals():
    """
    GET /ml_signals
    Returns all stored ML fraud signals as a JSON list.
    """
    return jsonify(ML_SIGNALS_DB), 200


def run_api():
    """
    Run the Flask app. In production, consider using gunicorn or similar.
    """
    app.run(host="127.0.0.1", port=7000, debug=False)


if __name__ == "__main__":
    run_api()
