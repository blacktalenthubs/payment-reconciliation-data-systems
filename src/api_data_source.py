# file: fraud_api.py
# Run this first: python fraud_api.py
# It starts a Flask server on port 5000

from flask import Flask, request, jsonify

app = Flask(__name__)

FRAUD_SIGNALS_DB = []
RISK_RULES_DB = []

@app.route("/fraud_signals", methods=["POST", "GET"])
def handle_fraud_signals():
    if request.method == "POST":
        # Client posted fraud signals (single or list)
        data = request.json
        if isinstance(data, list):
            FRAUD_SIGNALS_DB.extend(data)
            return jsonify({"status": "ok", "count": len(data)}), 200
        else:
            FRAUD_SIGNALS_DB.append(data)
            return jsonify({"status": "ok", "count": 1}), 200
    else:
        # GET request => return everything
        return jsonify(FRAUD_SIGNALS_DB), 200

@app.route("/risk_rules", methods=["POST", "GET"])
def handle_risk_rules():
    if request.method == "POST":
        # Client posted risk rules (single or list)
        data = request.json
        if isinstance(data, list):
            RISK_RULES_DB.extend(data)
            return jsonify({"status": "ok", "count": len(data)}), 200
        else:
            RISK_RULES_DB.append(data)
            return jsonify({"status": "ok", "count": 1}), 200
    else:
        # GET request => return everything
        return jsonify(RISK_RULES_DB), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6000)