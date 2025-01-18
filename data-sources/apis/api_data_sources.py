# Filename: api_server.py
# Usage:  python api_server.py
# Then visit:
#   - http://127.0.0.1:5000/transactions?count=10
#   - http://127.0.0.1:5000/policies?count=5
#   - http://127.0.0.1:5000/merchants?count=3
#   - http://127.0.0.1:5000/fraud_signals?count=8

from flask import Flask, request, jsonify
from faker import Faker
import random
import string

app = Flask(__name__)
fake = Faker()

@app.route('/transactions', methods=['GET'])
def get_transactions():
    """
    Generate synthetic transaction data.
    Example usage:
      GET /transactions?count=50
    """
    count = request.args.get('count', 10, type=int)  # Default is 10
    results = []

    for _ in range(count):
        transaction_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))
        user_id = fake.random_int(min=1000, max=9999)
        timestamp = fake.date_time_between(start_date="-30d", end_date="now")
        amount = round(random.uniform(5.0, 500.0), 2)
        currency = random.choice(['USD', 'EUR', 'GBP', 'CAD', 'AUD'])
        fraud_score = round(random.uniform(0.0, 1.0), 2)
        transaction_status = random.choice(['AUTHORIZED', 'DECLINED', 'PENDING', 'REFUNDED'])

        results.append({
            "transaction_id": transaction_id,
            "user_id": user_id,
            "timestamp": timestamp.isoformat(),
            "amount": amount,
            "currency": currency,
            "fraud_score": fraud_score,
            "transaction_status": transaction_status
        })

    return jsonify(results)

@app.route('/policies', methods=['GET'])
def get_policies():
    """
    Generate synthetic policy data.
    Example usage:
      GET /policies?count=5
    """
    count = request.args.get('count', 5, type=int)
    results = []

    for _ in range(count):
        policy_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        policy_name = fake.catch_phrase()
        effective_date = fake.date_time_between(start_date="-90d", end_date="now")
        policy_details = {
            "region_restrictions": random.choice([["US", "CA"], ["EU"], ["APAC", "AU"], []]),
            "velocity_limit": random.randint(1, 100),
            "risk_level": random.choice(["LOW", "MEDIUM", "HIGH"])
        }

        results.append({
            "policy_id": policy_id,
            "policy_name": policy_name,
            "effective_date": effective_date.isoformat(),
            "policy_details": policy_details
        })

    return jsonify(results)

@app.route('/merchants', methods=['GET'])
def get_merchants():
    """
    Generate synthetic merchant data.
    Example usage:
      GET /merchants?count=3
    """
    count = request.args.get('count', 3, type=int)
    results = []

    for _ in range(count):
        merchant_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        merchant_name = fake.company()
        category = random.choice(["Electronics", "Clothing", "Food", "Software", "Services"])
        risk_level = random.choice(["LOW", "MEDIUM", "HIGH"])

        results.append({
            "merchant_id": merchant_id,
            "merchant_name": merchant_name,
            "category": category,
            "risk_level": risk_level
        })

    return jsonify(results)

@app.route('/fraud_signals', methods=['GET'])
def get_fraud_signals():
    """
    Generate synthetic fraud signal data.
    These would normally come from an ML model inference pipeline.
    Example usage:
      GET /fraud_signals?count=8
    """
    count = request.args.get('count', 5, type=int)
    results = []

    for _ in range(count):
        transaction_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))
        model_version = random.choice(["v1.0", "v1.1", "v2.0"])
        fraud_score = round(random.uniform(0.0, 1.0), 2)
        inference_time = fake.date_time_between(start_date="-30d", end_date="now")
        explanations = {
            "reason": random.choice(["IP_MISMATCH", "VELOCITY_EXCEEDED", "SUSPICIOUS_LOCATION", "BLACKLISTED_USER"]),
            "confidence": round(random.uniform(0.5, 0.99), 2)
        }

        results.append({
            "transaction_id": transaction_id,
            "model_version": model_version,
            "fraud_score": fraud_score,
            "inference_time": inference_time.isoformat(),
            "explanations": explanations
        })

    return jsonify(results)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
