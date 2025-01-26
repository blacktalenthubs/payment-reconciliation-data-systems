#!/usr/bin/env python3
"""
app.py

Full Flask application that displays maximum possible data from a final index
and partial stream index, including:

1) Fraud Alerts (fraud_score)
2) Reconciliation Discrepancies (discrepancy != 0)
3) Merchant Performance (single merchant + aggregated view of all merchants)
4) User Spend (single user)
5) Compliance (risk_label)
6) Settlements (payout_due_date)
7) Final Index head (first 50 rows)
8) Stream Index head (first 50 rows)
9) Analytics Use Case (Top 5 merchants by total volume)

This code includes:
- API endpoints returning JSON
- Pages rendering HTML + JavaScript to fetch those endpoints
- A new aggregated merchant data view (/merchants_aggregate) to show a table
  of all merchants and their total volumes, transaction counts, etc.

Author: Your Name
Date: 2025-01-22
"""

import os
import logging
import pandas as pd
from flask import Flask, request, jsonify, render_template_string

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

DATA_DIR = os.getenv("DATA_PATH", "./data")
FINAL_INDEX_FILE = os.path.join(DATA_DIR, "final_index.parquet")
STREAM_INDEX_FILE = os.path.join(DATA_DIR, "partial_stream_index.parquet")

df_final = pd.DataFrame()
df_stream = pd.DataFrame()

def load_dataframes():
    global df_final, df_stream
    try:
        df_final = pd.read_parquet("/Volumes/develop/payment-reconciliation-index-systems/payment-recon-systems/src/output/final_index")
        logging.info(f"Loaded FINAL index: {len(df_final)} rows from {FINAL_INDEX_FILE}")
    except Exception as e:
        logging.warning(f"Could not load final_index: {e}")
        df_final = pd.DataFrame()

    try:
        df_stream = pd.read_parquet("/Volumes/develop/payment-reconciliation-index-systems/payment-recon-systems/src/output/partial_stream_index")
        logging.info(f"Loaded STREAM index: {len(df_stream)} rows from {STREAM_INDEX_FILE}")
    except Exception as e:
        logging.warning(f"Could not load partial_stream_index: {e}")
        df_stream = pd.DataFrame()

load_dataframes()

@app.route("/api/reload", methods=["POST"])
def api_reload():
    load_dataframes()
    return jsonify({
        "status": "reloaded",
        "final_rows": len(df_final),
        "stream_rows": len(df_stream)
    })

# --------------------------------------------------------------------
# API Endpoints
# --------------------------------------------------------------------

@app.route("/api/fraud_alerts", methods=["GET"])
def api_fraud_alerts():
    threshold = float(request.args.get("threshold", 0.8))
    if "fraud_score" in df_final.columns:
        suspicious = df_final[df_final["fraud_score"] > threshold]
    else:
        suspicious = pd.DataFrame()
    return jsonify({
        "count": len(suspicious),
        "threshold": threshold,
        "alerts": suspicious.to_dict(orient="records")
    })

@app.route("/api/reconcile", methods=["GET"])
def api_reconcile():
    if "discrepancy" not in df_final.columns:
        return jsonify({"error": "No discrepancy column"}), 400
    rec = df_final[df_final["discrepancy"] != 0]
    return jsonify({
        "count": len(rec),
        "records": rec.to_dict(orient="records")
    })

@app.route("/api/merchant/<merchant_id>", methods=["GET"])
def api_merchant(merchant_id):
    df_ = df_final[df_final["merchant_id"] == merchant_id]
    if df_.empty:
        return jsonify({"merchant_id": merchant_id, "message": "No data"}), 404
    total_vol = df_["amount"].sum() if "amount" in df_.columns else 0
    tx_count = len(df_)
    ref_count = df_[df_["status"] == "REFUNDED"].shape[0] if "status" in df_.columns else 0
    summary = {
        "merchant_id": merchant_id,
        "transaction_count": tx_count,
        "refund_count": ref_count,
        "total_volume": total_vol,
        "average_tx": (total_vol / tx_count) if tx_count else 0
    }
    return jsonify({
        "summary": summary,
        "details": df_.to_dict(orient="records")
    })

@app.route("/api/user/<user_id>", methods=["GET"])
def api_user(user_id):
    df_ = df_final[df_final["user_id"] == user_id]
    if df_.empty:
        return jsonify({"user_id": user_id, "message": "No data"}), 404
    total_spend = df_["amount"].sum() if "amount" in df_.columns else 0
    unique_merchants = df_["merchant_id"].nunique() if "merchant_id" in df_.columns else 0
    last_tx = df_["timestamp"].max() if "timestamp" in df_.columns else None
    summary = {
        "user_id": user_id,
        "total_spend": total_spend,
        "unique_merchants": unique_merchants,
        "last_tx_time": str(last_tx)
    }
    return jsonify({
        "summary": summary,
        "details": df_.to_dict(orient="records")
    })

@app.route("/api/compliance", methods=["GET"])
def api_compliance():
    if "risk_label" not in df_final.columns:
        return jsonify({"warning": "risk_label column not found"}), 200
    flagged = df_final[df_final["risk_label"].isin(["HIGH_RISK", "REQUIRES_KYC", "BLACKLISTED"])]
    return jsonify({
        "count": len(flagged),
        "cases": flagged.to_dict(orient="records")
    })

@app.route("/api/settlements", methods=["GET"])
def api_settlements():
    if "payout_due_date" not in df_final.columns:
        return jsonify({"message": "No payout_due_date column"}), 200
    st = df_final[~df_final["payout_due_date"].isnull()]
    return jsonify({
        "count": len(st),
        "records": st.to_dict(orient="records")
    })

@app.route("/api/all_data", methods=["GET"])
def api_all_data():
    limit = int(request.args.get("limit", 50))
    return jsonify(df_final.head(limit).to_dict(orient="records"))

@app.route("/api/stream_data", methods=["GET"])
def api_stream_data():
    limit = int(request.args.get("limit", 50))
    return jsonify(df_stream.head(limit).to_dict(orient="records"))

@app.route("/api/health", methods=["GET"])
def api_health():
    return jsonify({
        "status": "ok",
        "final_rows": len(df_final),
        "stream_rows": len(df_stream)
    })

@app.route("/api/merchant_aggregate", methods=["GET"])
def api_merchant_aggregate():
    if df_final.empty or "merchant_id" not in df_final.columns:
        return jsonify({"count": 0, "merchants": []})
    grp = df_final.groupby("merchant_id", dropna=False)
    df_agg = grp.agg({
        "amount": "sum",
        "transaction_id": "count"
    }).rename(columns={"amount": "total_volume", "transaction_id": "tx_count"}).reset_index()
    df_agg["average_tx"] = df_agg.apply(
        lambda x: x["total_volume"] / x["tx_count"] if x["tx_count"] else 0, axis=1
    )
    df_agg = df_agg.sort_values("total_volume", ascending=False)
    return jsonify({
        "count": len(df_agg),
        "merchants": df_agg.to_dict(orient="records")
    })

# --------------------------------------------------------------------
# HTML PAGES
# --------------------------------------------------------------------

BASE_HTML = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<title>Payment Recon</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/css/bootstrap.min.css">
</head>
<body class="p-4">
<nav class="navbar navbar-expand-lg navbar-dark bg-dark mb-3">
  <div class="container-fluid">
    <a class="navbar-brand" href="/">Payment Portal</a>
  </div>
</nav>
<div class="container">
%CONTENT%
</div>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""

@app.route("/")
def home_page():
    c = """
    <h1>Payment Reconciliation Portal</h1>
    <ul>
      <li><a href="/fraud">Fraud Alerts</a></li>
      <li><a href="/reconcile">Reconcile Discrepancies</a></li>
      <li><a href="/merchant_ui">Merchant Performance (Single)</a></li>
      <li><a href="/merchants_aggregate">All Merchants Aggregate</a></li>
      <li><a href="/user_ui">User Spend</a></li>
      <li><a href="/compliance_ui">Compliance</a></li>
      <li><a href="/settlements_ui">Settlements</a></li>
      <li><a href="/all_ui">Final Index (Head)</a></li>
      <li><a href="/stream_ui">Stream Index (Head)</a></li>
      <li><a href="/analytics">Analytics Use Case</a></li>
    </ul>
    """
    return render_template_string(BASE_HTML.replace("%CONTENT%", c))

@app.route("/fraud")
def page_fraud():
    c = """
    <h2>Fraud Alerts</h2>
    <label>Threshold: <input type="number" step="0.1" id="thVal" value="0.8"/> </label>
    <button class="btn btn-primary" onclick="loadFraud()">Refresh</button>
    <table class="table table-bordered mt-3" id="fraudTable">
      <thead><tr><th>Tx ID</th><th>Merchant</th><th>Amount</th><th>Fraud Score</th><th>Status</th></tr></thead>
      <tbody></tbody>
    </table>
    <script>
    function loadFraud(){
      const t = document.getElementById('thVal').value;
      fetch('/api/fraud_alerts?threshold='+t)
        .then(r=>r.json())
        .then(d=>{
          const tb = document.querySelector('#fraudTable tbody');
          tb.innerHTML = '';
          (d.alerts||[]).forEach(x=>{
            tb.innerHTML += `<tr>
              <td>${x.transaction_id||''}</td>
              <td>${x.merchant_id||''}</td>
              <td>${x.amount||''}</td>
              <td>${x.fraud_score||''}</td>
              <td>${x.status||''}</td>
            </tr>`;
          });
        });
    }
    loadFraud();
    </script>
    """
    return render_template_string(BASE_HTML.replace("%CONTENT%", c))

@app.route("/reconcile")
def page_reconcile():
    c = """
    <h2>Reconcile Discrepancies</h2>
    <table class="table table-sm" id="recTable">
      <thead><tr>
        <th>Tx ID</th><th>Merchant</th><th>Amount</th><th>Status</th><th>Discrepancy</th>
      </tr></thead>
      <tbody></tbody>
    </table>
    <script>
    fetch('/api/reconcile')
      .then(r=>r.json())
      .then(d=>{
        const tb = document.querySelector('#recTable tbody');
        tb.innerHTML='';
        (d.records||[]).forEach(x=>{
          tb.innerHTML += `<tr>
            <td>${x.transaction_id||''}</td>
            <td>${x.merchant_id||''}</td>
            <td>${x.amount||''}</td>
            <td>${x.status||''}</td>
            <td>${x.discrepancy||''}</td>
          </tr>`;
        });
      });
    </script>
    """
    return render_template_string(BASE_HTML.replace("%CONTENT%", c))

@app.route("/merchant_ui")
def page_merchant():
    c = """
    <h2>Merchant Performance (Single)</h2>
    <input type="text" id="merchId" placeholder="MCH-1234"/>
    <button class="btn btn-info" onclick="loadMerch()">Load</button>
    <div id="merchSummary" class="mt-3"></div>
    <table class="table mt-3" id="merchTable">
      <thead><tr><th>Tx ID</th><th>Amount</th><th>Status</th><th>Discrepancy</th></tr></thead>
      <tbody></tbody>
    </table>
    <script>
    function loadMerch(){
      const m = document.getElementById('merchId').value;
      fetch('/api/merchant/'+m)
        .then(r=>{
          if(!r.ok) throw new Error('Not found');
          return r.json();
        })
        .then(d=>{
          document.getElementById('merchSummary').innerHTML = `
            <div class="card p-3">
              <h5>Merchant: ${d.summary.merchant_id}</h5>
              <p>Tx Count: ${d.summary.transaction_count}</p>
              <p>Refund Count: ${d.summary.refund_count}</p>
              <p>Total Volume: ${d.summary.total_volume}</p>
              <p>Average Tx: ${d.summary.average_tx}</p>
            </div>
          `;
          const tb = document.querySelector('#merchTable tbody');
          tb.innerHTML='';
          (d.details||[]).forEach(x=>{
            tb.innerHTML += `<tr>
              <td>${x.transaction_id||''}</td>
              <td>${x.amount||''}</td>
              <td>${x.status||''}</td>
              <td>${x.discrepancy||''}</td>
            </tr>`;
          });
        })
        .catch(e=>{
          document.getElementById('merchSummary').innerHTML = '<div class="text-danger">'+ e.message +'</div>';
        });
    }
    </script>
    """
    return render_template_string(BASE_HTML.replace("%CONTENT%", c))

@app.route("/merchants_aggregate")
def page_merchants_aggregate():
    c = """
    <h2>All Merchants Aggregate</h2>
    <p>Sorted by total volume descending.</p>
    <table class="table table-bordered" id="aggTable">
      <thead><tr>
        <th>Merchant</th><th>Tx Count</th><th>Total Volume</th><th>Average Tx</th>
      </tr></thead>
      <tbody></tbody>
    </table>
    <script>
    fetch('/api/merchant_aggregate')
      .then(r=>r.json())
      .then(d=>{
        const tb = document.querySelector('#aggTable tbody');
        tb.innerHTML = '';
        (d.merchants||[]).forEach(x=>{
          tb.innerHTML += `<tr>
            <td>${x.merchant_id||''}</td>
            <td>${x.tx_count||''}</td>
            <td>${x.total_volume||''}</td>
            <td>${x.average_tx||''}</td>
          </tr>`;
        });
      });
    </script>
    """
    return render_template_string(BASE_HTML.replace("%CONTENT%", c))

@app.route("/user_ui")
def page_user():
    c = """
    <h2>User Spend (Single)</h2>
    <input type="text" id="uId" placeholder="USR-0001"/>
    <button class="btn btn-secondary" onclick="loadUser()">Load</button>
    <div id="userSummary" class="mt-3"></div>
    <table class="table" id="userTable">
      <thead><tr>
        <th>Tx ID</th><th>Amount</th><th>Currency</th><th>Merchant</th><th>Status</th>
      </tr></thead>
      <tbody></tbody>
    </table>
    <script>
    function loadUser(){
      const u = document.getElementById('uId').value;
      fetch('/api/user/'+u)
        .then(r=>{
          if(!r.ok) throw new Error('User not found');
          return r.json();
        })
        .then(d=>{
          document.getElementById('userSummary').innerHTML = `
            <div class="card p-3">
              <h5>User: ${d.summary.user_id}</h5>
              <p>Total Spend: ${d.summary.total_spend}</p>
              <p>Unique Merchants: ${d.summary.unique_merchants}</p>
              <p>Last Tx: ${d.summary.last_tx_time}</p>
            </div>
          `;
          const tb = document.querySelector('#userTable tbody');
          tb.innerHTML='';
          (d.details||[]).forEach(x=>{
            tb.innerHTML += `<tr>
              <td>${x.transaction_id||''}</td>
              <td>${x.amount||''}</td>
              <td>${x.currency||''}</td>
              <td>${x.merchant_id||''}</td>
              <td>${x.status||''}</td>
            </tr>`;
          });
        })
        .catch(e=>{
          document.getElementById('userSummary').innerHTML = '<div class="text-danger">'+ e.message +'</div>';
        });
    }
    </script>
    """
    return render_template_string(BASE_HTML.replace("%CONTENT%", c))

@app.route("/compliance_ui")
def page_compliance():
    c = """
    <h2>Compliance Cases</h2>
    <table class="table" id="compTable">
      <thead><tr>
        <th>Tx ID</th><th>Merchant</th><th>Risk Label</th><th>Amount</th><th>Status</th>
      </tr></thead>
      <tbody></tbody>
    </table>
    <script>
    fetch('/api/compliance')
      .then(r=>r.json())
      .then(d=>{
        const tb = document.querySelector('#compTable tbody');
        tb.innerHTML='';
        (d.cases||[]).forEach(x=>{
          tb.innerHTML+=`<tr>
            <td>${x.transaction_id||''}</td>
            <td>${x.merchant_id||''}</td>
            <td>${x.risk_label||''}</td>
            <td>${x.amount||''}</td>
            <td>${x.status||''}</td>
          </tr>`;
        });
      });
    </script>
    """
    return render_template_string(BASE_HTML.replace("%CONTENT%", c))

@app.route("/settlements_ui")
def page_settlements():
    c = """
    <h2>Settlements & Payouts</h2>
    <table class="table" id="setTable">
      <thead><tr>
        <th>Tx ID</th><th>Merchant</th><th>Payout Due</th><th>Amount</th><th>Status</th>
      </tr></thead>
      <tbody></tbody>
    </table>
    <script>
    fetch('/api/settlements')
      .then(r=>r.json())
      .then(d=>{
        const tb = document.querySelector('#setTable tbody');
        tb.innerHTML='';
        (d.records||[]).forEach(x=>{
          tb.innerHTML += `<tr>
            <td>${x.transaction_id||''}</td>
            <td>${x.merchant_id||''}</td>
            <td>${x.payout_due_date||''}</td>
            <td>${x.amount||''}</td>
            <td>${x.status||''}</td>
          </tr>`;
        });
      });
    </script>
    """
    return render_template_string(BASE_HTML.replace("%CONTENT%", c))

@app.route("/all_ui")
def page_all_ui():
    c = """
    <h2>Final Index (Head)</h2>
    <table class="table table-bordered" id="allTable">
      <thead><tr>
        <th>Tx ID</th><th>Merchant</th><th>User</th><th>Amount</th>
        <th>Status</th><th>Discrepancy</th><th>Fraud Score</th>
      </tr></thead>
      <tbody></tbody>
    </table>
    <script>
    fetch('/api/all_data?limit=50')
      .then(r=>r.json())
      .then(d=>{
        const tb = document.querySelector('#allTable tbody');
        tb.innerHTML='';
        d.forEach(x=>{
          tb.innerHTML+=`<tr>
            <td>${x.transaction_id||''}</td>
            <td>${x.merchant_id||''}</td>
            <td>${x.user_id||''}</td>
            <td>${x.amount||''}</td>
            <td>${x.status||''}</td>
            <td>${x.discrepancy||''}</td>
            <td>${x.fraud_score||''}</td>
          </tr>`;
        });
      });
    </script>
    """
    return render_template_string(BASE_HTML.replace("%CONTENT%", c))

@app.route("/stream_ui")
def page_stream_ui():
    c = """
    <h2>Stream Index (Head)</h2>
    <table class="table" id="streamTable">
      <thead><tr>
        <th>Tx ID</th><th>Merchant</th><th>User</th><th>Amount</th><th>Status</th>
      </tr></thead>
      <tbody></tbody>
    </table>
    <script>
    fetch('/api/stream_data?limit=50')
      .then(r=>r.json())
      .then(d=>{
        const tb = document.querySelector('#streamTable tbody');
        tb.innerHTML='';
        d.forEach(x=>{
          tb.innerHTML += `<tr>
            <td>${x.transaction_id||''}</td>
            <td>${x.merchant_id||''}</td>
            <td>${x.user_id||''}</td>
            <td>${x.amount||''}</td>
            <td>${x.status||''}</td>
          </tr>`;
        });
      });
    </script>
    """
    return render_template_string(BASE_HTML.replace("%CONTENT%", c))

@app.route("/analytics")
def page_analytics():
    c = """
    <h2>Analytics Use Case: Top 5 Merchants by Volume</h2>
    <p>Aggregating final index data to find the top 5 merchants by total volume.</p>
    <ul id="topMerchants"></ul>
    <script>
    fetch('/api/all_data?limit=9999999')
      .then(r=>r.json())
      .then(rows=>{
        let mMap = {};
        rows.forEach(x=>{
          let m = x.merchant_id||'UNKNOWN';
          let amt = parseFloat(x.amount||0);
          if(!mMap[m]) mMap[m]=0;
          mMap[m]+=amt;
        });
        let arr = Object.keys(mMap).map(k=>({merchant:k,volume:mMap[k]}));
        arr.sort((a,b)=>b.volume - a.volume);
        let top5 = arr.slice(0,5);
        const ul = document.getElementById('topMerchants');
        ul.innerHTML = top5.map(x=>`<li>${x.merchant}: ${x.volume.toFixed(2)}</li>`).join('');
      });
    </script>
    """
    return render_template_string(BASE_HTML.replace("%CONTENT%", c))

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)