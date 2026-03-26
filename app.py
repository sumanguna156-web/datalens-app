from flask import Flask, request, jsonify, render_template_string
import os
import re

app = Flask(__name__)
WAREHOUSE_ID = "3142abc42fac6a4c"

def get_token():
    return os.environ.get("DATABRICKS_TOKEN", "")

def get_host():
    return os.environ.get("DATABRICKS_HOST", "https://dbc-2a20265f-40c7.cloud.databricks.com")

def translate_with_claude(question):
    import anthropic
    token = get_token()
    host = get_host().replace("https://", "")
    client = anthropic.Anthropic(
        api_key="unused",
        base_url=f"https://{host}/serving-endpoints/anthropic",
        default_headers={"Authorization": f"Bearer {token}"}
    )
    msg = client.messages.create(
        model="databricks-claude-haiku-4-5",
        max_tokens=256,
        messages=[{
            "role": "user",
            "content": f"You are a SQL expert. Return ONLY raw SQL no markdown no backticks no explanation no semicolon. Table: dqm_metadata.dqm.orders. Columns: customer_id, order_id, status, region_code, order_amount, order_date. Question: {question}"
        }]
    )
    return msg.content[0].text.strip()

def clean_sql(raw):
    sql = re.sub(r"```sql", "", raw)
    sql = re.sub(r"```", "", sql)
    return sql.strip().rstrip(";").strip()

def run_query(sql):
    import requests as req
    token = get_token()
    host = get_host()
    url = f"{host}/api/2.0/sql/statements"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": sql,
        "wait_timeout": "50s",
        "on_wait_timeout": "CANCEL"
    }
    r = req.post(url, headers=headers, json=payload)
    data = r.json()
    if data.get("status", {}).get("state") != "SUCCEEDED":
        raise Exception(str(data.get("status", {}).get("error", data)))
    cols = [c["name"] for c in data["manifest"]["schema"]["columns"]]
    rows = data.get("result", {}).get("data_array", [])
    return rows, cols

HTML = """<!DOCTYPE html>
<html>
<head>
<title>Data Lens - Chryselys</title>
<link href="https://fonts.googleapis.com/css2?family=Roboto+Slab:wght@300;400;600;700&family=Roboto:wght@300;400;500&display=swap" rel="stylesheet">
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Roboto', sans-serif; background: #E8E1CE; color: #004567; min-height: 100vh; }
.topbar { background: #004567; padding: 14px 32px; display: flex; align-items: center; justify-content: space-between; }
.topbar-brand { font-family: 'Roboto Slab', serif; font-size: 20px; font-weight: 700; color: #C98B27; letter-spacing: 2px; }
.topbar-tag { font-size: 11px; color: #9296b2; letter-spacing: 1px; }
.topbar-product { font-size: 13px; color: #C6B78A; font-family: 'Roboto Slab', serif; }
.container { max-width: 1100px; margin: 0 auto; padding: 32px 24px; }
.hero { background: #004567; border-radius: 16px; padding: 32px 36px; margin-bottom: 28px; display: flex; align-items: center; justify-content: space-between; }
.hero-left h1 { font-family: 'Roboto Slab', serif; font-size: 32px; color: #C98B27; font-weight: 700; margin-bottom: 6px; }
.hero-left p { color: #C6B78A; font-size: 14px; letter-spacing: 0.5px; }
.hero-badge { background: #C98B27; color: #004567; font-size: 11px; font-weight: 700; padding: 6px 14px; border-radius: 20px; letter-spacing: 1px; font-family: 'Roboto Slab', serif; }
.scores { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; margin-bottom: 28px; }
.score-card { background: white; padding: 20px; border-radius: 12px; text-align: center; border-top: 4px solid #C98B27; }
.score-card .value { font-family: 'Roboto Slab', serif; font-size: 26px; font-weight: 700; color: #C98B27; }
.score-card.violations .value { color: #8B2000; }
.score-card .label { font-size: 11px; color: #5C6082; margin-top: 6px; letter-spacing: 0.5px; text-transform: uppercase; font-weight: 500; }
.input-section { background: white; border-radius: 14px; padding: 24px; margin-bottom: 20px; border: 1px solid #C6B78A; }
.input-label { font-family: 'Roboto Slab', serif; font-size: 13px; color: #5C6082; margin-bottom: 10px; letter-spacing: 0.5px; text-transform: uppercase; }
.input-row { display: flex; gap: 10px; margin-bottom: 16px; }
.input-row input { flex: 1; padding: 14px 18px; border-radius: 8px; border: 1.5px solid #C6B78A; font-size: 15px; color: #004567; font-family: 'Roboto', sans-serif; background: #FAFAF8; outline: none; }
.input-row input:focus { border-color: #C98B27; }
.input-row button { padding: 14px 28px; background: #C98B27; color: white; border: none; border-radius: 8px; font-size: 15px; cursor: pointer; font-family: 'Roboto Slab', serif; font-weight: 600; letter-spacing: 0.5px; }
.input-row button:hover { background: #004567; }
.examples { display: flex; gap: 8px; flex-wrap: wrap; }
.example { padding: 7px 14px; background: #E8E1CE; border: 1.5px solid #C6B78A; border-radius: 20px; cursor: pointer; font-size: 12px; color: #004567; font-weight: 500; }
.example:hover { background: #C98B27; color: white; border-color: #C98B27; }
.loading { text-align: center; color: #C98B27; display: none; padding: 24px; font-size: 15px; font-family: 'Roboto Slab', serif; }
.loading::after { content: ''; display: inline-block; width: 16px; height: 16px; border: 2px solid #C98B27; border-top-color: transparent; border-radius: 50%; animation: spin 0.8s linear infinite; margin-left: 8px; vertical-align: middle; }
@keyframes spin { to { transform: rotate(360deg); } }
.error { background: #FFF0EC; border: 1.5px solid #C0392B; border-radius: 10px; padding: 14px 18px; color: #8B2000; margin-bottom: 16px; display: none; font-size: 13px; }
.answer-box { background: white; border-radius: 14px; padding: 24px; margin-top: 20px; display: none; border: 1px solid #C6B78A; }
.answer-question { font-family: 'Roboto Slab', serif; font-size: 17px; color: #004567; font-weight: 600; margin-bottom: 14px; }
.sql-box { background: #E8E1CE; padding: 12px 16px; border-radius: 8px; font-family: monospace; font-size: 12px; color: #5C6082; margin-bottom: 18px; border-left: 4px solid #C98B27; word-break: break-all; }
.sql-label { font-size: 10px; color: #9296b2; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 4px; font-weight: 600; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
thead th { background: #004567; color: white; padding: 12px 14px; text-align: left; font-family: 'Roboto Slab', serif; font-weight: 600; font-size: 12px; letter-spacing: 0.5px; }
tbody td { padding: 11px 14px; border-bottom: 1px solid #E8E1CE; color: #004567; }
tbody tr:hover td { background: #F5F2EC; }
.caveat { background: #004567; border-radius: 10px; padding: 16px 18px; margin-top: 18px; color: #C6B78A; font-size: 12px; line-height: 1.6; }
.caveat-title { color: #C98B27; font-family: 'Roboto Slab', serif; font-weight: 600; font-size: 13px; margin-bottom: 6px; }
.footer { text-align: center; padding: 24px; color: #9296b2; font-size: 11px; letter-spacing: 1px; }
.footer span { color: #C98B27; font-family: 'Roboto Slab', serif; font-weight: 600; }
</style>
</head>
<body>
<div class="topbar">
  <div>
    <div class="topbar-brand">CHRYSELYS</div>
    <div class="topbar-tag">DATA. IMPACTS. LIVES.</div>
  </div>
  <div class="topbar-product">Data Lens</div>
</div>
<div class="container">
  <div class="hero">
    <div class="hero-left">
      <h1>Data Lens</h1>
      <p>Ask questions about your data in plain English — quality-aware answers, every time.</p>
    </div>
    <div class="hero-badge">POWERED BY DATABRICKS</div>
  </div>
  <div class="scores">
    <div class="score-card"><div class="value" id="ts">-</div><div class="label">Trust Score</div></div>
    <div class="score-card violations"><div class="value" id="viol">-</div><div class="label">Violations</div></div>
    <div class="score-card"><div class="value" id="comp">-</div><div class="label">Completeness</div></div>
    <div class="score-card"><div class="value" id="rp">-</div><div class="label">Rule Pass Rate</div></div>
  </div>
  <div class="input-section">
    <div class="input-label">Ask your data a question</div>
    <div class="input-row">
      <input type="text" id="question" placeholder="e.g. What is the total revenue by region?" onkeypress="if(event.key==='Enter') ask()"/>
      <button onclick="ask()">Ask</button>
    </div>
    <div class="examples">
      <div class="example" onclick="setQ('What is the total revenue?')">Total revenue</div>
      <div class="example" onclick="setQ('How many orders per region?')">Orders per region</div>
      <div class="example" onclick="setQ('What is the average order amount?')">Average order value</div>
      <div class="example" onclick="setQ('Which region has the highest revenue?')">Top performing region</div>
    </div>
  </div>
  <div class="loading" id="loading">Analysing your question</div>
  <div class="error" id="error"></div>
  <div class="answer-box" id="answer-box">
    <div class="answer-question" id="answer-question"></div>
    <div class="sql-label">Generated SQL</div>
    <div class="sql-box" id="answer-sql"></div>
    <table><thead id="answer-thead"></thead><tbody id="answer-tbody"></tbody></table>
    <div class="caveat">
      <div class="caveat-title">Quality Caveat</div>
      <div id="caveat-text"></div>
    </div>
  </div>
</div>
<div class="footer"><span>CHRYSELYS</span> &nbsp;|&nbsp; Data. Impacts. Lives. &nbsp;|&nbsp; Powered by Databricks</div>
<script>
function setQ(q){document.getElementById("question").value=q;}
async function loadScores(){
  try{
    const r=await fetch("/scores");
    const d=await r.json();
    if(d.error)return;
    document.getElementById("ts").textContent=d.trust_score+"/100";
    document.getElementById("viol").textContent=d.active_violations;
    document.getElementById("comp").textContent=d.completeness+"%";
    document.getElementById("rp").textContent=d.rule_pass_rate+"%";
  }catch(e){}
}
async function ask(){
  const q=document.getElementById("question").value.trim();
  if(!q)return;
  document.getElementById("loading").style.display="block";
  document.getElementById("answer-box").style.display="none";
  document.getElementById("error").style.display="none";
  try{
    const r=await fetch("/ask",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({question:q})});
    const d=await r.json();
    document.getElementById("loading").style.display="none";
    if(d.error){document.getElementById("error").textContent=d.error;document.getElementById("error").style.display="block";return;}
    document.getElementById("answer-question").textContent=q;
    document.getElementById("answer-sql").textContent=d.sql;
    document.getElementById("answer-thead").innerHTML="<tr>"+d.columns.map(c=>"<th>"+c+"</th>").join("")+"</tr>";
    document.getElementById("answer-tbody").innerHTML=d.rows.map(r=>"<tr>"+r.map(v=>"<td>"+v+"</td>").join("")+"</tr>").join("");
    document.getElementById("caveat-text").textContent=d.caveat;
    document.getElementById("answer-box").style.display="block";
    loadScores();
  }catch(e){
    document.getElementById("loading").style.display="none";
    document.getElementById("error").textContent="Error: "+e.message;
    document.getElementById("error").style.display="block";
  }
}
loadScores();
</script>
</body>
</html>"""

@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/health")
def health():
    return jsonify({"status": "ok", "host": get_host(), "has_token": bool(get_token())})

@app.route("/scores")
def scores():
    try:
        rows, cols = run_query("SELECT trust_score, active_violations, completeness, rule_pass_rate FROM dqm_metadata.dqm.trust_score_history ORDER BY computed_at DESC LIMIT 1")
        return jsonify(dict(zip(cols, rows[0])))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/ask", methods=["POST"])
def ask():
    question = request.json.get("question", "")
    try:
        raw_sql = translate_with_claude(question)
        sql = clean_sql(raw_sql)
        data_rows, cols = run_query(sql)
        score_rows, score_cols = run_query("SELECT trust_score, active_violations, completeness, rule_pass_rate FROM dqm_metadata.dqm.trust_score_history ORDER BY computed_at DESC LIMIT 1")
        score = dict(zip(score_cols, score_rows[0]))
        ts = score["trust_score"]
        caveat = f"Trust Score {ts}/100 — {score['active_violations']} active violations detected. Completeness: {score['completeness']}% | Rule pass rate: {score['rule_pass_rate']}%. Treat results as approximate until violations are resolved."
        return jsonify({"sql": sql, "columns": cols, "rows": [list(r) for r in data_rows[:20]], "caveat": caveat})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False, threaded=True)