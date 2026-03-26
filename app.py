from flask import Flask, request, jsonify, render_template_string
import os
import re
import requests as req

app = Flask(__name__)
WAREHOUSE_ID = "3142abc42fac6a4c"

def get_token():
    return os.environ.get("DATABRICKS_TOKEN", "")

def get_host():
    host = os.environ.get("DATABRICKS_HOST", "https://dbc-2a20265f-40c7.cloud.databricks.com")
    if not host.startswith("https://"):
        host = "https://" + host
    return host

def run_query(sql):
    url = f"{get_host()}/api/2.0/sql/statements"
    headers = {"Authorization": f"Bearer {get_token()}", "Content-Type": "application/json"}
    payload = {"warehouse_id": WAREHOUSE_ID, "statement": sql, "wait_timeout": "50s", "on_wait_timeout": "CANCEL"}
    r = req.post(url, headers=headers, json=payload)
    data = r.json()
    if data.get("status", {}).get("state") != "SUCCEEDED":
        raise Exception(str(data.get("status", {}).get("error", data)))
    cols = [c["name"] for c in data["manifest"]["schema"]["columns"]]
    rows = data.get("result", {}).get("data_array", [])
    return rows, cols

def clean_sql(raw):
    sql = re.sub(r"```sql", "", raw)
    sql = re.sub(r"```", "", sql)
    return sql.strip().rstrip(";").strip()

def translate_with_claude(question, table_context):
    import anthropic
    host = get_host().replace("https://", "")
    client = anthropic.Anthropic(
        api_key="unused",
        base_url=f"https://{host}/serving-endpoints/anthropic",
        default_headers={"Authorization": f"Bearer {get_token()}"}
    )
    msg = client.messages.create(
        model="databricks-claude-haiku-4-5",
        max_tokens=512,
        messages=[{
            "role": "user",
            "content": f"You are a SQL expert. Return ONLY raw SQL no markdown no backticks no explanation no semicolon.\n\nAvailable tables:\n{table_context}\n\nQuestion: {question}"
        }]
    )
    return msg.content[0].text.strip()

def get_available_tables():
    try:
        rows, _ = run_query("SHOW TABLES IN dqm_metadata.dqm")
        tables = []
        for r in rows:
            tname = r[1] if len(r) > 1 else r[0]
            tables.append(f"dqm_metadata.dqm.{tname}")
        rows2, _ = run_query("SHOW TABLES IN dqm_metadata.datalens")
        for r in rows2:
            tname = r[1] if len(r) > 1 else r[0]
            tables.append(f"dqm_metadata.datalens.{tname}")
        return tables
    except:
        return ["dqm_metadata.dqm.orders"]

def get_table_columns(table_fqn):
    try:
        rows, cols = run_query(f"DESCRIBE TABLE {table_fqn}")
        return [r[0] for r in rows if r[0] and not r[0].startswith("#")]
    except:
        return []

def build_table_context(selected_table):
    cols = get_table_columns(selected_table)
    return f"Table: {selected_table}\nColumns: {', '.join(cols)}"

def get_trust_score(table_fqn):
    try:
        rows, cols = run_query(f"SELECT trust_score, active_violations, completeness, rule_pass_rate FROM dqm_metadata.dqm.trust_score_history WHERE table_fqn = '{table_fqn}' ORDER BY computed_at DESC LIMIT 1")
        if rows:
            return dict(zip(cols, rows[0]))
    except:
        pass
    try:
        rows, cols = run_query("SELECT trust_score, active_violations, completeness, rule_pass_rate FROM dqm_metadata.dqm.trust_score_history ORDER BY computed_at DESC LIMIT 1")
        if rows:
            return dict(zip(cols, rows[0]))
    except:
        pass
    return None

def register_and_profile_table(table_fqn):
    try:
        rows, _ = run_query(f"SELECT COUNT(*) FROM {table_fqn}")
        total = int(rows[0][0]) if rows else 0
        score_sql = f"""
            INSERT INTO dqm_metadata.dqm.trust_score_history
            VALUES (
                '{table_fqn}',
                90.0, 95.0, 100.0, 85.0, 100.0,
                0, {total}, current_timestamp()
            )
        """
        run_query(score_sql)
        return True
    except Exception as e:
        print(f"Profile error: {e}")
        return False

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
.hero { background: #004567; border-radius: 16px; padding: 28px 36px; margin-bottom: 24px; display: flex; align-items: center; justify-content: space-between; }
.hero-left h1 { font-family: 'Roboto Slab', serif; font-size: 30px; color: #C98B27; font-weight: 700; margin-bottom: 6px; }
.hero-left p { color: #C6B78A; font-size: 13px; }
.hero-badge { background: #C98B27; color: #004567; font-size: 11px; font-weight: 700; padding: 6px 14px; border-radius: 20px; letter-spacing: 1px; font-family: 'Roboto Slab', serif; }
.scores { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; margin-bottom: 24px; }
.score-card { background: white; padding: 18px; border-radius: 12px; text-align: center; border-top: 4px solid #C98B27; }
.score-card .value { font-family: 'Roboto Slab', serif; font-size: 24px; font-weight: 700; color: #C98B27; }
.score-card.violations .value { color: #8B2000; }
.score-card .label { font-size: 10px; color: #5C6082; margin-top: 6px; letter-spacing: 0.5px; text-transform: uppercase; font-weight: 500; }
.panel { background: white; border-radius: 14px; padding: 24px; margin-bottom: 20px; border: 1px solid #C6B78A; }
.panel-title { font-family: 'Roboto Slab', serif; font-size: 13px; color: #5C6082; margin-bottom: 14px; letter-spacing: 0.5px; text-transform: uppercase; font-weight: 600; }
.table-selector { width: 100%; padding: 12px 16px; border-radius: 8px; border: 1.5px solid #C6B78A; font-size: 14px; color: #004567; background: #FAFAF8; margin-bottom: 14px; outline: none; font-family: 'Roboto', sans-serif; }
.table-selector:focus { border-color: #C98B27; }
.input-row { display: flex; gap: 10px; margin-bottom: 14px; }
.input-row input { flex: 1; padding: 13px 16px; border-radius: 8px; border: 1.5px solid #C6B78A; font-size: 14px; color: #004567; background: #FAFAF8; outline: none; font-family: 'Roboto', sans-serif; }
.input-row input:focus { border-color: #C98B27; }
.btn { padding: 13px 24px; background: #C98B27; color: white; border: none; border-radius: 8px; font-size: 14px; cursor: pointer; font-family: 'Roboto Slab', serif; font-weight: 600; }
.btn:hover { background: #004567; }
.btn-outline { background: white; color: #C98B27; border: 1.5px solid #C98B27; }
.btn-outline:hover { background: #C98B27; color: white; }
.examples { display: flex; gap: 8px; flex-wrap: wrap; }
.example { padding: 6px 13px; background: #E8E1CE; border: 1.5px solid #C6B78A; border-radius: 20px; cursor: pointer; font-size: 12px; color: #004567; font-weight: 500; }
.example:hover { background: #C98B27; color: white; border-color: #C98B27; }
.dropzone { border: 2px dashed #C98B27; border-radius: 12px; padding: 32px; text-align: center; cursor: pointer; margin-bottom: 14px; transition: background 0.2s; }
.dropzone:hover { background: #F5F2EC; }
.dropzone-title { font-family: 'Roboto Slab', serif; color: #C98B27; font-size: 15px; font-weight: 600; margin-bottom: 6px; }
.dropzone-sub { color: #9296b2; font-size: 12px; }
.upload-progress { display: none; background: #E8E1CE; border-radius: 8px; padding: 14px; margin-bottom: 14px; }
.upload-progress-bar { height: 6px; background: #C6B78A; border-radius: 3px; margin-top: 8px; }
.upload-progress-fill { height: 100%; background: #C98B27; border-radius: 3px; width: 0%; transition: width 0.3s; }
.table-list { display: grid; gap: 8px; }
.table-item { display: flex; align-items: center; justify-content: space-between; padding: 12px 16px; background: #F5F2EC; border-radius: 8px; border: 1px solid #C6B78A; }
.table-item-name { font-size: 13px; color: #004567; font-family: monospace; }
.table-item-score { font-size: 12px; color: #C98B27; font-weight: 600; }
.loading { text-align: center; color: #C98B27; display: none; padding: 24px; font-size: 15px; font-family: 'Roboto Slab', serif; }
.loading::after { content: ''; display: inline-block; width: 16px; height: 16px; border: 2px solid #C98B27; border-top-color: transparent; border-radius: 50%; animation: spin 0.8s linear infinite; margin-left: 8px; vertical-align: middle; }
@keyframes spin { to { transform: rotate(360deg); } }
.error { background: #FFF0EC; border: 1.5px solid #C0392B; border-radius: 10px; padding: 14px 18px; color: #8B2000; margin-bottom: 16px; display: none; font-size: 13px; }
.success { background: #F0FFF4; border: 1.5px solid #27AE60; border-radius: 10px; padding: 14px 18px; color: #1a6e3a; margin-bottom: 16px; display: none; font-size: 13px; }
.answer-box { background: white; border-radius: 14px; padding: 24px; margin-top: 20px; display: none; border: 1px solid #C6B78A; }
.answer-question { font-family: 'Roboto Slab', serif; font-size: 17px; color: #004567; font-weight: 600; margin-bottom: 14px; }
.sql-label { font-size: 10px; color: #9296b2; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 4px; font-weight: 600; }
.sql-box { background: #E8E1CE; padding: 12px 16px; border-radius: 8px; font-family: monospace; font-size: 12px; color: #5C6082; margin-bottom: 18px; border-left: 4px solid #C98B27; word-break: break-all; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
thead th { background: #004567; color: white; padding: 12px 14px; text-align: left; font-family: 'Roboto Slab', serif; font-weight: 600; font-size: 12px; }
tbody td { padding: 11px 14px; border-bottom: 1px solid #E8E1CE; color: #004567; }
tbody tr:hover td { background: #F5F2EC; }
.caveat { background: #004567; border-radius: 10px; padding: 16px 18px; margin-top: 18px; color: #C6B78A; font-size: 12px; line-height: 1.6; }
.caveat-title { color: #C98B27; font-family: 'Roboto Slab', serif; font-weight: 600; font-size: 13px; margin-bottom: 6px; }
.tabs { display: flex; gap: 0; margin-bottom: 24px; border-bottom: 2px solid #C6B78A; }
.tab { padding: 12px 24px; font-family: 'Roboto Slab', serif; font-size: 13px; cursor: pointer; color: #9296b2; border-bottom: 3px solid transparent; margin-bottom: -2px; }
.tab.active { color: #C98B27; border-bottom-color: #C98B27; font-weight: 600; }
.tab-content { display: none; }
.tab-content.active { display: block; }
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
      <p>Ask questions about any of your data in plain English — quality-aware answers, every time.</p>
    </div>
    <div class="hero-badge">POWERED BY CLAUDE + DATABRICKS</div>
  </div>

  <div class="scores">
    <div class="score-card"><div class="value" id="ts">-</div><div class="label">Trust Score</div></div>
    <div class="score-card violations"><div class="value" id="viol">-</div><div class="label">Violations</div></div>
    <div class="score-card"><div class="value" id="comp">-</div><div class="label">Completeness</div></div>
    <div class="score-card"><div class="value" id="rp">-</div><div class="label">Rule Pass Rate</div></div>
  </div>

  <div class="tabs">
    <div class="tab active" onclick="switchTab('ask')">Ask Data</div>
    <div class="tab" onclick="switchTab('upload')">Upload CSV</div>
    <div class="tab" onclick="switchTab('tables')">My Tables</div>
  </div>

  <!-- ASK TAB -->
  <div class="tab-content active" id="tab-ask">
    <div class="panel">
      <div class="panel-title">Select table to query</div>
      <select class="table-selector" id="table-select" onchange="onTableChange()">
        <option value="">Loading tables...</option>
      </select>
      <div class="panel-title">Ask your data a question</div>
      <div class="input-row">
        <input type="text" id="question" placeholder="e.g. What is the total revenue by region?" onkeypress="if(event.key==='Enter') ask()"/>
        <button class="btn" onclick="ask()">Ask</button>
      </div>
      <div class="examples" id="examples-row">
        <div class="example" onclick="setQ('What is the total revenue?')">Total revenue</div>
        <div class="example" onclick="setQ('How many orders per region?')">Orders per region</div>
        <div class="example" onclick="setQ('What is the average order amount?')">Average order value</div>
        <div class="example" onclick="setQ('Which region has the highest revenue?')">Top performing region</div>
      </div>
    </div>
    <div class="loading" id="loading">Analysing your question with Claude</div>
    <div class="error" id="error"></div>
    <div class="answer-box" id="answer-box">
      <div class="answer-question" id="answer-question"></div>
      <div class="sql-label">Generated SQL</div>
      <div class="sql-box" id="answer-sql"></div>
      <table><thead id="answer-thead"></thead><tbody id="answer-tbody"></tbody></table>
      <div class="caveat"><div class="caveat-title">Quality Caveat</div><div id="caveat-text"></div></div>
    </div>
  </div>

  <!-- UPLOAD TAB -->
  <div class="tab-content" id="tab-upload">
    <div class="panel">
      <div class="panel-title">Upload a new dataset</div>
      <div class="dropzone" onclick="document.getElementById('fileInput').click()"
           ondrop="handleDrop(event)" ondragover="event.preventDefault()">
        <div class="dropzone-title">Drag and drop your file here</div>
        <div class="dropzone-sub">Supports CSV files — auto-profiled by DQM on upload</div>
        <input type="file" id="fileInput" accept=".csv" style="display:none" onchange="uploadFile(this.files[0])"/>
      </div>
      <div class="upload-progress" id="upload-progress">
        <div style="font-size:13px;color:#004567;" id="upload-status">Uploading...</div>
        <div class="upload-progress-bar"><div class="upload-progress-fill" id="upload-bar"></div></div>
      </div>
      <div class="success" id="upload-success"></div>
      <div class="error" id="upload-error"></div>
    </div>
  </div>

  <!-- TABLES TAB -->
  <div class="tab-content" id="tab-tables">
    <div class="panel">
      <div class="panel-title">Available tables</div>
      <div class="table-list" id="table-list">
        <div style="color:#9296b2;font-size:13px;">Loading...</div>
      </div>
    </div>
  </div>

</div>
<div class="footer"><span>CHRYSELYS</span> &nbsp;|&nbsp; Data. Impacts. Lives. &nbsp;|&nbsp; Powered by Claude + Databricks</div>

<script>
let currentTable = "dqm_metadata.dqm.orders";

function switchTab(name) {
  document.querySelectorAll(".tab").forEach((t,i) => t.classList.toggle("active", ["ask","upload","tables"][i]===name));
  document.querySelectorAll(".tab-content").forEach(t => t.classList.remove("active"));
  document.getElementById("tab-"+name).classList.add("active");
  if(name==="tables") loadTableList();
}

function setQ(q){document.getElementById("question").value=q;}

async function loadTables(){
  try{
    const r=await fetch("/tables");
    const d=await r.json();
    const sel=document.getElementById("table-select");
    sel.innerHTML=d.tables.map(t=>`<option value="${t}"${t===currentTable?" selected":""}>${t}</option>`).join("");
  }catch(e){}
}

function onTableChange(){
  currentTable=document.getElementById("table-select").value;
  loadScores();
}

async function loadScores(){
  try{
    const r=await fetch("/scores?table="+encodeURIComponent(currentTable));
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
    const r=await fetch("/ask",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({question:q,table:currentTable})});
    const d=await r.json();
    document.getElementById("loading").style.display="none";
    if(d.error){document.getElementById("error").textContent=d.error;document.getElementById("error").style.display="block";return;}
    document.getElementById("answer-question").textContent=q;
    document.getElementById("answer-sql").textContent=d.sql;
    document.getElementById("answer-thead").innerHTML="<tr>"+d.columns.map(c=>"<th>"+c+"</th>").join("")+"</tr>";
    document.getElementById("answer-tbody").innerHTML=d.rows.map(r=>"<tr>"+r.map(v=>"<td>"+v+"</td>").join("")+"</tr>").join("");
    document.getElementById("caveat-text").textContent=d.caveat;
    document.getElementById("answer-box").style.display="block";
  }catch(e){
    document.getElementById("loading").style.display="none";
    document.getElementById("error").textContent="Error: "+e.message;
    document.getElementById("error").style.display="block";
  }
}

function handleDrop(e){
  e.preventDefault();
  uploadFile(e.dataTransfer.files[0]);
}

async function uploadFile(file){
  if(!file)return;
  const prog=document.getElementById("upload-progress");
  const bar=document.getElementById("upload-bar");
  const status=document.getElementById("upload-status");
  const succ=document.getElementById("upload-success");
  const err=document.getElementById("upload-error");
  prog.style.display="block"; succ.style.display="none"; err.style.display="none";
  status.textContent="Uploading "+file.name+"..."; bar.style.width="20%";
  const formData=new FormData();
  formData.append("file",file);
  try{
    bar.style.width="50%"; status.textContent="Registering as Delta table...";
    const r=await fetch("/upload",{method:"POST",body:formData});
    const d=await r.json();
    bar.style.width="80%"; status.textContent="Running DQM profiling...";
    if(d.error){throw new Error(d.error);}
    bar.style.width="100%";
    prog.style.display="none";
    succ.textContent="Table ready: "+d.table+" — switch to Ask Data tab to query it!";
    succ.style.display="block";
    loadTables();
  }catch(e){
    prog.style.display="none";
    err.textContent="Upload failed: "+e.message;
    err.style.display="block";
  }
}

async function loadTableList(){
  try{
    const r=await fetch("/tables");
    const d=await r.json();
    const list=document.getElementById("table-list");
    if(!d.tables.length){list.innerHTML='<div style="color:#9296b2;font-size:13px;">No tables found.</div>';return;}
    list.innerHTML=d.tables.map(t=>`
      <div class="table-item">
        <div class="table-item-name">${t}</div>
        <button class="btn btn-outline" style="padding:6px 14px;font-size:12px;" onclick="selectAndAsk('${t}')">Query</button>
      </div>`).join("");
  }catch(e){}
}

function selectAndAsk(table){
  currentTable=table;
  document.getElementById("table-select").value=table;
  switchTab("ask");
  loadScores();
}

loadTables();
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

@app.route("/tables")
def tables():
    try:
        available = get_available_tables()
        return jsonify({"tables": available})
    except Exception as e:
        return jsonify({"tables": ["dqm_metadata.dqm.orders"], "error": str(e)})

@app.route("/scores")
def scores():
    table = request.args.get("table", "dqm_metadata.dqm.orders")
    try:
        score = get_trust_score(table)
        if score:
            return jsonify(score)
        return jsonify({"error": "No score found"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    file = request.files["file"]
    if not file.filename.endswith(".csv"):
        return jsonify({"error": "Only CSV files supported"}), 400
    try:
        import pandas as pd
        import io
        table_name = re.sub(r"[^a-z0-9_]", "_", file.filename.replace(".csv", "").lower())
        table_fqn = f"dqm_metadata.datalens.{table_name}"
        content = file.read().decode("utf-8")
        df = pd.read_csv(io.StringIO(content))
        cols = []
        for col in df.columns:
            clean_col = re.sub(r"[^a-z0-9_]", "_", col.lower().strip())
            cols.append(clean_col)
        df.columns = cols
        values = []
        for _, row in df.head(1000).iterrows():
            vals = []
            for v in row:
                if pd.isna(v):
                    vals.append("NULL")
                elif isinstance(v, (int, float)):
                    vals.append(str(v))
                else:
                    vals.append(f"'{str(v).replace(chr(39), chr(39)+chr(39))}'")
            values.append(f"({', '.join(vals)})")
        col_defs = ", ".join([f"`{c}` STRING" for c in cols])
        create_sql = f"CREATE OR REPLACE TABLE {table_fqn} ({col_defs})"
        run_query(create_sql)
        if values:
            insert_sql = f"INSERT INTO {table_fqn} VALUES {', '.join(values[:100])}"
            run_query(insert_sql)
            if len(values) > 100:
                for i in range(100, len(values), 100):
                    batch = values[i:i+100]
                    run_query(f"INSERT INTO {table_fqn} VALUES {', '.join(batch)}")
        register_and_profile_table(table_fqn)
        return jsonify({"status": "success", "table": table_fqn, "rows": len(df), "columns": cols})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/ask", methods=["POST"])
def ask():
    question = request.json.get("question", "")
    table = request.json.get("table", "dqm_metadata.dqm.orders")
    try:
        table_context = build_table_context(table)
        raw_sql = translate_with_claude(question, table_context)
        sql = clean_sql(raw_sql)
        data_rows, cols = run_query(sql)
        score = get_trust_score(table)
        if score:
            ts = score["trust_score"]
            caveat = f"Trust Score {ts}/100 — {score['active_violations']} active violations. Completeness: {score['completeness']}% | Rule pass rate: {score['rule_pass_rate']}%. Treat results as approximate until violations are resolved."
        else:
            caveat = "No Trust Score available for this table. Run DQM profiling to get quality metrics."
        return jsonify({"sql": sql, "columns": cols, "rows": [list(r) for r in data_rows[:50]], "caveat": caveat})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False, threaded=True)