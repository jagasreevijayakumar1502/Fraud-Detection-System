from flask import Flask, render_template, request, jsonify
import hashlib
import pandas as pd
from datetime import datetime
import sqlite3
import os

from flask import session, redirect, url_for
app = Flask(__name__)
app.secret_key = 'your_secret_key_here'  # Change this to a secure random value


# ==============================

# ==============================
# LOGIN/REGISTRATION CONFIG
# ==============================

ADMIN_CREDENTIALS = {'admin': 'password123'}  # In production, use hashed passwords

INITIAL_BUDGET = 1000000
SYSTEM_STATUS = "ACTIVE"
DB_FILE = "fraud_system.db"
LEDGER_FILE = "ledger.txt"
REGISTRY_FILE = "jan_dhan_registry_advanced.xlsx"
CRISIS_DATASET_FILE = "jan_dhan_derived_columns.xlsx"
BUDGET_REDUCTION_FACTOR = 0.80
LOWEST_INCOME_TIER_LABELS = {"low", "l1", "tier_1", "tier1"}
FROZEN_CITIZEN_HASHES = set()


def effective_initial_budget():
    return int(INITIAL_BUDGET * BUDGET_REDUCTION_FACTOR)


# ==============================
# DATABASE HELPERS
# ==============================

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ledger_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            citizen_hash TEXT NOT NULL,
            scheme TEXT NOT NULL,
            amount REAL NOT NULL,
            previous_hash TEXT NOT NULL,
            current_hash TEXT NOT NULL
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS citizens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            citizen_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            password TEXT NOT NULL DEFAULT '',
            account_status TEXT NOT NULL,
            aadhaar_linked INTEGER NOT NULL,
            scheme_eligibility TEXT NOT NULL,
            scheme_amount REAL NOT NULL,
            claim_count INTEGER NOT NULL,
            last_claim_date TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()
    ensure_citizens_password_column()
    backfill_ledger_from_file()
    backfill_citizens_from_excel()


def ensure_citizens_password_column():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(citizens)")
    columns = {row[1] for row in cursor.fetchall()}
    if "password" not in columns:
        cursor.execute("ALTER TABLE citizens ADD COLUMN password TEXT NOT NULL DEFAULT ''")
        conn.commit()
    conn.close()


def backfill_ledger_from_file():
    if not os.path.exists(LEDGER_FILE):
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    with open(LEDGER_FILE, "r") as f:
        lines = [line.strip() for line in f.readlines() if line.strip()]

    for line in lines:
        parts = line.split("|")
        if len(parts) != 6:
            continue
        timestamp, citizen_hash, scheme, amount_str, previous_hash, current_hash = parts
        cursor.execute("SELECT 1 FROM ledger_entries WHERE current_hash = ?", (current_hash,))
        if cursor.fetchone():
            continue
        try:
            amount_value = float(amount_str)
        except ValueError:
            amount_value = 0.0
        cursor.execute(
            """
            INSERT INTO ledger_entries (timestamp, citizen_hash, scheme, amount, previous_hash, current_hash)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (timestamp, citizen_hash, scheme, amount_value, previous_hash, current_hash)
        )

    conn.commit()
    conn.close()


def backfill_citizens_from_excel():
    if not os.path.exists(REGISTRY_FILE):
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        df = pd.read_excel(REGISTRY_FILE)
    except Exception:
        conn.close()
        return

    required_cols = {"Citizen_ID", "Name", "Account_Status", "Aadhaar_Linked", "Scheme_Eligibility",
                     "Scheme_Amount", "Claim_Count", "Last_Claim_Date"}
    if not required_cols.issubset(set(df.columns)):
        conn.close()
        return

    df["Citizen_ID"] = df["Citizen_ID"].astype(str)
    df["Aadhaar_Linked"] = df["Aadhaar_Linked"].apply(lambda v: 1 if bool(v) else 0)
    df["Scheme_Amount"] = df["Scheme_Amount"].astype(float)
    df["Claim_Count"] = df["Claim_Count"].fillna(0).astype(int)
    df["Last_Claim_Date"] = pd.to_datetime(df["Last_Claim_Date"]).dt.strftime("%Y-%m-%d")

    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO citizens
            (citizen_id, name, account_status, aadhaar_linked, scheme_eligibility, scheme_amount, claim_count, last_claim_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(citizen_id) DO UPDATE SET
                name=excluded.name,
                account_status=excluded.account_status,
                aadhaar_linked=excluded.aadhaar_linked,
                scheme_eligibility=excluded.scheme_eligibility,
                scheme_amount=excluded.scheme_amount,
                claim_count=excluded.claim_count,
                last_claim_date=excluded.last_claim_date
            """,
            (
                row["Citizen_ID"],
                row["Name"],
                row["Account_Status"],
                int(row["Aadhaar_Linked"]),
                row["Scheme_Eligibility"],
                float(row["Scheme_Amount"]),
                int(row["Claim_Count"]),
                row["Last_Claim_Date"],
            )
        )

    conn.commit()
    conn.close()


# ==============================
# LEDGER HELPERS
# ==============================

def hash_id(citizen_id):
    normalized = normalize_citizen_id(citizen_id)
    return hashlib.sha256(normalized.encode()).hexdigest()


def normalize_citizen_id(citizen_id):
    # Remove hidden/formatting characters and normalize to a canonical digit-only token.
    raw = str(citizen_id or "")
    compact = "".join(ch for ch in raw if ch.isdigit())
    return compact.strip()


def amount_hash_value(amount):
    try:
        amount_float = float(amount)
        if amount_float.is_integer():
            return str(int(amount_float))
        return str(amount_float)
    except (TypeError, ValueError):
        return str(amount)


def generate_hash(timestamp, citizen_hash, scheme, amount, previous_hash):
    record = f"{timestamp}{citizen_hash}{scheme}{amount}{previous_hash}"
    return hashlib.sha256(record.encode()).hexdigest()


def get_previous_hash():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT current_hash FROM ledger_entries ORDER BY timestamp DESC, id DESC LIMIT 1")
    row = cursor.fetchone()
    conn.close()
    if not row:
        return "GENESIS"
    return row[0]


def fetch_ledger_rows():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT timestamp, citizen_hash, scheme, amount, previous_hash, current_hash FROM ledger_entries ORDER BY timestamp"
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def verify_ledger_integrity():
    rows = fetch_ledger_rows()
    previous_hash = "GENESIS"

    for row in rows:
        timestamp, citizen_hash, scheme, amount, prev_hash, curr_hash = row
        amount_str = amount_hash_value(amount)
        recalculated_hash = generate_hash(timestamp, citizen_hash, scheme, amount_str, prev_hash)
        if recalculated_hash != curr_hash or prev_hash != previous_hash:
            return False
        previous_hash = curr_hash

    return True


def calculate_remaining_budget():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM ledger_entries")
    total_disbursed = cursor.fetchone()[0]
    conn.close()
    total_disbursed = float(total_disbursed or 0)
    return max(effective_initial_budget() - total_disbursed, 0)


def map_dataset_column(df, candidates):
    lower_map = {str(col).strip().lower(): col for col in df.columns}
    for name in candidates:
        key = name.strip().lower()
        if key in lower_map:
            return lower_map[key]
    return None


def load_crisis_dataset():
    if not os.path.exists(CRISIS_DATASET_FILE):
        return None
    try:
        return pd.read_excel(CRISIS_DATASET_FILE)
    except Exception:
        return None


def extract_fraud_clusters(df):
    if df is None or df.empty:
        return []

    citizen_col = map_dataset_column(df, ["Citizen_ID", "citizen_id", "Citizen Id"])
    region_col = map_dataset_column(df, ["Region_Code", "region_code", "Region Code"])
    if not citizen_col or not region_col:
        return []

    work = df[[citizen_col, region_col]].copy()
    work[citizen_col] = work[citizen_col].apply(normalize_citizen_id)
    work = work[work[citizen_col] != ""]
    work["citizen_hash"] = work[citizen_col].apply(hash_id)

    grouped = work.groupby("citizen_hash")
    clusters = []
    for hash_value, group in grouped:
        regions = sorted(set(str(v).strip() for v in group[region_col].tolist() if str(v).strip()))
        if len(regions) > 1:
            ids = sorted(set(group[citizen_col].tolist()))
            clusters.append({
                "citizen_hash": hash_value,
                "region_codes": regions,
                "normalized_citizen_ids": ids,
                "records": int(len(group)),
            })

    return clusters


def refresh_fraud_freeze_list():
    global FROZEN_CITIZEN_HASHES
    clusters = extract_fraud_clusters(load_crisis_dataset())
    FROZEN_CITIZEN_HASHES = {c["citizen_hash"] for c in clusters}
    return clusters


def parse_income_tier_rank(value):
    raw = str(value or "").strip().lower()
    if raw in LOWEST_INCOME_TIER_LABELS:
        return 1
    digits = "".join(ch for ch in raw if ch.isdigit())
    if digits:
        return int(digits)
    return 999


def build_dynamic_budget_context():
    df = load_crisis_dataset()
    if df is None or df.empty:
        return {"required_total": 0.0, "lowest_rank": None, "citizen_ranks": {}}

    citizen_col = map_dataset_column(df, ["Citizen_ID", "citizen_id", "Citizen Id"])
    income_col = map_dataset_column(df, ["Income_Tier", "income_tier", "Income Tier"])
    amount_col = map_dataset_column(df, ["Required_Amount", "required_amount", "Scheme_Amount", "Amount"])
    status_col = map_dataset_column(df, ["Claim_Status", "Transaction_Status", "Status"])

    if not citizen_col or not income_col:
        return {"required_total": 0.0, "lowest_rank": None, "citizen_ranks": {}}

    work = df.copy()
    work[citizen_col] = work[citizen_col].apply(normalize_citizen_id)
    work = work[work[citizen_col] != ""]

    if status_col:
        pending_mask = work[status_col].astype(str).str.strip().str.lower().eq("pending")
        pending = work[pending_mask]
        if pending.empty:
            pending = work
    else:
        pending = work

    required_total = 0.0
    if amount_col:
        required_total = float(pd.to_numeric(pending[amount_col], errors="coerce").fillna(0).sum())

    pending["income_rank"] = pending[income_col].apply(parse_income_tier_rank)
    lowest_rank = int(pending["income_rank"].min()) if not pending.empty else None
    citizen_ranks = (
        pending.groupby(citizen_col)["income_rank"]
        .min()
        .to_dict()
    )

    return {
        "required_total": required_total,
        "lowest_rank": lowest_rank,
        "citizen_ranks": citizen_ranks,
    }


def build_pending_reallocation_queue():
    df = load_crisis_dataset()
    if df is None or df.empty:
        return {
            "required_total": 0.0,
            "remaining_budget": float(calculate_remaining_budget()),
            "lowest_rank": None,
            "queue": [],
        }

    citizen_col = map_dataset_column(df, ["Citizen_ID", "citizen_id", "Citizen Id"])
    income_col = map_dataset_column(df, ["Income_Tier", "income_tier", "Income Tier"])
    amount_col = map_dataset_column(df, ["Required_Amount", "required_amount", "Scheme_Amount", "Amount"])
    status_col = map_dataset_column(df, ["Claim_Status", "Transaction_Status", "Status"])
    region_col = map_dataset_column(df, ["Region_Code", "region_code", "Region Code"])

    if not citizen_col or not income_col:
        return {
            "required_total": 0.0,
            "remaining_budget": float(calculate_remaining_budget()),
            "lowest_rank": None,
            "queue": [],
        }

    work = df.copy()
    work[citizen_col] = work[citizen_col].apply(normalize_citizen_id)
    work = work[work[citizen_col] != ""]
    if status_col:
        pending = work[work[status_col].astype(str).str.strip().str.lower().eq("pending")]
        if pending.empty:
            pending = work
    else:
        pending = work

    if amount_col:
        pending["required_amount"] = pd.to_numeric(pending[amount_col], errors="coerce").fillna(0.0).astype(float)
    else:
        pending["required_amount"] = 0.0
    pending["income_rank"] = pending[income_col].apply(parse_income_tier_rank)
    pending["income_tier"] = pending[income_col].astype(str)
    pending["region_code"] = pending[region_col].astype(str) if region_col else ""
    pending["citizen_hash"] = pending[citizen_col].apply(hash_id)
    pending = pending.sort_values(by=["income_rank", "required_amount", citizen_col], ascending=[True, True, True])

    remaining_budget = float(calculate_remaining_budget())
    required_total = float(pending["required_amount"].sum())
    lowest_rank = int(pending["income_rank"].min()) if not pending.empty else None
    constrained = remaining_budget < required_total and lowest_rank is not None
    budget_left = remaining_budget

    queue = []
    for _, row in pending.iterrows():
        rank = int(row["income_rank"])
        amount = float(row["required_amount"])
        if constrained and rank > int(lowest_rank):
            decision = "deferred_higher_tier"
        elif amount > budget_left:
            decision = "deferred_budget_exhausted"
        else:
            decision = "approved"
            budget_left -= amount

        queue.append({
            "citizen_id": row[citizen_col],
            "citizen_hash": row["citizen_hash"],
            "income_tier": row["income_tier"],
            "income_rank": rank,
            "required_amount": amount,
            "region_code": row["region_code"],
            "decision": decision,
        })

    return {
        "required_total": required_total,
        "remaining_budget": remaining_budget,
        "lowest_rank": lowest_rank,
        "queue": queue,
    }


# ==============================
# CITIZEN HELPERS
# ==============================

def prepare_citizen_record(row):
    if not row:
        return None
    return {
        "Citizen_ID": row["citizen_id"],
        "Name": row["name"],
        "Account_Status": row["account_status"],
        "Aadhaar_Linked": bool(row["aadhaar_linked"]),
        "Scheme_Eligibility": row["scheme_eligibility"],
        "Scheme_Amount": float(row["scheme_amount"]),
        "Claim_Count": int(row["claim_count"]),
        "Last_Claim_Date": row["last_claim_date"],
    }


def get_citizen_record(citizen_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM citizens WHERE citizen_id = ?", (citizen_id,))
    row = cursor.fetchone()
    conn.close()
    return prepare_citizen_record(row)


def get_all_citizens():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM citizens ORDER BY name")
    rows = cursor.fetchall()
    conn.close()
    return [prepare_citizen_record(row) for row in rows]


def normalize_bool_flag(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return False


def validate_citizen_payload(data):
    citizen_id = str(data.get("citizen_id", "")).strip()
    if len(citizen_id) != 12 or not citizen_id.isdigit():
        raise ValueError("Citizen ID must be a 12 digit number")

    name = data.get("name", "").strip()
    if not name:
        raise ValueError("Name is required")

    account_status = data.get("account_status", "Active").strip() or "Active"
    aadhaar_linked = normalize_bool_flag(data.get("aadhaar_linked", False))
    scheme = data.get("scheme_eligibility", "").strip()
    if not scheme:
        raise ValueError("Scheme eligibility is required")

    try:
        scheme_amount = float(data.get("scheme_amount", 0))
    except (TypeError, ValueError):
        raise ValueError("Scheme amount must be a number")
    if scheme_amount <= 0:
        raise ValueError("Scheme amount must be greater than zero")

    try:
        claim_count = int(data.get("claim_count", 0))
    except (TypeError, ValueError):
        raise ValueError("Claim count must be an integer")
    if claim_count < 0:
        raise ValueError("Claim count cannot be negative")

    last_claim_date = data.get("last_claim_date") or datetime.today().strftime("%Y-%m-%d")
    try:
        datetime.strptime(last_claim_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Last claim date must be in YYYY-MM-DD format")

    return {
        "citizen_id": citizen_id,
        "name": name,
        "account_status": account_status,
        "aadhaar_linked": 1 if aadhaar_linked else 0,
        "scheme_eligibility": scheme,
        "scheme_amount": scheme_amount,
        "claim_count": claim_count,
        "last_claim_date": last_claim_date,
    }


def upsert_citizen(record):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO citizens
        (citizen_id, name, account_status, aadhaar_linked, scheme_eligibility, scheme_amount, claim_count, last_claim_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(citizen_id) DO UPDATE SET
            name=excluded.name,
            account_status=excluded.account_status,
            aadhaar_linked=excluded.aadhaar_linked,
            scheme_eligibility=excluded.scheme_eligibility,
            scheme_amount=excluded.scheme_amount,
            claim_count=excluded.claim_count,
            last_claim_date=excluded.last_claim_date
        """,
        (
            record["citizen_id"],
            record["name"],
            record["account_status"],
            record["aadhaar_linked"],
            record["scheme_eligibility"],
            record["scheme_amount"],
            record["claim_count"],
            record["last_claim_date"],
        )
    )
    conn.commit()
    conn.close()


# ==============================
# VALIDATION GATES
# ==============================

def eligibility_gate(row, scheme, amount):
    if row["Account_Status"] != "Active":
        return False, "Account Not Active"

    if row["Aadhaar_Linked"] is not True:
        return False, "Aadhaar Not Linked"

    registered_scheme = str(row.get("Scheme_Eligibility", "") or "").strip()
    if registered_scheme and registered_scheme != scheme:
        return False, "Scheme Not Eligible"

    registered_amount = float(row.get("Scheme_Amount", 0) or 0)
    if registered_amount > 0 and registered_amount != amount:
        return False, "Invalid Scheme Amount"

    if row["Claim_Count"] > 3:
        return False, "Claim Limit Exceeded"

    return True, "Eligible"


def frequency_gate(last_claim_date):
    try:
        last_date = datetime.strptime(str(last_claim_date), "%Y-%m-%d")
    except ValueError:
        return False, "Invalid last claim date"
    today = datetime.today()
    diff = (today - last_date).days

    if diff < 30:
        return False, f"Claim within 30 days not allowed (Last claim: {diff} days ago)"

    return True, "Frequency OK"


def budget_gate(amount):
    remaining = calculate_remaining_budget()
    if amount > remaining:
        return False, "Insufficient Budget"

    return True, "Budget Approved"


def fraud_cluster_gate(citizen_id):
    refresh_fraud_freeze_list()
    citizen_hash = hash_id(citizen_id)
    if citizen_hash in FROZEN_CITIZEN_HASHES:
        return False, "Cross-region duplicate identity ring detected. Claims paused for this identity."
    return True, "No fraud cluster hit"


def dynamic_budget_reallocation_gate(citizen_id, amount):
    queue_ctx = build_pending_reallocation_queue()
    required_total = float(queue_ctx.get("required_total", 0.0) or 0.0)
    remaining = float(queue_ctx.get("remaining_budget", 0.0) or 0.0)
    lowest_rank = queue_ctx.get("lowest_rank")
    queue = queue_ctx.get("queue", [])

    if required_total <= 0 or remaining >= required_total or lowest_rank is None:
        return True, "Budget Approved"

    normalized_id = normalize_citizen_id(citizen_id)
    for item in queue:
        if item["citizen_id"] == normalized_id:
            if item["decision"] == "approved":
                return True, "Budget Approved for lowest income tier"
            if item["decision"] == "deferred_higher_tier":
                return False, "Dynamic reallocation active: higher income tier deferred."
            return False, "Insufficient Budget after reallocation ordering."

    # If citizen is not in pending queue, allow only if the request is still affordable.
    if amount > remaining:
        return False, "Insufficient Budget"
    return True, "Budget Approved"


# ==============================
# MAIN TRANSACTION FUNCTION
# ==============================

def process_transaction(citizen_id, scheme, amount):
    global SYSTEM_STATUS

    if SYSTEM_STATUS != "ACTIVE":
        return {"success": False, "message": f"System is {SYSTEM_STATUS}. Transaction Blocked.", "gate": "system"}

    if not verify_ledger_integrity():
        SYSTEM_STATUS = "FROZEN"
        return {"success": False, "message": "Ledger Tampering Detected. System Frozen.", "gate": "integrity"}

    citizen_record = get_citizen_record(citizen_id)
    if not citizen_record:
        return {"success": False, "message": "Citizen Not Found", "gate": "lookup"}

    cluster_ok, cluster_message = fraud_cluster_gate(citizen_id)
    if not cluster_ok:
        return {"success": False, "message": cluster_message, "gate": "fraud_cluster"}

    row = citizen_record
    citizen_name = row.get("Name", "Unknown")

    # Gate 1
    eligible, message = eligibility_gate(row, scheme, amount)
    if not eligible:
        return {"success": False, "message": message, "gate": "eligibility", "citizen_name": citizen_name}

    # Gate 2
    freq_ok, message = frequency_gate(row["Last_Claim_Date"])
    if not freq_ok:
        return {"success": False, "message": message, "gate": "frequency", "citizen_name": citizen_name}

    # Gate 3
    budget_ok, message = budget_gate(amount)
    if not budget_ok:
        return {"success": False, "message": message, "gate": "budget", "citizen_name": citizen_name}

    reallocation_ok, reallocation_message = dynamic_budget_reallocation_gate(citizen_id, amount)
    if not reallocation_ok:
        return {
            "success": False,
            "message": reallocation_message,
            "gate": "budget_reallocation",
            "citizen_name": citizen_name,
        }

    # If all gates pass - Write to ledger
    citizen_hash = hash_id(citizen_id)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    previous_hash = get_previous_hash()

    amount_str = amount_hash_value(amount)
    current_hash = generate_hash(timestamp, citizen_hash, scheme, amount_str, previous_hash)

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO ledger_entries (timestamp, citizen_hash, scheme, amount, previous_hash, current_hash)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (timestamp, citizen_hash, scheme, amount, previous_hash, current_hash)
    )
    conn.commit()
    conn.close()

    remaining_budget = int(calculate_remaining_budget())

    return {
        "success": True,
        "message": "Transaction Approved",
        "citizen_name": citizen_name,
        "remaining_budget": remaining_budget,
        "transaction_hash": current_hash[:16] + "..."
    }



# ==============================
# ROUTES
# ==============================


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        usertype = request.form.get('usertype')
        username = str(request.form.get('username') or '').strip()
        password = request.form.get('password')
        if usertype == 'admin':
            if username in ADMIN_CREDENTIALS and ADMIN_CREDENTIALS[username] == password:
                session['username'] = username
                session['usertype'] = 'admin'
                return redirect(url_for('index'))
            else:
                error = 'Invalid admin credentials.'
        else:
            citizen_id = normalize_citizen_id(username)
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT * FROM citizens WHERE citizen_id=? AND account_status='Active'", (citizen_id,))
            citizen = cur.fetchone()
            conn.close()
            if citizen and (citizen["password"] or "") == password:
                session['username'] = citizen_id
                session['usertype'] = 'citizen'
                return redirect(url_for('index'))
            else:
                error = 'Invalid citizen ID or password.'
    return render_template('login.html', error=error)

@app.route('/register', methods=['GET', 'POST'])
def register():
    error = None
    success = None
    if request.method == 'POST':
        citizen_id = normalize_citizen_id(request.form.get('citizen_id'))
        name = request.form.get('name')
        password = request.form.get('password')
        account_status = request.form.get('account_status')
        aadhaar_linked = int(request.form.get('aadhaar_linked'))
        scheme_eligibility = ""
        scheme_amount = 0.0
        last_claim_date = datetime.today().strftime("%Y-%m-%d")
        if len(citizen_id) != 12:
            error = 'Citizen ID must be a 12 digit number.'
            return render_template('register.html', error=error, success=success)

        # Check if already exists
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM citizens WHERE citizen_id=?", (citizen_id,))
        if cur.fetchone():
            error = 'Citizen ID already registered.'
        else:
            cur.execute(
                """
                INSERT INTO citizens
                (citizen_id, name, password, account_status, aadhaar_linked, scheme_eligibility, scheme_amount, claim_count, last_claim_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (citizen_id, name, password, account_status, aadhaar_linked, scheme_eligibility, scheme_amount, last_claim_date)
            )
            conn.commit()
            success = 'Registration successful! You can now log in.'
        conn.close()
    return render_template('register.html', error=error, success=success)

@app.route('/logout')
def logout():
    session.pop('username', None)
    session.pop('usertype', None)
    return redirect(url_for('login'))

@app.route('/')
def index():
    if 'username' not in session:
        return redirect(url_for('login'))
    return render_template('index.html', usertype=session.get('usertype'))


@app.route('/process', methods=['POST'])
def process():
    data = request.json or {}
    citizen_id = normalize_citizen_id(data.get('citizen_id', ''))
    scheme = data.get('scheme', '')
    amount = int(data.get('amount', 0))

    result = process_transaction(citizen_id, scheme, amount)
    return jsonify(result)



@app.route('/ledger')
def get_ledger():
    if session.get('usertype') != 'admin':
        return jsonify({'error': 'Unauthorized'}), 403
    rows = fetch_ledger_rows()[::-1]
    records = []
    for timestamp, citizen_hash, scheme, amount, _, current_hash in rows:
        try:
            amount_float = float(amount)
            amount_value = int(amount_float) if amount_float.is_integer() else amount_float
        except (TypeError, ValueError):
            amount_value = amount
        records.append({
            "timestamp": timestamp,
            "citizen_hash": (citizen_hash or "")[:12] + "...",
            "scheme": scheme,
            "amount": amount_value,
            "block_hash": (current_hash or "")[:12] + "..."
        })
    return jsonify(records)



@app.route('/citizens', methods=['GET', 'POST'])
def citizens_endpoint():
    if session.get('usertype') != 'admin':
        return jsonify({'error': 'Unauthorized'}), 403
    if request.method == 'GET':
        return jsonify(get_all_citizens())

    data = request.json or {}
    try:
        record = validate_citizen_payload(data)
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400

    upsert_citizen(record)
    return jsonify({"success": True, "message": "Citizen saved successfully"})


@app.route('/status')
def get_status():
    global SYSTEM_STATUS
    integrity = verify_ledger_integrity()
    remaining = int(calculate_remaining_budget())
    return jsonify({
        "budget": remaining,
        "budget_mode": "DYNAMIC_REALLOCATION_20_PERCENT_CUT",
        "system_status": SYSTEM_STATUS,
        "ledger_integrity": integrity
    })


@app.route('/fraud-cluster-report')
def fraud_cluster_report():
    if session.get('usertype') != 'admin':
        return jsonify({'error': 'Unauthorized'}), 403
    clusters = refresh_fraud_freeze_list()
    return jsonify({
        "cluster_count": len(clusters),
        "paused_identity_count": len(FROZEN_CITIZEN_HASHES),
        "clusters": clusters,
    })


@app.route('/budget-reallocation-report')
def budget_reallocation_report():
    if session.get('usertype') != 'admin':
        return jsonify({'error': 'Unauthorized'}), 403
    ctx = build_pending_reallocation_queue()
    return jsonify({
        "budget_mode": "DYNAMIC_REALLOCATION_20_PERCENT_CUT",
        "required_total": ctx["required_total"],
        "remaining_budget": ctx["remaining_budget"],
        "lowest_rank": ctx["lowest_rank"],
        "queue": ctx["queue"],
    })


init_db()


if __name__ == '__main__':
    app.run(debug=True, port=5000)
