"""
utils/db_manager.py - v18.0
- تتبع تاريخ الأسعار (يحدث السعر إذا تغير)
- حفظ نقاط استئناف للمعالجة الخلفية
- قرارات لكل منتج (موافق/تأجيل/إزالة)
- سجل كامل بالتاريخ والوقت
"""
import hashlib
import logging
import sqlite3, json, os
from datetime import datetime

from utils.data_paths import get_data_db_path

_logger = logging.getLogger(__name__)

# ── حد أقصى لحجم JSON المُخزَّن في DB (4 ميغابايت) ──────────────────────
_MAX_JSON_BYTES = 4 * 1024 * 1024


def _safe_json_dump(data, max_bytes: int = _MAX_JSON_BYTES) -> str:
    """
    تسلسل JSON مع حد أقصى للحجم.
    إذا تجاوز الحد: يُزيل أولاً الحقول الثقيلة ("جميع_المنافسين")،
    ثم يقتطع عند آخر 1000 صف.
    """
    full = json.dumps(data, ensure_ascii=False, default=str)
    if len(full.encode('utf-8')) <= max_bytes:
        return full
    if isinstance(data, list):
        light = [{k: v for k, v in r.items() if k != 'جميع_المنافسين'} for r in data]
        s = json.dumps(light, ensure_ascii=False, default=str)
        if len(s.encode('utf-8')) <= max_bytes:
            _logger.warning("save_job_progress: results_json مقطوع الحقل الثقيل (%d صف)", len(data))
            return s
        trimmed = json.dumps(light[-1000:], ensure_ascii=False, default=str)
        _logger.warning("save_job_progress: results_json مقتطع إلى آخر 1000 صف من %d", len(data))
        return trimmed
    return full

# قاعدة SQLite الرئيسية — مسار الملف عبر get_data_db_path() (DATA_DIR على Railway)
_DB_NAME = "pricing_v18.db"
DB_PATH = get_data_db_path(_DB_NAME)


def _ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _date():
    return datetime.now().strftime("%Y-%m-%d")


def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    # WAL: يسمح بالقراءة والكتابة المتزامنة من threads مختلفة بدون تعارض
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")  # 30 ثانية انتظار بدل الخطأ الفوري
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()

    # أحداث عامة
    c.execute("""CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT, page TEXT,
        event_type TEXT, details TEXT,
        product_name TEXT, action_taken TEXT
    )""")

    # قرارات المستخدم (موافق/تأجيل/إزالة)
    c.execute("""CREATE TABLE IF NOT EXISTS decisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT, product_name TEXT,
        our_price REAL, comp_price REAL,
        diff REAL, competitor TEXT,
        old_status TEXT, new_status TEXT,
        reason TEXT, decided_by TEXT DEFAULT 'user'
    )""")

    # تاريخ الأسعار لكل منتج عند كل منافس
    c.execute("""CREATE TABLE IF NOT EXISTS price_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT, product_name TEXT,
        competitor TEXT, price REAL,
        our_price REAL, diff REAL,
        match_score REAL, decision TEXT,
        product_id TEXT DEFAULT ''
    )""")

    # نقطة الاستئناف للمعالجة الخلفية
    c.execute("""CREATE TABLE IF NOT EXISTS job_progress (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT UNIQUE,
        started_at TEXT, updated_at TEXT,
        status TEXT DEFAULT 'running',
        total INTEGER DEFAULT 0,
        processed INTEGER DEFAULT 0,
        results_json TEXT DEFAULT '[]',
        missing_json TEXT DEFAULT '[]',
        audit_json TEXT DEFAULT '{}',
        our_file TEXT, comp_files TEXT
    )""")
    # إضافة أعمدة غائبة — يُتجاهل الخطأ فقط عند وجود العمود مسبقاً
    try:
        c.execute("ALTER TABLE job_progress ADD COLUMN missing_json TEXT DEFAULT '[]'")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("ALTER TABLE job_progress ADD COLUMN audit_json TEXT DEFAULT '{}'")
    except sqlite3.OperationalError:
        pass

    # تاريخ التحليلات
    c.execute("""CREATE TABLE IF NOT EXISTS analysis_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT, our_file TEXT,
        comp_file TEXT, total_products INTEGER,
        matched INTEGER, missing INTEGER, summary TEXT
    )""")

    # AI cache
    c.execute("""CREATE TABLE IF NOT EXISTS ai_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT, prompt_hash TEXT UNIQUE,
        response TEXT, source TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS hidden_products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        product_key TEXT UNIQUE,
        product_name TEXT,
        action TEXT DEFAULT 'hidden'
    )""")

    # ══════════════════════════════════════════════════════════════════════
    #  جداول نظام المطابقة الذكي متعدد المراحل (v19.0)
    # ══════════════════════════════════════════════════════════════════════

    # حالة المنتج — State Machine (pending/matched/review/missing/migrated/price_alert/archived)
    c.execute("""CREATE TABLE IF NOT EXISTS product_states (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        product_key    TEXT UNIQUE NOT NULL,
        product_name   TEXT NOT NULL,
        state          TEXT NOT NULL DEFAULT 'pending',
        prev_state     TEXT DEFAULT '',
        competitor     TEXT DEFAULT '',
        match_score    REAL DEFAULT 0,
        match_pass     INTEGER DEFAULT 0,
        salla_id       TEXT DEFAULT '',
        salla_sku      TEXT DEFAULT '',
        migrated_at    TEXT DEFAULT '',
        created_at     TEXT NOT NULL,
        updated_at     TEXT NOT NULL,
        meta_json      TEXT DEFAULT '{}'
    )""")

    # سجل محاولات المطابقة — لكل منتج، لكل مرور
    c.execute("""CREATE TABLE IF NOT EXISTS match_attempts (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        product_key    TEXT NOT NULL,
        product_name   TEXT NOT NULL,
        pass_number    INTEGER NOT NULL,
        method         TEXT NOT NULL,
        best_candidate TEXT DEFAULT '',
        score          REAL DEFAULT 0,
        result         TEXT DEFAULT 'no_match',
        duration_ms    INTEGER DEFAULT 0,
        attempted_at   TEXT NOT NULL
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_match_product ON match_attempts(product_key)")

    # قائمة المراجعة — Event-Driven Review Queue
    c.execute("""CREATE TABLE IF NOT EXISTS review_queue (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        product_key    TEXT NOT NULL,
        product_name   TEXT NOT NULL,
        trigger_type   TEXT NOT NULL,
        trigger_detail TEXT DEFAULT '',
        old_value      TEXT DEFAULT '',
        new_value      TEXT DEFAULT '',
        priority       INTEGER DEFAULT 5,
        status         TEXT DEFAULT 'pending',
        created_at     TEXT NOT NULL,
        resolved_at    TEXT DEFAULT ''
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_rq_status ON review_queue(status, priority)")

    # كتالوج المنافسين — بطاقة مرنة لكل منافس لكل منتج
    c.execute("""CREATE TABLE IF NOT EXISTS competitor_intel (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        product_key    TEXT NOT NULL,
        product_name   TEXT NOT NULL,
        competitor     TEXT NOT NULL,
        comp_price     REAL DEFAULT 0,
        comp_url       TEXT DEFAULT '',
        comp_sku       TEXT DEFAULT '',
        availability   TEXT DEFAULT 'unknown',
        last_seen      TEXT NOT NULL,
        price_history  TEXT DEFAULT '[]',
        UNIQUE(product_key, competitor)
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ci_product ON competitor_intel(product_key)")

    # سجل تنبيهات الأسعار (Delta Events)
    c.execute("""CREATE TABLE IF NOT EXISTS price_alerts (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        product_key    TEXT NOT NULL,
        product_name   TEXT NOT NULL,
        competitor     TEXT NOT NULL,
        old_price      REAL NOT NULL,
        new_price      REAL NOT NULL,
        change_pct     REAL NOT NULL,
        direction      TEXT NOT NULL,
        alert_type     TEXT DEFAULT 'price_change',
        status         TEXT DEFAULT 'open',
        created_at     TEXT NOT NULL,
        acknowledged_at TEXT DEFAULT ''
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pa_status ON price_alerts(status, created_at)")

    # إضافة أعمدة للجداول القديمة عند الترقية
    _safe_alter = [
        ("product_states",    "match_pass   INTEGER DEFAULT 0"),
        ("product_states",    "salla_id     TEXT DEFAULT ''"),
        ("product_states",    "salla_sku    TEXT DEFAULT ''"),
        ("product_states",    "migrated_at  TEXT DEFAULT ''"),
        ("competitor_intel",  "comp_sku     TEXT DEFAULT ''"),
        ("competitor_intel",  "availability TEXT DEFAULT 'unknown'"),
    ]
    for tbl, col_def in _safe_alter:
        try:
            c.execute(f"ALTER TABLE {tbl} ADD COLUMN {col_def}")
        except sqlite3.OperationalError:
            pass

    conn.commit()
    conn.close()


# ─── أحداث ────────────────────────────────
def log_event(page, event_type, details="", product_name="", action=""):
    try:
        conn = get_db()
        conn.execute(
            "INSERT INTO events (timestamp,page,event_type,details,product_name,action_taken) VALUES (?,?,?,?,?,?)",
            (_ts(), page, event_type, details, product_name, action)
        )
        conn.commit(); conn.close()
    except Exception as _e:
        _logger.warning("log_event: فشل حفظ الحدث — %s", _e)


# ─── قرارات ────────────────────────────────
def log_decision(product_name, old_status, new_status, reason="",
                 our_price=0, comp_price=0, diff=0, competitor=""):
    try:
        conn = get_db()
        conn.execute(
            """INSERT INTO decisions
               (timestamp,product_name,our_price,comp_price,diff,competitor,
                old_status,new_status,reason)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (_ts(), product_name, our_price, comp_price, diff,
             competitor, old_status, new_status, reason)
        )
        conn.commit(); conn.close()
    except Exception as _e:
        _logger.warning("log_decision: فشل حفظ القرار '%s' — %s", product_name, _e)


def get_decisions(product_name=None, status=None, limit=100):
    try:
        conn = get_db()
        if product_name:
            rows = conn.execute(
                "SELECT * FROM decisions WHERE product_name LIKE ? ORDER BY id DESC LIMIT ?",
                (f"%{product_name}%", limit)
            ).fetchall()
        elif status:
            rows = conn.execute(
                "SELECT * FROM decisions WHERE new_status=? ORDER BY id DESC LIMIT ?",
                (status, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM decisions ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as _e:
        _logger.warning("get_decisions: فشل القراءة — %s", _e)
        return []


# ─── تاريخ الأسعار (الميزة الذكية) ──────────
def upsert_price_history(product_name, competitor, price,
                          our_price=0, diff=0, match_score=0,
                          decision="", product_id=""):
    """
    يحفظ السعر اليوم. إذا وُجد سعر سابق لنفس المنتج/المنافس اليوم → يحدّثه.
    إذا كان أمس → يضيف سجلاً جديداً لتتبع التغيير.
    يرجع True إذا تغير السعر عن آخر تسجيل.
    """
    conn = get_db()
    today = _date()
    price_changed = False
    try:
        # BEGIN EXCLUSIVE يمنع race condition في Read-Modify-Write
        # عند استدعاء متزامن من callbacks متعددة في Streamlit
        conn.execute("BEGIN EXCLUSIVE")

        last = conn.execute(
            """SELECT price, date FROM price_history
               WHERE product_name=? AND competitor=?
               ORDER BY id DESC LIMIT 1""",
            (product_name, competitor)
        ).fetchone()

        if last:
            last_price = last["price"]
            last_date  = last["date"]
            price_changed = abs(float(price) - float(last_price)) > 0.01

            if last_date == today:
                conn.execute(
                    """UPDATE price_history SET price=?,our_price=?,diff=?,
                       match_score=?,decision=?,product_id=?
                       WHERE product_name=? AND competitor=? AND date=?""",
                    (price, our_price, diff, match_score, decision,
                     product_id, product_name, competitor, today)
                )
            else:
                conn.execute(
                    """INSERT INTO price_history
                       (date,product_name,competitor,price,our_price,diff,
                        match_score,decision,product_id)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (today, product_name, competitor, price, our_price,
                     diff, match_score, decision, product_id)
                )
        else:
            conn.execute(
                """INSERT INTO price_history
                   (date,product_name,competitor,price,our_price,diff,
                    match_score,decision,product_id)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (today, product_name, competitor, price, our_price,
                 diff, match_score, decision, product_id)
            )
        conn.commit()
    except Exception as _e:
        conn.rollback()
        _logger.error("upsert_price_history: فشل — %s", _e)
        raise
    finally:
        conn.close()
    return price_changed


def get_price_history(product_name, competitor="", limit=30):
    try:
        conn = get_db()
        if competitor:
            rows = conn.execute(
                """SELECT * FROM price_history
                   WHERE product_name=? AND competitor=?
                   ORDER BY date DESC LIMIT ?""",
                (product_name, competitor, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT * FROM price_history WHERE product_name=?
                   ORDER BY date DESC LIMIT ?""",
                (product_name, limit)
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as _e:
        _logger.warning("get_price_history: فشل — %s", _e)
        return []


def get_price_changes(days=7):
    """منتجات تغير سعرها خلال X يوم"""
    try:
        conn = get_db()
        rows = conn.execute(
            """SELECT p1.product_name, p1.competitor,
                      p1.price as new_price, p2.price as old_price,
                      p1.date as new_date, p2.date as old_date,
                      (p1.price - p2.price) as price_diff
               FROM price_history p1
               JOIN price_history p2
                 ON p1.product_name=p2.product_name
                AND p1.competitor=p2.competitor
                AND p1.id > p2.id
               WHERE p1.date >= date('now', ?)
                 AND abs(p1.price - p2.price) > 0.01
               ORDER BY abs(p1.price - p2.price) DESC
               LIMIT 100""",
            (f"-{days} days",)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as _e:
        _logger.warning("get_price_changes: فشل — %s", _e)
        return []


# ─── المعالجة الخلفية ──────────────────────
def save_job_progress(job_id, total, processed, results, status="running",
                      our_file="", comp_files="", missing=None, audit_stats=None):
    missing_data = json.dumps(missing if missing else [], ensure_ascii=False, default=str)
    results_data = _safe_json_dump(results)
    audit_data   = json.dumps(audit_stats if audit_stats is not None else {},
                              ensure_ascii=False, default=str)
    with sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute(
            """INSERT OR REPLACE INTO job_progress
               (job_id,started_at,updated_at,status,total,processed,
                results_json,missing_json,our_file,comp_files,audit_json)
               VALUES (?,
                   COALESCE((SELECT started_at FROM job_progress WHERE job_id=?), ?),
                   ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (job_id, job_id, _ts(), _ts(), status, total, processed,
             results_data, missing_data, our_file, comp_files, audit_data)
        )
        conn.commit()


def get_job_progress(job_id):
    try:
        conn = get_db()
        row = conn.execute(
            "SELECT * FROM job_progress WHERE job_id=?", (job_id,)
        ).fetchone()
        conn.close()
        if row:
            d = dict(row)
            try: d["results"] = json.loads(d.get("results_json", "[]"))
            except Exception as _e:
                _logger.warning("get_job_progress: فشل تحليل results_json — %s", _e)
                d["results"] = []
            try: d["missing"] = json.loads(d.get("missing_json", "[]"))
            except Exception as _e:
                _logger.warning("get_job_progress: فشل تحليل missing_json — %s", _e)
                d["missing"] = []
            try: d["audit"] = json.loads(d.get("audit_json") or "{}")
            except Exception as _e:
                _logger.warning("get_job_progress: فشل تحليل audit_json — %s", _e)
                d["audit"] = {}
            return d
    except Exception as _e:
        _logger.warning("get_job_progress: فشل قراءة DB — %s", _e)
    return None


def get_last_job():
    try:
        conn = get_db()
        row = conn.execute(
            "SELECT * FROM job_progress ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if row:
            d = dict(row)
            try: d["results"] = json.loads(d.get("results_json", "[]"))
            except Exception as _e:
                _logger.warning("get_last_job: فشل تحليل results_json — %s", _e)
                d["results"] = []
            try: d["missing"] = json.loads(d.get("missing_json", "[]"))
            except Exception as _e:
                _logger.warning("get_last_job: فشل تحليل missing_json — %s", _e)
                d["missing"] = []
            try: d["audit"] = json.loads(d.get("audit_json") or "{}")
            except Exception as _e:
                _logger.warning("get_last_job: فشل تحليل audit_json — %s", _e)
                d["audit"] = {}
            return d
    except Exception as _e:
        _logger.warning("get_last_job: فشل قراءة DB — %s", _e)
    return None


# ─── سجل التحليلات ─────────────────────────
def log_analysis(our_file, comp_file, total, matched, missing, summary=""):
    try:
        conn = get_db()
        conn.execute(
            """INSERT INTO analysis_history
               (timestamp,our_file,comp_file,total_products,matched,missing,summary)
               VALUES (?,?,?,?,?,?,?)""",
            (_ts(), our_file, comp_file, total, matched, missing, summary)
        )
        conn.commit(); conn.close()
    except Exception as _e:
        _logger.warning("log_analysis: فشل حفظ سجل التحليل — %s", _e)


def get_analysis_history(limit=20):
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT * FROM analysis_history ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as _e:
        _logger.warning("get_analysis_history: فشل — %s", _e)
        return []


def get_events(page=None, limit=50):
    try:
        conn = get_db()
        if page:
            rows = conn.execute(
                "SELECT * FROM events WHERE page=? ORDER BY id DESC LIMIT ?",
                (page, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as _e:
        _logger.warning("get_events: فشل — %s", _e)
        return []


# ── دوال المنتجات المخفية الدائمة ──────────────────────
def save_hidden_product(product_key: str, product_name: str = "", action: str = "hidden"):
    """يحفظ منتجاً مخفياً في قاعدة البيانات بشكل دائم"""
    try:
        conn = get_db()
        conn.execute(
            """INSERT OR REPLACE INTO hidden_products
               (timestamp, product_key, product_name, action)
               VALUES (?, ?, ?, ?)""",
            (_ts(), product_key, product_name, action)
        )
        conn.commit()
        conn.close()
    except Exception as _e:
        _logger.warning("save_hidden_product: فشل حفظ المنتج المخفي '%s' — %s", product_key, _e)


def get_hidden_product_keys() -> set:
    """يُرجع مجموعة كل مفاتيح المنتجات المخفية من قاعدة البيانات"""
    try:
        conn = get_db()
        rows = conn.execute("SELECT product_key FROM hidden_products").fetchall()
        conn.close()
        return {r["product_key"] for r in rows}
    except Exception as _e:
        _logger.warning("get_hidden_product_keys: فشل قراءة DB — %s", _e)
        return set()


# ═══════════════════════════════════════════════════════════════
#  الرادار التسعيري — Competitor Price History
# ═══════════════════════════════════════════════════════════════

def _init_competitor_price_history():
    """يُنشئ جدول competitor_price_history إن لم يكن موجوداً."""
    try:
        conn = get_db()
        conn.execute("""CREATE TABLE IF NOT EXISTS competitor_price_history (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            comp_name     TEXT    NOT NULL,
            product_id    TEXT    NOT NULL,
            price         REAL    NOT NULL,
            last_seen_date TEXT   NOT NULL,
            UNIQUE(comp_name, product_id)
        )""")
        conn.commit()
        conn.close()
    except Exception as _e:
        _logger.error("_init_competitor_price_history: فشل إنشاء الجدول — %s", _e)


def _make_stable_product_id(raw_name: str) -> str:
    """
    [FIX CRITICAL-04] يُنتج معرّفاً مستقراً من اسم المنتج بعد التطبيع.

    المشكلة الأصلية: product_id = اسم نصي خام.
    إذا غيّر المنافس هجاء الاسم (مسافة زائدة، حرف مختلف) بين جلستين،
    يُسجَّل المنتج كمنتج جديد وتضيع بيانات تاريخ السعر.

    الحل: نُطبّع الاسم (lower + إزالة مسافات متعددة) ثم نأخذ أول 12 حرفاً
    من SHA-256 كمعرّف ثابت. النص الأصلي يُحفظ في عمود product_name_raw.
    """
    import hashlib, re
    norm = raw_name.strip().lower()
    norm = re.sub(r"\s+", " ", norm)
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()[:16]


def update_competitor_price(comp_name: str, product_id: str, current_price: float,
                             product_name_raw: str = ""):
    """
    يحدّث سعر منتج المنافس ويُرجع:
    - السعر القديم (float) إذا تغيّر السعر (لتتمكن الواجهة من عرض تنبيه)
    - None إذا لم يتغير السعر أو إذا كان المنتج جديداً

    [FIX CRITICAL-04] product_id الآن هو hash مستقر من الاسم المُطبَّع
    بدلاً من النص الخام، ما يضمن استمرارية تاريخ السعر حتى عند تغيير الهجاء.
    """
    if not comp_name or not product_id or current_price is None:
        return None

    # [FIX CRITICAL-04] حوّل product_id إلى hash مستقر
    # إذا مُرّر hash بالفعل (16 حرف hex) نُبقيه، وإلا نُعالجه
    stable_id = (product_id if (len(product_id) == 16 and
                                all(c in "0123456789abcdef" for c in product_id.lower()))
                 else _make_stable_product_id(product_id))
    raw_label  = product_name_raw or product_id   # للتسجيل والعرض

    try:
        today = _date()
        conn  = get_db()

        # تأكد من وجود عمود product_name_raw (migration آمن)
        try:
            conn.execute("ALTER TABLE competitor_price_history ADD COLUMN product_name_raw TEXT DEFAULT ''")
            conn.commit()
        except Exception:
            pass   # العمود موجود بالفعل — لا بأس

        row = conn.execute(
            "SELECT price FROM competitor_price_history WHERE comp_name=? AND product_id=?",
            (str(comp_name), stable_id)
        ).fetchone()

        if row is None:
            conn.execute(
                """INSERT INTO competitor_price_history
                   (comp_name, product_id, product_name_raw, price, last_seen_date)
                   VALUES (?,?,?,?,?)""",
                (str(comp_name), stable_id, raw_label, float(current_price), today)
            )
            conn.commit()
            conn.close()
            return None

        old_price     = float(row["price"])
        price_changed = abs(float(current_price) - old_price) > 0.09

        conn.execute(
            """UPDATE competitor_price_history
               SET price=?, last_seen_date=?, product_name_raw=?
               WHERE comp_name=? AND product_id=?""",
            (float(current_price), today, raw_label, str(comp_name), stable_id)
        )
        conn.commit()
        conn.close()

        return round(old_price, 2) if price_changed else None
    except Exception as _e:
        _logger.warning("update_competitor_price: فشل تحديث سعر '%s/%s' — %s",
                        comp_name, product_id, _e)
        return None


# ═══════════════════════════════════════════════════════════════
#  v26 — Upsert Catalog + Processed Products
# ═══════════════════════════════════════════════════════════════

def init_db_v26(conn=None):
    """إضافة جداول v26 للـ upsert ومتابعة المنتجات المعالجة"""
    c_conn = conn or get_db()
    cur = c_conn.cursor()

    # كتالوج مؤقت للمنافسين (يُحدَّث يومياً)
    cur.execute("""CREATE TABLE IF NOT EXISTS comp_catalog (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        competitor TEXT NOT NULL,
        product_name TEXT NOT NULL,
        norm_name TEXT,
        price REAL,
        first_seen TEXT,
        last_seen TEXT,
        UNIQUE(competitor, norm_name)
    )""")

    # كتالوج متجرنا (يُحدَّث يومياً)
    cur.execute("""CREATE TABLE IF NOT EXISTS our_catalog (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT UNIQUE,
        product_name TEXT NOT NULL,
        norm_name TEXT,
        price REAL,
        first_seen TEXT,
        last_seen TEXT
    )""")

    # المنتجات المعالجة (ترحيل/تسعير/إضافة)
    cur.execute("""CREATE TABLE IF NOT EXISTS processed_products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        product_key TEXT UNIQUE,
        product_name TEXT,
        competitor TEXT,
        action TEXT,
        old_price REAL,
        new_price REAL,
        product_id TEXT,
        notes TEXT
    )""")

    c_conn.commit()
    if not conn:
        c_conn.close()


def upsert_our_catalog(our_df, name_col="اسم المنتج", id_col="رقم المنتج", price_col="السعر"):
    """يُحدِّث كتالوج متجرنا عند كل رفع جديد — بدون تكرار"""
    import re
    conn = get_db()
    today = datetime.now().strftime("%Y-%m-%d")
    rows_updated = 0
    rows_inserted = 0

    for _, row in our_df.iterrows():
        name = str(row.get(name_col, "")).strip()
        if not name:
            continue
        norm = re.sub(r'\s+', ' ', name.lower().strip())
        pid  = str(row.get(id_col, "")).strip().rstrip(".0")
        try:
            price = float(str(row.get(price_col, 0)).replace(",", ""))
        except Exception:
            price = 0.0

        existing = conn.execute(
            "SELECT id, price FROM our_catalog WHERE product_id=? OR norm_name=?",
            (pid, norm)
        ).fetchone()

        if existing:
            conn.execute(
                "UPDATE our_catalog SET price=?, last_seen=?, norm_name=? WHERE id=?",
                (price, today, norm, existing[0])
            )
            rows_updated += 1
        else:
            conn.execute(
                """INSERT INTO our_catalog (product_id, product_name, norm_name, price, first_seen, last_seen)
                   VALUES (?,?,?,?,?,?)""",
                (pid, name, norm, price, today, today)
            )
            rows_inserted += 1

    conn.commit()
    conn.close()
    return {"updated": rows_updated, "inserted": rows_inserted}


def _comp_catalog_product_key(competitor: str, norm_name: str) -> str:
    """مفتاح مستقر لصف المنافس (يتوافق مع عمود comp_product_key إن وُجد)."""
    n = (norm_name or "").strip()
    c = (competitor or "").strip() or "unknown"
    if n:
        return f"{c}::{n}"
    h = hashlib.md5(f"{c}\0{n}".encode("utf-8")).hexdigest()[:16]
    return f"{c}::__{h}"


def _pragma_column_names(conn, table: str):
    """أسماء أعمدة جدول — متوافق مع sqlite3.Row (لا تعتمد على row[1] فقط)."""
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except Exception:
        return []
    out = []
    for r in rows:
        try:
            out.append(str(r["name"]))
        except (KeyError, IndexError, TypeError):
            try:
                out.append(str(r[1]))
            except Exception:
                continue
    return out


def _resolve_comp_name_price_columns(cdf):
    """
    يفضّل أعمدة apply_user_column_map القياسية (المنتج، سعر المنتج) ثم يعود للتخمين.
    """
    cols = list(cdf.columns)
    cs = set(cols)

    if "المنتج" in cs:
        name_col = "المنتج"
    elif "اسم المنتج" in cs:
        name_col = "اسم المنتج"
    else:
        name_col = None
        price_col = None
        for c in cols:
            sample = str(cdf[c].dropna().iloc[0]) if not cdf[c].dropna().empty else ""
            try:
                float(sample.replace(",", ""))
                if price_col is None:
                    price_col = c
            except Exception:
                if name_col is None and len(sample) > 5:
                    name_col = c
        if name_col is None:
            name_col = cols[0]
        if price_col is None:
            price_col = cols[1] if len(cols) > 1 else cols[0]
        return name_col, price_col

    if "سعر المنتج" in cs:
        price_col = "سعر المنتج"
    elif "السعر" in cs:
        price_col = "السعر"
    elif "سعر" in cs:
        price_col = "سعر"
    else:
        price_col = None
        for c in cols:
            if c == name_col:
                continue
            sample = str(cdf[c].dropna().iloc[0]) if not cdf[c].dropna().empty else ""
            try:
                float(str(sample).replace(",", ""))
                price_col = c
                break
            except Exception:
                continue
        if price_col is None:
            price_col = cols[1] if len(cols) > 1 else cols[0]

    return name_col, price_col


def upsert_comp_catalog(comp_dfs: dict):
    """يُحدِّث كتالوج المنافسين عند كل رفع جديد — بدون تكرار"""
    import re
    conn = get_db()
    today = datetime.now().strftime("%Y-%m-%d")
    total_new = 0
    rows_updated = 0
    _cc_cols = _pragma_column_names(conn, "comp_catalog")
    _has_cpk = any(c.lower() == "comp_product_key" for c in _cc_cols)

    for cname, cdf in comp_dfs.items():
        name_col, price_col = _resolve_comp_name_price_columns(cdf)

        for _, row in cdf.iterrows():
            name = str(row.get(name_col, "")).strip()
            if not name or len(name) < 4 or name.startswith("styles_"):
                continue
            norm = re.sub(r'\s+', ' ', name.lower().strip())
            try:
                price = float(str(row.get(price_col, 0)).replace(",", ""))
            except Exception:
                price = 0.0

            existing = conn.execute(
                "SELECT id FROM comp_catalog WHERE competitor=? AND norm_name=?",
                (cname, norm)
            ).fetchone()
            _cpk = _comp_catalog_product_key(cname, norm)

            if existing:
                rows_updated += 1
                if _has_cpk:
                    conn.execute(
                        "UPDATE comp_catalog SET price=?, last_seen=?, comp_product_key=? WHERE id=?",
                        (price, today, _cpk, existing[0]),
                    )
                else:
                    try:
                        conn.execute(
                            "UPDATE comp_catalog SET price=?, last_seen=? WHERE id=?",
                            (price, today, existing[0]),
                        )
                    except sqlite3.IntegrityError:
                        conn.execute(
                            "UPDATE comp_catalog SET price=?, last_seen=?, comp_product_key=? WHERE id=?",
                            (price, today, _cpk, existing[0]),
                        )
                        _has_cpk = True
            else:
                try:
                    if _has_cpk:
                        conn.execute(
                            """INSERT INTO comp_catalog (competitor, product_name, norm_name, price,
                                   first_seen, last_seen, comp_product_key)
                               VALUES (?,?,?,?,?,?,?)""",
                            (cname, name, norm, price, today, today, _cpk),
                        )
                    else:
                        conn.execute(
                            """INSERT INTO comp_catalog (competitor, product_name, norm_name, price, first_seen, last_seen)
                               VALUES (?,?,?,?,?,?)""",
                            (cname, name, norm, price, today, today),
                        )
                except sqlite3.IntegrityError as _ie:
                    _em = str(_ie).lower()
                    if "comp_product_key" in _em and not _has_cpk:
                        conn.execute(
                            """INSERT INTO comp_catalog (competitor, product_name, norm_name, price,
                                   first_seen, last_seen, comp_product_key)
                               VALUES (?,?,?,?,?,?,?)""",
                            (cname, name, norm, price, today, today, _cpk),
                        )
                        _has_cpk = True
                    else:
                        raise
                total_new += 1

    conn.commit()
    conn.close()
    return {"new_products": total_new, "updated": rows_updated}


def save_processed(product_key: str, product_name: str, competitor: str,
                   action: str, old_price=0.0, new_price=0.0,
                   product_id="", notes=""):
    """يحفظ منتجاً في قائمة المعالجة — مع منع التكرار، آمن للثريدات"""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=30000;")
            conn.execute(
                """INSERT OR REPLACE INTO processed_products
                   (timestamp, product_key, product_name, competitor, action,
                    old_price, new_price, product_id, notes)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (_ts(), product_key, product_name, competitor, action,
                 old_price, new_price, product_id, notes)
            )
            conn.commit()
    except Exception as _e:
        _logger.warning("save_processed: فشل حفظ المنتج المعالج '%s' — %s", product_key, _e)


def get_processed(limit=200) -> list:
    """يُعيد قائمة المنتجات المعالجة"""
    conn = get_db()
    rows = conn.execute(
        """SELECT timestamp, product_key, product_name, competitor,
                  action, old_price, new_price, product_id, notes
           FROM processed_products ORDER BY timestamp DESC LIMIT ?""",
        (limit,)
    ).fetchall()
    conn.close()
    keys = ["timestamp","product_key","product_name","competitor",
            "action","old_price","new_price","product_id","notes"]
    return [dict(zip(keys, r)) for r in rows]


def undo_processed(product_key: str) -> bool:
    """تراجع: إزالة المنتج من قائمة المعالجة"""
    conn = get_db()
    conn.execute("DELETE FROM processed_products WHERE product_key=?", (product_key,))
    conn.execute("DELETE FROM hidden_products WHERE product_key=?", (product_key,))
    conn.commit()
    conn.close()
    return True


def get_processed_keys() -> set:
    """مفاتيح المنتجات المعالجة لاستبعادها من القوائم"""
    conn = get_db()
    rows = conn.execute("SELECT product_key FROM processed_products").fetchall()
    conn.close()
    return {r[0] for r in rows}


# ═══════════════════════════════════════════════════════════════
#  v26.0 — Migration Script + Automation Log
# ═══════════════════════════════════════════════════════════════
def migrate_db_v26():
    """
    سكريبت ترحيل v26.0 — يُنفَّذ مرة واحدة فقط.
    يضمن وجود كل الجداول المطلوبة بدون فقدان أي بيانات.
    آمن للتشغيل المتكرر (idempotent).
    """
    try:
        conn = get_db()
        cur = conn.cursor()

        # ── 1. جدول سجل الأتمتة ──
        cur.execute("""CREATE TABLE IF NOT EXISTS automation_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT DEFAULT (datetime('now','localtime')),
            product_name TEXT,
            product_id TEXT,
            rule_name TEXT,
            action TEXT,
            old_price REAL,
            new_price REAL,
            comp_price REAL,
            competitor TEXT,
            match_score REAL,
            reason TEXT,
            pushed_to_make INTEGER DEFAULT 0
        )""")

        # ── 2. جدول إعدادات الأتمتة (للحفظ بين الجلسات) ──
        cur.execute("""CREATE TABLE IF NOT EXISTS automation_settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        )""")

        # ── 3. جدول نسخة قاعدة البيانات (لتتبع الترحيلات) ──
        cur.execute("""CREATE TABLE IF NOT EXISTS db_version (
            version TEXT PRIMARY KEY,
            applied_at TEXT DEFAULT (datetime('now','localtime')),
            description TEXT
        )""")

        # ── 4. تسجيل أن الترحيل v26.0 تم تنفيذه ──
        cur.execute("""INSERT OR IGNORE INTO db_version (version, description)
                       VALUES ('v26.0', 'إضافة جداول الأتمتة الذكية وسجل القرارات')""")

        # ── 5. إضافة أعمدة جديدة للجداول الموجودة (بأمان) ──
        # إضافة أعمدة جديدة — يُتجاهل الخطأ فقط عند وجود العمود مسبقاً
        for _stmt in [
            "ALTER TABLE our_catalog ADD COLUMN cost_price REAL DEFAULT 0",
            "ALTER TABLE processed_products ADD COLUMN auto_processed INTEGER DEFAULT 0",
            "ALTER TABLE comp_catalog ADD COLUMN comp_product_key TEXT",
            "ALTER TABLE job_progress ADD COLUMN audit_json TEXT DEFAULT '{}'",
        ]:
            try:
                cur.execute(_stmt)
            except sqlite3.OperationalError:
                pass

        try:
            cur.execute(
                """UPDATE comp_catalog SET comp_product_key = competitor || '::' || IFNULL(norm_name, '')
                   WHERE comp_product_key IS NULL OR TRIM(comp_product_key) = ''"""
            )
        except Exception as _e:
            _logger.warning("migrate_db_v26: فشل تعبئة comp_product_key — %s", _e)

        conn.commit()
        conn.close()
    except Exception as e:
        _logger.error("Migration v26 error: %s", e)
        try: conn.close()
        except Exception: pass


# ═══════════════════════════════════════════════════════════════
#  نقطة تهيئة واحدة — تُستدعى صراحةً من app.py عند الإقلاع
# ═══════════════════════════════════════════════════════════════
_DB_INITIALIZED = False


def initialize_database() -> None:
    """
    تهيّئ قاعدة البيانات كاملةً مرة واحدة فقط لكل عملية.
    تشمل: init_db + _init_competitor_price_history + init_db_v26 + migrate_db_v26.
    آمنة للاستدعاء المتعدد (idempotent بواسطة العلم _DB_INITIALIZED).
    """
    global _DB_INITIALIZED
    if _DB_INITIALIZED:
        return
    init_db()
    _init_competitor_price_history()
    init_db_v26()
    migrate_db_v26()
    _DB_INITIALIZED = True
    _logger.info("initialize_database: قاعدة البيانات جاهزة — %s", DB_PATH)

