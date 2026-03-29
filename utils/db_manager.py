"""
utils/db_manager.py - v18.0
- تتبع تاريخ الأسعار (يحدث السعر إذا تغير)
- حفظ نقاط استئناف للمعالجة الخلفية
- قرارات لكل منتج (موافق/تأجيل/إزالة)
- سجل كامل بالتاريخ والوقت
"""
import sqlite3, json, os, tempfile, logging
from datetime import datetime

_DB_NAME = "pricing_v18.db"


def _resolve_db_path() -> str:
    """
    مسار ملف SQLite قابل للكتابة على كل المنصات.
    - MAHWOUS_DB_DIR: فرض مجلد (الأولوية القصوى)
    - مجلد المشروع `data/pricing_v18.db` إن وُجد الملف أو المجلد — يوحّد القراءة/الكتابة مع الملفات على القرص
    - وإلا tempfile.gettempdir() (Streamlit Cloud / إلخ)
    """
    override = os.environ.get("MAHWOUS_DB_DIR", "").strip()
    if override:
        parent = os.path.abspath(override)
        os.makedirs(parent, exist_ok=True)
        return os.path.join(parent, _DB_NAME)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    local_data = os.path.join(project_root, "data")
    local_db = os.path.join(local_data, _DB_NAME)
    if os.path.isfile(local_db):
        return local_db
    try:
        os.makedirs(local_data, exist_ok=True)
    except Exception:
        pass
    if os.path.isdir(local_data):
        return local_db
    return os.path.join(tempfile.gettempdir(), _DB_NAME)


DB_PATH = _resolve_db_path()
logger = logging.getLogger(__name__)


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
        our_file TEXT, comp_files TEXT
    )""")
    # إضافة عمود missing_json إذا لم يكن موجوداً (للتوافق مع قواعد البيانات القديمة)
    try:
        c.execute("ALTER TABLE job_progress ADD COLUMN missing_json TEXT DEFAULT '[]'")
    except:
        pass  # العمود موجود بالفعل

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
    except: pass


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
    except: pass


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
    except: return []


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

    # آخر سعر مسجل لهذا المنتج/المنافس
    last = conn.execute(
        """SELECT price, date FROM price_history
           WHERE product_name=? AND competitor=?
           ORDER BY id DESC LIMIT 1""",
        (product_name, competitor)
    ).fetchone()

    price_changed = False
    if last:
        last_price = last["price"]
        last_date  = last["date"]
        price_changed = abs(float(price) - float(last_price)) > 0.01

        if last_date == today:
            # نفس اليوم → حدّث فقط
            conn.execute(
                """UPDATE price_history SET price=?,our_price=?,diff=?,
                   match_score=?,decision=?,product_id=?
                   WHERE product_name=? AND competitor=? AND date=?""",
                (price, our_price, diff, match_score, decision,
                 product_id, product_name, competitor, today)
            )
        else:
            # يوم جديد → أضف سجل
            conn.execute(
                """INSERT INTO price_history
                   (date,product_name,competitor,price,our_price,diff,
                    match_score,decision,product_id)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (today, product_name, competitor, price, our_price,
                 diff, match_score, decision, product_id)
            )
    else:
        # أول مرة
        conn.execute(
            """INSERT INTO price_history
               (date,product_name,competitor,price,our_price,diff,
                match_score,decision,product_id)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (today, product_name, competitor, price, our_price,
             diff, match_score, decision, product_id)
        )

    conn.commit(); conn.close()
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
    except: return []


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
    except: return []


# ─── المعالجة الخلفية ──────────────────────
def save_job_progress(job_id, total, processed, results, status="running",
                      our_file="", comp_files="", missing=None):
    missing_data = json.dumps(missing if missing else [], ensure_ascii=False, default=str)
    results_data = json.dumps(results, ensure_ascii=False, default=str)
    with sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute(
            """INSERT OR REPLACE INTO job_progress
               (job_id,started_at,updated_at,status,total,processed,
                results_json,missing_json,our_file,comp_files)
               VALUES (?,
                   COALESCE((SELECT started_at FROM job_progress WHERE job_id=?), ?),
                   ?, ?, ?, ?, ?, ?, ?, ?)""",
            (job_id, job_id, _ts(), _ts(), status, total, processed,
             results_data, missing_data, our_file, comp_files)
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
            _rj = d.get("results_json") or "[]"
            _mj = d.get("missing_json") or "[]"
            try:
                d["results"] = json.loads(_rj)
            except Exception:
                d["results"] = []
            try:
                d["missing"] = json.loads(_mj)
            except Exception:
                d["missing"] = []
            return d
    except: pass
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
            _rj = d.get("results_json") or "[]"
            _mj = d.get("missing_json") or "[]"
            try:
                d["results"] = json.loads(_rj)
            except Exception:
                d["results"] = []
            try:
                d["missing"] = json.loads(_mj)
            except Exception:
                d["missing"] = []
            return d
    except: pass
    return None


def clear_missing_from_last_job():
    """يمسح missing_json لآخر مهمة محفوظة حتى لا تُعاد المفقودات عند تحديث الصفحة."""
    j = get_last_job()
    if not j or not j.get("job_id"):
        return False
    try:
        save_job_progress(
            str(j["job_id"]),
            int(j.get("total") or 0),
            int(j.get("processed") or j.get("total") or 0),
            j.get("results") or [],
            status=str(j.get("status") or "done"),
            our_file=j.get("our_file") or "",
            comp_files=j.get("comp_files") or "",
            missing=[],
        )
        return True
    except Exception:
        return False


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
    except: pass


def get_analysis_history(limit=20):
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT * FROM analysis_history ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except: return []


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
    except: return []


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
    except:
        pass

def get_hidden_product_keys() -> set:
    """يُرجع مجموعة كل مفاتيح المنتجات المخفية من قاعدة البيانات"""
    try:
        conn = get_db()
        rows = conn.execute("SELECT product_key FROM hidden_products").fetchall()
        conn.close()
        return {r["product_key"] for r in rows}
    except:
        return set()


init_db()


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

    # كتالوج متجرنا (يُحدَّث يومياً) — يشمل التكلفة ورابط صورة المنتج للمقارنة البصرية
    cur.execute("""CREATE TABLE IF NOT EXISTS our_catalog (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT UNIQUE,
        product_name TEXT NOT NULL,
        norm_name TEXT,
        price REAL,
        cost_price REAL DEFAULT 0,
        image_url TEXT DEFAULT '',
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

    # منتجات المنافس المكشوطة لحظياً (مرآة دائمة لـ product_state — مفتاح comp_url)
    cur.execute("""CREATE TABLE IF NOT EXISTS scraped_competitor_products (
        comp_url TEXT PRIMARY KEY,
        name TEXT,
        price REAL,
        brand TEXT,
        image_url TEXT,
        sku TEXT,
        competitor TEXT NOT NULL,
        extraction_method TEXT,
        first_seen TEXT,
        last_seen TEXT NOT NULL
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


def replace_our_catalog_from_dataframe(
    our_df,
    *,
    name_col: str | None = None,
    id_col: str | None = None,
    price_col: str | None = None,
    cost_col: str | None = None,
    image_col: str | None = None,
) -> dict:
    """
    يفرّغ جدول our_catalog ويعيد ملءه من DataFrame — للرفع الكامل أو استبدال بيانات الاختبار
    بـ mahwous_catalog.csv الحقيقي.
    """
    import hashlib
    import re

    import pandas as pd

    if our_df is None or our_df.empty:
        return {"inserted": 0, "cleared": False}

    df = our_df.copy()

    def _pick(candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None

    nc = name_col or _pick(
        ("اسم المنتج", "أسم المنتج", "name", "product_name", "المنتج", "Product Name")
    )
    ic = id_col or _pick(
        ("رقم المنتج", "رمز المنتج sku", "sku", "SKU", "product_id", "Product ID")
    )
    pc = price_col or _pick(("السعر", "سعر المنتج", "price", "Price"))
    cc = cost_col or _pick(
        ("التكلفة", "سعر التكلفة", "cost", "cost_price", "تكلفة", "Cost")
    )
    img_c = image_col or _pick(
        ("صورة المنتج", "image", "image_url", "الصورة", "img", "رابط الصورة")
    )

    if not nc or not pc:
        logger.error("replace_our_catalog_from_dataframe: missing name or price column")
        return {"inserted": 0, "cleared": False}

    if ic and ic in df.columns:
        df = df.drop_duplicates(subset=[ic], keep="last")

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE our_catalog ADD COLUMN cost_price REAL DEFAULT 0")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE our_catalog ADD COLUMN image_url TEXT DEFAULT ''")
    except Exception:
        pass
    cur.execute("DELETE FROM our_catalog")
    conn.commit()

    today = datetime.now().strftime("%Y-%m-%d")
    inserted = 0
    for _, row in df.iterrows():
        name = str(row.get(nc, "")).strip()
        if not name or name.lower() in ("nan", "none", ""):
            continue
        norm = re.sub(r"\s+", " ", name.lower().strip())
        pid = ""
        if ic:
            pid = str(row.get(ic, "")).strip().rstrip(".0")
        if not pid:
            pid = "auto-" + hashlib.sha256(norm.encode("utf-8")).hexdigest()[:16]
        try:
            price = float(str(row.get(pc, 0)).replace(",", ""))
        except Exception:
            price = 0.0
        cost = 0.0
        if cc:
            try:
                cost = float(str(row.get(cc, 0)).replace(",", ""))
            except Exception:
                cost = 0.0
        img_val = ""
        if img_c:
            img_val = str(row.get(img_c, "") or "").strip()
            if img_val.lower() in ("nan", "none", "null", ""):
                img_val = ""
        cur.execute(
            """INSERT INTO our_catalog (product_id, product_name, norm_name, price, cost_price, image_url, first_seen, last_seen)
               VALUES (?,?,?,?,?,?,?,?)""",
            (pid, name, norm, price, cost, img_val, today, today),
        )
        inserted += 1

    conn.commit()
    conn.close()
    logger.info("replace_our_catalog_from_dataframe: inserted %s rows", inserted)
    return {"inserted": inserted, "cleared": True}


def upsert_comp_catalog(comp_dfs: dict):
    """يُحدِّث كتالوج المنافسين عند كل رفع جديد — بدون تكرار"""
    import re
    conn = get_db()
    today = datetime.now().strftime("%Y-%m-%d")
    total_new = 0

    for cname, cdf in comp_dfs.items():
        # استكشاف الأعمدة — يفضّل الأسماء المعيارية بعد التعيين اليدوي
        cols = list(cdf.columns)
        name_col  = "اسم المنتج" if "اسم المنتج" in cols else None
        price_col = "السعر" if "السعر" in cols else None
        for c in cols:
            sample = str(cdf[c].dropna().iloc[0]) if not cdf[c].dropna().empty else ""
            try:
                float(sample.replace(",",""))
                if price_col is None:
                    price_col = c
            except Exception:
                if name_col is None and len(sample) > 5:
                    name_col = c

        if name_col is None:
            name_col = cols[0]
        if price_col is None:
            price_col = cols[1] if len(cols) > 1 else cols[0]

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

            if existing:
                conn.execute(
                    "UPDATE comp_catalog SET price=?, last_seen=? WHERE id=?",
                    (price, today, existing[0])
                )
            else:
                conn.execute(
                    """INSERT INTO comp_catalog (competitor, product_name, norm_name, price, first_seen, last_seen)
                       VALUES (?,?,?,?,?,?)""",
                    (cname, name, norm, price, today, today)
                )
                total_new += 1

    conn.commit()
    conn.close()
    return {"new_products": total_new}


def upsert_scraped_competitor_product(row: dict, extraction_method: str = "") -> None:
    """
    يحفظ كل صف منتج منافس ناجح من الكاشط في SQLite الدائمة (DB_PATH).
    المفتاح: comp_url — يُحدَّث عند كل كشط ناجح.
    """
    from urllib.parse import urlparse

    if not row:
        return
    comp_url = str(row.get("comp_url", "") or "").strip()
    if not comp_url:
        return
    host = (urlparse(comp_url).netloc or "").lower() or "unknown"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    name = str(row.get("name", "") or "")
    try:
        price = float(row.get("price", 0) or 0)
    except (TypeError, ValueError):
        price = 0.0
    brand = str(row.get("brand", "") or "")
    image_url = str(row.get("image_url", "") or row.get("comp_image_url", "") or "")
    sku = str(row.get("sku", "") or "")
    ext = (extraction_method or str(row.get("Extraction_Method", "") or "")).strip()[:120]

    try:
        conn = get_db()
        prev = conn.execute(
            "SELECT comp_url FROM scraped_competitor_products WHERE comp_url=?",
            (comp_url,),
        ).fetchone()
        if prev is None:
            conn.execute(
                """INSERT INTO scraped_competitor_products
                   (comp_url, name, price, brand, image_url, sku, competitor,
                    extraction_method, first_seen, last_seen)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (comp_url, name, price, brand, image_url, sku, host, ext, now, now),
            )
        else:
            conn.execute(
                """UPDATE scraped_competitor_products SET
                   name=?, price=?, brand=?, image_url=?, sku=?, competitor=?,
                   extraction_method=?, last_seen=?
                   WHERE comp_url=?""",
                (name, price, brand, image_url, sku, host, ext, now, comp_url),
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug("upsert_scraped_competitor_product skip: %s", e)


def upsert_product(row: dict, extraction_method: str = "") -> None:
    """واجهة موحّدة: حفظ منتج منافس مكشوط (JSON-LD / CSS / Regex) في القاعدة الدائمة."""
    upsert_scraped_competitor_product(row, extraction_method=extraction_method)


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
    except Exception:
        pass  # لا يوقف الثريد الخلفي


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
        # إضافة عمود cost_price لجدول our_catalog إذا لم يكن موجوداً
        try:
            cur.execute("ALTER TABLE our_catalog ADD COLUMN cost_price REAL DEFAULT 0")
        except Exception:
            pass  # العمود موجود مسبقاً
        try:
            cur.execute("ALTER TABLE our_catalog ADD COLUMN image_url TEXT DEFAULT ''")
        except Exception:
            pass

        # إضافة عمود auto_processed لجدول processed_products
        try:
            cur.execute("ALTER TABLE processed_products ADD COLUMN auto_processed INTEGER DEFAULT 0")
        except Exception:
            pass

        cur.execute("""CREATE TABLE IF NOT EXISTS scraped_competitor_products (
            comp_url TEXT PRIMARY KEY,
            name TEXT,
            price REAL,
            brand TEXT,
            image_url TEXT,
            sku TEXT,
            competitor TEXT NOT NULL,
            extraction_method TEXT,
            first_seen TEXT,
            last_seen TEXT NOT NULL
        )""")

        conn.commit()
        conn.close()
    except Exception as e:
        logger.exception("Migration v26 error: %s", e)
        try: conn.close()
        except: pass



# ═══════════════════════════════════════════════════════════════
#  v26.1 — منع التكرار الصارم (Strict Duplicate Prevention)
# ═══════════════════════════════════════════════════════════════

import re as _re

def _normalize_for_dedup(name: str) -> str:
    """
    تطبيع الاسم لفحص التكرار الصارم:
    - تحويل لأحرف صغيرة
    - إزالة المسافات الزائدة
    - توحيد الأحجام: 100ML = 100ml
    - إزالة علامات الترقيم
    """
    if not name:
        return ""
    n = name.lower().strip()
    n = _re.sub(r'\s+', ' ', n)
    n = _re.sub(r'[^\w\s]', '', n)
    # توحيد الأحجام: 100 ml = 100ml
    n = _re.sub(r'(\d+)\s*ml', r'ml', n)
    n = _re.sub(r'(\d+)\s*م\s*ل', r'ml', n)
    return n.strip()


def check_strict_duplicate(product_name: str,
                             sku: str = "",
                             brand: str = "",
                             catalog: str = "our") -> dict:
    """
    فحص تكرار صارم قبل إضافة أي منتج.
    يتحقق من 3 معايير:
    1. الاسم المطبّع (Normalized Name)
    2. الباركود / SKU (إذا متوفر)
    3. العلامة التجارية + اسم مطبّع مختصر

    المُعيد:
    {
        "is_duplicate": bool,
        "method": "name" / "sku" / "brand+name" / None,
        "existing_name": str,
        "existing_id": str
    }
    """
    table = "our_catalog" if catalog == "our" else "comp_catalog"
    norm = _normalize_for_dedup(product_name)
    result = {"is_duplicate": False, "method": None,
              "existing_name": "", "existing_id": ""}

    try:
        conn = get_db()

        # ── فحص 1: الاسم المطبّع ──
        if norm:
            row = conn.execute(
                f"SELECT product_name, product_id FROM {table} WHERE norm_name=? LIMIT 1",
                (norm,)
            ).fetchone()
            if row:
                result.update({"is_duplicate": True, "method": "name",
                                "existing_name": row[0], "existing_id": row[1] or ""})
                conn.close()
                return result

        # ── فحص 2: SKU / Barcode ──
        if sku and sku.strip() and catalog == "our":
            row = conn.execute(
                "SELECT product_name, product_id FROM our_catalog WHERE product_id=? LIMIT 1",
                (sku.strip(),)
            ).fetchone()
            if row:
                result.update({"is_duplicate": True, "method": "sku",
                                "existing_name": row[0], "existing_id": row[1] or ""})
                conn.close()
                return result

        # ── فحص 3: العلامة التجارية + الاسم المختصر ──
        if brand and norm:
            brand_norm = _normalize_for_dedup(brand)
            # استخرج الكلمات الجوهرية (أول 4 كلمات بعد الماركة)
            name_without_brand = norm.replace(brand_norm, "").strip()
            key_words = " ".join(name_without_brand.split()[:4])
            if len(key_words) > 5:
                rows = conn.execute(
                    f"SELECT product_name, product_id, norm_name FROM {table} "                    f"WHERE norm_name LIKE ? LIMIT 5",
                    (f"%{brand_norm}%",)
                ).fetchall()
                for r in rows:
                    existing_norm = r[2] or ""
                    existing_key = " ".join(
                        existing_norm.replace(brand_norm, "").strip().split()[:4]
                    )
                    if existing_key and existing_key == key_words:
                        result.update({"is_duplicate": True, "method": "brand+name",
                                        "existing_name": r[0], "existing_id": r[1] or ""})
                        conn.close()
                        return result

        conn.close()
    except Exception as e:
        pass  # لا يوقف سير العمل

    return result


def bulk_check_duplicates(products: list, catalog: str = "our") -> list:
    """
    فحص تكرار مجمّع لقائمة منتجات.
    كل عنصر في products: {"name": str, "sku": str, "brand": str}
    يُعيد قائمة: [{...original..., "duplicate_check": {...}}, ...]
    """
    results = []
    for p in products:
        check = check_strict_duplicate(
            product_name=p.get("name", ""),
            sku=p.get("sku", ""),
            brand=p.get("brand", ""),
            catalog=catalog
        )
        results.append({**p, "duplicate_check": check})
    return results
