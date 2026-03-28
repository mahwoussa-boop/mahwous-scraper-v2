"""
تخزين لحظي لبطاقات المقارنة + سجل العمليات — SQLite (WAL) مع قفل كتابة لتقليل تعارض القراءة/الكتابة مع CSV.
"""
from __future__ import annotations

import hashlib
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from utils.scrape_live_cards import bucket_final_priced_df, classify_pricing_row

LIVE_DB = os.path.join("data", "live_pricing_ui.db")
_WRITE_LOCK = threading.RLock()
MAX_ROWS_PER_BUCKET = 500
MAX_LOG_ROWS = 350
_READ_RETRIES = 6
_READ_BACKOFF = 0.12


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_live_db() -> None:
    os.makedirs("data", exist_ok=True)
    with _WRITE_LOCK:
        conn = sqlite3.connect(LIVE_DB, timeout=45)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=45000;")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS live_cards (
                row_key TEXT PRIMARY KEY,
                bucket TEXT NOT NULL,
                product_name TEXT,
                comp_name TEXT,
                price REAL,
                comp_price REAL,
                comp_url TEXT,
                image_our TEXT,
                image_comp TEXT,
                match_score REAL,
                ai_state TEXT,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS activity_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                message TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_live_cards_bucket ON live_cards(bucket);
            """
        )
        conn.commit()
        conn.close()


def append_activity_log(message: str) -> None:
    init_live_db()
    msg = (message or "").strip()[:2000]
    if not msg:
        return
    with _WRITE_LOCK:
        conn = sqlite3.connect(LIVE_DB, timeout=45)
        conn.execute("PRAGMA busy_timeout=45000;")
        conn.execute(
            "INSERT INTO activity_log (ts, message) VALUES (?, ?)",
            (_utc_iso(), msg),
        )
        conn.commit()
        # تقليم السجل — الإبقاء على آخر MAX_LOG_ROWS
        n = conn.execute("SELECT COUNT(*) FROM activity_log").fetchone()[0]
        if n > MAX_LOG_ROWS + 80:
            to_drop = n - MAX_LOG_ROWS
            conn.execute(
                """
                DELETE FROM activity_log WHERE rowid IN (
                    SELECT rowid FROM activity_log ORDER BY rowid ASC LIMIT ?
                )
                """,
                (to_drop,),
            )
            conn.commit()
        conn.close()


def get_recent_logs(limit: int = 35) -> List[Dict[str, str]]:
    init_live_db()
    conn = sqlite3.connect(LIVE_DB, timeout=15)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT ts, message FROM activity_log
            ORDER BY id DESC LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
    finally:
        conn.close()
    out = [{"ts": str(r["ts"]), "message": str(r["message"])} for r in rows]
    out.reverse()
    return out


def get_cards_for_bucket(bucket: str, limit: int = 150) -> List[Dict[str, Any]]:
    init_live_db()
    conn = sqlite3.connect(LIVE_DB, timeout=15)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT row_key, bucket, product_name, comp_name, price, comp_price,
                   comp_url, image_our, image_comp, match_score, ai_state, updated_at
            FROM live_cards
            WHERE bucket = ?
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (bucket, max(1, int(limit))),
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def count_by_bucket() -> Dict[str, int]:
    init_live_db()
    conn = sqlite3.connect(LIVE_DB, timeout=15)
    try:
        rows = conn.execute(
            "SELECT bucket, COUNT(1) FROM live_cards GROUP BY bucket"
        ).fetchall()
    finally:
        conn.close()
    return {str(b): int(c) for b, c in rows}


def _read_final_priced_df(path: str) -> Optional[pd.DataFrame]:
    for attempt in range(_READ_RETRIES):
        try:
            return pd.read_csv(path, encoding="utf-8-sig")
        except (PermissionError, OSError, pd.errors.EmptyDataError):
            time.sleep(_READ_BACKOFF * (attempt + 1))
        except Exception:
            break
    return None


def sync_from_final_priced_csv(
    log_callback: Optional[Callable[[str], None]] = None,
) -> bool:
    """
    يقرأ final_priced_latest.csv (مع إعادة محاولة عند قفل الملف) ويعيد ملء live_cards.
    """
    path = os.path.join("data", "final_priced_latest.csv")
    if not os.path.isfile(path):
        if log_callback:
            log_callback("⏳ لا يوجد final_priced_latest.csv بعد — انتظر خط التسعير التلقائي.")
        return False

    df = _read_final_priced_df(path)
    if df is None or df.empty:
        if log_callback:
            log_callback("⚠️ تعذّر قراءة final_priced_latest.csv أو الملف فارغ.")
        return False

    now = _utc_iso()
    rows_sql: List[tuple] = []

    # تصنيف صفاً بصف لضمان تطابق منطق الواجهة مع scrape_live_cards
    for _, row in df.iterrows():
        bucket = classify_pricing_row(row)
        sku = str(row.get("sku", "") or "").strip()
        comp_url = str(row.get("comp_url", "") or "").strip()
        rk = hashlib.sha256(f"{sku}|{comp_url}".encode("utf-8")).hexdigest()[:40]
        pname = str(row.get("name", "") or "")
        cname = str(row.get("comp_name", row.get("name_comp", "")) or "")
        try:
            price = float(pd.to_numeric(row.get("price"), errors="coerce") or 0)
        except Exception:
            price = 0.0
        try:
            cprice = float(pd.to_numeric(row.get("comp_price"), errors="coerce") or 0)
        except Exception:
            cprice = 0.0
        img_o = str(row.get("image_url", "") or "").strip()
        img_c = str(row.get("comp_image_url", "") or "").strip()
        mscore = row.get("match_score")
        try:
            mscore_f = float(mscore) if mscore is not None and str(mscore) != "nan" else None
        except Exception:
            mscore_f = None
        ai_st = str(row.get("ai_verification_state", "") or "")

        # حدّ لكل قسم لاحقاً عبر تجميع — هنا نمرّ على كل الصفوف ثم نقتصر عند الإدراج
        rows_sql.append(
            (
                rk,
                bucket,
                pname,
                cname,
                price,
                cprice,
                comp_url,
                img_o,
                img_c,
                mscore_f,
                ai_st,
                now,
            )
        )

    # فرز حسب الحاوية وحد أعلى لكل bucket
    tmp: Dict[str, List[tuple]] = {k: [] for k in ("higher", "lower", "ok", "missing", "review")}
    for t in rows_sql:
        b = t[1]
        if b in tmp:
            tmp[b].append(t)
    trimmed: List[tuple] = []
    for b, lst in tmp.items():
        trimmed.extend(lst[:MAX_ROWS_PER_BUCKET])

    init_live_db()
    with _WRITE_LOCK:
        conn = sqlite3.connect(LIVE_DB, timeout=90)
        conn.execute("PRAGMA busy_timeout=60000;")
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM live_cards")
            conn.executemany(
                """
                INSERT INTO live_cards (
                    row_key, bucket, product_name, comp_name, price, comp_price,
                    comp_url, image_our, image_comp, match_score, ai_state, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                trimmed,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            conn.close()
            if log_callback:
                log_callback("❌ فشل كتابة قاعدة اللوحة الحية (SQLite).")
            return False
        conn.close()

    counts = bucket_final_priced_df(df)
    summary = ", ".join(f"{k}={len(v)}" for k, v in counts.items())
    msg = f"✅ مزامنة اللوحة الحية: {len(df)} صف في المصدر — {summary}"
    append_activity_log(msg)
    if log_callback:
        log_callback(msg)
    return True
