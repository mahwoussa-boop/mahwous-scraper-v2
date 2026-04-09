"""
utils/product_state.py
======================
State Machine لتتبع حالة المنتج عبر دورة حياته الكاملة.

الحالات:
  pending      → قيد المعالجة / المطابقة
  matched      → تم تطابق مؤكد (مع منتجنا)
  review       → نطاق رمادي — يحتاج قرار بشري
  missing      → مفقود مؤكد (لا مطابق بعد 4 مرور)
  migrated     → تم الترحيل لسلة
  price_alert  → تنبيه: المنافس غيّر سعره بشكل مؤثر
  archived     → مؤرشف (انتهت صلاحية المراجعة أو تم الرفض)

الانتقالات المسموح بها:
  pending → matched | review | missing
  review  → matched | missing | archived
  matched → price_alert | migrated
  price_alert → matched | review
  migrated → price_alert
  أي حالة → archived (إجراء يدوي)
"""

import json
import sqlite3
from datetime import datetime, timezone

from config import ProductState
from utils.db_manager import DB_PATH

# ── تعريف الانتقالات الصحيحة ─────────────────────────────────────────────
_ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    ProductState.PENDING:     {ProductState.MATCHED, ProductState.REVIEW, ProductState.MISSING},
    ProductState.REVIEW:      {ProductState.MATCHED, ProductState.MISSING, ProductState.ARCHIVED},
    ProductState.MATCHED:     {ProductState.PRICE_ALERT, ProductState.MIGRATED},
    ProductState.PRICE_ALERT: {ProductState.MATCHED, ProductState.REVIEW},
    ProductState.MIGRATED:    {ProductState.PRICE_ALERT},
    ProductState.MISSING:     {ProductState.REVIEW, ProductState.MATCHED},   # إعادة فحص
    ProductState.ARCHIVED:    set(),                                          # نهائي
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, timeout=10)
    c.row_factory = sqlite3.Row
    return c


# ─────────────────────────────────────────────────────────────────────────────
#  الدوال الأساسية
# ─────────────────────────────────────────────────────────────────────────────

def get_state(product_key: str) -> dict | None:
    """إرجاع سجل حالة المنتج الكامل أو None إذا لم يوجد."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT * FROM product_states WHERE product_key = ?",
            (product_key,)
        ).fetchone()
    return dict(row) if row else None


def init_product(product_key: str, product_name: str,
                 competitor: str = "", meta: dict | None = None) -> dict:
    """
    تهيئة سجل حالة جديد للمنتج (pending) إذا لم يكن موجوداً.
    يُعيد السجل الحالي إذا كان موجوداً بالفعل.
    """
    existing = get_state(product_key)
    if existing:
        return existing

    now = _now()
    with _conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO product_states
            (product_key, product_name, state, competitor, created_at, updated_at, meta_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            product_key, product_name,
            ProductState.PENDING, competitor,
            now, now,
            json.dumps(meta or {}, ensure_ascii=False),
        ))
        conn.commit()

    return get_state(product_key)


def transition(product_key: str, new_state: str, *,
               match_score: float = 0.0,
               match_pass: int = 0,
               salla_id: str = "",
               salla_sku: str = "",
               meta: dict | None = None,
               force: bool = False) -> bool:
    """
    نقل المنتج إلى حالة جديدة مع التحقق من صحة الانتقال.

    Parameters
    ----------
    product_key : str
    new_state   : str   — الحالة المستهدفة (ProductState.*)
    force       : bool  — تجاوز قواعد الانتقال (للاستخدام الإداري فقط)

    Returns
    -------
    True  إذا نجح الانتقال
    False إذا كان الانتقال غير مسموح أو المنتج غير موجود
    """
    record = get_state(product_key)
    if not record:
        return False

    current = record["state"]
    if not force and new_state not in _ALLOWED_TRANSITIONS.get(current, set()):
        return False   # انتقال مرفوض

    # دمج meta الموجودة مع الجديدة
    existing_meta = json.loads(record.get("meta_json") or "{}")
    if meta:
        existing_meta.update(meta)

    migrated_at = record.get("migrated_at", "")
    if new_state == ProductState.MIGRATED:
        migrated_at = _now()

    with _conn() as conn:
        conn.execute("""
            UPDATE product_states
            SET state       = ?,
                prev_state  = ?,
                match_score = CASE WHEN ? > 0 THEN ? ELSE match_score END,
                match_pass  = CASE WHEN ? > 0 THEN ? ELSE match_pass  END,
                salla_id    = CASE WHEN ? != '' THEN ? ELSE salla_id   END,
                salla_sku   = CASE WHEN ? != '' THEN ? ELSE salla_sku  END,
                migrated_at = ?,
                updated_at  = ?,
                meta_json   = ?
            WHERE product_key = ?
        """, (
            new_state, current,
            match_score, match_score,
            match_pass,  match_pass,
            salla_id,    salla_id,
            salla_sku,   salla_sku,
            migrated_at, _now(),
            json.dumps(existing_meta, ensure_ascii=False),
            product_key,
        ))
        conn.commit()

    return True


def bulk_init(records: list[dict]) -> int:
    """
    تهيئة جماعية للمنتجات (pending) — يتجاهل المنتجات الموجودة.

    Parameters
    ----------
    records : list of dicts with keys: product_key, product_name, [competitor]

    Returns
    -------
    عدد المنتجات المُهيَّأة فعلاً
    """
    now = _now()
    rows = [
        (r["product_key"], r["product_name"],
         ProductState.PENDING, r.get("competitor", ""),
         now, now, "{}")
        for r in records
    ]
    with _conn() as conn:
        conn.executemany("""
            INSERT OR IGNORE INTO product_states
            (product_key, product_name, state, competitor, created_at, updated_at, meta_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        return conn.execute("SELECT changes()").fetchone()[0]


def get_products_by_state(state: str, limit: int = 500) -> list[dict]:
    """إرجاع قائمة منتجات في حالة معينة."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM product_states WHERE state = ? ORDER BY updated_at DESC LIMIT ?",
            (state, limit)
        ).fetchall()
    return [dict(r) for r in rows]


def mark_migrated(product_key: str, salla_id: str = "", salla_sku: str = "") -> bool:
    """تسجيل أن المنتج تم ترحيله لسلة بنجاح."""
    return transition(
        product_key,
        ProductState.MIGRATED,
        salla_id=salla_id,
        salla_sku=salla_sku,
    )


def stats() -> dict:
    """إحصائيات سريعة لحالات المنتجات."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT state, COUNT(*) AS cnt FROM product_states GROUP BY state"
        ).fetchall()
    return {r["state"]: r["cnt"] for r in rows}
