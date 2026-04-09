"""
utils/event_bus.py
==================
Event Router — محرك الأحداث الداخلي.

يستمع للأحداث (price_change, new_competitor, match_failed, manual_edit …)
ويوزّعها على المعالجات المناسبة مع تسجيل الحدث في قاعدة البيانات.

الأحداث المدعومة:
  PRICE_CHANGE      — تغيّر سعر منافس بشكل مؤثر
  NEW_COMPETITOR    — منافس جديد عَرض نفس المنتج
  MATCH_FAILED      — فشلت جميع مرور المطابقة الأربعة
  MANUAL_EDIT       — تعديل يدوي في سلة (سعر / اسم / حالة)
  MIGRATION_DONE    — تم الترحيل لسلة بنجاح
  REVIEW_REQUIRED   — منتج يحتاج مراجعة بشرية
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Callable

from config import (
    PRICE_ALERT_THRESHOLD_ABS,
    PRICE_ALERT_THRESHOLD_PCT,
    ProductState,
)
from utils.db_manager import DB_PATH

_logger = logging.getLogger(__name__)

# ── أنواع الأحداث ──────────────────────────────────────────────────────────
class EventType:
    PRICE_CHANGE     = "price_change"
    NEW_COMPETITOR   = "new_competitor"
    MATCH_FAILED     = "match_failed"
    MANUAL_EDIT      = "manual_edit"
    MIGRATION_DONE   = "migration_done"
    REVIEW_REQUIRED  = "review_required"


# ── سجل المعالجات (Handlers) ──────────────────────────────────────────────
_handlers: dict[str, list[Callable]] = {}


def on(event_type: str):
    """Decorator: تسجيل دالة كمعالج لنوع حدث معين."""
    def decorator(fn: Callable) -> Callable:
        _handlers.setdefault(event_type, []).append(fn)
        return fn
    return decorator


def emit(event_type: str, payload: dict[str, Any]) -> int:
    """
    إطلاق حدث وتوزيعه على جميع المعالجات المسجّلة.

    Parameters
    ----------
    event_type : str          — نوع الحدث (EventType.*)
    payload    : dict         — بيانات الحدث

    Returns
    -------
    عدد المعالجات التي استقبلت الحدث
    """
    payload.setdefault("event_type", event_type)
    payload.setdefault("emitted_at", datetime.now(timezone.utc).isoformat())

    # حفظ الحدث في review_queue أو price_alerts حسب النوع
    _persist_event(event_type, payload)

    count = 0
    for handler in _handlers.get(event_type, []):
        try:
            handler(payload)
            count += 1
        except Exception as exc:
            _logger.exception("Event handler %s failed for event %s: %s",
                              handler.__name__, event_type, exc)

    return count


# ─────────────────────────────────────────────────────────────────────────────
#  المعالجات الأساسية المدمجة (Built-in Handlers)
# ─────────────────────────────────────────────────────────────────────────────

@on(EventType.PRICE_CHANGE)
def _handle_price_change(payload: dict) -> None:
    """
    عند تغيير سعر منافس:
    1. تسجيل تنبيه في جدول price_alerts
    2. تحديث حالة المنتج إلى price_alert
    3. إدراج المنتج في review_queue بأولوية عالية
    """
    from utils.product_state import transition   # lazy import لتجنب الدوري

    pk         = payload.get("product_key", "")
    pname      = payload.get("product_name", "")
    comp       = payload.get("competitor", "")
    old_price  = float(payload.get("old_price", 0))
    new_price  = float(payload.get("new_price", 0))

    if old_price <= 0:
        return

    change_pct = abs(new_price - old_price) / old_price * 100
    change_abs = abs(new_price - old_price)

    if change_pct < PRICE_ALERT_THRESHOLD_PCT and change_abs < PRICE_ALERT_THRESHOLD_ABS:
        return   # تغيير ضئيل — لا يستوجب تنبيهاً

    direction = "up" if new_price > old_price else "down"
    now = datetime.now(timezone.utc).isoformat()

    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        conn.execute("""
            INSERT INTO price_alerts
            (product_key, product_name, competitor, old_price, new_price,
             change_pct, direction, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'open', ?)
        """, (pk, pname, comp, old_price, new_price, round(change_pct, 2), direction, now))
        conn.commit()

    # انتقال الحالة → price_alert
    transition(pk, ProductState.PRICE_ALERT,
               meta={"last_alert": now, "change_pct": round(change_pct, 2)},
               force=True)

    _logger.info("PRICE_ALERT: %s | %s → %s (%.1f%%)", pname, old_price, new_price, change_pct)


@on(EventType.MATCH_FAILED)
def _handle_match_failed(payload: dict) -> None:
    """
    بعد فشل جميع المرور الأربعة → تحويل المنتج لـ missing.
    """
    from utils.product_state import transition

    pk    = payload.get("product_key", "")
    pname = payload.get("product_name", "")

    transition(pk, ProductState.MISSING,
               meta={"failed_passes": payload.get("failed_passes", 4)})

    _logger.info("MATCH_FAILED → missing: %s", pname)


@on(EventType.REVIEW_REQUIRED)
def _handle_review_required(payload: dict) -> None:
    """
    إدراج المنتج في قائمة المراجعة.
    """
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        conn.execute("""
            INSERT INTO review_queue
            (product_key, product_name, trigger_type, trigger_detail,
             old_value, new_value, priority, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
        """, (
            payload.get("product_key", ""),
            payload.get("product_name", ""),
            payload.get("trigger_type", EventType.REVIEW_REQUIRED),
            payload.get("trigger_detail", ""),
            str(payload.get("old_value", "")),
            str(payload.get("new_value", "")),
            int(payload.get("priority", 5)),
            now,
        ))
        conn.commit()


@on(EventType.MIGRATION_DONE)
def _handle_migration(payload: dict) -> None:
    """تسجيل الترحيل لسلة."""
    from utils.product_state import transition

    pk     = payload.get("product_key", "")
    sid    = str(payload.get("salla_id", ""))
    ssku   = str(payload.get("salla_sku", ""))
    transition(pk, ProductState.MIGRATED, salla_id=sid, salla_sku=ssku, force=True)
    _logger.info("MIGRATION_DONE: %s → salla_id=%s", payload.get("product_name", ""), sid)


@on(EventType.NEW_COMPETITOR)
def _handle_new_competitor(payload: dict) -> None:
    """
    رصد منافس جديد → تحديث جدول competitor_intel وإدراج تنبيه مراجعة
    إذا كان السعر مختلفاً بشكل مؤثر.
    """
    now  = datetime.now(timezone.utc).isoformat()
    pk   = payload.get("product_key", "")
    comp = payload.get("competitor", "")
    price = float(payload.get("comp_price", 0))
    url  = payload.get("comp_url", "")

    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        # [FIX P2] INSERT-only — كل منطق التحديث والتنبيه مُفوَّض لـ delta_detector
        # INSERT OR REPLACE يضمن تحديث السعر حتى لو المنتج موجود (stale snapshot)
        conn.execute("""
            INSERT OR REPLACE INTO competitor_intel
            (product_key, product_name, competitor,
             comp_price, comp_url, last_seen, price_history)
            VALUES (
                ?, ?, ?, ?, ?, ?,
                COALESCE(
                    (SELECT price_history FROM competitor_intel
                     WHERE product_key=? AND competitor=?),
                    '[]'
                )
            )
        """, (pk, payload.get("product_name", ""), comp,
              price, url, now, pk, comp))
        conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
#  دوال مساعدة
# ─────────────────────────────────────────────────────────────────────────────

def get_review_queue(status: str = "pending", limit: int = 200) -> list[dict]:
    """إرجاع قائمة المراجعة."""
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT * FROM review_queue
            WHERE status = ?
            ORDER BY priority ASC, created_at ASC
            LIMIT ?
        """, (status, limit)).fetchall()
    return [dict(r) for r in rows]


def resolve_review(review_id: int, action: str = "resolved") -> None:
    """تسوية عنصر في قائمة المراجعة."""
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        conn.execute("""
            UPDATE review_queue SET status=?, resolved_at=? WHERE id=?
        """, (action, now, review_id))
        conn.commit()


def get_open_price_alerts(limit: int = 100) -> list[dict]:
    """إرجاع التنبيهات المفتوحة."""
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT * FROM price_alerts WHERE status='open'
            ORDER BY change_pct DESC LIMIT ?
        """, (limit,)).fetchall()
    return [dict(r) for r in rows]


def acknowledge_alert(alert_id: int) -> None:
    """تأكيد الاطلاع على تنبيه السعر."""
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        conn.execute(
            "UPDATE price_alerts SET status='acknowledged', acknowledged_at=? WHERE id=?",
            (now, alert_id)
        )
        conn.commit()


def _persist_event(event_type: str, payload: dict) -> None:
    """حفظ أحداث review_required في قاعدة البيانات."""
    if event_type == EventType.REVIEW_REQUIRED:
        return   # المعالج المضمن يتولى الحفظ بالفعل
    # يمكن توسيع هذه الدالة لحفظ سجل عام لكل الأحداث مستقبلاً
