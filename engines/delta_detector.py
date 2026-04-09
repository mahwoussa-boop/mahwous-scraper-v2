"""
engines/delta_detector.py
==========================
كاشف التغييرات الحية (Delta Detector) v1.0
=============================================
يُقارن أسعار المنافسين الحالية مع آخر قيمة مُسجَّلة في competitor_intel
ويُطلق أحداث price_change / new_competitor عبر event_bus.

يُستدعى من:
  - scheduler.py بعد كل دورة كشط
  - app.py عند رفع ملف منافس جديد

الاستخدام:
    from engines.delta_detector import detect_deltas
    report = detect_deltas(new_data_df, competitor_name="نايس ون")
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from config import PRICE_ALERT_THRESHOLD_ABS, PRICE_ALERT_THRESHOLD_PCT
from utils.db_manager import DB_PATH
from utils.event_bus import EventType, emit

_logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
#  بنية تقرير التغييرات
# ─────────────────────────────────────────────────────────────────────────────
def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_current_intel(competitor: str) -> dict[str, dict]:
    """تحميل بيانات المنافس الحالية من قاعدة البيانات."""
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT product_key, comp_price, comp_url, availability FROM competitor_intel WHERE competitor = ?",
            (competitor,)
        ).fetchall()
    return {r["product_key"]: dict(r) for r in rows}


# ─────────────────────────────────────────────────────────────────────────────
#  الدالة الرئيسية
# ─────────────────────────────────────────────────────────────────────────────
def detect_deltas(
    new_data:    pd.DataFrame,
    competitor:  str,
    *,
    name_col:    str = "منتج_المنافس",
    price_col:   str = "سعر_المنافس",
    url_col:     str = "رابط_المنافس",
    product_key_col: str = "product_key",
) -> dict[str, Any]:
    """
    اكتشاف التغييرات بين البيانات الجديدة وما هو مُخزَّن في competitor_intel.

    Parameters
    ----------
    new_data    : pd.DataFrame — البيانات الجديدة من الكشط أو الملف
    competitor  : str          — اسم المنافس
    name_col    : str          — عمود اسم المنتج
    price_col   : str          — عمود السعر
    url_col     : str          — عمود الرابط

    Returns
    -------
    dict مع:
      new_products   : int — منتجات جديدة لم تكن موجودة
      price_changes  : int — تغييرات سعر مؤثرة
      price_ups      : int — ارتفاعات سعر
      price_downs    : int — انخفاضات سعر
      details        : list[dict] — تفاصيل كل تغيير
    """
    if new_data is None or new_data.empty:
        return {"new_products": 0, "price_changes": 0, "price_ups": 0,
                "price_downs": 0, "details": []}

    current_intel = _load_current_intel(competitor)
    details: list[dict] = []
    new_count = price_changes = price_ups = price_downs = 0

    for _, row in new_data.iterrows():
        pname = str(row.get(name_col, "") or "").strip()
        if not pname:
            continue

        new_price = _safe_float(row.get(price_col, 0))
        url       = str(row.get(url_col, "") or "").strip()

        # توليد product_key
        if product_key_col in row.index and row[product_key_col]:
            pkey = str(row[product_key_col]).strip()
        else:
            brand = str(row.get("الماركة", "") or "").strip()
            pkey  = f"comp_{competitor}_{pname}_{brand}"[:120]

        payload_base = {
            "product_key":  pkey,
            "product_name": pname,
            "competitor":   competitor,
            "comp_url":     url,
            "comp_price":   new_price,
        }

        if pkey not in current_intel:
            # منتج جديد لم يُرصَد من قبل
            new_count += 1
            emit(EventType.NEW_COMPETITOR, payload_base)
            details.append({
                "type": "new_product", "product": pname,
                "old_price": None, "new_price": new_price,
            })
        else:
            # منتج موجود — تحقق من تغيير السعر
            old_price = float(current_intel[pkey].get("comp_price") or 0)
            if old_price > 0 and new_price > 0:
                change_abs = abs(new_price - old_price)
                change_pct = change_abs / old_price * 100

                if (change_pct >= PRICE_ALERT_THRESHOLD_PCT or
                        change_abs >= PRICE_ALERT_THRESHOLD_ABS):
                    price_changes += 1
                    direction = "up" if new_price > old_price else "down"
                    if direction == "up":
                        price_ups += 1
                    else:
                        price_downs += 1

                    emit(EventType.PRICE_CHANGE, {
                        **payload_base,
                        "old_price":  old_price,
                        "new_price":  new_price,
                        "change_pct": round(change_pct, 2),
                        "direction":  direction,
                    })
                    details.append({
                        "type":      "price_change",
                        "product":   pname,
                        "old_price": old_price,
                        "new_price": new_price,
                        "change_pct": round(change_pct, 2),
                        "direction": direction,
                    })
                    _logger.info(
                        "DELTA [%s] %s: %.0f → %.0f (%.1f%% %s)",
                        competitor, pname[:50], old_price, new_price,
                        change_pct, direction
                    )

            # تحديث competitor_intel بالقيم الجديدة
            _update_intel(pkey, pname, competitor, new_price, url,
                          old_price=old_price)

    report = {
        "new_products":  new_count,
        "price_changes": price_changes,
        "price_ups":     price_ups,
        "price_downs":   price_downs,
        "details":       details,
        "checked_at":    _now(),
        "competitor":    competitor,
        "total_checked": len(new_data),
    }
    _logger.info(
        "DELTA REPORT [%s]: new=%d, price_changes=%d (↑%d ↓%d)",
        competitor, new_count, price_changes, price_ups, price_downs
    )
    return report


def detect_from_session_results(
    results: dict[str, pd.DataFrame],
    competitor_col: str = "المنافس",
) -> dict[str, dict]:
    """
    تطبيق Delta Detector على نتائج جلسة التحليل الكاملة (st.session_state.results).

    Parameters
    ----------
    results : dict — {'priced': df, 'missing': df, ...}

    Returns
    -------
    dict — {competitor_name: report}
    """
    all_reports: dict[str, dict] = {}
    all_dfs = []

    for key in ("priced", "missing", "excluded"):
        df = results.get(key)
        if df is not None and not df.empty and competitor_col in df.columns:
            all_dfs.append(df)

    if not all_dfs:
        return {}

    combined = pd.concat(all_dfs, ignore_index=True)

    for comp, group in combined.groupby(competitor_col):
        report = detect_deltas(group, str(comp))
        all_reports[str(comp)] = report

    return all_reports


# ─────────────────────────────────────────────────────────────────────────────
#  دوال مساعدة
# ─────────────────────────────────────────────────────────────────────────────
def _safe_float(val) -> float:
    try:
        return float(str(val).replace(",", "").strip() or 0)
    except (ValueError, TypeError):
        return 0.0


def _update_intel(product_key: str, product_name: str, competitor: str,
                  new_price: float, url: str, old_price: float = 0) -> None:
    """تحديث سجل competitor_intel في قاعدة البيانات."""
    import json as _json

    now = _now()
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        existing = conn.execute(
            "SELECT price_history FROM competitor_intel WHERE product_key=? AND competitor=?",
            (product_key, competitor)
        ).fetchone()

        if existing:
            history = _json.loads(existing[0] or "[]")
            if old_price > 0:
                history.append({"price": old_price, "at": now})
            history = history[-50:]   # آخر 50 تسجيل فقط

            conn.execute("""
                UPDATE competitor_intel
                SET comp_price=?, comp_url=?, last_seen=?, price_history=?
                WHERE product_key=? AND competitor=?
            """, (new_price, url, now, _json.dumps(history), product_key, competitor))
        else:
            conn.execute("""
                INSERT OR IGNORE INTO competitor_intel
                (product_key, product_name, competitor, comp_price, comp_url, last_seen, price_history)
                VALUES (?, ?, ?, ?, ?, ?, '[]')
            """, (product_key, product_name, competitor, new_price, url, now))

        conn.commit()


def get_competitor_price_history(product_key: str) -> dict[str, list]:
    """إرجاع تاريخ الأسعار لكل منافس لمنتج معين."""
    import json as _json

    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT competitor, comp_price, price_history FROM competitor_intel WHERE product_key=?",
            (product_key,)
        ).fetchall()

    result = {}
    for row in rows:
        history = _json.loads(row["price_history"] or "[]")
        history.append({"price": row["comp_price"], "at": _now()})  # السعر الحالي
        result[row["competitor"]] = history
    return result
