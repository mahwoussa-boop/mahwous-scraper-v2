"""
مخزن ذاكرة مشترك (thread-safe) بين خيط الكشط async وواجهة Streamlit.

- منتجات ناجحة تُدفع فوراً بعد التحليل (قبل دورة تصدير CSV الكاملة).
- معاينة صفوف التسعير المصنّفة تُحدَّث من خط التسعير قبل/مع المزامنة إلى SQLite.
- فشول حديثة مع سبب قصير للعرض التشخيصي.

ملاحظة: الكاشط يُشغَّل غالباً في **عملية منفصلة** (run_background_worker)، لذا الذاكرة هنا
لا تصل للواجهة. استخدم snapshot_products_cross_process / snapshot_failures_cross_process
للقراءة من ملفات SQLite على القرص.
"""
from __future__ import annotations

import hashlib
import os
import sqlite3
import threading
import time
from collections import deque
from typing import Any, Dict, List, Optional

import pandas as pd

from utils.scrape_live_cards import classify_pricing_row

_LOCK = threading.RLock()

# آخر منتجات مكشوطة بنجاح (عرض تدفّقي)
_MAX_PRODUCTS = 450
_recent_products: deque = deque(maxlen=_MAX_PRODUCTS)

# آخر فشول مع سبب
_MAX_FAILURES = 600
_recent_failures: deque = deque(maxlen=_MAX_FAILURES)

# معاينة بطاقات التسعير حسب الحاوية (تُملأ من pricing_pipeline)
_MAX_PER_BUCKET = 180
_pricing_preview: Dict[str, List[Dict[str, Any]]] = {
    "higher": [],
    "lower": [],
    "ok": [],
    "missing": [],
    "review": [],
}
_pricing_preview_ts: float = 0.0


def _utc_ts() -> float:
    return time.time()


def _row_key_from_pricing_row(row: pd.Series) -> str:
    sku = str(row.get("sku", "") or "").strip()
    comp_url = str(row.get("comp_url", "") or "").strip()
    return hashlib.sha256(f"{sku}|{comp_url}".encode("utf-8")).hexdigest()[:40]


def _pricing_row_to_card(row: pd.Series, bucket: str, updated_at: str) -> Dict[str, Any]:
    rk = _row_key_from_pricing_row(row)
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
    comp_url = str(row.get("comp_url", "") or "").strip()
    img_o = str(row.get("image_url", "") or "").strip()
    img_c = str(row.get("comp_image_url", "") or "").strip()
    mscore = row.get("match_score")
    try:
        mscore_f = float(mscore) if mscore is not None and str(mscore) != "nan" else None
    except Exception:
        mscore_f = None
    ai_st = str(row.get("ai_verification_state", "") or "")
    return {
        "row_key": rk,
        "bucket": bucket,
        "product_name": pname,
        "comp_name": cname,
        "price": price,
        "comp_price": cprice,
        "comp_url": comp_url,
        "image_our": img_o,
        "image_comp": img_c,
        "match_score": mscore_f,
        "ai_state": ai_st,
        "updated_at": updated_at,
    }


def replace_pricing_preview(df: pd.DataFrame) -> None:
    """يستبدل معاينة البطاقات المصنّفة (يُستدعى من pricing_pipeline بعد بناء priced_df)."""
    global _pricing_preview_ts
    if df is None or df.empty:
        with _LOCK:
            for k in _pricing_preview:
                _pricing_preview[k] = []
            _pricing_preview_ts = _utc_ts()
        return
    now_iso = str(pd.Timestamp.utcnow().isoformat())
    tmp: Dict[str, List[Dict[str, Any]]] = {k: [] for k in _pricing_preview}
    for _, row in df.iterrows():
        bucket = classify_pricing_row(row)
        if bucket not in tmp:
            continue
        card = _pricing_row_to_card(row, bucket, now_iso)
        tmp[bucket].append(card)
    with _LOCK:
        for b, lst in tmp.items():
            _pricing_preview[b] = lst[:_MAX_PER_BUCKET]
        _pricing_preview_ts = _utc_ts()


def push_scraped_product(
    row: Dict[str, Any],
    extraction_method: str = "",
) -> None:
    """دفع فوري لصف منتج منافس ناجح (قبل تصدير CSV الدفعي)."""
    if not row:
        return
    img = str(row.get("image_url", "") or row.get("comp_image_url", "") or "").strip()
    entry = {
        "ts": _utc_ts(),
        "name": str(row.get("name", "") or ""),
        "price": float(row.get("price", 0) or 0),
        "brand": str(row.get("brand", "") or ""),
        "comp_url": str(row.get("comp_url", "") or ""),
        "image_url": img,
        "sku": str(row.get("sku", "") or ""),
        "competitor": str(row.get("competitor", "") or "").strip(),
        "Extraction_Method": (extraction_method or row.get("extraction_method") or "").strip()
        or "unknown",
        "image_status": "ok" if img else "pending_search",
    }
    with _LOCK:
        _recent_products.append(entry)


def push_scrape_failure(url: str, reason: str) -> None:
    u = (url or "").strip()[:2000]
    r = (reason or "unknown").strip()[:500]
    if not u:
        return
    with _LOCK:
        _recent_failures.append({"ts": _utc_ts(), "url": u, "reason": r})


def snapshot_products(limit: int = 60) -> List[Dict[str, Any]]:
    n = max(1, int(limit))
    with _LOCK:
        return list(_recent_products)[-n:]


def snapshot_failures(limit: int = 50) -> List[Dict[str, Any]]:
    n = max(1, int(limit))
    with _LOCK:
        return list(_recent_failures)[-n:]


def _data_path(*parts: str) -> str:
    return os.path.join(os.getcwd(), *parts)


def snapshot_products_cross_process(limit: int = 60) -> List[Dict[str, Any]]:
    """
    آخر منتجات ناجحة من القرص — يعمل عندما يكون الكاشط في subprocess.
    يجمع: live_pricing_ui.scraped_competitor_live ثم scraper_state.product_state.
    """
    n = max(1, int(limit))
    out: List[Dict[str, Any]] = []
    seen_url: set = set()

    def _add_row(
        name: str,
        price: float,
        brand: str,
        comp_url: str,
        image_url: str,
        sku: str,
        ext: str,
        ts_hint: str,
    ) -> None:
        u = (comp_url or "").strip()
        if not u or u in seen_url:
            return
        seen_url.add(u)
        out.append(
            {
                "ts": time.time(),
                "name": name or "",
                "price": float(price or 0),
                "brand": brand or "",
                "comp_url": u,
                "image_url": image_url or "",
                "sku": sku or "",
                "Extraction_Method": (ext or "db").strip()[:120] or "db",
                "image_status": "ok" if (image_url or "").strip() else "pending_search",
                "updated_at": ts_hint,
            }
        )

    live_db = _data_path("data", "live_pricing_ui.db")
    if os.path.isfile(live_db):
        try:
            conn = sqlite3.connect(live_db, timeout=15)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT name, price, brand, image_url, sku, extraction_method, updated_at, comp_url
                FROM scraped_competitor_live
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (n * 2,),
            ).fetchall()
            conn.close()
            for r in rows:
                if len(out) >= n:
                    break
                _add_row(
                    str(r["name"] or ""),
                    float(r["price"] or 0),
                    str(r["brand"] or ""),
                    str(r["comp_url"] or ""),
                    str(r["image_url"] or ""),
                    str(r["sku"] or ""),
                    str(r["extraction_method"] or ""),
                    str(r["updated_at"] or ""),
                )
        except Exception:
            pass

    state_db = _data_path("data", "scraper_state.db")
    if len(out) < n and os.path.isfile(state_db):
        try:
            conn = sqlite3.connect(state_db, timeout=15)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT name, price, brand, image_url, sku, comp_url, last_seen_at
                FROM product_state
                ORDER BY last_seen_at DESC
                LIMIT ?
                """,
                (n * 2,),
            ).fetchall()
            conn.close()
            for r in rows:
                if len(out) >= n:
                    break
                _add_row(
                    str(r["name"] or ""),
                    float(r["price"] or 0),
                    str(r["brand"] or ""),
                    str(r["comp_url"] or ""),
                    str(r["image_url"] or ""),
                    str(r["sku"] or ""),
                    "product_state",
                    str(r["last_seen_at"] or ""),
                )
        except Exception:
            pass

    return out[:n]


def snapshot_failures_cross_process(limit: int = 50) -> List[Dict[str, Any]]:
    """
    آخر فشول مميّزة بـ URL (بدون تكرار نفس الرابط — أحدث خطأ فقط).
    """
    n = max(1, int(limit))
    state_db = _data_path("data", "scraper_state.db")
    if not os.path.isfile(state_db):
        return []
    try:
        conn = sqlite3.connect(state_db, timeout=15)
        rows = conn.execute(
            """
            SELECT url, last_error, updated_at
            FROM url_queue
            WHERE status = 'failed'
              AND IFNULL(TRIM(last_error), '') != ''
            ORDER BY updated_at DESC
            LIMIT 800
            """
        ).fetchall()
        conn.close()
    except Exception:
        return []

    seen: set = set()
    out: List[Dict[str, Any]] = []
    for url, err, upd in rows:
        u = (url or "").strip()
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(
            {
                "ts": time.time(),
                "url": u[:2000],
                "reason": (err or "unknown").strip()[:500],
                "updated_at": str(upd or ""),
            }
        )
        if len(out) >= n:
            break
    return out


def snapshot_products_for_ui(limit: int = 60) -> List[Dict[str, Any]]:
    """للواجهة: دمج ذاكرة العملية الحالية + القرص (الأحدث يظهر أولاً)."""
    n = max(1, int(limit))
    disk = snapshot_products_cross_process(n)
    mem = snapshot_products(n)
    if not mem:
        return disk
    if not disk:
        return mem
    seen = {str(x.get("comp_url", "")).strip() for x in disk if x.get("comp_url")}
    for m in reversed(mem):
        u = str(m.get("comp_url", "") or "").strip()
        if u and u not in seen:
            seen.add(u)
            disk.insert(0, m)
    return disk[:n]


def snapshot_failures_for_ui(limit: int = 50) -> List[Dict[str, Any]]:
    disk = snapshot_failures_cross_process(limit)
    mem = snapshot_failures(limit)
    if not mem:
        return disk
    if not disk:
        return mem
    seen = {str(x.get("url", "")).strip() for x in disk}
    for m in reversed(mem):
        u = str(m.get("url", "") or "").strip()
        if u and u not in seen:
            seen.add(u)
            disk.insert(0, m)
    return disk[: max(1, int(limit))]


def get_pricing_preview_for_bucket(bucket: str, limit: int) -> List[Dict[str, Any]]:
    n = max(1, int(limit))
    with _LOCK:
        lst = list(_pricing_preview.get(bucket, []))
    return lst[:n]


def pricing_preview_age_seconds() -> float:
    with _LOCK:
        if _pricing_preview_ts <= 0:
            return -1.0
        return max(0.0, _utc_ts() - _pricing_preview_ts)


def merge_cards_for_bucket(
    bucket: str,
    sqlite_cards: List[Dict[str, Any]],
    limit: int,
) -> List[Dict[str, Any]]:
    """
    يدمج معاينة التسعير في الذاكرة (الأحدث) مع بطاقات SQLite، بدون تكرار row_key.
    """
    n = max(1, int(limit))
    preview = get_pricing_preview_for_bucket(bucket, n)
    seen: set = set()
    out: List[Dict[str, Any]] = []
    for c in preview:
        rk = str(c.get("row_key", "") or "")
        if rk and rk in seen:
            continue
        if rk:
            seen.add(rk)
        out.append(c)
        if len(out) >= n:
            return out
    for c in sqlite_cards:
        rk = str(c.get("row_key", "") or "")
        if rk and rk in seen:
            continue
        if rk:
            seen.add(rk)
        out.append(c)
        if len(out) >= n:
            break
    return out
