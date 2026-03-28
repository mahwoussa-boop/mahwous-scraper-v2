"""
Continuous competitor scraper + resilient state.

Features:
1) SQLite state (pending/completed/failed) with resume on restart/crash.
2) Sitemap sync every 2 hours (discover new URLs without duplicates).
3) Price-change prioritization in competitors_latest.csv (new/changed on top).
4) Auto-trigger AI pricing pipeline in background after updated batches.
"""
from __future__ import annotations

import asyncio
import ast
import hashlib
import json
import logging
import os
import random
import re
import shutil
import sqlite3
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from datetime import timedelta
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, unquote

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from config import MAIN_STORE_DOMAIN, is_main_store_domain
from utils.scrape_live_buffer import push_scrape_failure, push_scraped_product
from utils.sitemap_resolve import resolve_sitemap_url_async

logger = logging.getLogger(__name__)

DATA_DIR = "data"
SCRAPER_LAST_RUN_JSON = os.path.join(DATA_DIR, "scraper_last_run.json")
SCRAPER_PROGRESS_JSON = os.path.join(DATA_DIR, "scraper_progress.json")
STATE_DB_PATH = os.path.join(DATA_DIR, "scraper_state.db")
COMPETITOR_CSV = os.path.join(DATA_DIR, "competitors_latest.csv")
COMPETITOR_TMP_CSV = os.path.join(DATA_DIR, "competitors_temp.csv")
SCRAPER_STOP_FLAG_PATH = os.path.join(DATA_DIR, "scraper_stop.flag")
COMPETITORS_FILE = os.path.join(DATA_DIR, "competitors_list.json")

# لمنع تراكم الفشل/التكرار غير المفيد
MAX_URL_ATTEMPTS = int(os.environ.get("SCRAPER_MAX_URL_ATTEMPTS", "3"))

_USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

_ACCEPT_LANGUAGES: List[str] = [
    "ar-SA,ar;q=0.9,en-US;q=0.8,en;q=0.7",
    "ar,en-US;q=0.9,en;q=0.8",
    "en-US,en;q=0.9,ar-SA;q=0.8,ar;q=0.7",
    "ar-SA,ar;q=0.95,en-GB;q=0.85,en;q=0.8",
    "en-GB,en;q=0.9,ar;q=0.8",
]

# فلترة روابط غير المنتجات قبل الطابور
_NON_PRODUCT_PATH_RE = re.compile(
    r"/(cart|checkout|wishlist|basket|account|login|signin|signup|register|logout|"
    r"privacy|policy|policies|terms|conditions|about|contact|faq|help|support|"
    r"search|compare|orders?|thank-you|unsubscribe|subscribe|gift)",
    re.I,
)

_SAR_PRICE_AFTER = re.compile(
    r"(?:SAR|ر\.?\s*س(?:المملكة)?|ريال(?:\s*سعودي)?|SR)\s*[:\s]*"
    r"(\d{1,3}(?:[,\s]\d{3})*(?:\.\d{1,3})?)",
    re.I,
)
_SAR_PRICE_BEFORE = re.compile(
    r"(\d{1,3}(?:[,\s]\d{3})*(?:\.\d{1,3})?)\s*(?:SAR|ر\.?\s*س|ريال|SR)\b",
    re.I,
)

# تسلسل CSS: (وسم_التشخيص، محددات_الاسم، محددات_السعر)
_HYBRID_CSS_LAYERS: List[Tuple[str, List[str], List[str]]] = [
    (
        "css_salla",
        [
            "h1.product__title",
            "h1.detail__title",
            ".product-title h1",
            ".product-details h1",
            "h1",
        ],
        [
            ".product-price",
            ".detail-price",
            ".price-current",
            "[data-product-price]",
            ".price-wrapper .amount",
            "span.price",
            ".product-form__info .price",
            ".product__price",
        ],
    ),
    (
        "css_shopify",
        [
            "h1.product__title",
            "h1.product-single__title",
            "[data-product-title]",
            ".product-meta h1",
            "h1.h0",
            "h1",
        ],
        [
            ".price-item--sale",
            ".price-item--regular",
            "span.product-price",
            "[data-product-price]",
            ".price .money",
            ".price__regular .money",
            "ins .money",
            ".product__price",
        ],
    ),
    (
        "css_zid",
        [
            "h1.product-title",
            "h1.detail-title",
            ".product-details h1",
            ".product-info h1",
            "h1",
        ],
        [
            ".product-price",
            ".price-value",
            "[data-price]",
            ".current-price",
            ".product-form .price",
            ".price",
        ],
    ),
    (
        "css_schema_generic",
        [
            "[itemprop=name]",
            "h1",
        ],
        [
            "[itemprop=price]",
            "meta[itemprop=price]",
            "[data-price]",
        ],
    ),
]


@dataclass
class ScrapeAttempt:
    """نتيجة جلب رابط واحد (نجاح + صف المنتج، أو كود خطأ تشخيصي)."""

    ok: bool
    row: Optional[Dict[str, Any]] = None
    error_code: str = ""
    error_detail: str = ""
    extraction_method: str = ""

    def failure_message(self) -> str:
        if self.ok:
            return ""
        parts = [self.error_code or "fail"]
        if self.error_detail:
            parts.append(self.error_detail[:240])
        return " | ".join(parts)


FAILED_RETENTION_HOURS = int(os.environ.get("SCRAPER_FAILED_RETENTION_HOURS", "72"))
COMPLETED_RETENTION_HOURS = int(os.environ.get("SCRAPER_COMPLETED_RETENTION_HOURS", "168"))


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_scraper_last_run_meta(payload: Dict[str, Any]) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(SCRAPER_LAST_RUN_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _merge_scraper_progress(updates: Dict[str, Any]) -> None:
    prev: Dict[str, Any] = {}
    if os.path.exists(SCRAPER_PROGRESS_JSON):
        try:
            with open(SCRAPER_PROGRESS_JSON, "r", encoding="utf-8") as f:
                prev = json.load(f)
        except Exception:
            pass
    prev.update(updates)
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(SCRAPER_PROGRESS_JSON, "w", encoding="utf-8") as f:
        json.dump(prev, f, ensure_ascii=False, indent=2)


def _get_state_conn() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(STATE_DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


def _init_state_db() -> None:
    conn = _get_state_conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS url_queue (
            url TEXT PRIMARY KEY,
            sitemap_url TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            attempt_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS product_state (
            comp_url TEXT PRIMARY KEY,
            name TEXT,
            price REAL,
            brand TEXT,
            image_url TEXT,
            sku TEXT,
            competitor TEXT,
            is_new INTEGER NOT NULL DEFAULT 1,
            changed INTEGER NOT NULL DEFAULT 1,
            last_changed_at TEXT,
            last_seen_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS scraper_meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def _load_competitor_sitemaps() -> List[Dict[str, str]]:
    if not os.path.exists(COMPETITORS_FILE):
        return []
    try:
        with open(COMPETITORS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, list):
                return []
            out: List[Dict[str, str]] = []
            for item in data:
                if isinstance(item, dict):
                    domain = str(item.get("domain", "")).strip()
                    # بعض الإدخالات الفاسدة قد تخزن dict كنص/URL-encoded داخل domain.
                    domain = _normalize_competitor_domain(domain)
                    name = str(item.get("name", "")).strip() or domain
                    if domain and not is_main_store_domain(domain):
                        out.append({"name": name, "domain": domain})
                elif isinstance(item, str):
                    u = _normalize_competitor_domain(item.strip())
                    # دعم حالات legacy التي خزّنت dict كنص
                    if u.startswith("{") and "domain" in u:
                        parsed = None
                        try:
                            parsed = json.loads(u)
                        except Exception:
                            try:
                                parsed = ast.literal_eval(u)
                            except Exception:
                                parsed = None
                        if isinstance(parsed, dict):
                            d = str(parsed.get("domain", "")).strip()
                            n = str(parsed.get("name", "")).strip() or d
                            if d and not is_main_store_domain(d):
                                out.append({"name": n, "domain": d})
                            continue
                    if u and not is_main_store_domain(u):
                        out.append({"name": u, "domain": u})
            return out
    except Exception:
        return []


def _normalize_competitor_domain(raw: str) -> str:
    """تنظيف إدخال المنافس: يدعم URL-encoded dict/string legacy."""
    s = str(raw or "").strip()
    if not s:
        return ""
    try:
        dec = unquote(s).strip()
        if dec:
            s = dec
    except Exception:
        pass
    if s.startswith("{") and "domain" in s:
        parsed = None
        try:
            parsed = json.loads(s)
        except Exception:
            try:
                parsed = ast.literal_eval(s)
            except Exception:
                parsed = None
        if isinstance(parsed, dict):
            s = str(parsed.get("domain", "")).strip()
    return s


def _is_processable_product_url(url: str) -> bool:
    """يستبعد صفحات الحساب/السلة/السياسات وغيرها — يركّز الطابور على صفحات منتج محتملة."""
    if not url or not str(url).strip().lower().startswith("http"):
        return False
    try:
        p = urlparse(url.strip())
        pl = (p.path or "").lower()
    except Exception:
        return False
    if _NON_PRODUCT_PATH_RE.search(pl):
        return False
    if pl.endswith(".xml") or "sitemap" in pl:
        return False
    if any(
        x in pl
        for x in (
            "/blog/",
            "/blogs/",
            "/magazine/",
            "/news/",
            "/article/",
            "/tag/",
            "/tags/",
        )
    ):
        return False
    if "/collection" in pl and "/products/" not in pl:
        return False
    if "/category" in pl and "/product" not in pl:
        return False
    if re.search(r"/p\d+$", pl):
        return True
    if "/products/" in pl:
        return True
    if re.search(r"/product/[^/]+", pl, re.I):
        return True
    if re.search(r"/item/[^/]+", pl, re.I):
        return True
    if re.search(r"-p-\d+(\.html)?$", pl, re.I):
        return True
    return False


def _insert_discovered_urls(sitemap_url: str, urls: List[str]) -> int:
    if not urls:
        return 0
    urls = [u for u in urls if _is_processable_product_url(u)]
    if not urls:
        return 0
    now = _utc_now()
    conn = _get_state_conn()
    cur = conn.cursor()
    inserted = 0
    for u in urls:
        cur.execute(
            """
            INSERT OR IGNORE INTO url_queue
            (url, sitemap_url, status, attempt_count, last_error, created_at, updated_at, last_seen_at)
            VALUES (?, ?, 'pending', 0, NULL, ?, ?, ?)
            """,
            (u, sitemap_url, now, now, now),
        )
        inserted += cur.rowcount
        cur.execute(
            "UPDATE url_queue SET last_seen_at=?, updated_at=? WHERE url=?",
            (now, now, u),
        )
    conn.commit()
    conn.close()
    return inserted


def _load_pending_urls(limit: int) -> List[str]:
    want = max(1, int(limit))
    fetch_n = min(max(want * 8, want), 4000)
    conn = _get_state_conn()
    rows = conn.execute(
        """
        SELECT url
        FROM url_queue
        WHERE status='pending'
          AND attempt_count < ?
        ORDER BY updated_at ASC, created_at ASC
        LIMIT ?
        """,
        (int(MAX_URL_ATTEMPTS), fetch_n),
    ).fetchall()
    conn.close()
    out: List[str] = []
    for r in rows:
        u = str(r["url"])
        if _is_processable_product_url(u):
            out.append(u)
        if len(out) >= want:
            break
    return out


def _mark_url_status(url: str, status: str, error: str = "") -> None:
    now = _utc_now()
    conn = _get_state_conn()
    conn.execute(
        """
        UPDATE url_queue
        SET status=?,
            attempt_count=attempt_count+1,
            last_error=?,
            updated_at=?
        WHERE url=?
        """,
        (status, error[:500], now, url),
    )
    conn.commit()
    conn.close()


def _upsert_product_and_get_change(row: Dict[str, Any]) -> Tuple[bool, bool]:
    """Returns (is_new_or_changed, inserted_new)."""
    comp_url = str(row.get("comp_url", "")).strip()
    if not comp_url:
        return (False, False)

    price = float(row.get("price", 0) or 0)
    now = _utc_now()
    competitor = urlparse(comp_url).netloc.lower()

    conn = _get_state_conn()
    prev = conn.execute(
        """
        SELECT price FROM product_state WHERE comp_url=?
        """,
        (comp_url,),
    ).fetchone()

    if prev is None:
        conn.execute(
            """
            INSERT INTO product_state
            (comp_url, name, price, brand, image_url, sku, competitor, is_new, changed, last_changed_at, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?)
            """,
            (
                comp_url,
                str(row.get("name", "")),
                price,
                str(row.get("brand", "")),
                str(row.get("image_url", "")),
                str(row.get("sku", "")),
                competitor,
                now,
                now,
            ),
        )
        conn.commit()
        conn.close()
        return (True, True)

    prev_price = float(prev["price"] or 0)
    changed = abs(prev_price - price) > 1e-9
    conn.execute(
        """
        UPDATE product_state
        SET name=?,
            price=?,
            brand=?,
            image_url=?,
            sku=?,
            competitor=?,
            is_new=0,
            changed=?,
            last_changed_at=CASE WHEN ?=1 THEN ? ELSE last_changed_at END,
            last_seen_at=?
        WHERE comp_url=?
        """,
        (
            str(row.get("name", "")),
            price,
            str(row.get("brand", "")),
            str(row.get("image_url", "")),
            str(row.get("sku", "")),
            competitor,
            1 if changed else 0,
            1 if changed else 0,
            now,
            now,
            comp_url,
        ),
    )
    conn.commit()
    conn.close()
    return (changed, False)


def _export_competitors_csv_prioritized() -> int:
    conn = _get_state_conn()
    rows = conn.execute(
        """
        SELECT
            name,
            price,
            brand,
            image_url,
            comp_url,
            sku,
            is_new,
            changed,
            COALESCE(last_changed_at, last_seen_at) AS changed_ts,
            last_seen_at
        FROM product_state
        ORDER BY
            CASE WHEN is_new=1 OR changed=1 THEN 0 ELSE 1 END ASC,
            changed_ts DESC,
            last_seen_at DESC
        """
    ).fetchall()
    conn.close()

    if not rows:
        if os.path.exists(COMPETITOR_CSV):
            os.remove(COMPETITOR_CSV)
        return 0

    df = pd.DataFrame([dict(r) for r in rows])
    out = df.rename(
        columns={
            "name": "الاسم",
            "price": "السعر",
            "brand": "الماركة",
            "image_url": "رابط_الصورة",
            "comp_url": "رابط_المنتج",
        }
    )
    out = out[["الاسم", "السعر", "الماركة", "رابط_الصورة", "رابط_المنتج", "sku"]]
    tmp_path = os.path.join(
        DATA_DIR,
        f"competitors_temp_{os.getpid()}_{int(time.time())}.csv",
    )
    try:
        out.to_csv(tmp_path, index=False, encoding="utf-8-sig")
        # على ويندوز قد يحدث lock مؤقت على الملف الهدف (Excel/قارئ خارجي).
        # استخدام tmp فريد يقلل احتمال القفل على نفس الملف.
        for _ in range(3):
            try:
                os.replace(tmp_path, COMPETITOR_CSV)
                tmp_path = ""  # moved successfully
                break
            except PermissionError:
                time.sleep(0.4)
    except PermissionError as e:
        logger.warning("CSV export skipped بسبب قفل ملف: %s", e)
        try:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        # لا نكسر دورة الكشط؛ نُبقي الملف الحالي كما هو.
        if os.path.exists(COMPETITOR_CSV):
            try:
                return int(len(pd.read_csv(COMPETITOR_CSV)))
            except Exception:
                return len(out)
        return len(out)
    finally:
        # تنظيف tmp إن فشل النقل
        try:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
    return len(out)


def _get_queue_counters() -> Dict[str, int]:
    conn = _get_state_conn()
    rows = conn.execute(
        "SELECT status, COUNT(*) AS cnt FROM url_queue GROUP BY status"
    ).fetchall()
    conn.close()
    out = {"pending": 0, "completed": 0, "failed": 0}
    for r in rows:
        out[str(r["status"])] = int(r["cnt"])
    return out


def _progress_from_queue() -> Dict[str, Any]:
    """للواجهة: «المعالَج» = مكتمل + فاشل (وليس المكتمل فقط)."""
    q = _get_queue_counters()
    pending = int(q.get("pending", 0) or 0)
    completed = int(q.get("completed", 0) or 0)
    failed = int(q.get("failed", 0) or 0)
    total = pending + completed + failed
    handled = completed + failed
    return {
        "urls_total": total,
        "urls_processed": handled,
        "urls_completed": completed,
        "urls_failed": failed,
        "urls_pending": pending,
    }


def _cleanup_state_queues() -> None:
    """
    تنظيف دوري لتقليل تراكم `url_queue`/التكرارات غير المفيدة.
    الهدف: تشغيل مستمر بدون تضخم طابور الفشل.
    """
    try:
        cutoff_failed = _utc_now_dt() - timedelta(hours=FAILED_RETENTION_HOURS)
        cutoff_completed = _utc_now_dt() - timedelta(hours=COMPLETED_RETENTION_HOURS)
        cutoff_failed_iso = cutoff_failed.isoformat()
        cutoff_completed_iso = cutoff_completed.isoformat()

        conn = _get_state_conn()
        cur = conn.cursor()
        # حذف الفشل القديم أو الذي تعدّى الحد
        cur.execute(
            """
            DELETE FROM url_queue
            WHERE (status='failed' AND attempt_count >= ?)
               OR (status='failed' AND last_seen_at < ?)
            """,
            (MAX_URL_ATTEMPTS, cutoff_failed_iso),
        )
        # حذف المكتمل القديم (لا نريد تخزينه للأبد)
        cur.execute(
            """
            DELETE FROM url_queue
            WHERE status='completed' AND last_seen_at < ?
            """,
            (cutoff_completed_iso,),
        )
        conn.commit()
        conn.close()
    except Exception:
        # لا نريد كسر الخدمة بسبب تنظيف غير حاسم
        logger.exception("cleanup_state_queues failed")


def _utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)


async def _trigger_ai_pipeline_async(reason: str, changed_rows: int) -> None:
    """Runs matcher + Gemini pricing engine in background after updates."""
    if changed_rows <= 0:
        return
    try:
        from utils.pricing_pipeline import run_auto_pricing_pipeline_background

        await asyncio.to_thread(
            run_auto_pricing_pipeline_background,
            reason=reason,
            changed_rows=changed_rows,
        )
    except Exception as e:
        logger.error("Auto pipeline trigger failed: %s", e)


def _tag_local(tag: str) -> str:
    return tag.split("}")[-1] if "}" in tag else tag


def _stable_sku_from_url(url: str) -> str:
    """معرّف ثابت للمنافس يُطابق عمود sku في المطابقة."""
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]


def _extract_brand_from_product(data: dict) -> str:
    """brand في schema.org قد يكون نصاً أو Brand { name }."""
    b = data.get("brand")
    if b is None:
        return ""
    if isinstance(b, str):
        return b.strip()
    if isinstance(b, dict):
        n = b.get("name") or b.get("@value")
        if isinstance(n, dict):
            n = n.get("value") or n.get("text")
        return str(n or "").strip()
    if isinstance(b, list) and b:
        x = b[0]
        if isinstance(x, dict):
            return _extract_brand_from_product({"brand": x})
        return str(x).strip()
    return str(b).strip()


def _extract_image_url_from_product(data: dict) -> str:
    """صورة المنتج: نص، أو ImageObject، أو قائمة."""
    img = data.get("image")
    if img is None:
        return ""
    if isinstance(img, str):
        return img.strip()
    if isinstance(img, dict):
        u = img.get("url") or img.get("contentUrl") or img.get("@id")
        return str(u or "").strip()
    if isinstance(img, list) and img:
        first = img[0]
        if isinstance(first, str):
            return first.strip()
        if isinstance(first, dict):
            u = first.get("url") or first.get("contentUrl")
            return str(u or "").strip()
    return ""


def _filter_salla_like_product_urls(urls: List[str]) -> List[str]:
    """
    يحتفظ بصفحات منتج محتملة: سلة (…/p123)، زد/شوبيفاي (/products/)، مسارات تحتوي /product/، إلخ.
    يستبعد CDN والمدونات الواضحة.
    """
    out: List[str] = []
    skip_blog = re.compile(r"/(blog|news|article|magazine|tag|category|collection)(/|$)", re.I)
    for u in urls:
        try:
            p = urlparse(u)
        except Exception:
            continue
        host = (p.netloc or "").lower()
        if "cdn.salla.sa" in host or host.startswith("cdn."):
            continue
        path = p.path or ""
        if skip_blog.search(path):
            continue
        if re.search(r"/p\d+$", path):
            out.append(u)
            continue
        if re.search(r"/products/[^/]+/?$", path, re.I):
            out.append(u)
            continue
        if re.search(r"/product/[^/]+/?$", path, re.I):
            out.append(u)
            continue
        if re.search(r"/item/[^/]+/?$", path, re.I):
            out.append(u)
            continue
        if re.search(r"-p-\d+(\.html)?$", path, re.I):
            out.append(u)
            continue
    return list(dict.fromkeys(out))


def _parse_price_from_text(text: str) -> Optional[float]:
    if not text:
        return None
    t = re.sub(r"[^\d.,]", "", str(text).replace(",", ""))
    if not t:
        return None
    try:
        return float(t)
    except ValueError:
        return None


def _price_from_offers(offers: Any) -> Optional[float]:
    """يستخرج السعر من كتلة offers (قائمة، Offer، AggregateOffer)."""
    if offers is None:
        return None
    if isinstance(offers, list):
        if not offers:
            return None
        offers = offers[0]
    if not isinstance(offers, dict):
        return _parse_price_from_text(str(offers))
    otype = offers.get("@type", "")
    if otype == "AggregateOffer":
        p = offers.get("lowPrice") or offers.get("highPrice") or offers.get("price")
    else:
        p = offers.get("price")
    if p is None:
        return None
    if isinstance(p, (int, float)):
        return float(p)
    return _parse_price_from_text(str(p))


def _is_product_type(t: Any) -> bool:
    if isinstance(t, list):
        return any(x in ("Product", "ProductGroup") for x in t)
    return t in ("Product", "ProductGroup")


def _first_product_node(obj: Any) -> Optional[dict]:
    """أول كائن JSON-LD من نوع Product / ProductGroup (يشمل @graph وقوائم)."""
    if isinstance(obj, list):
        for x in obj:
            found = _first_product_node(x)
            if found is not None:
                return found
        return None
    if isinstance(obj, dict):
        tt = obj.get("@type")
        if _is_product_type(tt):
            return obj
        if "@graph" in obj:
            found = _first_product_node(obj["@graph"])
            if found is not None:
                return found
        for v in obj.values():
            if isinstance(v, (dict, list)):
                found = _first_product_node(v)
                if found is not None:
                    return found
    return None


class AsyncCompetitorScraper:
    """محرك هجين: JSON-LD → CSS منصات → Meta → Regex — مع تراجع تكيفي عند الحظر الناعم."""

    def __init__(self, concurrency_limit: int = 15):
        self.concurrency_limit = max(1, int(concurrency_limit))
        self.semaphore = asyncio.Semaphore(self.concurrency_limit)
        self._adaptive_extra_delay = 0.0
        self._backoff_lock = asyncio.Lock()

    def _get_headers(
        self,
        referer: Optional[str] = None,
        user_agent: Optional[str] = None,
        url: Optional[str] = None,
    ) -> Dict[str, str]:
        """رؤوس متنوّعة (لغة، مرجع، وكيل) لتقليل بصمة آلية ثابتة."""
        if referer is not None:
            ref = referer
        elif url and random.random() < 0.72:
            ref = self._referer_for_url(url)
        else:
            ref = random.choice(
                (
                    "https://www.google.com/",
                    "https://www.bing.com/",
                    "https://duckduckgo.com/",
                )
            )
        ua = (user_agent or random.choice(_USER_AGENTS)).strip()
        accept_lang = random.choice(_ACCEPT_LANGUAGES)
        return {
            "User-Agent": ua,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Accept-Language": accept_lang,
            "Accept-Encoding": "gzip, deflate",
            "Referer": ref,
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }

    def _referer_for_url(self, url: str) -> str:
        try:
            p = urlparse(url)
            if p.scheme and p.netloc:
                return f"{p.scheme}://{p.netloc}/"
        except Exception:
            pass
        return "https://www.google.com/"

    async def scan_sitemap(
        self, session: aiohttp.ClientSession, sitemap_url: str
    ) -> tuple[List[str], Dict[str, Any]]:
        """يجلب sitemap أو sitemapindex (يتفرع بشكل متكرر لكل sub-sitemap).

        يعيد (الروابط، تشخيصاً لآخر طلب مباشر على هذا الرابط أو للفهرس الفرعي).
        """
        collected: List[str] = []
        diag: Dict[str, Any] = {"http_status": None, "fetch_error": None, "parse_error": None}
        ref = self._referer_for_url(sitemap_url)
        try:
            async with session.get(
                sitemap_url,
                timeout=aiohttp.ClientTimeout(total=180),
                headers=self._get_headers(referer=ref),
            ) as resp:
                diag["http_status"] = resp.status
                if resp.status != 200:
                    logger.warning("Sitemap HTTP %s for %s", resp.status, sitemap_url)
                    return [], diag
                text = await resp.text()
        except Exception as e:
            logger.error("Sitemap fetch failed %s: %s", sitemap_url, e)
            diag["fetch_error"] = str(e)
            return [], diag

        try:
            root = ET.fromstring(text)
        except ET.ParseError as e:
            logger.error("Sitemap XML parse error %s: %s", sitemap_url, e)
            diag["parse_error"] = str(e)
            return [], diag

        root_local = _tag_local(root.tag)
        if root_local == "sitemapindex":
            child_locs: List[str] = []
            for el in root.iter():
                if _tag_local(el.tag) == "loc" and el.text:
                    u = el.text.strip()
                    if u.startswith("http"):
                        child_locs.append(u)
            for child in child_locs:
                sub, _ = await self.scan_sitemap(session, child)
                collected.extend(sub)
            return list(dict.fromkeys(collected)), diag

        if root_local == "urlset":
            for el in root.iter():
                if _tag_local(el.tag) == "loc" and el.text:
                    u = el.text.strip()
                    if u.startswith("http"):
                        collected.append(u)
            return list(dict.fromkeys(collected)), diag

        for el in root.iter():
            if _tag_local(el.tag) == "loc" and el.text:
                u = el.text.strip()
                if u.startswith("http"):
                    collected.append(u)
        return list(dict.fromkeys(collected)), diag

    def _extract_from_json(self, data: dict, url: str) -> Optional[Dict[str, Any]]:
        """يستخرج حقولاً من كائن Product / ProductGroup (Salla وغيرها)."""
        tt = data.get("@type")
        types = tt if isinstance(tt, list) else ([tt] if tt else [])
        if "ProductGroup" in types and "Product" not in types:
            hv = data.get("hasVariant") or data.get("variesBy")
            if isinstance(hv, list) and hv and isinstance(hv[0], dict):
                return self._extract_from_json(hv[0], url)

        name = data.get("name")
        if isinstance(name, dict):
            name = name.get("value") or name.get("text") or name.get("@value") or str(name)
        if name is None:
            return None
        name = str(name).strip()
        if not name:
            return None

        offers = data.get("offers")
        price_val = _price_from_offers(offers)
        if price_val is None and data.get("productGroupID"):
            pass

        sku = data.get("sku") or data.get("mpn") or data.get("productID")
        if sku is None or str(sku).strip() == "":
            sku = _stable_sku_from_url(url)
        else:
            sku = str(sku).strip()

        price_out = float(price_val) if price_val is not None else 0.0
        brand = _extract_brand_from_product(data)
        image_url = _extract_image_url_from_product(data)
        return {
            "name": name,
            "price": price_out,
            "brand": brand,
            "image_url": image_url,
            "comp_image_url": image_url,
            "comp_url": url,
            "sku": sku,
        }

    def _meta_extract_name_price_img(self, soup: BeautifulSoup) -> Tuple[str, Optional[float], str]:
        """اسم + سعر + صورة من meta / title (طبقة وسيطة)."""
        name_el = soup.find("meta", property="og:title") or soup.find(
            "meta", attrs={"name": "twitter:title"}
        )
        name = ""
        if name_el and name_el.get("content"):
            name = str(name_el["content"]).strip()
        elif soup.title and soup.title.string:
            name = str(soup.title.string).strip()

        price: Optional[float] = None
        for pel in (
            soup.find("meta", property="product:price:amount"),
            soup.find("meta", property="og:price:amount"),
            soup.find("meta", attrs={"itemprop": "price"}),
        ):
            if pel and pel.get("content"):
                price = _parse_price_from_text(str(pel["content"]))
                if price is not None:
                    break

        og_img = soup.find("meta", property="og:image")
        image_url = str(og_img["content"]).strip() if og_img and og_img.get("content") else ""
        return name, price, image_url

    def _classify_soft_block(self, html: str, status: int) -> Tuple[bool, str]:
        """كشف حظر ناعم: HTML قصير جداً أو صفحات تحقق/حظر."""
        if status != 200:
            return False, ""
        if not html:
            return True, "empty_html"
        ln = len(html)
        if ln < 2048:
            return True, f"tiny_html_{ln}b_under_2kb"
        head = html[:12000].lower()
        keys = (
            "captcha",
            "cloudflare",
            "forbidden",
            "access denied",
            "blocked",
            "robot check",
            "ddos protection",
            "attention required",
            "enable javascript",
            "حظر",
            "verify you are human",
        )
        if any(k in head for k in keys):
            return True, "block_keyword_in_body"
        return False, ""

    def _css_pick_name_price(
        self, soup: BeautifulSoup, name_sels: List[str], price_sels: List[str]
    ) -> Tuple[str, Optional[float]]:
        name = ""
        for sel in name_sels:
            try:
                el = soup.select_one(sel)
            except Exception:
                continue
            if el:
                t = el.get_text(strip=True) if hasattr(el, "get_text") else ""
                if t and 1 < len(t) < 800:
                    name = t[:500]
                    break
        price_v: Optional[float] = None
        for sel in price_sels:
            try:
                el = soup.select_one(sel)
            except Exception:
                continue
            if not el:
                continue
            raw = (
                el.get("content")
                or el.get("data-price")
                or el.get("data-product-price")
                or el.get("data-current-price")
                or el.get("data-price-amount")
                or (el.get_text(strip=True) if hasattr(el, "get_text") else "")
            )
            p = _parse_price_from_text(str(raw or ""))
            if p is not None and p > 0:
                price_v = float(p)
                break
        return name, price_v

    def _extract_css_layers(self, soup: BeautifulSoup, url: str) -> Optional[Tuple[Dict[str, Any], str]]:
        host = urlparse(url).netloc.lower()
        if "shopify" in host or "myshopify" in host:
            prio = {"css_shopify": 0, "css_salla": 1, "css_zid": 2, "css_schema_generic": 3}
        elif "zid" in host:
            prio = {"css_zid": 0, "css_salla": 1, "css_shopify": 2, "css_schema_generic": 3}
        elif "salla" in host:
            prio = {"css_salla": 0, "css_zid": 1, "css_shopify": 2, "css_schema_generic": 3}
        else:
            prio = {m: i for i, (m, _, _) in enumerate(_HYBRID_CSS_LAYERS)}
        ordered = sorted(_HYBRID_CSS_LAYERS, key=lambda x: prio.get(x[0], 99))

        for method, ns, ps in ordered:
            n, p = self._css_pick_name_price(soup, ns, ps)
            if n and p and p > 0:
                og_img = soup.find("meta", property="og:image")
                img = str(og_img["content"]).strip() if og_img and og_img.get("content") else ""
                return (
                    {
                        "name": n,
                        "price": float(p),
                        "brand": "",
                        "image_url": img,
                        "comp_image_url": img,
                        "comp_url": url,
                        "sku": _stable_sku_from_url(url),
                    },
                    method,
                )
        return None

    def _extract_regex_layer(self, html: str, soup: BeautifulSoup, url: str) -> Optional[Dict[str, Any]]:
        chunk = html[: min(len(html), 900_000)]
        found: List[float] = []
        for rx in (_SAR_PRICE_AFTER, _SAR_PRICE_BEFORE):
            for m in rx.finditer(chunk):
                v = _parse_price_from_text(m.group(1))
                if v is not None and 3.0 < v < 250_000:
                    found.append(float(v))
        if not found:
            for pat in (
                r'"price"\s*:\s*"?([\d.,]+)"?',
                r'product:price:amount"\s+content="([\d.,]+)"',
            ):
                mm = re.search(pat, chunk, re.I)
                if mm:
                    v = _parse_price_from_text(mm.group(1))
                    if v is not None and 3.0 < v < 250_000:
                        found.append(float(v))
        if not found:
            return None
        price = max(found)
        name = ""
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            name = str(og["content"]).strip()
        if not name and soup.title and soup.title.string:
            name = str(soup.title.string).strip()
        name = re.split(r"\s*[\-|–|]\s*", name, maxsplit=1)[0].strip()[:400]
        if not name or len(name) < 2:
            return None
        og_img = soup.find("meta", property="og:image")
        img = str(og_img["content"]).strip() if og_img and og_img.get("content") else ""
        return {
            "name": name,
            "price": price,
            "brand": "",
            "image_url": img,
            "comp_image_url": img,
            "comp_url": url,
            "sku": _stable_sku_from_url(url),
        }

    def _hybrid_parse_html(self, html: str, url: str) -> Tuple[Optional[Dict[str, Any]], str, str]:
        """استخراج هجين متعدد الطبقات → (صف، خطأ، طريقة)."""
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception as e:
            return None, f"parse_exc:{type(e).__name__}", ""

        r_ld = self._parse_json_ld_scripts(soup, url)
        if r_ld and r_ld.get("name"):
            try:
                p0 = float(r_ld.get("price", 0) or 0)
            except (TypeError, ValueError):
                p0 = 0.0
            if p0 > 0:
                r_ld["extraction_method"] = "json_ld"
                return r_ld, "", "json_ld"

            partial_name = str(r_ld["name"]).strip()
            partial_brand = str(r_ld.get("brand", "") or "")
            partial_img = str(r_ld.get("image_url", "") or "")
            sku_ld = str(r_ld.get("sku") or _stable_sku_from_url(url))

            css_hit = self._extract_css_layers(soup, url)
            if css_hit:
                row, method = css_hit
                row["name"] = row.get("name") or partial_name
                row["brand"] = row.get("brand") or partial_brand
                if partial_img and not (row.get("image_url") or "").strip():
                    row["image_url"] = partial_img
                    row["comp_image_url"] = partial_img
                row["sku"] = sku_ld
                row["extraction_method"] = f"json_ld+{method}"
                return row, "", row["extraction_method"]

            m_name, m_price, m_img = self._meta_extract_name_price_img(soup)
            if m_price is not None and m_price > 0:
                nm = partial_name or m_name
                if nm:
                    row = {
                        "name": nm,
                        "price": float(m_price),
                        "brand": partial_brand,
                        "image_url": (partial_img or m_img or ""),
                        "comp_image_url": (partial_img or m_img or ""),
                        "comp_url": url,
                        "sku": sku_ld,
                    }
                    row["extraction_method"] = "json_ld+meta_og"
                    return row, "", row["extraction_method"]

            rx_row = self._extract_regex_layer(html, soup, url)
            if rx_row:
                rx_row["name"] = partial_name or rx_row["name"]
                rx_row["brand"] = partial_brand or rx_row.get("brand", "")
                if partial_img and not (rx_row.get("image_url") or "").strip():
                    rx_row["image_url"] = partial_img
                    rx_row["comp_image_url"] = partial_img
                rx_row["sku"] = sku_ld
                rx_row["extraction_method"] = "json_ld+regex_sar"
                return rx_row, "", rx_row["extraction_method"]

        m_name, m_price, m_img = self._meta_extract_name_price_img(soup)
        if m_name and m_price is not None and m_price > 0:
            row = {
                "name": m_name,
                "price": float(m_price),
                "brand": "",
                "image_url": m_img,
                "comp_image_url": m_img,
                "comp_url": url,
                "sku": _stable_sku_from_url(url),
            }
            row["extraction_method"] = "meta_og"
            return row, "", "meta_og"

        css_hit = self._extract_css_layers(soup, url)
        if css_hit:
            row, method = css_hit
            row["extraction_method"] = method
            return row, "", method

        rx_row = self._extract_regex_layer(html, soup, url)
        if rx_row:
            rx_row["extraction_method"] = "regex_sar"
            return rx_row, "", "regex_sar"

        if re.search(r"(captcha|cloudflare|access\.denied|حظر|robot)", html, re.I):
            return None, "blocked_or_login_page", ""

        return None, "parse_no_layer_matched", ""

    def _parse_json_ld_scripts(self, soup: BeautifulSoup, url: str) -> Optional[Dict[str, Any]]:
        scripts = soup.find_all("script", type="application/ld+json")
        for script in scripts:
            raw = script.string or script.get_text() or ""
            raw = raw.strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            node = _first_product_node(data)
            if isinstance(node, dict):
                row = self._extract_from_json(node, url)
                if row and row.get("name"):
                    return row
        return None

    async def fetch_and_parse_url(
        self, session: aiohttp.ClientSession, url: str
    ) -> ScrapeAttempt:
        max_tries = int(os.environ.get("SCRAPER_FETCH_RETRIES", "4"))
        max_tries = max(1, min(max_tries, 7))
        min_d = float(os.environ.get("SCRAPER_MIN_DELAY", "0.12"))
        max_d = float(os.environ.get("SCRAPER_MAX_DELAY", "0.85"))
        if max_d < min_d:
            min_d, max_d = max_d, min_d

        last_code = "unknown"
        last_detail = ""
        saw_soft_block = False

        for attempt in range(max_tries):
            html_content = ""
            ua = _USER_AGENTS[(attempt + random.randint(0, len(_USER_AGENTS) - 1)) % len(_USER_AGENTS)]
            async with self._backoff_lock:
                extra_backoff = self._adaptive_extra_delay
            async with self.semaphore:
                delay = random.uniform(min_d, max_d) + extra_backoff
                await asyncio.sleep(delay)
                ref = self._referer_for_url(url)
                try:
                    async with session.get(
                        url,
                        headers=self._get_headers(referer=ref, user_agent=ua, url=url),
                        timeout=aiohttp.ClientTimeout(total=55),
                        allow_redirects=True,
                    ) as response:
                        st = response.status
                        ctype = (response.headers.get("Content-Type") or "").lower()
                        if "xml" in ctype and url.lower().endswith(".xml"):
                            return ScrapeAttempt(
                                False,
                                error_code="wrong_content_xml",
                                error_detail="url_points_to_xml",
                            )
                        if st in (403, 429, 502, 503):
                            last_code = f"HTTP_{st}"
                            last_detail = (response.reason or "retryable")[:120]
                            async with self._backoff_lock:
                                self._adaptive_extra_delay = min(
                                    self._adaptive_extra_delay + 0.65, 16.0
                                )
                            await asyncio.sleep(1.1 + 0.7 * attempt)
                            continue
                        if st == 404:
                            return ScrapeAttempt(
                                False,
                                error_code="HTTP_404",
                                error_detail="الصفحة غير موجودة أو أُزيلت",
                            )
                        if st != 200:
                            return ScrapeAttempt(
                                False,
                                error_code=f"HTTP_{st}",
                                error_detail=(response.reason or "")[:200],
                            )
                        html_content = await response.text(errors="replace")
                except asyncio.TimeoutError:
                    last_code = "fetch_timeout"
                    last_detail = f"محاولة {attempt + 1}/{max_tries}"
                    async with self._backoff_lock:
                        self._adaptive_extra_delay = min(self._adaptive_extra_delay + 0.35, 14.0)
                    continue
                except aiohttp.ClientError as e:
                    last_code = f"fetch_{type(e).__name__}"
                    last_detail = str(e)[:220]
                    continue
                except Exception as e:
                    logger.error("Error scraping %s: %s", url, e)
                    last_code = f"fetch_{type(e).__name__}"
                    last_detail = str(e)[:220]
                    continue

            if not html_content or len(html_content.strip()) < 60:
                last_code = "empty_or_tiny_html"
                last_detail = f"طول={len(html_content or '')}"
                continue

            soft, sreason = self._classify_soft_block(html_content, 200)
            if soft:
                saw_soft_block = True
                last_code = "soft_block"
                last_detail = sreason
                async with self._backoff_lock:
                    self._adaptive_extra_delay = min(self._adaptive_extra_delay + 0.85, 18.0)
                await asyncio.sleep(1.4 + 0.85 * attempt)
                continue

            row, perr, method = self._hybrid_parse_html(html_content, url)
            if row is not None:
                async with self._backoff_lock:
                    self._adaptive_extra_delay = max(
                        0.0, self._adaptive_extra_delay * 0.86 - 0.06
                    )
                ext = str(row.get("extraction_method") or method or "")
                if saw_soft_block and (
                    ext.startswith("css_")
                    or "regex" in ext
                    or ext.startswith("json_ld+")
                    or ext == "meta_og"
                ):
                    try:
                        from utils.live_price_store import append_activity_log

                        host = urlparse(url).netloc[:100]
                        append_activity_log(
                            f"✅ تعافي استخراج بعد حظر/رد ضعيف: {host} — طبقة {ext}"
                        )
                    except Exception:
                        pass
                return ScrapeAttempt(True, row=row, extraction_method=ext)

            last_code = perr or "parse_no_row"
            last_detail = f"try {attempt + 1}/{max_tries}"
            await asyncio.sleep(0.2 + 0.18 * attempt)

        return ScrapeAttempt(False, error_code=last_code, error_detail=last_detail)


async def _sync_sitemaps_once(
    session: aiohttp.ClientSession, scraper: AsyncCompetitorScraper
) -> List[Dict[str, Any]]:
    competitors = _load_competitor_sitemaps()
    diagnostics: List[Dict[str, Any]] = []
    if not competitors:
        diagnostics.append(
            {
                "competitor": "guard",
                "domain": MAIN_STORE_DOMAIN,
                "sitemap": None,
                "urls_found": 0,
                "urls_product_pages": 0,
                "new_pending_added": 0,
                "http_status": None,
                "fetch_error": "no_competitors_or_main_store_filtered",
                "parse_error": None,
            }
        )
    for c in competitors:
        comp_name = c.get("name", "")
        domain = c.get("domain", "")
        sitemap = await resolve_sitemap_url_async(domain)
        if not sitemap:
            diagnostics.append(
                {
                    "competitor": comp_name,
                    "domain": domain,
                    "sitemap": None,
                    "urls_found": 0,
                    "urls_product_pages": 0,
                    "new_pending_added": 0,
                    "http_status": None,
                    "fetch_error": "sitemap_not_found",
                    "parse_error": None,
                }
            )
            continue
        urls, diag = await scraper.scan_sitemap(session, sitemap)
        raw_len = len(urls)
        urls = _filter_salla_like_product_urls(urls)
        inserted = _insert_discovered_urls(sitemap, urls)
        diagnostics.append(
            {
                "competitor": comp_name,
                "domain": domain,
                "sitemap": sitemap,
                "urls_found": raw_len,
                "urls_product_pages": len(urls),
                "new_pending_added": inserted,
                "http_status": diag.get("http_status"),
                "fetch_error": diag.get("fetch_error"),
                "parse_error": diag.get("parse_error"),
            }
        )
    return diagnostics


async def _process_pending_batch(
    session: aiohttp.ClientSession,
    scraper: AsyncCompetitorScraper,
    batch_size: int,
) -> Dict[str, int]:
    pending_urls = _load_pending_urls(batch_size)
    if not pending_urls:
        return {"queued": 0, "processed": 0, "updated_rows": 0, "failed": 0, "null": 0}

    tasks = [scraper.fetch_and_parse_url(session, u) for u in pending_urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    updated_rows = 0
    failed = 0
    parse_null = 0
    fail_by_code: Dict[str, int] = {}
    verbose_fail_log = os.environ.get("SCRAPER_LOG_EACH_FAILURE", "").strip() == "1"

    def _bump_code(code: str) -> None:
        fail_by_code[code] = fail_by_code.get(code, 0) + 1

    for url, res in zip(pending_urls, results):
        if isinstance(res, Exception):
            err = f"{type(res).__name__}: {str(res)[:220]}"
            push_scrape_failure(url, err)
            _mark_url_status(url, "failed", err[:500])
            failed += 1
            _bump_code(type(res).__name__)
            if verbose_fail_log:
                try:
                    from utils.live_price_store import append_activity_log

                    append_activity_log(f"فشل كشط | {err[:160]} | {url[:100]}")
                except Exception:
                    pass
            continue
        if not isinstance(res, ScrapeAttempt):
            msg = f"bad_result:{type(res).__name__}"
            push_scrape_failure(url, msg)
            _mark_url_status(url, "failed", msg)
            parse_null += 1
            _bump_code("bad_result")
            continue
        att: ScrapeAttempt = res
        if att.ok and att.row:
            row_db = dict(att.row)
            ext_method = str(row_db.pop("extraction_method", "") or att.extraction_method or "")
            # ذاكرة حية فورية — قبل SQLite/CSV الدفعي (مع طريقة الاستخراج للتشخيص)
            push_scraped_product(row_db, extraction_method=ext_method)
            changed, _is_new = _upsert_product_and_get_change(row_db)
            _mark_url_status(url, "completed", "")
            if changed:
                updated_rows += 1
            continue

        fm = att.failure_message() or "unknown"
        push_scrape_failure(url, fm)
        _mark_url_status(url, "failed", fm[:500])
        parse_null += 1
        _bump_code(att.error_code or "fail")
        if verbose_fail_log:
            try:
                from utils.live_price_store import append_activity_log

                append_activity_log(f"فشل كشط | {fm[:160]} | {url[:100]}")
            except Exception:
                pass

    if fail_by_code and not verbose_fail_log and (failed + parse_null) > 0:
        try:
            from utils.live_price_store import append_activity_log

            top = sorted(fail_by_code.items(), key=lambda x: -x[1])[:10]
            summary = ", ".join(f"{k}={v}" for k, v in top)
            append_activity_log(
                f"📛 دفعة كشط: أخطاء {failed + parse_null} — {summary}"
            )
        except Exception:
            pass

    rows_in_csv = _export_competitors_csv_prioritized()
    _merge_scraper_progress(
        {"running": True, "rows_in_csv": rows_in_csv, **_progress_from_queue()}
    )
    _cleanup_state_queues()
    if updated_rows > 0:
        await _trigger_ai_pipeline_async("batch_update", updated_rows)

    return {
        "queued": len(pending_urls),
        "processed": len(pending_urls),
        "updated_rows": updated_rows,
        "failed": failed,
        "null": parse_null,
    }


async def run_scraper_engine() -> None:
    """Single full run:
    - sync sitemap URLs to SQLite queue
    - process all pending URLs (resume-safe)
    - export prioritized CSV
    - auto-trigger pricing pipeline for updated/new rows
    """
    _init_state_db()
    started_t = time.perf_counter()
    started_at = _utc_now()

    _merge_scraper_progress(
        {
            "running": True,
            "started_at": started_at,
            "finished_at": None,
            "last_error": None,
            "rows_in_csv": 0,
            "current_sitemap": None,
            "mode": "single_run",
            **_progress_from_queue(),
        }
    )

    scraper = AsyncCompetitorScraper(concurrency_limit=15)
    diagnostics: List[Dict[str, Any]] = []
    counters = {
        "fetch_exceptions": 0,
        "parse_null": 0,
        "updated_rows": 0,
        "processed_batches": 0,
    }

    try:
        async with aiohttp.ClientSession() as session:
            diagnostics = await _sync_sitemaps_once(session, scraper)
            _merge_scraper_progress({"running": True, **_progress_from_queue()})

            batch_size = int(os.environ.get("SCRAPER_PENDING_BATCH_SIZE", "200"))
            batch_size = max(20, min(batch_size, 1000))

            while True:
                out = await _process_pending_batch(session, scraper, batch_size=batch_size)
                if out["processed"] == 0:
                    break
                counters["processed_batches"] += 1
                counters["fetch_exceptions"] += out["failed"]
                counters["parse_null"] += out["null"]
                counters["updated_rows"] += out["updated_rows"]

    except Exception as e:
        logger.exception("run_scraper_engine failed: %s", e)
        _merge_scraper_progress({"last_error": str(e)})
        raise
    finally:
        q = _get_queue_counters()
        rows_in_csv = _export_competitors_csv_prioritized()
        finished_at = _utc_now()
        duration = round(time.perf_counter() - started_t, 2)
        status = "ok" if rows_in_csv > 0 else "empty"
        _write_scraper_last_run_meta(
            {
                "status": status,
                "finished_at": finished_at,
                "duration_seconds": duration,
                "sitemaps_count": len(_load_competitor_sitemaps()),
                "urls_queued": q.get("pending", 0) + q.get("completed", 0) + q.get("failed", 0),
                "rows_extracted_before_dedupe": rows_in_csv,
                "rows_written_csv": rows_in_csv,
                "fetch_exceptions": counters["fetch_exceptions"],
                "parse_null": counters["parse_null"],
                "success_rate_pct": (
                    round((q.get("completed", 0) / max(1, q.get("completed", 0) + q.get("failed", 0))) * 100, 2)
                ),
                "sitemap_diagnostics": diagnostics,
            }
        )
        _merge_scraper_progress(
            {
                "running": False,
                "finished_at": finished_at,
                "rows_in_csv": rows_in_csv,
                **_progress_from_queue(),
            }
        )


async def run_continuous_scraper_service() -> None:
    """Continuous, fault-tolerant scraper:
    - sitemap sync every 2 hours
    - keeps processing pending queue forever
    """
    _init_state_db()
    scraper = AsyncCompetitorScraper(concurrency_limit=15)
    sync_every_seconds = 2 * 60 * 60
    poll_seconds = int(os.environ.get("SCRAPER_IDLE_POLL_SECONDS", "20"))
    batch_size = int(os.environ.get("SCRAPER_PENDING_BATCH_SIZE", "200"))
    batch_size = max(20, min(batch_size, 1000))
    next_sync_at = 0.0

    logger.info("Continuous scraper service started.")
    _merge_scraper_progress({"running": True, "mode": "continuous"})

    async with aiohttp.ClientSession() as session:
        while True:
            if os.path.exists(SCRAPER_STOP_FLAG_PATH):
                _merge_scraper_progress(
                    {
                        "running": False,
                        "mode": "stopped_by_flag",
                        "last_error": "stopped_by_user_flag",
                    }
                )
                break
            now = time.time()
            if now >= next_sync_at:
                try:
                    _merge_scraper_progress({"phase": "sync", "last_sync_started_at": _utc_now()})
                    diagnostics = await _sync_sitemaps_once(session, scraper)
                    q = _get_queue_counters()
                    completed = q.get("completed", 0)
                    failed = q.get("failed", 0)
                    sr = (completed / max(1, completed + failed)) * 100.0
                    _write_scraper_last_run_meta(
                        {
                            "status": "sync_only",
                            "finished_at": _utc_now(),
                            "duration_seconds": 0,
                            "sitemaps_count": len(_load_competitor_sitemaps()),
                            "urls_queued": _get_queue_counters().get("pending", 0),
                            "rows_extracted_before_dedupe": 0,
                            "rows_written_csv": _export_competitors_csv_prioritized(),
                            "fetch_exceptions": 0,
                            "parse_null": 0,
                            "success_rate_pct": round(sr, 2),
                            "sitemap_diagnostics": diagnostics,
                        }
                    )
                    _merge_scraper_progress({"phase": "process", **_progress_from_queue()})
                except Exception as e:
                    logger.error("Periodic sitemap sync failed: %s", e)
                    _merge_scraper_progress({"last_error": str(e)})
                next_sync_at = now + sync_every_seconds

            try:
                out = await _process_pending_batch(session, scraper, batch_size=batch_size)
                if out["processed"] == 0:
                    await asyncio.sleep(max(5, poll_seconds))
                else:
                    _merge_scraper_progress(
                        {"phase": "process", "running": True, **_progress_from_queue()}
                    )
            except Exception as e:
                logger.exception("Batch processing failed, will continue: %s", e)
                _merge_scraper_progress({"last_error": str(e)})
                await asyncio.sleep(10)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # SCRAPER_CONTINUOUS=1 => service mode (sync each 2h + process pending forever)
    if os.environ.get("SCRAPER_CONTINUOUS", "0").strip() == "1":
        asyncio.run(run_continuous_scraper_service())
    else:
        asyncio.run(run_scraper_engine())


if __name__ == "__main__":
    main()
