"""
scrapers/async_scraper.py — محرك الكشط غير المتزامن v1.0
══════════════════════════════════════════════════════════
• يقرأ قائمة المتاجر من data/competitors_list.json
• يحدّد Sitemap لكل متجر عبر sitemap_resolve.py
• يسحب بيانات المنتج (JSON-LD أو meta tags أو بنية HTML)
• يكتب النتائج في data/competitors_latest.csv
• يحدّث data/scraper_progress.json لحظياً (للـ Dashboard)

التشغيل:
  python scrapers/async_scraper.py
  python scrapers/async_scraper.py --max-products 500 --concurrency 5
"""
import argparse
import asyncio
import csv
import json
import logging
import os
import random
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
from aiohttp import ClientTimeout

from scrapers.anti_ban import (
    get_browser_headers,
    get_rate_limiter,
    fetch_with_retry,
    try_cloudscraper,
)

# ── مسارات ────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parent.parent
_DATA_DIR = Path(os.environ.get("DATA_DIR", "")).resolve() if os.environ.get("DATA_DIR") else _ROOT / "data"
_DATA_DIR.mkdir(parents=True, exist_ok=True)

COMPETITORS_FILE = _DATA_DIR / "competitors_list.json"
OUTPUT_CSV       = _DATA_DIR / "competitors_latest.csv"
PROGRESS_FILE    = _DATA_DIR / "scraper_progress.json"
STATE_FILE       = _DATA_DIR / "scraper_state.json"
LASTMOD_FILE     = _DATA_DIR / "scraper_lastmod_cache.json"

# ── وكلاء المستخدم (User-Agent Rotation) ─────────────────────────────────
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    "Googlebot/2.1 (+http://www.google.com/bot.html)",
]

# ── إعداد logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("scraper")

# ── Regexes للاستخراج ─────────────────────────────────────────────────────
_JSON_LD_RE   = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.S | re.I,
)
_PRICE_RE     = re.compile(r'"price"\s*:\s*"?([\d.,]+)"?')
_NAME_RE      = re.compile(r'"name"\s*:\s*"([^"]{4,200})"')
_IMAGE_RE     = re.compile(r'"image"\s*:\s*(?:"([^"]+)"|(?:\["([^"]+)")')
_PRODUCT_TYPE = re.compile(r'"@type"\s*:\s*"Product"', re.I)
_OG_TITLE_RE  = re.compile(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', re.I)
_OG_PRICE_RE  = re.compile(r'<meta[^>]+property=["\']product:price:amount["\'][^>]+content=["\']([^"\']+)["\']', re.I)
_OG_IMAGE_RE  = re.compile(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', re.I)


# ══════════════════════════════════════════════════════════════════════════
#  State & Checkpointing — الاستئناف الذكي
# ══════════════════════════════════════════════════════════════════════════
def load_scraper_state() -> dict:
    if STATE_FILE.exists():
        try: return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except: return {}
    return {}

def save_scraper_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

def load_lastmod_cache() -> dict:
    if LASTMOD_FILE.exists():
        try: return json.loads(LASTMOD_FILE.read_text(encoding="utf-8"))
        except: return {}
    return {}

def save_lastmod_cache(cache: dict):
    LASTMOD_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

# ══════════════════════════════════════════════════════════════════════════
#  Progress — يُحدِّث ملف JSON الحي
# ══════════════════════════════════════════════════════════════════════════
class Progress:
    def __init__(self, stores: list[str], total_urls: int):
        self._file = PROGRESS_FILE
        self._data = {
            "running":           True,
            "started_at":        datetime.now().isoformat(),
            "updated_at":        datetime.now().isoformat(),
            "stores_total":      len(stores),
            "stores_done":       0,
            "urls_total":        total_urls,
            "urls_processed":    0,
            "rows_in_csv":       0,
            "fetch_exceptions":  0,
            "success_rate_pct":  0,
            "current_store":     "",
            "last_error":        "",
            "output_file":       str(OUTPUT_CSV),
            # تتبع تقدم المتجر الحالي
            "store_urls_total":  0,
            "store_urls_done":   0,
            "store_started_at":  "",
            # نتائج كل متجر {domain: عدد_المنتجات}
            "stores_results":    {},
        }
        self._flush()

    def update(self, **kwargs) -> None:
        self._data.update(kwargs)
        self._data["updated_at"] = datetime.now().isoformat()
        if self._data["urls_processed"] > 0:
            ok = self._data["rows_in_csv"]
            tot = self._data["urls_processed"]
            self._data["success_rate_pct"] = round(ok / tot * 100, 1)
        self._flush()

    def done(self, rows: int) -> None:
        self._data.update({
            "running":        False,
            "rows_in_csv":    rows,
            "updated_at":     datetime.now().isoformat(),
            "finished_at":    datetime.now().isoformat(),
            "current_store":  "",
        })
        self._flush()

    def error(self, msg: str) -> None:
        self._data["last_error"] = msg[:500]
        self._data["fetch_exceptions"] = self._data.get("fetch_exceptions", 0) + 1
        self._flush()

    def _flush(self) -> None:
        try:
            PROGRESS_FILE.write_text(
                json.dumps(self._data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.warning("progress flush failed: %s", exc)


# ══════════════════════════════════════════════════════════════════════════
#  استخراج بيانات المنتج من HTML
# ══════════════════════════════════════════════════════════════════════════
def _extract_from_jsonld(html: str) -> dict | None:
    """أفضل مصدر: JSON-LD schema.org/Product."""
    for block in _JSON_LD_RE.findall(html):
        if not _PRODUCT_TYPE.search(block):
            continue
        try:
            data = json.loads(block)
            # قد يكون الـ JSON-LD قائمة
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and "Product" in str(item.get("@type", "")):
                        data = item
                        break
            name  = str(data.get("name", "")).strip()
            price = 0.0
            offers = data.get("offers") or data.get("Offers") or {}
            if isinstance(offers, list):
                offers = offers[0] if offers else {}
            price_raw = str(offers.get("price", data.get("price", "0"))).replace(",", "")
            try:
                price = float(price_raw)
            except (ValueError, TypeError):
                pass
            image = ""
            img_field = data.get("image", "")
            if isinstance(img_field, list):
                image = img_field[0] if img_field else ""
            elif isinstance(img_field, str):
                image = img_field
            url = str(data.get("url", data.get("@id", ""))).strip()
            brand_field = data.get("brand") or {}
            brand = str(brand_field.get("name", "") if isinstance(brand_field, dict) else brand_field).strip()
            sku = str(data.get("sku", data.get("productID", ""))).strip()
            if name and price > 0:
                return {"name": name, "price": price, "image": image,
                        "url": url, "brand": brand, "sku": sku}
        except Exception:
            continue
    return None


def _extract_from_og(html: str, page_url: str) -> dict | None:
    """Fallback: Open Graph meta tags."""
    name_m  = _OG_TITLE_RE.search(html)
    price_m = _OG_PRICE_RE.search(html)
    image_m = _OG_IMAGE_RE.search(html)
    if not name_m or not price_m:
        return None
    try:
        price = float(price_m.group(1).replace(",", ""))
    except ValueError:
        return None
    return {
        "name":  name_m.group(1).strip(),
        "price": price,
        "image": image_m.group(1) if image_m else "",
        "url":   page_url,
        "brand": "",
        "sku":   "",
    }


def extract_product(html: str, page_url: str) -> dict | None:
    """JSON-LD أولاً، ثم OG."""
    return _extract_from_jsonld(html) or _extract_from_og(html, page_url)


# ══════════════════════════════════════════════════════════════════════════
#  جلب صفحة منتج واحدة — مع retry + anti-ban + cloudscraper fallback
# ══════════════════════════════════════════════════════════════════════════
async def fetch_product(
    session: aiohttp.ClientSession,
    url: str,
    store_domain: str,
    sem: asyncio.Semaphore,
    progress: Progress,
) -> dict | None:
    async with sem:
        resp = await fetch_with_retry(
            session, url, max_retries=3, referer=f"https://{store_domain}/"
        )

        html: str | None = None
        if resp is not None:
            try:
                html = await resp.text(errors="ignore")
            except Exception:
                html = None

        # Cloudflare fallback — مزامن في executor لكي لا يجمّد event loop
        if not html:
            loop = asyncio.get_event_loop()
            html = await loop.run_in_executor(None, try_cloudscraper, url)

        progress.update(urls_processed=progress._data["urls_processed"] + 1)

        if not html:
            progress.error(f"{url} → تعذّر الجلب بعد كل المحاولات")
            return None

        product = extract_product(html, url)
        if product:
            product["store"] = store_domain
            product["scraped_at"] = datetime.now().strftime("%Y-%m-%d")
        return product


# ══════════════════════════════════════════════════════════════════════════
#  المحرك الرئيسي
# ══════════════════════════════════════════════════════════════════════════
async def run_scraper(
    max_products_per_store: int = 0,
    concurrency: int = 8,
) -> int:
    """
    يشغّل الكشط الكامل ويُرجع عدد الصفوف المكتوبة.

    max_products_per_store=0 → كشط جميع المنتجات بدون سقف.
    """
    # ── قراءة قائمة المتاجر ────────────────────────────────────────────────
    if not COMPETITORS_FILE.exists():
        logger.error("ملف المنافسين غير موجود: %s", COMPETITORS_FILE)
        return 0

    stores: list[str] = json.loads(COMPETITORS_FILE.read_text(encoding="utf-8"))
    if not stores:
        logger.error("قائمة المتاجر فارغة")
        return 0

    logger.info("بدء الكشط — %d متجر، %d منتج/متجر، تزامن %d",
                len(stores), max_products_per_store, concurrency)

    # ── تهيئة CSV + Progress ───────────────────────────────────────────────
    csv_cols = ["store", "name", "price", "image", "url", "brand", "sku", "scraped_at"]
    csv_fh   = OUTPUT_CSV.open("w", newline="", encoding="utf-8-sig")
    writer   = csv.DictWriter(csv_fh, fieldnames=csv_cols, extrasaction="ignore")
    writer.writeheader()

    progress = Progress(stores, total_urls=len(stores) * max_products_per_store)
    rows_written = 0
    sem = asyncio.Semaphore(concurrency)

    from scrapers.sitemap_resolve import resolve_product_urls

    connector = aiohttp.TCPConnector(ssl=False, limit=concurrency + 4)
    timeout   = ClientTimeout(total=30, connect=10)

    # headers مشتركة للـ session (يُعاد تعريفها لكل طلب في fetch_with_retry)
    default_headers = get_browser_headers()

    async with aiohttp.ClientSession(connector=connector, timeout=timeout,
                                    headers=default_headers) as session:
        state = load_scraper_state()
        lastmod_cache = load_lastmod_cache()
        
        for store_url in stores:
            domain = urlparse(store_url).netloc
            
            # التحديثات الذكية (Delta Updates)
            last_scraped = lastmod_cache.get(domain, "2000-01-01")
            progress.update(
                current_store=domain,
                stores_done=progress._data["stores_done"],
                store_urls_total=0,
                store_urls_done=0,
                store_started_at=datetime.now().isoformat(),
            )
            logger.info("↳ %s", domain)

            try:
                product_urls = await resolve_product_urls(
                    store_url, session, max_products=max_products_per_store,
                    since_date=last_scraped
                )
            except Exception as exc:
                progress.error(f"resolve {domain}: {exc}")
                product_urls = []

            if not product_urls:
                logger.warning("لا روابط منتجات لـ %s", domain)
                _res = dict(progress._data.get("stores_results") or {})
                _res[domain] = 0
                progress.update(
                    stores_done=progress._data["stores_done"] + 1,
                    stores_results=_res,
                )
                continue

            _capped_urls = (
                product_urls if max_products_per_store <= 0
                else product_urls[:max_products_per_store]
            )
            # أعلم الـ Progress بإجمالي روابط المتجر الحالي
            progress.update(store_urls_total=len(_capped_urls), store_urls_done=0)

            tasks = [
                fetch_product(session, url, domain, sem, progress)
                for url in _capped_urls
            ]

            _store_rows = 0
            for _url_idx, coro in enumerate(asyncio.as_completed(tasks)):
                result = await coro
                # تحديث عداد روابط المتجر الحالي
                progress.update(store_urls_done=_url_idx + 1)
                if result:
                    writer.writerow(result)
                    rows_written += 1
                    _store_rows += 1
                    if rows_written % 50 == 0:
                        csv_fh.flush()
                        progress.update(rows_in_csv=rows_written)

            # تسجيل نتيجة المتجر المنتهي
            _res = dict(progress._data.get("stores_results") or {})
            _res[domain] = _store_rows
            progress.update(
                stores_done=progress._data["stores_done"] + 1,
                rows_in_csv=rows_written,
                stores_results=_res,
                store_urls_done=0,
                store_urls_total=0,
            )
            logger.info("  ✓ %s — %d منتج", domain, _store_rows)
            # تحديث تاريخ آخر كشط للمتجر
            lastmod_cache[domain] = datetime.now().strftime("%Y-%m-%d")
            save_lastmod_cache(lastmod_cache)

    csv_fh.close()
    progress.done(rows_written)
    logger.info("اكتمل الكشط — %d منتج في %s", rows_written, OUTPUT_CSV)
    return rows_written


# ══════════════════════════════════════════════════════════════════════════
#  نقطة الدخول عند التشغيل المباشر
# ══════════════════════════════════════════════════════════════════════════
def main() -> None:
    parser = argparse.ArgumentParser(description="Async Competitor Scraper")
    parser.add_argument("--max-products", type=int, default=0,
                        help="أقصى عدد منتجات لكل متجر (0 = جميع المنتجات بلا سقف)")
    parser.add_argument("--concurrency", type=int, default=8,
                        help="عدد الطلبات المتزامنة")
    args = parser.parse_args()

    # تأكد من أن مجلد data/ موجود
    _DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        rows = asyncio.run(
            run_scraper(
                max_products_per_store=args.max_products,
                concurrency=args.concurrency,
            )
        )
        sys.exit(0 if rows > 0 else 1)
    except KeyboardInterrupt:
        # كتابة حالة إيقاف في progress
        try:
            prog = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
            prog["running"] = False
            prog["last_error"] = "⛔ أُوقف يدوياً"
            PROGRESS_FILE.write_text(
                json.dumps(prog, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception:
            pass
        logger.info("الكشط أُوقف يدوياً")
        sys.exit(0)
    except Exception as exc:
        logger.error("خطأ فادح: %s", exc)
        try:
            err_state = {
                "running": False, "last_error": str(exc)[:500],
                "updated_at": datetime.now().isoformat(),
            }
            PROGRESS_FILE.write_text(
                json.dumps(err_state, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
