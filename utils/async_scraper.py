"""
Async competitor sitemap scraper — يقرأ روابط Sitemap من data/competitors_list.json
ويُخرج data/competitors_latest.csv

استخراج JSON-LD أولاً (Salla / Zid) ثم وسوم meta — أقل اعتماداً على CSS.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import shutil
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SCRAPER_LAST_RUN_JSON = os.path.join("data", "scraper_last_run.json")
SCRAPER_PROGRESS_JSON = os.path.join("data", "scraper_progress.json")


def _write_scraper_last_run_meta(payload: Dict[str, Any]) -> None:
    os.makedirs("data", exist_ok=True)
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
    os.makedirs("data", exist_ok=True)
    with open(SCRAPER_PROGRESS_JSON, "w", encoding="utf-8") as f:
        json.dump(prev, f, ensure_ascii=False, indent=2)


def _save_competitor_csv_rows(rows: List[Dict[str, Any]]) -> int:
    """يكتب competitors_latest.csv من قائمة صفوف. يعيد عدد الصفوف بعد إزالة التكرار."""
    if not rows:
        return 0
    _col_order = ["name", "price", "brand", "image_url", "comp_url", "sku"]
    df = pd.DataFrame(rows).drop_duplicates(subset=["comp_url"])
    for c in _col_order:
        if c not in df.columns:
            df[c] = ""
    df = df[_col_order]
    temp_file = "data/competitors_temp.csv"
    final_file = "data/competitors_latest.csv"
    df_ar = df.rename(
        columns={
            "name": "الاسم",
            "price": "السعر",
            "brand": "الماركة",
            "image_url": "رابط_الصورة",
            "comp_url": "رابط_المنتج",
            "sku": "sku",
        }
    )
    df_ar.to_csv(temp_file, index=False, encoding="utf-8-sig")
    shutil.move(temp_file, final_file)
    return len(df)


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
    """يحتفظ بصفحات منتج سلة/زد النموذجية (.../اسم-المنتج/p123456789) ويستبعد المدونة والأقسام وروابط CDN."""
    out: List[str] = []
    for u in urls:
        try:
            p = urlparse(u)
        except Exception:
            continue
        host = (p.netloc or "").lower()
        if "cdn.salla.sa" in host or host.startswith("cdn."):
            continue
        path = p.path or ""
        if re.search(r"/p\d+$", path):
            out.append(u)
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
    """جلب صفحات المنتجات — JSON-LD أولاً، ثم meta، مع حد تزامن وتأخير مهذب."""

    def __init__(self, concurrency_limit: int = 15):
        self.concurrency_limit = max(1, int(concurrency_limit))
        self.semaphore = asyncio.Semaphore(self.concurrency_limit)

    def _get_headers(self, referer: Optional[str] = None) -> Dict[str, str]:
        """رؤوس واقعية لتقليل الحظر (Referer + Accept-Encoding + …)."""
        ref = referer or "https://www.google.com/"
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Accept-Language": "ar-SA,ar;q=0.9,en-US;q=0.8,en;q=0.7",
            # بدون br: aiohttp يحتاج حزمة brotli لفك br؛ gzip/deflate كافٍ لمعظم الخوادم
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
            "comp_url": url,
            "sku": sku,
        }

    def _extract_meta_fallback(self, soup: BeautifulSoup, url: str) -> Optional[Dict[str, Any]]:
        """وسوم meta ثابتة نسبياً (og / product)."""
        name_el = soup.find("meta", property="og:title") or soup.find(
            "meta", attrs={"name": "twitter:title"}
        )
        price_el = (
            soup.find("meta", property="product:price:amount")
            or soup.find("meta", property="og:price:amount")
            or soup.find("meta", attrs={"itemprop": "price"})
        )
        if not name_el or not name_el.get("content"):
            if soup.title and soup.title.string:
                name = str(soup.title.string).strip()
            else:
                return None
        else:
            name = str(name_el["content"]).strip()

        price = None
        if price_el and price_el.get("content"):
            price = _parse_price_from_text(str(price_el["content"]))
        if price is None:
            return None

        og_img = soup.find("meta", property="og:image")
        image_url = str(og_img["content"]).strip() if og_img and og_img.get("content") else ""

        return {
            "name": name,
            "price": float(price),
            "brand": "",
            "image_url": image_url,
            "comp_url": url,
            "sku": _stable_sku_from_url(url),
        }

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
    ) -> Optional[Dict[str, Any]]:
        async with self.semaphore:
            await asyncio.sleep(random.uniform(1.0, 3.0))
            ref = self._referer_for_url(url)
            try:
                async with session.get(
                    url,
                    headers=self._get_headers(referer=ref),
                    timeout=aiohttp.ClientTimeout(total=45),
                ) as response:
                    if response.status != 200:
                        return None
                    ctype = (response.headers.get("Content-Type") or "").lower()
                    if "xml" in ctype and url.lower().endswith(".xml"):
                        return None
                    html_content = await response.text(errors="replace")
            except Exception as e:
                logger.error("Error scraping %s: %s", url, e)
                return None

        try:
            soup = BeautifulSoup(html_content, "html.parser")

            row = self._parse_json_ld_scripts(soup, url)
            if row is not None:
                return row

            row = self._extract_meta_fallback(soup, url)
            if row is not None:
                return row

            # Fallback خفيف: أنماط سعر في HTML الخام
            for pat in (
                r'"price"\s*:\s*"?([\d.,]+)"?',
                r"product:price:amount\"\s+content=\"([\d.,]+)\"",
            ):
                mm = re.search(pat, html_content, re.I)
                if mm:
                    p = _parse_price_from_text(mm.group(1))
                    if p is not None:
                        tit = soup.find("meta", property="og:title")
                        nm = (
                            str(tit["content"]).strip()
                            if tit and tit.get("content")
                            else (str(soup.title.string).strip() if soup.title and soup.title.string else "")
                        )
                        if nm:
                            og_img = soup.find("meta", property="og:image")
                            image_url = (
                                str(og_img["content"]).strip()
                                if og_img and og_img.get("content")
                                else ""
                            )
                            return {
                                "name": nm,
                                "price": float(p),
                                "brand": "",
                                "image_url": image_url,
                                "comp_url": url,
                                "sku": _stable_sku_from_url(url),
                            }
            return None
        except Exception as e:
            logger.error("Error parsing %s: %s", url, e)
            return None


async def run_scraper_engine() -> None:
    """المحرك الرئيسي الذي يشغل العملية بالكامل ويقرأ من JSON.
    يحدّث `competitors_latest.csv` و `scraper_progress.json` بعد كل دفعة جلب."""
    logger.info("Starting High-Speed Async Scraper Engine...")
    t0 = time.perf_counter()
    finished_at = datetime.now(timezone.utc).isoformat()

    competitors_file = "data/competitors_list.json"
    competitor_sitemaps: List[str] = []

    if os.path.exists(competitors_file):
        try:
            with open(competitors_file, "r", encoding="utf-8") as f:
                competitor_sitemaps = json.load(f)
        except Exception as e:
            logger.error("Failed to load competitors JSON: %s", e)

    if not competitor_sitemaps:
        logger.info("No sitemaps found in config. Please add them via the UI. Exiting.")
        _merge_scraper_progress(
            {
                "running": False,
                "finished_at": finished_at,
                "last_error": None,
            }
        )
        _write_scraper_last_run_meta(
            {
                "status": "no_sitemaps",
                "finished_at": finished_at,
                "duration_seconds": round(time.perf_counter() - t0, 2),
                "sitemaps_count": 0,
                "urls_queued": 0,
                "rows_extracted_before_dedupe": 0,
                "rows_written_csv": 0,
                "fetch_exceptions": 0,
                "parse_null": 0,
                "success_rate_pct": 0.0,
                "sitemap_diagnostics": [],
            }
        )
        return

    scraper = AsyncCompetitorScraper(concurrency_limit=15)
    all_results: List[Dict[str, Any]] = []
    urls_queued = 0
    urls_processed_total = 0
    fetch_exceptions = 0
    parse_null = 0
    sitemap_diagnostics: List[Dict[str, Any]] = []
    started = datetime.now(timezone.utc).isoformat()
    _merge_scraper_progress(
        {
            "running": True,
            "started_at": started,
            "finished_at": None,
            "urls_total": 0,
            "urls_processed": 0,
            "rows_in_csv": 0,
            "current_sitemap": None,
            "last_error": None,
        }
    )

    try:
        async with aiohttp.ClientSession() as session:
            for sitemap in competitor_sitemaps:
                urls, diag = await scraper.scan_sitemap(session, sitemap)
                raw_len = len(urls)
                urls = _filter_salla_like_product_urls(urls)
                sitemap_diagnostics.append(
                    {
                        "sitemap": sitemap,
                        "urls_found": raw_len,
                        "urls_product_pages": len(urls),
                        "http_status": diag.get("http_status"),
                        "fetch_error": diag.get("fetch_error"),
                        "parse_error": diag.get("parse_error"),
                    }
                )
                if not urls:
                    logger.warning(
                        "No product-like URLs after filter (raw from sitemap: %s).",
                        raw_len,
                    )
                    continue

                max_urls_env = os.environ.get("SCRAPER_MAX_URLS", "").strip()
                if max_urls_env:
                    try:
                        lim = int(max_urls_env)
                        if lim > 0 and len(urls) > lim:
                            logger.info(
                                "SCRAPER_MAX_URLS=%s — limiting to %s URLs", lim, lim
                            )
                            urls = urls[:lim]
                    except ValueError:
                        pass

                urls_queued += len(urls)
                _merge_scraper_progress(
                    {
                        "current_sitemap": sitemap,
                        "urls_total": urls_queued,
                    }
                )
                logger.info(
                    "Starting async fetch for %s products from %s...", len(urls), sitemap
                )
                tasks = [scraper.fetch_and_parse_url(session, url) for url in urls]

                chunk_size = 400
                for i in range(0, len(tasks), chunk_size):
                    chunk_tasks = tasks[i : i + chunk_size]
                    results = await asyncio.gather(*chunk_tasks, return_exceptions=True)

                    for r in results:
                        if isinstance(r, Exception):
                            fetch_exceptions += 1
                        elif r is None:
                            parse_null += 1

                    valid_results = [
                        r for r in results if r is not None and not isinstance(r, Exception)
                    ]
                    all_results.extend(valid_results)
                    urls_processed_total += len(chunk_tasks)

                    rows_saved = _save_competitor_csv_rows(all_results)
                    _merge_scraper_progress(
                        {
                            "running": True,
                            "urls_total": urls_queued,
                            "urls_processed": urls_processed_total,
                            "rows_in_csv": rows_saved,
                            "current_sitemap": sitemap,
                        }
                    )

                logger.info("Finished processing %s.", sitemap)

        duration = round(time.perf_counter() - t0, 2)
        finished_at = datetime.now(timezone.utc).isoformat()
        rows_before = len(all_results)
        rows_written = (
            len(pd.DataFrame(all_results).drop_duplicates(subset=["comp_url"]))
            if all_results
            else 0
        )

        if all_results:
            logger.info(
                "JOB DONE. Saved %s records to data/competitors_latest.csv securely.",
                rows_written,
            )
            success_rate = (
                round((rows_written / urls_queued) * 100, 2) if urls_queued else 0.0
            )
            _write_scraper_last_run_meta(
                {
                    "status": "ok",
                    "finished_at": finished_at,
                    "duration_seconds": duration,
                    "sitemaps_count": len(competitor_sitemaps),
                    "urls_queued": urls_queued,
                    "rows_extracted_before_dedupe": rows_before,
                    "rows_written_csv": rows_written,
                    "fetch_exceptions": fetch_exceptions,
                    "parse_null": parse_null,
                    "success_rate_pct": success_rate,
                    "sitemap_diagnostics": sitemap_diagnostics,
                }
            )
        else:
            _write_scraper_last_run_meta(
                {
                    "status": "empty",
                    "finished_at": finished_at,
                    "duration_seconds": duration,
                    "sitemaps_count": len(competitor_sitemaps),
                    "urls_queued": urls_queued,
                    "rows_extracted_before_dedupe": 0,
                    "rows_written_csv": 0,
                    "fetch_exceptions": fetch_exceptions,
                    "parse_null": parse_null,
                    "success_rate_pct": 0.0,
                    "sitemap_diagnostics": sitemap_diagnostics,
                }
            )
    except Exception as e:
        logger.exception("Scraper engine failed: %s", e)
        _merge_scraper_progress(
            {
                "last_error": str(e),
            }
        )
        raise
    finally:
        _rows_final = (
            len(pd.DataFrame(all_results).drop_duplicates(subset=["comp_url"]))
            if all_results
            else 0
        )
        _merge_scraper_progress(
            {
                "running": False,
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "urls_total": urls_queued,
                "urls_processed": urls_processed_total,
                "rows_in_csv": _rows_final,
            }
        )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(run_scraper_engine())


if __name__ == "__main__":
    main()
