"""
scrapers/sitemap_resolve.py — حل روابط Sitemap للمتاجر الإلكترونية
═══════════════════════════════════════════════════════════════════
يحدّد مسار Sitemap لأي متجر بأولوية:
  1. /sitemap_index.xml  (Shopify/WooCommerce)
  2. /sitemap.xml        (المعيار العام)
  3. robots.txt → سطر Sitemap:
  4. مسارات مخصصة لمتاجر سلة / زيد / Salla

يُعيد قائمة URLs لمنتجات المتجر جاهزة للكشط.
"""
import asyncio
import logging
import re
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse

import aiohttp

logger = logging.getLogger(__name__)

# ── ثوابت ─────────────────────────────────────────────────────────────────
_TIMEOUT = aiohttp.ClientTimeout(total=20)

_SITEMAP_CANDIDATES = [
    "/sitemap_index.xml",
    "/sitemap.xml",
    "/sitemap-products.xml",
    "/products-sitemap.xml",
    "/page-sitemap.xml",
]

# متاجر سلة: Sitemap في مسار مختلف
_SALLA_PATTERN = re.compile(
    r"(?:salla\.sa|salla\.store|salla\.store|\.myshopify\.com|\.sa/store)", re.I
)

# كلمات دالة على صفحة منتج
_PRODUCT_URL_KEYWORDS = re.compile(
    r"/product[s]?/|/p/|/item/|/shop/|/ar/p/|/en/p/|منتج|product",
    re.I,
)


def _base_url(url: str) -> str:
    """يُرجع https://example.com بدون مسار."""
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


async def _fetch_text(session: aiohttp.ClientSession, url: str) -> str | None:
    """GET مع تجاهل أخطاء TLS وإرجاع None عند الفشل."""
    try:
        async with session.get(url, allow_redirects=True, ssl=False) as resp:
            if resp.status == 200:
                return await resp.text(errors="ignore")
    except Exception as exc:
        logger.debug("fetch_text %s → %s", url, exc)
    return None


def _parse_sitemap_urls(xml_text: str, base: str) -> list[dict]:
    """
    يُحلِّل XML ويُرجع قائمة بـ dicts تحتوي على loc و lastmod.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    results: list[dict] = []

    # sitemap index
    for sm in root.findall(".//sm:sitemap", ns):
        loc = sm.find("sm:loc", ns)
        lastmod = sm.find("sm:lastmod", ns)
        if loc is not None and loc.text:
            results.append({
                "loc": loc.text.strip(),
                "lastmod": lastmod.text.strip() if lastmod is not None and lastmod.text else None,
                "is_index": True
            })

    # عادي
    for url in root.findall(".//sm:url", ns):
        loc = url.find("sm:loc", ns)
        lastmod = url.find("sm:lastmod", ns)
        if loc is not None and loc.text:
            results.append({
                "loc": loc.text.strip(),
                "lastmod": lastmod.text.strip() if lastmod is not None and lastmod.text else None,
                "is_index": False
            })

    # بدون namespace
    if not results:
        for loc in root.iter("loc"):
            if loc.text:
                results.append({"loc": loc.text.strip(), "lastmod": None, "is_index": False})

    return results


async def _sitemap_from_robots(
    session: aiohttp.ClientSession, base: str
) -> list[str]:
    """يستخرج روابط Sitemap من robots.txt."""
    text = await _fetch_text(session, f"{base}/robots.txt")
    if not text:
        return []
    found = []
    for line in text.splitlines():
        if line.lower().startswith("sitemap:"):
            url = line.split(":", 1)[1].strip()
            if url.startswith("http"):
                found.append(url)
    return found


async def resolve_product_urls(
    store_url: str,
    session: aiohttp.ClientSession,
    *,
    max_products: int = 0,
    since_date: str = None
) -> list[str]:
    """
    الدالة الرئيسية — تُرجع قائمة URLs لصفحات المنتجات المحدثة (Enterprise v2.0).
    """
    _no_limit = (max_products <= 0)
    base = _base_url(store_url)
    product_urls: list[str] = []

    # ── جمع مرشحي Sitemap ──────────────────────────────────────────────────
    sitemap_entries: list[dict] = []

    for path in _SITEMAP_CANDIDATES:
        text = await _fetch_text(session, f"{base}{path}")
        if text and ("<urlset" in text or "<sitemapindex" in text):
            parsed = _parse_sitemap_urls(text, base)
            sitemap_entries.extend(parsed)
            if parsed:
                break

    if not sitemap_entries:
        # تحويل روابط robots إلى entries
        robots_urls = await _sitemap_from_robots(session, base)
        sitemap_entries = [{"loc": u, "lastmod": None, "is_index": u.endswith(".xml")} for u in robots_urls]

    # ── تتبع sitemap_index متداخل ───────────────────────
    all_final_entries: list[dict] = []
    for entry in sitemap_entries:
        if entry.get("is_index") or entry["loc"].endswith(".xml"):
            text = await _fetch_text(session, entry["loc"])
            if text:
                all_final_entries.extend(_parse_sitemap_urls(text, base))
        else:
            all_final_entries.append(entry)

    # ── فلترة صفحات المنتجات + التحديثات الذكية ────────────────────────────────
    for entry in all_final_entries:
        url = entry["loc"]
        lastmod = entry.get("lastmod")
        
        # فلترة حسب التاريخ (Delta Update)
        if since_date and lastmod:
            if lastmod < since_date:
                continue
        
        if _PRODUCT_URL_KEYWORDS.search(url):
            product_urls.append(url)
            
        if not _no_limit and len(product_urls) >= max_products:
            break

    logger.info("resolve_product_urls %s → %d URLs (Delta: %s)", base, len(product_urls), since_date)
    return product_urls
