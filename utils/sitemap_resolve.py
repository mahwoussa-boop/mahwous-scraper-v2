"""
Dynamic sitemap resolver (async-first) for mixed ecommerce platforms.

Supports:
- direct sitemap URLs
- robots.txt extraction
- common fallback paths (Salla/Zid/Shopify/custom)
"""
from __future__ import annotations

import asyncio
import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import aiohttp
import requests

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/xml,text/xml,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ar-SA,ar;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
}

COMMON_SITEMAP_PATHS = [
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap_products.xml",
    "/sitemap_products_1.xml",
    "/sitemap-products.xml",
    "/sitemap-products-1.xml",
    "/product-sitemap.xml",
    "/sitemap/sitemap-index.xml",
    "/sitemap/sitemap.xml",
]


def _parse_origin(url_or_domain: str) -> Optional[str]:
    raw = (url_or_domain or "").strip()
    if not raw:
        return None
    if not raw.lower().startswith(("http://", "https://")):
        raw = "https://" + raw
    p = urlparse(raw)
    if not p.netloc:
        return None
    scheme = p.scheme if p.scheme in ("http", "https") else "https"
    return f"{scheme}://{p.netloc}"


def _looks_like_direct_sitemap_url(value: str) -> bool:
    p = urlparse((value or "").strip())
    path = (p.path or "").lower()
    return path.endswith(".xml") and "sitemap" in path


def _is_sitemap_xml(text: str) -> bool:
    t = (text or "").lstrip()
    if not t:
        return False
    if not (t.startswith("<") or t.startswith("<?xml")):
        return False
    return bool(re.search(r"<(?:urlset|sitemapindex)\b", t[:3000], re.I))


async def _probe_url(session: aiohttp.ClientSession, url: str, timeout: float = 20.0) -> bool:
    try:
        async with session.get(
            url,
            headers=_HEADERS,
            timeout=aiohttp.ClientTimeout(total=timeout),
            allow_redirects=True,
        ) as resp:
            if resp.status != 200:
                return False
            body = await resp.text(errors="replace")
            return _is_sitemap_xml(body)
    except Exception:
        return False


async def _robots_sitemaps_async(session: aiohttp.ClientSession, origin: str) -> List[str]:
    robots = origin.rstrip("/") + "/robots.txt"
    out: List[str] = []
    try:
        async with session.get(
            robots,
            headers=_HEADERS,
            timeout=aiohttp.ClientTimeout(total=15),
            allow_redirects=True,
        ) as resp:
            if resp.status != 200:
                return out
            txt = await resp.text(errors="replace")
    except Exception:
        return out

    for ln in txt.splitlines():
        s = ln.strip()
        if not s or s.startswith("#"):
            continue
        if s.lower().startswith("sitemap:"):
            u = s.split(":", 1)[1].strip()
            if u.startswith("http"):
                out.append(u)
    return out


async def resolve_sitemap_url_async(domain_or_url: str) -> Optional[str]:
    """
    Takes a domain/store URL and returns the first valid sitemap URL using aiohttp.
    """
    origin = _parse_origin(domain_or_url)
    if not origin:
        return None

    # If direct sitemap URL passed, test it first.
    raw = (domain_or_url or "").strip()
    direct_candidates: List[str] = []
    if _looks_like_direct_sitemap_url(raw):
        p = urlparse(raw if raw.startswith("http") else ("https://" + raw))
        direct_candidates.append(
            urlunparse((p.scheme, p.netloc, p.path.rstrip("/") or "/", "", "", ""))
        )

    async with aiohttp.ClientSession() as session:
        for u in direct_candidates:
            if await _probe_url(session, u):
                return u

        robots_urls = await _robots_sitemaps_async(session, origin)
        preferred = sorted(
            robots_urls,
            key=lambda x: (0 if "product" in x.lower() else 1, x),
        )
        for u in preferred:
            if await _probe_url(session, u):
                return u

        fallback_urls = [urljoin(origin.rstrip("/") + "/", p.lstrip("/")) for p in COMMON_SITEMAP_PATHS]
        # probe concurrently for speed
        checks = await asyncio.gather(*[_probe_url(session, u) for u in fallback_urls], return_exceptions=False)
        for u, ok in zip(fallback_urls, checks):
            if ok:
                return u
    return None


def resolve_store_to_sitemap_url(user_input: str) -> Tuple[Optional[str], str]:
    """
    Backward-compatible sync wrapper used by UI forms.
    """
    try:
        found = asyncio.run(resolve_sitemap_url_async(user_input))
    except RuntimeError:
        # If called from an already-running loop, fallback sync probing.
        found = None

    if found:
        return found, f"تم الاستنتاج تلقائياً: `{found}`"
    return None, "لم يتم العثور على Sitemap صالح تلقائياً."


def resolve_sitemap_url_sync(domain_or_url: str) -> Optional[str]:
    """
    Sync fallback for contexts that cannot await.
    """
    origin = _parse_origin(domain_or_url)
    if not origin:
        return None
    candidates: List[str] = []
    if _looks_like_direct_sitemap_url(domain_or_url):
        p = urlparse(domain_or_url if domain_or_url.startswith("http") else ("https://" + domain_or_url))
        candidates.append(urlunparse((p.scheme, p.netloc, p.path.rstrip("/") or "/", "", "", "")))
    candidates.extend(urljoin(origin.rstrip("/") + "/", p.lstrip("/")) for p in COMMON_SITEMAP_PATHS)

    for u in candidates:
        try:
            r = requests.get(u, headers=_HEADERS, timeout=15, allow_redirects=True)
            if r.status_code == 200 and _is_sitemap_xml(r.text):
                return u
        except Exception:
            continue
    return None
