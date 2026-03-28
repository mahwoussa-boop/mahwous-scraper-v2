"""
تحويل رابط المتجر (مثل https://mahwous.com/) إلى رابط Sitemap صالح للكشط.
يستخرج Sitemap من robots.txt ثم يجرّب مسارات شائعة (سلة/زد وغيرها).
"""
from __future__ import annotations

import re
from typing import List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

import requests

# مطابق تقريباً لرؤوس async_scraper لتقليل الحظر
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/xml,text/xml,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ar-SA,ar;q=0.9,en-US;q=0.8",
}


def _parse_origin(url: str) -> Optional[str]:
    u = (url or "").strip()
    if not u:
        return None
    if not u.lower().startswith(("http://", "https://")):
        u = "https://" + u
    p = urlparse(u)
    if not p.netloc:
        return None
    scheme = p.scheme if p.scheme in ("http", "https") else "https"
    return f"{scheme}://{p.netloc}"


def _looks_like_direct_sitemap_url(url: str) -> bool:
    p = urlparse(url.strip())
    path = (p.path or "").lower()
    return path.endswith(".xml") and ("sitemap" in path or "blog-" in path)


def _response_is_sitemap_xml(text: str) -> bool:
    t = (text or "").lstrip()
    if not t:
        return False
    if t.startswith("<?xml") or t.startswith("<"):
        return bool(
            re.search(r"<(?:urlset|sitemapindex)\b", t[:2000], re.I)
        )
    return False


def _probe_sitemap_url(url: str, timeout: float = 20.0) -> bool:
    try:
        r = requests.get(
            url,
            headers=_BROWSER_HEADERS,
            timeout=timeout,
            allow_redirects=True,
        )
        if r.status_code != 200:
            return False
        return _response_is_sitemap_xml(r.text)
    except requests.RequestException:
        return False


def _sitemap_urls_from_robots(origin: str, timeout: float = 15.0) -> List[str]:
    robots_url = origin.rstrip("/") + "/robots.txt"
    out: List[str] = []
    try:
        r = requests.get(
            robots_url,
            headers=_BROWSER_HEADERS,
            timeout=timeout,
            allow_redirects=True,
        )
        if r.status_code != 200:
            return []
        for line in r.text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            low = line.lower()
            if low.startswith("sitemap:"):
                u = line.split(":", 1)[1].strip()
                if u.startswith("http"):
                    out.append(u)
    except requests.RequestException:
        pass
    return out


def _fallback_candidates(origin: str) -> List[str]:
    base = origin.rstrip("/")
    return [
        f"{base}/sitemap.xml",
        f"{base}/sitemap_products.xml",
        f"{base}/sitemap_index.xml",
        f"{base}/sitemap-products.xml",
    ]


def resolve_store_to_sitemap_url(user_input: str) -> Tuple[Optional[str], str]:
    """
    يعيد (رابط الـ sitemap الجاهز للكشط، رسالة توضيحية للمستخدم).
    إذا فشل كل شيء يعيد (None, سبب).
    """
    raw = (user_input or "").strip()
    if not raw:
        return None, "الرجاء إدخال رابط."

    # رابط مباشر لملف xml — تحقق أولاً
    if not raw.lower().startswith(("http://", "https://")):
        raw = "https://" + raw
    p = urlparse(raw)
    if not p.netloc:
        return None, "تعذر قراءة نطاق الرابط."

    if _looks_like_direct_sitemap_url(raw):
        candidate = urlunparse(
            (p.scheme, p.netloc, p.path.rstrip("/") or "/", "", "", "")
        )
        if _probe_sitemap_url(candidate):
            return candidate, f"تم اعتماد رابط الـ Sitemap مباشرة: `{candidate}`"
        # رابط xml قديم/معطّل (مثل sitemap_products.xml → 410) — نكمل الاكتشاف من جذر الموقع
        origin = _parse_origin(raw)
        if not origin:
            return (
                None,
                "الرابط ينتهي بـ .xml لكن الخادم لم يُرجع XML صالحاً.",
            )
    else:
        origin = _parse_origin(raw)
    if not origin:
        return None, "رابط المتجر غير صالح."

    # من robots.txt (الأولوية لما يعلنه الموقع — غالباً sitemap.xml)
    from_robots = _sitemap_urls_from_robots(origin)
    preferred: List[str] = []
    rest: List[str] = []
    for u in from_robots:
        lu = u.lower()
        if "product" in lu:
            preferred.append(u)
        else:
            rest.append(u)
    ordered = preferred + rest
    for u in ordered:
        if _probe_sitemap_url(u):
            return u, f"تم الاستنتاج من robots.txt: `{u}`"

    # مسارات شائعة على جذر المتجر
    for u in _fallback_candidates(origin):
        if _probe_sitemap_url(u):
            return u, f"تم الاستنتاج تلقائياً: `{u}`"

    hint = ""
    if from_robots:
        hint = f" وُجد في robots.txt: {', '.join(from_robots[:3])}"
        if len(from_robots) > 3:
            hint += "…"
    return (
        None,
        "لم يُعثر على Sitemap يعمل (HTTP 200 وXML)."
        + hint
        + " جرّب فتح الرابط في المتصفح أو أضف رابط sitemap يدوياً من لوحة المتجر.",
    )
