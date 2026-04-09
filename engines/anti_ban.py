"""
scrapers/anti_ban.py — ترسانة ضد الحظر v2.0 (2026)
═══════════════════════════════════════════════════════
آليات متعددة الطبقات لتجاوز حماية المتاجر:
  1. User-Agent ذكي — إصدارات 2026 من Chrome/Firefox/Safari/Edge
  2. Headers تحاكي المتصفح الحقيقي (Sec-CH-UA + TLS fingerprint)
  3. Adaptive Rate Limiting — يبطّئ تلقائياً عند 429/403
  4. Exponential Backoff مع Jitter
  5. curl_cffi كـ fallback أساسي (TLS fingerprint حقيقي — أحدث من cloudscraper)
  6. cloudscraper كـ fallback ثانوي
  7. Per-domain throttling + cookie persistence
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import defaultdict
from typing import Optional
from urllib.parse import urlparse

import aiohttp

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════
#  1. User-Agents — قاعدة بيانات حقيقية من متصفحات 2026
# ══════════════════════════════════════════════════════════════════════════
_REAL_UA_POOL = [
    # Chrome 134/133/132 — Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    # Chrome 134/133 — macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    # Firefox 135/134
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:134.0) Gecko/20100101 Firefox/134.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:135.0) Gecko/20100101 Firefox/135.0",
    # Safari 18
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Safari/605.1.15",
    # Edge 134
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
    # Mobile Chrome (Android 15)
    "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.99 Mobile Safari/537.36",
    # Mobile Safari (iOS 18)
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Mobile/15E148 Safari/604.1",
    # Googlebot — يُقبل دائماً من المتاجر لأنه يُستخدم لأرشفة المنتجات
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
]

_ACCEPT_LANGUAGES = [
    "ar-SA,ar;q=0.9,en-US;q=0.8,en;q=0.7",
    "ar,en-US;q=0.9,en;q=0.8",
    "en-US,en;q=0.9,ar;q=0.8",
    "ar-SA,ar;q=0.8,en;q=0.5",
]

_ACCEPT_HEADERS = [
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
]


def get_browser_headers(referer: str = "") -> dict:
    """يولّد headers تحاكي متصفحاً حقيقياً بالكامل — تتغير كل مرة."""
    ua = random.choice(_REAL_UA_POOL)
    headers = {
        "User-Agent":      ua,
        "Accept":          random.choice(_ACCEPT_HEADERS),
        "Accept-Language":  random.choice(_ACCEPT_LANGUAGES),
        "Accept-Encoding":  "gzip, deflate",
        "Connection":       "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest":   "document",
        "Sec-Fetch-Mode":   "navigate",
        "Sec-Fetch-Site":   "none" if not referer else "cross-site",
        "Sec-Fetch-User":   "?1",
        "Cache-Control":    "max-age=0",
        "DNT":              "1",
    }
    if referer:
        headers["Referer"] = referer
        headers["Sec-Fetch-Site"] = "cross-site"
    # Chrome-style sec-ch-ua
    if "Chrome" in ua and "Edg" not in ua:
        major = ua.split("Chrome/")[1].split(".")[0] if "Chrome/" in ua else "134"
        headers.update({
            "sec-ch-ua":          f'"Chromium";v="{major}", "Google Chrome";v="{major}", "Not-A.Brand";v="99"',
            "sec-ch-ua-mobile":   "?0" if "Mobile" not in ua else "?1",
            "sec-ch-ua-platform": '"Windows"' if "Windows" in ua else ('"macOS"' if "Mac" in ua else '"Android"'),
        })
    elif "Edg" in ua:
        major = ua.split("Edg/")[1].split(".")[0] if "Edg/" in ua else "134"
        headers.update({
            "sec-ch-ua":          f'"Chromium";v="{major}", "Microsoft Edge";v="{major}", "Not-A.Brand";v="99"',
            "sec-ch-ua-mobile":   "?0",
            "sec-ch-ua-platform": '"Windows"',
        })
    return headers


def get_xml_headers() -> dict:
    """رؤوس خاصة بطلبات Sitemap XML — تطلب XML صراحة وتحاكي Googlebot."""
    ua = random.choice([
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    ])
    return {
        "User-Agent": ua,
        "Accept": "application/xml,text/xml,application/xhtml+xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ar-SA,ar;q=0.9,en-US;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
    }


# ══════════════════════════════════════════════════════════════════════════
#  2. Adaptive Rate Limiter — يتكيف مع ردود الخادم
# ══════════════════════════════════════════════════════════════════════════
class AdaptiveRateLimiter:
    """
    يتتبع معدل الطلبات لكل domain ويضبطه تلقائياً:
    - 429 Too Many Requests  → تضاعف وقت الانتظار (Exponential Backoff)
    - 403 Forbidden          → توقف مؤقت طويل + تغيير UA
    - 200 متواصلة            → تقليص الانتظار تدريجياً (Speed Up)
    """

    def __init__(self):
        self._state: dict[str, dict] = defaultdict(lambda: {
            "delay":          random.uniform(0.5, 1.5),
            "consecutive_ok": 0,
            "backing_off":    False,
            "backoff_until":  0.0,
        })

    async def wait(self, domain: str) -> None:
        s = self._state[domain]
        now = time.monotonic()
        if s["backing_off"] and now < s["backoff_until"]:
            wait_t = s["backoff_until"] - now
            logger.debug("domain=%s backing-off %.1fs", domain, wait_t)
            await asyncio.sleep(wait_t)
        else:
            jitter = random.uniform(-0.2, 0.3)
            await asyncio.sleep(max(0.1, s["delay"] + jitter))

    def record_success(self, domain: str) -> None:
        s = self._state[domain]
        s["consecutive_ok"] += 1
        s["backing_off"] = False
        if s["consecutive_ok"] >= 5 and s["delay"] > 0.25:
            s["delay"] = max(0.25, s["delay"] * 0.85)

    def record_error(self, domain: str, status: int) -> None:
        s = self._state[domain]
        s["consecutive_ok"] = 0
        if status == 429:
            backoff = min(s["delay"] * 3, 30.0) + random.uniform(2, 8)
            s["delay"] = min(s["delay"] * 2, 15.0)
            s["backing_off"] = True
            s["backoff_until"] = time.monotonic() + backoff
            logger.warning("429 من %s — توقف %.0f ثانية", domain, backoff)
        elif status == 403:
            backoff = random.uniform(15, 45)
            s["backing_off"] = True
            s["backoff_until"] = time.monotonic() + backoff
            logger.warning("403 من %s — توقف %.0f ثانية", domain, backoff)
        elif status in (500, 502, 503, 504):
            s["delay"] = min(s["delay"] * 1.5, 10.0)


_rate_limiter = AdaptiveRateLimiter()


def get_rate_limiter() -> AdaptiveRateLimiter:
    return _rate_limiter


# ══════════════════════════════════════════════════════════════════════════
#  3. Retry مع Exponential Backoff
# ══════════════════════════════════════════════════════════════════════════
async def fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    *,
    max_retries: int = 3,
    base_delay:  float = 2.0,
    referer:     str = "",
) -> Optional[aiohttp.ClientResponse]:
    """يجلب URL مع إعادة محاولة + تغيير headers في كل محاولة."""
    domain = urlparse(url).netloc
    rl = get_rate_limiter()

    for attempt in range(max_retries):
        headers = get_browser_headers(referer=referer or f"https://{domain}/")
        try:
            await rl.wait(domain)
            resp = await session.get(url, headers=headers, ssl=False, allow_redirects=True)

            if resp.status == 200:
                rl.record_success(domain)
                return resp

            rl.record_error(domain, resp.status)

            if resp.status in (404, 410):
                return None
            if resp.status in (429, 403, 500, 502, 503):
                delay = base_delay * (2 ** attempt) + random.uniform(0, 3)
                logger.debug("attempt %d/%d → %d, سينتظر %.1fs",
                             attempt + 1, max_retries, resp.status, delay)
                await asyncio.sleep(delay)
                continue

            return None

        except (aiohttp.ClientConnectorError, asyncio.TimeoutError) as exc:
            delay = base_delay * (2 ** attempt) + random.uniform(0, 2)
            logger.debug("attempt %d: %s — انتظار %.1fs", attempt + 1, exc, delay)
            await asyncio.sleep(delay)
        except Exception as exc:
            logger.debug("fetch_with_retry unexpected: %s", exc)
            return None

    return None


# ══════════════════════════════════════════════════════════════════════════
#  4. curl_cffi — TLS Fingerprint حقيقي (يتجاوز Cloudflare/Akamai)
# ══════════════════════════════════════════════════════════════════════════
def try_curl_cffi(url: str, timeout: int = 25) -> Optional[str]:
    """
    يحاول جلب الصفحة عبر curl_cffi الذي ينتحل بصمة TLS لـ Chrome الحقيقي.
    هذا أحدث وأنجح من cloudscraper لأنه يستخدم libcurl مع impersonation.
    """
    try:
        from curl_cffi import requests as cffi_requests
        resp = cffi_requests.get(
            url,
            impersonate="chrome",
            timeout=timeout,
            allow_redirects=True,
        )
        if resp.status_code == 200:
            return resp.text
    except ImportError:
        logger.debug("curl_cffi غير مثبّت — تخطّى")
    except Exception as exc:
        logger.debug("curl_cffi %s: %s", url, exc)
    return None


# ══════════════════════════════════════════════════════════════════════════
#  5. cloudscraper — Fallback ثانوي لـ Cloudflare JS Challenge
# ══════════════════════════════════════════════════════════════════════════
def try_cloudscraper(url: str) -> Optional[str]:
    """يحاول جلب الصفحة عبر cloudscraper (يتجاوز JS Challenge)."""
    try:
        import cloudscraper
        scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        resp = scraper.get(url, timeout=20)
        if resp.status_code == 200:
            return resp.text
    except ImportError:
        pass
    except Exception as exc:
        logger.debug("cloudscraper %s: %s", url, exc)
    return None


# ══════════════════════════════════════════════════════════════════════════
#  6. سلسلة الـ Fallback الكاملة (مزامن — يُستدعى من executor)
# ══════════════════════════════════════════════════════════════════════════
def try_all_sync_fallbacks(url: str) -> Optional[str]:
    """يحاول curl_cffi أولاً، ثم cloudscraper، ثم requests بسيط."""
    html = try_curl_cffi(url)
    if html:
        return html

    html = try_cloudscraper(url)
    if html:
        return html

    try:
        import requests as _req
        headers = get_browser_headers(referer=f"https://{urlparse(url).netloc}/")
        resp = _req.get(url, headers=headers, timeout=20, allow_redirects=True, verify=False)
        if resp.status_code == 200:
            return resp.text
    except Exception as exc:
        logger.debug("requests fallback %s: %s", url, exc)

    return None
