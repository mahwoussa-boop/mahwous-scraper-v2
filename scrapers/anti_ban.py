"""
scrapers/anti_ban.py — ترسانة ضد الحظر v1.0
═══════════════════════════════════════════════
آليات متعددة الطبقات لتجاوز الحماية:
  1. User-Agent ذكي (real-browser database)
  2. Headers تحاكي المتصفح الحقيقي
  3. Adaptive Rate Limiting — يبطّئ تلقائياً عند 429
  4. Exponential Backoff مع Jitter
  5. cloudscraper كـ fallback لـ Cloudflare
  6. Per-domain throttling منفصل
"""
import asyncio
import logging
import random
import time
from collections import defaultdict
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════
#  1. User-Agents — قاعدة بيانات حقيقية من متصفحات 2024-2025
# ══════════════════════════════════════════════════════════════════════════
_REAL_UA_POOL = [
    # Chrome Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    # Chrome Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    # Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.6; rv:131.0) Gecko/20100101 Firefox/131.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
    # Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
    # Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
    # Mobile Chrome (Android)
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.135 Mobile Safari/537.36",
    # Mobile Safari (iOS)
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1",
    # Googlebot (يُقبَل دائماً)
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
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
]


def get_browser_headers(referer: str = "") -> dict:
    """
    يولّد headers تحاكي متصفحاً حقيقياً بالكامل.
    """
    ua = random.choice(_REAL_UA_POOL)
    headers = {
        "User-Agent":      ua,
        "Accept":          random.choice(_ACCEPT_HEADERS),
        "Accept-Language": random.choice(_ACCEPT_LANGUAGES),
        "Accept-Encoding": "gzip, deflate, br",
        "Connection":      "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest":  "document",
        "Sec-Fetch-Mode":  "navigate",
        "Sec-Fetch-Site":  "none",
        "Sec-Fetch-User":  "?1",
        "Cache-Control":   "max-age=0",
        "DNT":             "1",
    }
    if referer:
        headers["Referer"] = referer
    # Chrome-style sec-ch headers
    if "Chrome" in ua:
        major = ua.split("Chrome/")[1].split(".")[0] if "Chrome/" in ua else "131"
        headers.update({
            "sec-ch-ua":          f'"Google Chrome";v="{major}", "Chromium";v="{major}", "Not_A Brand";v="24"',
            "sec-ch-ua-mobile":   "?0",
            "sec-ch-ua-platform": '"Windows"' if "Windows" in ua else '"macOS"',
        })
    return headers


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
        # قاموس: domain → (last_delay, consecutive_ok, last_error_time)
        self._state: dict[str, dict] = defaultdict(lambda: {
            "delay":          random.uniform(0.5, 1.5),
            "consecutive_ok": 0,
            "backing_off":    False,
            "backoff_until":  0.0,
        })

    async def wait(self, domain: str) -> None:
        """ينتظر الوقت المناسب قبل الطلب التالي لهذا الـ domain."""
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
        # تسريع تدريجي بعد 5 نجاحات متواصلة
        if s["consecutive_ok"] >= 5 and s["delay"] > 0.3:
            s["delay"] = max(0.3, s["delay"] * 0.85)

    def record_error(self, domain: str, status: int) -> None:
        s = self._state[domain]
        s["consecutive_ok"] = 0
        if status == 429:
            # تضاعف وقت الانتظار
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


# Singleton للمشاركة بين جميع coroutines
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
    """
    يجلب الـ URL مع إعادة المحاولة التلقائية + تغيير الـ headers في كل محاولة.
    يُرجع الـ response عند النجاح أو None عند الفشل الكامل.
    """
    from urllib.parse import urlparse as _up
    domain = _up(url).netloc
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
                return None  # لا فائدة من إعادة المحاولة
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
#  4. Cloudscraper fallback (مزامن — للمتاجر ذات Cloudflare)
# ══════════════════════════════════════════════════════════════════════════
def try_cloudscraper(url: str) -> Optional[str]:
    """
    يحاول جلب الصفحة عبر cloudscraper (يتجاوز JS Challenge).
    يُعيد HTML أو None.
    """
    try:
        import cloudscraper
        scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        resp = scraper.get(url, timeout=20)
        if resp.status_code == 200:
            return resp.text
    except ImportError:
        pass  # cloudscraper غير مثبّت — تخطّى
    except Exception as exc:
        logger.debug("cloudscraper %s: %s", url, exc)
    return None
