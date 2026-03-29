"""
utils/gemini_visual_verifier.py — Supreme Visual Verifier (Gemini 2.5 Flash)
============================================================================
القاضي النهائي للمطابقات الضبابية: يدمج أسماء المنتجات + صوراً (عند صحة الرابط).

• نطاق التشغيل من SmartMatcher: 50% ≤ score ≤ 89% (لا اعتماد ولا رفض آلي).
• النموذج: gemini-2.5-flash-preview (مع احتياطي gemini-1.5-flash).
• الكاش: match_cache_v21.db (جدول cache) + threading.Lock عند الكتابة.
"""
from __future__ import annotations

import threading

_cache_lock = threading.Lock()

import base64
import hashlib
import json
import logging
import os
import re
import sqlite3
import time
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# كاش بصري: sqlite3 على ملف match_cache_v21.db + threading.Lock في cache_get_visual/cache_put_visual
# دالة التحقق من الرابط: is_valid_image_url (أدناه)

# نفس ملف الكاش المستخدم في engines.engine للمطابقة النصية
MATCH_CACHE_V21_DB = os.environ.get("MATCH_CACHE_V21_DB", "match_cache_v21.db")

VISUAL_MODEL = "gemini-2.5-flash-preview-04-17"
FALLBACK_MODEL = "gemini-1.5-flash"

# نطاق «المنطقة الرمادية» بعد SmartMatcher
SUPREME_SCORE_MIN = 50.0
SUPREME_SCORE_MAX = 89.0

# عتبة قديمة: أي score أقل من هذا قد يستدعي تحققاً بصرياً عاماً
VISUAL_VERIFY_THRESHOLD = 90.0

_MAX_URL_LEN = 2048

_SUPREME_PROMPT = """أنت خبير عطور (Perfumer & Retail SKU Analyst) تعمل لمتجر مهووس — لست مُصرّفاً عاماً.

## مهمتك
قرر هل **منتج المتجر** و**منتج المنافس** هما **نفس الصنف القابل للبيع** (نفس العطر/الإصدار/الحجم/التركيز المنطقي)، أم لا.

## مدخلاتك
- الاسمان النصيّان (قد يكون أحدهما عربي والآخر لاتيني).
- صورتان اختياريتان: زجاجة، غطاء، علبة كرتون، أو لقطة منتج من الموقع.

## كيف «تفكّر» كخبير
1) **الهوية**: نفس خط العطر (flanker مختلف = منتج مختلف، مثل Sauvage vs Sauvage Elixir).
2) **التركيز**: EDP ≠ EDT ≠ Parfum/Extrait ≠ Cologne — اختلاف التركيز يرفض التطابق إلا إن الأدلة بصرية+نصية قاطعة لصنف مركّب واحد.
3) **الحجم**: 50ml ≠ 100ml ≠ 200ml — رفض إن اختلف الحجم المعروض بوضوح في الاسم أو الصورة (تسامح طفيف فقط لخطأ تصوير).
4) **الإصدار**: Limited / Collector / Tester / Set / Refill / Body Spray يختلف عن الإصدار القياسي.
5) **الصورة**: قارن شكل الزجاجة، لون السائل الظاهر، غطاء/بخاخ، تخطيط الملصق، ألوان العلبة. اختلاف تصميم كرتون **قد** يكون نفس المنتج بمنطقة بيع مختلفة — اذكر ذلك في التعليل.
6) إن غابت الصور أو كانت رديئة، اعتمد على النص بحذر وأنقص الثقة.

## المخرجات
أجب بـ JSON صارم فقط (بدون markdown):
{{
  "is_match": true أو false,
  "confidence": عدد عشري بين 0 و 1 (احتمال قرارك، مثل 0.87),
  "reasoning": "شرح بالعربية: لماذا تطابق أو لماذا لا — مع ذكر الحجم/التركيز/العلامة إن أمكن",
  "key_difference": "أهم فارق إن وجد" أو null
}}"""

# قالب مبسّط للمسار العام (verify)
_PROMPT_TEMPLATE = _SUPREME_PROMPT + """

## منتج المتجر (مهووس)
- الاسم: {name_mine}
{img_mine_section}

## منتج المنافس
- الاسم: {name_comp}
{img_comp_section}
"""


@dataclass
class VisualMatchVerdict:
    """قرار التحقق البصري/الذكي — ليس مجرد True/False."""

    is_match: bool
    confidence: float  # 0.0 .. 1.0
    reasoning: str
    key_difference: Optional[str] = None
    method: str = "visual_gemini"
    from_cache: bool = False

    def to_legacy_dict(self) -> Dict[str, Any]:
        """توافق مع كود يتوقع confidence 0-100 و reason."""
        return {
            "is_match": self.is_match,
            "confidence": int(round(max(0.0, min(1.0, self.confidence)) * 100)),
            "reason": self.reasoning,
            "key_difference": self.key_difference,
            "method": self.method,
            "from_cache": self.from_cache,
        }


def is_valid_image_url(url: str) -> bool:
    """
    يمنع روابط غير صالحة من الوصول لـ Gemini (تجنّب أخطاء payload / طلبات فارغة).
    """
    if not url or not isinstance(url, str):
        return False
    u = url.strip()
    if not u or len(u) > _MAX_URL_LEN:
        return False
    low = u.lower()
    if low.startswith("data:") or low.startswith("javascript:") or low.startswith("file:"):
        return False
    try:
        parsed = urllib.parse.urlparse(u)
    except Exception:
        return False
    if parsed.scheme not in ("http", "https"):
        return False
    if not parsed.netloc or "." not in parsed.netloc:
        return False
    # رفض مسارات وهمية شديدة القصر
    if len(parsed.path or "") < 1 and "/" not in u.rstrip("/"):
        return False
    return True


def supreme_band(match_score: float) -> bool:
    """هل نسبة SmartMatcher ضمن المنطقة التي تستدعي القاضي البصري؟"""
    try:
        s = float(match_score)
    except (TypeError, ValueError):
        return False
    return SUPREME_SCORE_MIN <= s <= SUPREME_SCORE_MAX


def _cache_key(
    name_mine: str,
    name_comp: str,
    img_mine_url: str,
    img_comp_url: str,
) -> str:
    payload = {
        "v": 2,
        "model": VISUAL_MODEL,
        "a": str(name_mine or "")[:600],
        "b": str(name_comp or "")[:600],
        "u1": str(img_mine_url or "").strip()[:900],
        "u2": str(img_comp_url or "").strip()[:900],
    }
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    h = hashlib.sha256(blob.encode("utf-8")).hexdigest()
    return f"sup_visual:{h}"


def _ensure_cache_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS cache(h TEXT PRIMARY KEY, v TEXT, ts TEXT)"
    )


def cache_get_visual(key: str) -> Optional[Dict[str, Any]]:
    with _cache_lock:
        try:
            conn = sqlite3.connect(MATCH_CACHE_V21_DB, check_same_thread=False)
            _ensure_cache_table(conn)
            row = conn.execute("SELECT v FROM cache WHERE h=?", (key,)).fetchone()
            conn.close()
            if not row or not row[0]:
                return None
            return json.loads(row[0])
        except Exception as e:
            logger.debug("visual cache read failed: %s", e)
            return None


def cache_put_visual(key: str, verdict_dict: Dict[str, Any]) -> None:
    with _cache_lock:
        try:
            conn = sqlite3.connect(MATCH_CACHE_V21_DB, check_same_thread=False)
            _ensure_cache_table(conn)
            conn.execute(
                "INSERT OR REPLACE INTO cache VALUES(?,?,?)",
                (key, json.dumps(verdict_dict, ensure_ascii=False), datetime.now().isoformat()),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning("visual cache write failed: %s", e)


def _fetch_image_as_b64(url: str, timeout: int = 10) -> Optional[str]:
    if not is_valid_image_url(url):
        return None
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; MahwousVisualVerifier/2.1)"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
            ctype = (resp.headers.get("Content-Type") or "").lower()
        if not data or len(data) > 12_000_000:
            return None
        if "image" not in ctype and not url.lower().endswith(
            (".jpg", ".jpeg", ".png", ".webp", ".gif")
        ):
            if "octet-stream" not in ctype:
                logger.debug("skip non-image content-type for %s", url[:80])
                return None
        return base64.b64encode(data).decode("utf-8")
    except Exception as e:
        logger.debug("Image fetch failed for %s: %s", url[:100], e)
        return None


def _mime_from_b64_prefix(b64: str) -> str:
    try:
        raw = base64.b64decode(b64[:128] + "==", validate=False)[:24]
    except Exception:
        return "image/jpeg"
    if raw[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if raw[:4] == b"RIFF" and raw[8:12] == b"WEBP":
        return "image/webp"
    if raw[:2] == b"\xff\xd8":
        return "image/jpeg"
    return "image/jpeg"


def _build_prompt_parts(
    name_mine: str,
    name_comp: str,
    img_mine_b64: Optional[str],
    img_comp_b64: Optional[str],
) -> list:
    parts: list = []
    img_mine_section = (
        "- الصورة: غير متاحة أو لم تُحمَّل" if not img_mine_b64 else "- الصورة: مرفقة بعد النص"
    )
    img_comp_section = (
        "- الصورة: غير متاحة أو لم تُحمَّل" if not img_comp_b64 else "- الصورة: مرفقة بعد النص"
    )
    text = _PROMPT_TEMPLATE.format(
        name_mine=name_mine or "—",
        name_comp=name_comp or "—",
        img_mine_section=img_mine_section,
        img_comp_section=img_comp_section,
    )
    parts.append({"text": text})
    if img_mine_b64:
        parts.append({"text": "[صورة منتج مهووس]"})
        parts.append(
            {
                "inline_data": {
                    "mime_type": _mime_from_b64_prefix(img_mine_b64),
                    "data": img_mine_b64,
                }
            }
        )
    if img_comp_b64:
        parts.append({"text": "[صورة منتج المنافس]"})
        parts.append(
            {
                "inline_data": {
                    "mime_type": _mime_from_b64_prefix(img_comp_b64),
                    "data": img_comp_b64,
                }
            }
        )
    return parts


def _parse_verdict(raw_text: str) -> VisualMatchVerdict:
    txt = re.sub(r"```(?:json)?", "", raw_text, flags=re.IGNORECASE).replace("```", "").strip()
    m = re.search(r"\{.*\}", txt, re.DOTALL)
    if not m:
        return VisualMatchVerdict(
            is_match=False,
            confidence=0.0,
            reasoning="فشل في تحليل رد النموذج",
            key_difference=None,
            method="parse_error",
        )
    try:
        obj = json.loads(m.group())
    except json.JSONDecodeError:
        return VisualMatchVerdict(
            is_match=False,
            confidence=0.0,
            reasoning="JSON غير صالح من النموذج",
            key_difference=None,
            method="parse_error",
        )

    is_match = bool(obj.get("is_match", False))
    conf_raw = obj.get("confidence", 0)
    try:
        c = float(conf_raw)
    except (TypeError, ValueError):
        c = 0.0
    if c > 1.0:
        c = c / 100.0
    c = max(0.0, min(1.0, c))

    reasoning = str(obj.get("reasoning") or obj.get("reason") or "").strip()
    if not reasoning:
        reasoning = "لم يُرجع النموذج تعليلاً"

    kd = obj.get("key_difference")
    key_diff = None if kd in (None, "", "null") else str(kd).strip()

    return VisualMatchVerdict(
        is_match=is_match,
        confidence=c,
        reasoning=reasoning[:1200],
        key_difference=key_diff,
        method="visual_gemini",
    )


class GeminiVisualVerifier:
    """
    Supreme Visual Verifier — يقرأ الأسماء + الصور (بعد التحقق من صحة الروابط).
    """

    def __init__(self, api_key: str = ""):
        self._api_key = api_key or self._load_api_key()
        self.enabled = bool(self._api_key)
        if not self.enabled:
            logger.warning("GeminiVisualVerifier: لا يوجد API key — المعطّل")

    @staticmethod
    def _load_api_key() -> str:
        try:
            from config import GEMINI_API_KEYS

            for k in GEMINI_API_KEYS or []:
                ks = str(k).strip()
                if ks:
                    return ks
        except Exception:
            pass
        try:
            import streamlit as st

            for key_name in ("GEMINI_API_KEY", "GEMINI_API_KEYS", "GOOGLE_API_KEY"):
                v = str(st.secrets.get(key_name, "")).strip()
                if v:
                    if v.startswith("[") or "," in v:
                        try:
                            arr = json.loads(v)
                            if isinstance(arr, list) and arr:
                                return str(arr[0]).strip()
                        except Exception:
                            pass
                    return v.split(",")[0].strip()
        except Exception:
            pass
        for k in ("GEMINI_API_KEY", "GEMINI_API_KEYS", "GOOGLE_API_KEY"):
            v = os.environ.get(k, "").strip()
            if v:
                return v.split(",")[0].strip()
        return ""

    @staticmethod
    def should_verify(match_score: float) -> bool:
        return float(match_score) < VISUAL_VERIFY_THRESHOLD

    def verify_supreme(
        self,
        name_mine: str,
        name_comp: str,
        img_mine_url: str = "",
        img_comp_url: str = "",
        use_cache: bool = True,
        fetch_images: bool = True,
        max_retries: int = 2,
    ) -> VisualMatchVerdict:
        """
        مسار خط التسعير: كاش → Gemini (صور بعد التحقق من URL فقط).
        """
        key = _cache_key(name_mine, name_comp, img_mine_url, img_comp_url)
        if use_cache:
            cached = cache_get_visual(key)
            if cached:
                try:
                    return VisualMatchVerdict(
                        is_match=bool(cached.get("is_match")),
                        confidence=float(cached.get("confidence", 0)),
                        reasoning=str(cached.get("reasoning", "")),
                        key_difference=cached.get("key_difference"),
                        method=str(cached.get("method", "cache")),
                        from_cache=True,
                    )
                except Exception:
                    pass

        if not self.enabled:
            v = VisualMatchVerdict(
                is_match=False,
                confidence=0.0,
                reasoning="لا يوجد مفتاح Gemini مُهيأ",
                method="fallback",
            )
            return v

        img_mine_b64 = None
        img_comp_b64 = None
        if fetch_images:
            if img_mine_url and is_valid_image_url(img_mine_url):
                img_mine_b64 = _fetch_image_as_b64(img_mine_url)
            if img_comp_url and is_valid_image_url(img_comp_url):
                img_comp_b64 = _fetch_image_as_b64(img_comp_url)

        method = "visual_gemini" if (img_mine_b64 or img_comp_b64) else "text_only_gemini"
        verdict: Optional[VisualMatchVerdict] = None
        for attempt in range(max_retries + 1):
            try:
                raw = self._call_gemini_raw(name_mine, name_comp, img_mine_b64, img_comp_b64)
                verdict = _parse_verdict(raw)
                verdict.method = method
                break
            except Exception as e:
                logger.warning("GeminiVisualVerifier supreme attempt %s: %s", attempt + 1, e)
                if attempt < max_retries:
                    time.sleep(1.2 * (attempt + 1))

        if verdict is None:
            verdict = VisualMatchVerdict(
                is_match=False,
                confidence=0.0,
                reasoning="فشلت جميع محاولات الاتصال بالنموذج",
                method="fallback",
            )

        if use_cache and verdict.method != "parse_error":
            cache_put_visual(
                key,
                {
                    "is_match": verdict.is_match,
                    "confidence": verdict.confidence,
                    "reasoning": verdict.reasoning,
                    "key_difference": verdict.key_difference,
                    "method": verdict.method,
                },
            )
        return verdict

    def _call_gemini_raw(
        self,
        name_mine: str,
        name_comp: str,
        img_mine_b64: Optional[str],
        img_comp_b64: Optional[str],
    ) -> str:
        import json as _json
        import urllib.request as _req

        parts = _build_prompt_parts(name_mine, name_comp, img_mine_b64, img_comp_b64)
        payload = {
            "contents": [{"parts": parts}],
            "generationConfig": {
                "temperature": 0.12,
                "maxOutputTokens": 512,
                "responseMimeType": "application/json",
            },
        }
        data = _json.dumps(payload).encode("utf-8")
        last_err: Optional[Exception] = None
        for model in (VISUAL_MODEL, FALLBACK_MODEL):
            url = (
                f"https://generativelanguage.googleapis.com/v1beta/models/"
                f"{model}:generateContent?key={self._api_key}"
            )
            try:
                http_req = _req.Request(
                    url,
                    data=data,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with _req.urlopen(http_req, timeout=45) as resp:
                    body = _json.loads(resp.read().decode("utf-8"))
                candidates = body.get("candidates", [])
                if not candidates:
                    continue
                return (
                    candidates[0]
                    .get("content", {})
                    .get("parts", [{}])[0]
                    .get("text", "")
                )
            except Exception as e:
                last_err = e
                logger.debug("model %s: %s", model, e)
                continue
        raise RuntimeError(str(last_err) if last_err else "no candidates")

    def verify(
        self,
        name_mine: str,
        name_comp: str,
        img_mine_url: str = "",
        img_comp_url: str = "",
        fetch_images: bool = True,
        max_retries: int = 2,
    ) -> Dict[str, Any]:
        """واجهة قديمة: يعيد dict بمفتاح confidence 0-100."""
        v = self.verify_supreme(
            name_mine,
            name_comp,
            img_mine_url,
            img_comp_url,
            use_cache=True,
            fetch_images=fetch_images,
            max_retries=max_retries,
        )
        d = v.to_legacy_dict()
        return d

    @staticmethod
    def _fallback_result(reason: str) -> Dict[str, Any]:
        return {
            "is_match": False,
            "confidence": 0,
            "reason": reason,
            "key_difference": None,
            "method": "fallback",
        }


_verifier_singleton: Optional[GeminiVisualVerifier] = None


def get_visual_verifier() -> GeminiVisualVerifier:
    global _verifier_singleton
    if _verifier_singleton is None:
        _verifier_singleton = GeminiVisualVerifier()
    return _verifier_singleton


def verify_if_needed(
    match_score: float,
    name_mine: str,
    name_comp: str,
    img_mine_url: str = "",
    img_comp_url: str = "",
) -> Optional[Dict[str, Any]]:
    if not GeminiVisualVerifier.should_verify(match_score):
        return None
    return get_visual_verifier().verify(name_mine, name_comp, img_mine_url, img_comp_url)
