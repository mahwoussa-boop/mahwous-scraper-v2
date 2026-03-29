"""
utils/gemini_visual_verifier.py — محقق Gemini البصري v2.0
=============================================================
يُستدعى عندما تكون نسبة التطابق النصي < 90%
يستخدم gemini-2.5-flash-preview لتحليل:
  • صورة منتجنا + صورة المنافس
  • اسم منتجنا + اسم المنافس
ويُعطي قراراً نهائياً: متطابق / غير متطابق + سبب واضح.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import re
import time
import urllib.request
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# نموذج Gemini المستخدم للتحقق البصري
VISUAL_MODEL = "gemini-2.5-flash-preview-04-17"
FALLBACK_MODEL = "gemini-1.5-flash"

# عتبة نسبة التطابق التي تستدعي التحقق البصري
VISUAL_VERIFY_THRESHOLD = 90.0

_PROMPT_TEMPLATE = """أنت خبير خبرة في تحليل منتجات متاجر التجزئة.

مهمتك: تحديد ما إذا كان المنتجان التاليان متطابقَين (نفس المنتج من نفس العلامة التجارية).

## منتج المتجر (مهووس):
- الاسم: {name_mine}
{img_mine_section}

## منتج المنافس:
- الاسم: {name_comp}
{img_comp_section}

## تعليمات:
- انظر للاسم والصورة معاً
- تجاهل الاختلافات الطفيفة في الهجاء أو اللغة إذا كانا نفس المنتج
- انتبه للحجم/الكمية (مثل 100ml ≠ 200ml)
- انتبه للنوع (EDP ≠ EDT ≠ Parfum)

أجب فقط بـ JSON صارم بهذا الشكل (بدون markdown):
{{
  "is_match": true/false,
  "confidence": 0-100,
  "reason": "تبرير قصير وواضح باللغة العربية",
  "key_difference": "أهم فارق إن وجد أو null"
}}"""


def _fetch_image_as_b64(url: str, timeout: int = 8) -> Optional[str]:
    """تحميل صورة من URL وتحويلها لـ base64."""
    if not url or not str(url).startswith("http"):
        return None
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; MahwousBot/2.0)"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
        if not data:
            return None
        return base64.b64encode(data).decode("utf-8")
    except Exception as e:
        logger.debug("Image fetch failed for %s: %s", url, e)
        return None


def _build_prompt_parts(
    name_mine: str,
    name_comp: str,
    img_mine_b64: Optional[str],
    img_comp_b64: Optional[str],
) -> list:
    """بناء قائمة الأجزاء لـ Gemini API."""
    parts = []

    # ── جزء النص ──
    img_mine_section = "- الصورة: غير متاحة" if not img_mine_b64 else "- الصورة: مرفقة أدناه"
    img_comp_section = "- الصورة: غير متاحة" if not img_comp_b64 else "- الصورة: مرفقة أدناه"

    text = _PROMPT_TEMPLATE.format(
        name_mine=name_mine,
        name_comp=name_comp,
        img_mine_section=img_mine_section,
        img_comp_section=img_comp_section,
    )
    parts.append({"text": text})

    # ── صورة منتجنا ──
    if img_mine_b64:
        # نكتشف نوع الصورة من البايتات الأولى
        sig = base64.b64decode(img_mine_b64[:20])
        mime = "image/jpeg"
        if sig[:8] == b"\x89PNG\r\n\x1a\n":
            mime = "image/png"
        elif sig[:4] == b"RIFF":
            mime = "image/webp"
        parts.append({"text": "[صورة منتج مهووس]"})
        parts.append({
            "inline_data": {
                "mime_type": mime,
                "data": img_mine_b64,
            }
        })

    # ── صورة المنافس ──
    if img_comp_b64:
        sig = base64.b64decode(img_comp_b64[:20])
        mime = "image/jpeg"
        if sig[:8] == b"\x89PNG\r\n\x1a\n":
            mime = "image/png"
        elif sig[:4] == b"RIFF":
            mime = "image/webp"
        parts.append({"text": "[صورة منتج المنافس]"})
        parts.append({
            "inline_data": {
                "mime_type": mime,
                "data": img_comp_b64,
            }
        })

    return parts


def _parse_response(raw_text: str) -> Dict[str, Any]:
    """استخراج JSON من رد Gemini مع معالجة الحالات الخاصة."""
    txt = re.sub(r"```(?:json)?", "", raw_text, flags=re.IGNORECASE).replace("```", "").strip()
    # استخراج أول كتلة JSON
    match = re.search(r"\{.*\}", txt, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return {"is_match": False, "confidence": 0, "reason": "فشل في تحليل الرد", "key_difference": None}


class GeminiVisualVerifier:
    """
    محقق Gemini البصري — القاضي النهائي في حالات التطابق الغامض.

    الاستخدام:
        verifier = GeminiVisualVerifier()
        if verifier.should_verify(match_score):
            result = verifier.verify(
                name_mine="Dior Sauvage EDP 100ml",
                name_comp="Sauvage Dior EDP 100 ml",
                img_mine_url="https://...",
                img_comp_url="https://...",
            )
            # result: {"is_match": True, "confidence": 97, "reason": "...", ...}
    """

    def __init__(self, api_key: str = ""):
        self._api_key = api_key or self._load_api_key()
        self.enabled = bool(self._api_key)
        if not self.enabled:
            logger.warning("GeminiVisualVerifier: لا يوجد API key — التحقق البصري معطل")

    @staticmethod
    def _load_api_key() -> str:
        # 1. Streamlit secrets
        try:
            import streamlit as st
            for k in ("GEMINI_API_KEY", "GEMINI_API_KEYS", "GOOGLE_API_KEY"):
                v = str(st.secrets.get(k, "")).strip()
                if v:
                    return v.split(",")[0].strip()
        except Exception:
            pass
        # 2. Environment variables
        for k in ("GEMINI_API_KEY", "GEMINI_API_KEYS", "GOOGLE_API_KEY"):
            v = os.environ.get(k, "").strip()
            if v:
                return v.split(",")[0].strip()
        return ""

    @staticmethod
    def should_verify(match_score: float) -> bool:
        """هل يجب التحقق البصري لهذه النسبة؟"""
        return match_score < VISUAL_VERIFY_THRESHOLD

    def verify(
        self,
        name_mine: str,
        name_comp: str,
        img_mine_url: str = "",
        img_comp_url: str = "",
        fetch_images: bool = True,
        max_retries: int = 2,
    ) -> Dict[str, Any]:
        """
        التحقق البصري الكامل.

        Returns:
            {
                "is_match": bool,
                "confidence": int (0-100),
                "reason": str,
                "key_difference": str | None,
                "method": "visual_gemini" | "text_only_gemini" | "fallback"
            }
        """
        if not self.enabled:
            return self._fallback_result("لا يوجد API key")

        # تحميل الصور
        img_mine_b64 = None
        img_comp_b64 = None
        if fetch_images:
            img_mine_b64 = _fetch_image_as_b64(img_mine_url) if img_mine_url else None
            img_comp_b64 = _fetch_image_as_b64(img_comp_url) if img_comp_url else None

        method = "visual_gemini" if (img_mine_b64 or img_comp_b64) else "text_only_gemini"

        for attempt in range(max_retries + 1):
            try:
                result = self._call_gemini(
                    name_mine, name_comp, img_mine_b64, img_comp_b64
                )
                result["method"] = method
                return result
            except Exception as e:
                logger.warning("GeminiVisualVerifier attempt %d failed: %s", attempt + 1, e)
                if attempt < max_retries:
                    time.sleep(1.5 * (attempt + 1))

        return self._fallback_result("فشلت جميع المحاولات")

    def _call_gemini(
        self,
        name_mine: str,
        name_comp: str,
        img_mine_b64: Optional[str],
        img_comp_b64: Optional[str],
    ) -> Dict[str, Any]:
        """استدعاء Gemini API مباشرة عبر requests."""
        import urllib.request as _req
        import json as _json

        parts = _build_prompt_parts(name_mine, name_comp, img_mine_b64, img_comp_b64)
        payload = {
            "contents": [{"parts": parts}],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": 300,
                "responseMimeType": "application/json",
            },
        }

        # جرّب النموذج الرئيسي أولاً ثم الاحتياطي
        for model in (VISUAL_MODEL, FALLBACK_MODEL):
            url = (
                f"https://generativelanguage.googleapis.com/v1beta/models/"
                f"{model}:generateContent?key={self._api_key}"
            )
            try:
                data = _json.dumps(payload).encode("utf-8")
                http_req = _req.Request(
                    url,
                    data=data,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with _req.urlopen(http_req, timeout=30) as resp:
                    body = _json.loads(resp.read().decode("utf-8"))

                candidates = body.get("candidates", [])
                if not candidates:
                    continue
                raw_text = (
                    candidates[0]
                    .get("content", {})
                    .get("parts", [{}])[0]
                    .get("text", "")
                )
                return _parse_response(raw_text)
            except Exception as e:
                logger.debug("Model %s failed: %s", model, e)
                continue

        return self._fallback_result("كلا النموذجَين فشلا")

    @staticmethod
    def _fallback_result(reason: str) -> Dict[str, Any]:
        return {
            "is_match": False,
            "confidence": 0,
            "reason": reason,
            "key_difference": None,
            "method": "fallback",
        }


# ── دالة سريعة للاستخدام في pricing_pipeline ──────────────
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
    """
    استدعاء التحقق البصري فقط إذا كانت نسبة التطابق < VISUAL_VERIFY_THRESHOLD.
    يُعيد None إذا لم يكن التحقق مطلوباً.
    """
    if not GeminiVisualVerifier.should_verify(match_score):
        return None
    return get_visual_verifier().verify(name_mine, name_comp, img_mine_url, img_comp_url)
