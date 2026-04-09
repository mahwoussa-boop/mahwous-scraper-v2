"""
utils/sku_resolver.py
======================
[FIX CRITICAL-03] حل مشكلة غياب SKU/Barcode في ملفات المنافسين.

المشكلة:
    ملفات المنافسين تحتوي فقط على: رابط | صورة | اسم_المنتج | سعر
    لا يوجد SKU أو Barcode أو معرّف موحّد.
    المطابقة تعتمد كلياً على الاسم، مع نقاط مُضلِّلة لحقول الماركة والحجم.

الحل — ثلاث طبقات:
    1. استخراج مكوّنات دلالية من الاسم (brand, size, type, model)
    2. توليد fingerprint مستقر من هذه المكوّنات
    3. تطبيع عمود الاسم قبل إدخاله للمطابقة
"""

from __future__ import annotations

import hashlib
import re
from typing import Optional

# ─────────────────────────────────────────────────────────────────────────────
#  قواعد بيانات الماركات المعروفة
# ─────────────────────────────────────────────────────────────────────────────
_KNOWN_BRANDS = [
    # عالمية
    "chanel", "dior", "gucci", "yves saint laurent", "ysl", "versace",
    "armani", "giorgio armani", "prada", "burberry", "givenchy", "hermes",
    "lancome", "cartier", "montblanc", "hugo boss", "boss", "calvin klein",
    "dolce gabbana", "d&g", "tom ford", "creed", "baccarat rouge",
    "maison margiela", "jo malone", "acqua di parma", "byredo",
    "narciso rodriguez", "jimmy choo", "valentino", "rabanne", "paco rabanne",
    "thierry mugler", "mugler", "jean paul gaultier", "jpg", "kenzo",
    "issey miyake", "davidoff", "lacoste", "ralph lauren", "polo",
    "carolina herrera", "coach", "michael kors", "marc jacobs",
    "viktor rolf", "viktor&rolf", "escada", "bvlgari", "bulgari",
    "ferrari", "porsche design", "dunhill",
    # عربية/شرقية
    "rasasi", "ajmal", "lattafa", "swiss arabian", "swiss swiss arabian",
    "arabian oud", "oud elite", "hamidi", "surrati", "nabeel",
    "al haramain", "al rehab", "zimaya", "oud ispahan",
    "musk", "oud", "bakhoor",
    # ماركات نيشاني وعبر الإنترنت
    "nishane", "initio", "xerjoff", "roja dove", "roja parfums",
    "parfums de marly", "moresque", "morgenthau", "memo paris",
    "strangers parfumerie",
]

_BRAND_RE = re.compile(
    r"\b(" + "|".join(re.escape(b) for b in sorted(_KNOWN_BRANDS, key=len, reverse=True)) + r")\b",
    re.IGNORECASE | re.UNICODE,
)

# ─────────────────────────────────────────────────────────────────────────────
#  Regex المساعدة
# ─────────────────────────────────────────────────────────────────────────────
_AR_DIGIT = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_FA_DIGIT = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")

_SIZE_RE = re.compile(
    r"\b(\d+(?:\.\d+)?)\s*(ml|مل|gm|جم|g\b|oz|fl\.?\s*oz)\b",
    re.IGNORECASE | re.UNICODE,
)
_TYPE_RE = re.compile(
    r"\b(edp|edt|edc|parfum|cologne|perfume|eau\s+de\s+parfum|eau\s+de\s+toilette)\b",
    re.IGNORECASE,
)
_MODEL_NUM_RE = re.compile(r"\b([A-Z]{1,4}\d{2,6}[A-Z]?)\b")
_NBSP_RE = re.compile(r"[\u00a0\u200b\u200c\u200d\u202f\ufeff]")


# ─────────────────────────────────────────────────────────────────────────────
#  الدوال الأساسية
# ─────────────────────────────────────────────────────────────────────────────

def _base_normalize(text: str) -> str:
    """تطبيع أساسي مشترك — يُستخدم داخل هذه الوحدة فقط."""
    if not text:
        return ""
    text = _NBSP_RE.sub(" ", str(text))
    text = text.translate(_AR_DIGIT).translate(_FA_DIGIT)
    text = text.lower()
    text = re.sub(r"[^\w\s\u0600-\u06ff]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_brand(product_name: str) -> str:
    """استخراج الماركة من اسم المنتج باستخدام قاعدة بيانات الماركات."""
    m = _BRAND_RE.search(product_name)
    return m.group(0).lower().strip() if m else ""


def extract_size(product_name: str) -> float:
    """استخراج الحجم الرقمي من اسم المنتج (ml / مل / gm …)."""
    norm = _base_normalize(product_name)
    m = _SIZE_RE.search(norm)
    return float(m.group(1)) if m else 0.0


def extract_type(product_name: str) -> str:
    """استخراج نوع العطر (EDP / EDT / …) من اسم المنتج."""
    m = _TYPE_RE.search(product_name)
    if not m:
        return ""
    raw = m.group(0).lower().replace(" ", "")
    return {
        "eaudeparfum": "edp",
        "eaudetoilette": "edt",
        "eaudecologne": "edc",
    }.get(raw, raw)


def extract_model_number(product_name: str) -> str:
    """استخراج رقم الموديل إن وُجد (مثل EDP1234A)."""
    m = _MODEL_NUM_RE.search(product_name)
    return m.group(0).upper() if m else ""


def parse_product_name(product_name: str) -> dict:
    """
    تفكيك اسم المنتج إلى مكوّناته الدلالية.

    Returns
    -------
    dict مع المفاتيح:
        raw        : str   — الاسم الخام
        normalized : str   — الاسم المُطبَّع
        brand      : str   — الماركة المُستخرَجة
        size_ml    : float — الحجم بـ ml (0.0 إذا غير معروف)
        frag_type  : str   — EDP / EDT / … ('' إذا غير معروف)
        model_num  : str   — رقم الموديل ('' إذا غير موجود)
        fingerprint: str   — معرّف مستقر 16 حرف hex
    """
    norm = _base_normalize(product_name)
    brand = extract_brand(product_name)
    size = extract_size(product_name)
    ftype = extract_type(product_name)
    model = extract_model_number(product_name)

    # fingerprint: نعتمد على المكوّنات الهيكلية فقط — لا على النص الخام
    # هذا يضمن ثباته حتى مع اختلاف المسافات أو الهجاء الطفيف في الاسم.
    # حجم الـ ml يُقرَّب إلى أقرب 10 (تسامح: 95ml ≈ 100ml نفس المنتج)
    size_bucket = str(round(size / 10) * 10) if size > 0 else "?"
    # الاسم المُنظَّف: نحذف وحدات الحجم والنوع ثم نأخذ أول 30 حرفاً فقط
    name_stripped = re.sub(r"\d+\s*(ml|مل|gm|جم|g|oz)\b", "", norm, flags=re.I | re.UNICODE)
    name_stripped = re.sub(r"\b(edp|edt|edc|parfum|cologne)\b", "", name_stripped, flags=re.I)
    name_stripped = re.sub(r"\s+", " ", name_stripped).strip()[:30]
    fp_src = f"{brand}|{size_bucket}|{ftype}|{model}|{name_stripped}"
    fingerprint = hashlib.sha256(fp_src.encode("utf-8")).hexdigest()[:16]

    return {
        "raw":         product_name,
        "normalized":  norm,
        "brand":       brand,
        "size_ml":     size,
        "frag_type":   ftype,
        "model_num":   model,
        "fingerprint": fingerprint,
    }


def enrich_competitor_df(df, name_col: str = "منتج_المنافس") -> "pd.DataFrame":
    """
    يُضيف أعمدة مُستخرَجة إلى DataFrame المنافس قبل إدخاله للمطابقة.

    الأعمدة المُضافة:
        _brand       : الماركة المُستخرَجة
        _size_ml     : الحجم بـ ml
        _frag_type   : نوع العطر
        _model_num   : رقم الموديل
        _fingerprint : معرّف مستقر

    هذه الأعمدة تُعوِّض غياب SKU/Barcode وتُحسِّن دقة المطابقة في Pass2.
    """
    import pandas as pd

    if name_col not in df.columns:
        return df

    parsed = df[name_col].fillna("").apply(parse_product_name)
    parsed_df = pd.DataFrame(parsed.tolist(), index=df.index)

    df = df.copy()
    df["_brand"]       = parsed_df["brand"]
    df["_size_ml"]     = parsed_df["size_ml"]
    df["_frag_type"]   = parsed_df["frag_type"]
    df["_model_num"]   = parsed_df["model_num"]
    df["_fingerprint"] = parsed_df["fingerprint"]

    # إذا عمود الماركة فارغ، نملأه من الاستخراج التلقائي
    if "الماركة" not in df.columns or df["الماركة"].fillna("").eq("").all():
        df["الماركة"] = df["_brand"]
    else:
        mask = df["الماركة"].fillna("").eq("")
        df.loc[mask, "الماركة"] = df.loc[mask, "_brand"]

    return df


def make_product_key(competitor_name: str, parsed: dict, max_len: int = 120) -> str:
    """
    يُولّد مفتاحاً مستقراً للمنتج يُستخدم في DB وsession.

    يعتمد على fingerprint بدلاً من الاسم الخام → ثابت حتى عند تغيير الهجاء.
    """
    prefix = f"comp_{competitor_name}_{parsed['fingerprint']}"
    return prefix[:max_len]
