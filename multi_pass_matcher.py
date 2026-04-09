"""
engines/multi_pass_matcher.py
==============================
محرك المطابقة متعدد المراحل v1.1
===================================
يُمرّر كل منتج عبر 4 مرور متصاعدة التعقيد:

  المرور 1 — Exact + RapidFuzz:       دقيق وسريع جداً
  المرور 2 — Weighted Attribute Score: متعدد الأوزان (اسم+ماركة+حجم+نوع)
  المرور 3 — TF-IDF Cosine Similarity: دلالي للأسماء المتشابهة لغوياً
  المرور 4 — AI Embeddings (Gemini):   فهم السياق الكامل

إذا نجح أي مرور → يُوقف السلسلة ويُعيد النتيجة.
إذا فشلت الأربعة → يُطلق حدث MATCH_FAILED.

التحسينات v1.1:
  - دعم الأرقام العربية/الفارسية في normalize() و extract_size()
  - _SIZE_RE يدعم مل/جم/جرام (إزالة \b الأولى للتوافق مع النص العربي)
  - Pass-2: to_dict('records') بدل iterrows() → ~20x أسرع
  - Pass-3/4: TF-IDF cache يُبنى مرة واحدة في match_dataframe بدل 1000+
  - Batching: _persist_attempts يُستدعى مرة واحدة لكل batch لا لكل منتج
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

_logger = logging.getLogger(__name__)

# ── استيراد كسول (lazy) للمكتبات الثقيلة ─────────────────────────────────
def _rapidfuzz():
    from rapidfuzz import fuzz, process
    return fuzz, process


def _sklearn_tfidf():
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    return TfidfVectorizer, cosine_similarity


# ─────────────────────────────────────────────────────────────────────────────
#  إعدادات (تُقرأ من config لتجنب القيم الصلبة)
# ─────────────────────────────────────────────────────────────────────────────
from config import (
    MATCH_PASS1_FUZZY_THRESHOLD,
    MATCH_PASS2_CONFIRMED,
    MATCH_PASS2_REVIEW,
    MATCH_PASS3_COSINE_THRESHOLD,
    MATCH_PASS4_EMBED_CONFIRMED,
    MATCH_PASS4_EMBED_REVIEW,
    MATCH_WEIGHTS,
    ProductState,
)


# ─────────────────────────────────────────────────────────────────────────────
#  بنية نتيجة المطابقة
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class MatchResult:
    matched:        bool         = False
    state:          str          = ProductState.MISSING
    score:          float        = 0.0
    pass_number:    int          = 0
    method:         str          = ""
    candidate:      str          = ""
    candidate_row:  Optional[pd.Series] = field(default=None, repr=False)
    attempts:       list[dict]   = field(default_factory=list)
    duration_ms:    int          = 0


# ─────────────────────────────────────────────────────────────────────────────
#  دوال تطبيع النصوص
# ─────────────────────────────────────────────────────────────────────────────

# [FIX LOGICAL-01] جدول ترجمة الأرقام العربية (٠-٩) والفارسية (۰-۹) إلى لاتينية
_AR_DIGIT_TABLE = str.maketrans(
    "٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹",
    "01234567890123456789",
)

# [FIX LOGICAL-02] إضافة مل/جم/جرام — وحذف \b الأولى لأن 100مل بدون word boundary
_SIZE_RE  = re.compile(
    r"(\d+(?:\.\d+)?)\s*(ml|مل|gm|جم|جرام|g|oz|fl\.?\s*oz)(?!\w)",
    re.I | re.UNICODE,
)
_TYPE_RE  = re.compile(r"\b(edp|edt|edc|parfum|cologne|perfume|eau\s+de\s+\w+)\b", re.I)
_NBSP_RE  = re.compile(r"[\u00a0\u200b\u200c\u200d\u202f\ufeff]")


def normalize(text: str) -> str:
    """تطبيع النص للمقارنة: أرقام عربية → لاتينية + تصغير + إزالة ترقيم."""
    if not text:
        return ""
    text = _NBSP_RE.sub(" ", str(text))
    text = text.translate(_AR_DIGIT_TABLE)   # [FIX LOGICAL-01]
    text = text.lower()
    text = re.sub(r"[^\w\s\u0600-\u06ff]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_size(text: str) -> float:
    """استخراج الحجم الرقمي (ml/مل/gm/جم) من نص المنتج."""
    text = str(text).translate(_AR_DIGIT_TABLE)   # [FIX LOGICAL-01] أرقام عربية
    m = _SIZE_RE.search(text)
    return float(m.group(1)) if m else 0.0


def extract_type(text: str) -> str:
    """استخراج نوع العطر (EDP/EDT/…) من نص المنتج."""
    m = _TYPE_RE.search(str(text))
    if not m:
        return ""
    raw = m.group(0).lower().replace(" ", "")
    return {"eaudetoilette": "edt", "eaudeparfum": "edp",
            "eaudecologne": "edc"}.get(raw, raw)


# ─────────────────────────────────────────────────────────────────────────────
#  المرور الأول — Exact + RapidFuzz token_sort_ratio
# ─────────────────────────────────────────────────────────────────────────────
def _pass1_fuzzy(query: str, catalog: pd.DataFrame,
                  name_col: str = "المنتج") -> MatchResult:
    """
    يُطابق الاسم الأساسي فقط (بعد التطبيع) بـ token_sort_ratio.
    عتبة النجاح: MATCH_PASS1_FUZZY_THRESHOLD (افتراضي 92).
    """
    fuzz, process = _rapidfuzz()
    q_norm = normalize(query)

    names_norm = catalog[name_col].fillna("").apply(normalize).tolist()
    if not names_norm:
        return MatchResult(pass_number=1, method="pass1_fuzzy")

    result = process.extractOne(q_norm, names_norm,
                                 scorer=fuzz.token_sort_ratio,
                                 score_cutoff=MATCH_PASS1_FUZZY_THRESHOLD)
    if result:
        cand_text, score, idx = result
        return MatchResult(
            matched=True,
            state=ProductState.MATCHED,
            score=float(score),
            pass_number=1,
            method="pass1_token_sort_ratio",
            candidate=str(catalog.iloc[idx][name_col]),
            candidate_row=catalog.iloc[idx],
        )
    return MatchResult(pass_number=1, method="pass1_fuzzy")


# ─────────────────────────────────────────────────────────────────────────────
#  المرور الثاني — Weighted Attribute Scoring
# ─────────────────────────────────────────────────────────────────────────────
def _score_row(query_attrs: dict, row: pd.Series) -> float:
    """نقاط مرجّحة — يقبل pandas Series (للتوافق مع المستدعين القديمين)."""
    return _score_row_dict(query_attrs, dict(row))


def _score_row_dict(query_attrs: dict, rec: dict) -> float:
    """
    [FIX LOGICAL-03] نسخة dict من _score_row — ~20x أسرع من Series.
    يحسب نقاطاً مرجّحة بين سمات المنتج المُستعلَم وسجل المقارنة.
    يعيد قيمة 0–100.
    """
    fuzz, _ = _rapidfuzz()
    total_weight = sum(MATCH_WEIGHTS.values())
    score = 0.0

    # الاسم
    w = MATCH_WEIGHTS["name"]
    s = fuzz.token_sort_ratio(
        normalize(query_attrs.get("name", "")),
        normalize(str(rec.get("المنتج", "") or rec.get("منتج_المنافس", "")))
    )
    score += (s / 100) * w

    # الماركة
    w = MATCH_WEIGHTS["brand"]
    q_brand = normalize(query_attrs.get("brand", ""))
    r_brand = normalize(str(rec.get("الماركة", "")))
    if q_brand and r_brand:
        s = fuzz.partial_ratio(q_brand, r_brand)
        score += (s / 100) * w
    else:
        score += 0.5 * w

    # الحجم
    w = MATCH_WEIGHTS["size"]
    q_size = query_attrs.get("size", 0.0)
    r_size = extract_size(str(rec.get("الحجم", "") or rec.get("منتج_المنافس", "")))
    if q_size > 0 and r_size > 0:
        diff_pct = abs(q_size - r_size) / max(q_size, r_size)
        score += max(0, 1 - diff_pct * 2) * w
    elif q_size == 0 or r_size == 0:
        score += 0.5 * w

    # النوع
    w = MATCH_WEIGHTS["type"]
    q_type = query_attrs.get("type", "")
    r_type = extract_type(str(rec.get("النوع", "") or rec.get("منتج_المنافس", "")))
    if q_type and r_type:
        score += w if q_type == r_type else 0
    else:
        score += 0.5 * w

    return round(score / total_weight * 100, 2)


import threading

# ── [FIX P0] Cache آمن للخيوط — مفتاحه id(catalog) لا len ────────────────
_CATALOG_RECORDS_LOCK  = threading.Lock()
_CATALOG_RECORDS_CACHE: dict = {"_id": None, "_records": []}


def _pass2_weighted(query_attrs: dict, catalog: pd.DataFrame) -> MatchResult:
    """
    يُطبّق نقاط الأوزان على كل سجل ويختار الأعلى.
    [FIX LOGICAL-03] يستخدم to_dict('records') بدل iterrows() → ~20x أسرع.
    MATCH_PASS2_CONFIRMED (85) → matched
    MATCH_PASS2_REVIEW    (68) → review
    """
    # [FIX P0] تحديث cache بمفتاح id(catalog) + Lock للأمان مع خيوط Streamlit
    with _CATALOG_RECORDS_LOCK:
        if id(catalog) != _CATALOG_RECORDS_CACHE["_id"]:
            _CATALOG_RECORDS_CACHE["_records"] = catalog.to_dict("records")
            _CATALOG_RECORDS_CACHE["_id"] = id(catalog)
        records = _CATALOG_RECORDS_CACHE["_records"]
    best_score = 0.0
    best_idx   = -1

    for i, rec in enumerate(records):
        s = _score_row_dict(query_attrs, rec)
        if s > best_score:
            best_score = s
            best_idx   = i

    if best_idx < 0:
        return MatchResult(pass_number=2, method="pass2_weighted")

    best_rec  = records[best_idx]
    best_name = str(best_rec.get("المنتج", "") or best_rec.get("منتج_المنافس", ""))
    best_row  = catalog.iloc[best_idx]

    if best_score >= MATCH_PASS2_CONFIRMED:
        return MatchResult(
            matched=True, state=ProductState.MATCHED,
            score=best_score, pass_number=2,
            method="pass2_weighted", candidate=best_name,
            candidate_row=best_row,
        )
    if best_score >= MATCH_PASS2_REVIEW:
        return MatchResult(
            matched=True, state=ProductState.REVIEW,
            score=best_score, pass_number=2,
            method="pass2_weighted", candidate=best_name,
            candidate_row=best_row,
        )
    return MatchResult(pass_number=2, method="pass2_weighted",
                       score=best_score, candidate=best_name)


# ─────────────────────────────────────────────────────────────────────────────
#  المرور الثالث — TF-IDF Cosine Similarity
# ─────────────────────────────────────────────────────────────────────────────
def _pass3_tfidf(
    query:       str,
    catalog:     pd.DataFrame,
    name_col:    str = "المنتج",
    *,
    _tfidf_cache = None,   # [OPT-02] (vec, cat_matrix, names) مُعدّ مسبقاً
) -> MatchResult:
    """
    يُنشئ TF-IDF matrix للكتالوج ويحسب cosine similarity مع الاستعلام.
    [OPT-02] إذا مُرّر _tfidf_cache يستخدم vec.transform فقط (لا fit_transform).
    عتبة النجاح: MATCH_PASS3_COSINE_THRESHOLD (0.72) → REVIEW.
    """
    TfidfVectorizer, cosine_similarity = _sklearn_tfidf()

    try:
        if _tfidf_cache is not None:
            vec, cat_matrix, names = _tfidf_cache
        else:
            names = catalog[name_col].fillna("").apply(normalize).tolist()
            if not any(names):
                return MatchResult(pass_number=3, method="pass3_tfidf")
            vec       = TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 4))
            cat_matrix = vec.fit_transform(names)

        q_vec = vec.transform([normalize(query)])
        sims  = cosine_similarity(q_vec, cat_matrix).flatten()
    except Exception as exc:
        _logger.warning("TF-IDF failed: %s", exc)
        return MatchResult(pass_number=3, method="pass3_tfidf")

    best_idx   = int(sims.argmax())
    best_score = float(sims[best_idx])

    if best_score >= MATCH_PASS3_COSINE_THRESHOLD:
        return MatchResult(
            matched=True, state=ProductState.REVIEW,
            score=round(best_score * 100, 2),
            pass_number=3, method="pass3_tfidf",
            candidate=str(catalog.iloc[best_idx][name_col]),
            candidate_row=catalog.iloc[best_idx],
        )
    return MatchResult(pass_number=3, method="pass3_tfidf",
                       score=round(best_score * 100, 2))


# ─────────────────────────────────────────────────────────────────────────────
#  المرور الرابع — AI Embeddings (Gemini)
# ─────────────────────────────────────────────────────────────────────────────
def _pass4_ai_embedding(
    query:    str,
    catalog:  pd.DataFrame,
    name_col: str = "المنتج",
    *,
    _tfidf_cache = None,   # [OPT-02] نفس cache المرور الثالث
) -> MatchResult:
    """
    يُولّد embedding للاستعلام وأفضل 30 مرشح من الكتالوج (بـ TF-IDF pre-screen)
    ثم يحسب cosine similarity بينهم.
    [OPT-02] يستخدم _tfidf_cache إذا كان متاحاً بدلاً من إعادة بناء Matrix.
    """
    import numpy as np
    from engines.ai_engine import get_product_embedding

    q_vec = get_product_embedding(query)
    if q_vec is None:
        _logger.warning("Pass4: embedding فشل للاستعلام '%s'", query[:50])
        return MatchResult(pass_number=4, method="pass4_embedding_failed")

    names = catalog[name_col].fillna("").tolist()
    best_score = 0.0
    best_idx   = -1

    # pre-screen بـ TF-IDF للحصول على أفضل 30 مرشح
    try:
        TfidfVectorizer, cosine_similarity = _sklearn_tfidf()
        if _tfidf_cache is not None:
            vec, cat_matrix, names_norm = _tfidf_cache
            q_mat  = vec.transform([normalize(query)])
            _sims  = cosine_similarity(q_mat, cat_matrix).flatten()
        else:
            names_norm = [normalize(n) for n in names]
            _vec  = TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 4))
            _mat  = _vec.fit_transform(names_norm + [normalize(query)])
            _sims = cosine_similarity(_mat[-1], _mat[:-1]).flatten()
        top30 = sorted(range(len(names)), key=lambda x: _sims[x], reverse=True)[:30]
    except Exception:
        top30 = list(range(min(30, len(names))))

    for i in top30:
        cand_vec = get_product_embedding(names[i])
        if cand_vec is None:
            continue
        sim = float(np.dot(q_vec, cand_vec) /
                    (np.linalg.norm(q_vec) * np.linalg.norm(cand_vec) + 1e-9))
        if sim > best_score:
            best_score = sim
            best_idx   = i

    if best_idx < 0:
        return MatchResult(pass_number=4, method="pass4_ai_embedding")

    cand_name = str(catalog.iloc[best_idx][name_col])

    if best_score >= MATCH_PASS4_EMBED_CONFIRMED:
        return MatchResult(
            matched=True, state=ProductState.MATCHED,
            score=round(best_score * 100, 2),
            pass_number=4, method="pass4_ai_embedding",
            candidate=cand_name, candidate_row=catalog.iloc[best_idx],
        )
    if best_score >= MATCH_PASS4_EMBED_REVIEW:
        return MatchResult(
            matched=True, state=ProductState.REVIEW,
            score=round(best_score * 100, 2),
            pass_number=4, method="pass4_ai_embedding",
            candidate=cand_name, candidate_row=catalog.iloc[best_idx],
        )
    return MatchResult(pass_number=4, method="pass4_ai_embedding",
                       score=round(best_score * 100, 2), candidate=cand_name)


# ─────────────────────────────────────────────────────────────────────────────
#  المحرك الرئيسي — Multi-Pass Match
# ─────────────────────────────────────────────────────────────────────────────
def match_product(
    product_key:    str,
    product_name:   str,
    catalog:        pd.DataFrame,
    *,
    brand:          str   = "",
    size:           float = 0.0,
    ptype:          str   = "",
    name_col:       str   = "المنتج",
    skip_pass4:     bool  = False,
    competitor:     str   = "",
    _defer_persist: bool  = False,   # [FIX CRITICAL-02] تأجيل الكتابة للـ batch
    _tfidf_cache          = None,    # [OPT-02] TF-IDF cache مُعدّ من match_dataframe
    _skip_init:     bool  = False,   # [FIX P1] تجاوز init_product عند استخدام bulk_init
) -> MatchResult:
    """
    تشغيل دورة المطابقة الكاملة (4 مرور) لمنتج واحد.

    Parameters
    ----------
    _defer_persist : bool — إذا True لا تُكتب محاولات DB فوراً (للـ batch).
    _tfidf_cache   : tuple | None — (vec, matrix, names) مُعدّ مسبقاً.
    """
    from utils.event_bus import EventType, emit
    from utils.product_state import init_product, transition

    t0 = time.monotonic()
    if not _skip_init:   # [FIX P1] bulk_init يُستدعى مسبقاً في match_dataframe
        init_product(product_key, product_name, competitor=competitor)

    if catalog is None or catalog.empty or name_col not in catalog.columns:
        result = MatchResult(pass_number=0, method="empty_catalog")
        emit(EventType.MATCH_FAILED, {
            "product_key": product_key, "product_name": product_name,
            "failed_passes": 0,
        })
        return result

    query_attrs = {
        "name":  product_name,
        "brand": brand,
        "size":  size or extract_size(product_name),
        "type":  ptype or extract_type(product_name),
    }

    passes = [
        ("p1", lambda: _pass1_fuzzy(product_name, catalog, name_col)),
        ("p2", lambda: _pass2_weighted(query_attrs, catalog)),
        ("p3", lambda: _pass3_tfidf(product_name, catalog, name_col,
                                     _tfidf_cache=_tfidf_cache)),
    ]
    if not skip_pass4:
        passes.append(
            ("p4", lambda: _pass4_ai_embedding(product_name, catalog, name_col,
                                                _tfidf_cache=_tfidf_cache))
        )

    final_result = MatchResult()
    attempts     = []

    for pass_label, pass_fn in passes:
        pass_t0 = time.monotonic()
        try:
            res = pass_fn()
        except Exception as exc:
            _logger.exception("Pass %s exception for '%s': %s",
                              pass_label, product_name, exc)
            # [FIX P3] استخراج رقم المرور بأمان لأي تنسيق (p1, pass1, p10)
            _pnum = re.search(r'\d+', pass_label)
            res = MatchResult(pass_number=int(_pnum.group()) if _pnum else 0,
                              method=f"{pass_label}_error")

        dur_ms = int((time.monotonic() - pass_t0) * 1000)
        res.duration_ms = dur_ms

        attempts.append({
            "product_key":    product_key,
            "product_name":   product_name,
            "pass_number":    res.pass_number,
            "method":         res.method,
            "best_candidate": res.candidate,
            "score":          res.score,
            "result":         "matched" if res.matched else "no_match",
            "duration_ms":    dur_ms,
        })

        if res.matched:
            final_result = res
            break

    final_result.attempts    = attempts
    final_result.duration_ms = int((time.monotonic() - t0) * 1000)

    # [FIX CRITICAL-02] يكتب للـ DB فوراً فقط عند الاستدعاء المباشر
    if not _defer_persist:
        _persist_attempts(attempts)

    if final_result.matched:
        transition(
            product_key, final_result.state,
            match_score=final_result.score,
            match_pass=final_result.pass_number,
        )
        if final_result.state == ProductState.REVIEW:
            emit(EventType.REVIEW_REQUIRED, {
                "product_key":    product_key,
                "product_name":   product_name,
                "trigger_type":   EventType.REVIEW_REQUIRED,
                "trigger_detail": (f"Pass {final_result.pass_number} → "
                                   f"{final_result.method} score={final_result.score}"),
                "priority":       3,
            })
    else:
        emit(EventType.MATCH_FAILED, {
            "product_key":   product_key,
            "product_name":  product_name,
            "failed_passes": len(passes),
        })

    return final_result


def match_dataframe(
    competitor_df: pd.DataFrame,
    our_catalog:   pd.DataFrame,
    *,
    comp_name_col: str  = "منتج_المنافس",
    our_name_col:  str  = "المنتج",
    brand_col:     str  = "الماركة",
    size_col:      str  = "الحجم",
    type_col:      str  = "النوع",
    competitor:    str  = "",
    skip_pass4:    bool = False,
    progress_cb         = None,
) -> pd.DataFrame:
    """
    تطبيق دورة المطابقة على DataFrame كامل من منتجات المنافس.

    [FIX CRITICAL-02] جميع كتابات DB تُجمع وتُرسل دفعة واحدة بعد الحلقة.
    [OPT-02] TF-IDF matrix يُبنى مرة واحدة قبل الحلقة.
    [FIX P1] bulk_init يُهيّئ جميع المنتجات في استعلام DB واحد.

    Returns
    -------
    competitor_df مُضافاً إليه أعمدة:
        match_state | match_score | match_pass | match_method | match_candidate
    """
    from utils.product_state import bulk_init
    TfidfVectorizer, _ = _sklearn_tfidf()

    # [FIX P1] تهيئة جميع المنتجات في استعلام DB واحد (بدل init_product لكل منتج)
    _bulk_records = []
    for _, _row in competitor_df.iterrows():
        _pn = str(_row.get(comp_name_col, "") or "").strip()
        _br = str(_row.get(brand_col, "") or "").strip()
        _sz = extract_size(str(_row.get(size_col, "") or "") + " " + _pn)
        _pk = f"comp_{competitor}_{_pn}_{_br}_{int(_sz)}"[:120]
        _bulk_records.append({"product_key": _pk, "product_name": _pn,
                               "competitor": competitor})
    if _bulk_records:
        try:
            bulk_init(_bulk_records)
            _logger.info("bulk_init: %d products initialized", len(_bulk_records))
        except Exception as exc:
            _logger.warning("bulk_init failed, will fallback to per-product init: %s", exc)

    # [OPT-02] بناء TF-IDF cache مرة واحدة قبل الحلقة
    _tfidf_cache = None
    if our_catalog is not None and not our_catalog.empty and our_name_col in our_catalog.columns:
        try:
            names_norm = our_catalog[our_name_col].fillna("").apply(normalize).tolist()
            if any(names_norm):
                vec        = TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 4))
                cat_matrix = vec.fit_transform(names_norm)
                _tfidf_cache = (vec, cat_matrix, names_norm)
                _logger.info("TF-IDF cache built: %d entries", len(names_norm))
        except Exception as exc:
            _logger.warning("TF-IDF cache build failed, fallback to per-product: %s", exc)

    total      = len(competitor_df)
    results    = []
    all_attempts: list[dict] = []   # [FIX CRITICAL-02] buffer مركزي

    try:
        for i, (_, row) in enumerate(competitor_df.iterrows()):
            pname = str(row.get(comp_name_col, "") or "").strip()
            brand = str(row.get(brand_col, "") or "").strip()
            size  = extract_size(str(row.get(size_col, "") or "") + " " + pname)
            ptype = extract_type(str(row.get(type_col, "") or "") + " " + pname)
            pkey  = f"comp_{competitor}_{pname}_{brand}_{int(size)}"[:120]

            res = match_product(
                pkey, pname, our_catalog,
                brand=brand, size=size, ptype=ptype,
                name_col=our_name_col,
                skip_pass4=skip_pass4,
                competitor=competitor,
                _defer_persist=True,       # [FIX CRITICAL-02]
                _tfidf_cache=_tfidf_cache, # [OPT-02]
                _skip_init=True,           # [FIX P1] bulk_init سبق وهيّأهم
            )
            all_attempts.extend(res.attempts)   # [FIX CRITICAL-02]

            results.append({
                "match_state":     res.state,
                "match_score":     res.score,
                "match_pass":      res.pass_number,
                "match_method":    res.method,
                "match_candidate": res.candidate,
            })

            if progress_cb:
                try:
                    progress_cb(i + 1, total)
                except Exception:
                    pass

    finally:
        # [FIX CRITICAL-02] flush واحد بعد كل الحلقة (أو عند أي خروج)
        if all_attempts:
            try:
                _persist_attempts(all_attempts)
                _logger.info("Persisted %d match attempts in one batch", len(all_attempts))
            except Exception as exc:
                _logger.error("Failed to persist match attempts: %s", exc)

    result_df = pd.DataFrame(results, index=competitor_df.index)
    return pd.concat([competitor_df, result_df], axis=1)


# ─────────────────────────────────────────────────────────────────────────────
#  دوال مساعدة
# ─────────────────────────────────────────────────────────────────────────────
def _persist_attempts(attempts: list[dict]) -> None:
    """حفظ سجل محاولات المطابقة في قاعدة البيانات — دائماً batch."""
    if not attempts:
        return
    import sqlite3
    from datetime import datetime, timezone
    from utils.db_manager import DB_PATH

    now = datetime.now(timezone.utc).isoformat()
    rows = [(
        a["product_key"], a["product_name"],
        a["pass_number"], a["method"],
        a["best_candidate"], a["score"],
        a["result"], a["duration_ms"], now
    ) for a in attempts]

    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        conn.executemany("""
            INSERT INTO match_attempts
            (product_key, product_name, pass_number, method,
             best_candidate, score, result, duration_ms, attempted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
