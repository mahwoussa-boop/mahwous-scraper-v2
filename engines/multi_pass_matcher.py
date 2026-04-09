"""
engines/multi_pass_matcher.py
==============================
محرك المطابقة متعدد المراحل v1.0
===================================
يُمرّر كل منتج عبر 4 مرور متصاعدة التعقيد:

  المرور 1 — Exact + RapidFuzz:       دقيق وسريع جداً
  المرور 2 — Weighted Attribute Score: متعدد الأوزان (اسم+ماركة+حجم+نوع)
  المرور 3 — TF-IDF Cosine Similarity: دلالي للأسماء المتشابهة لغوياً
  المرور 4 — AI Embeddings (Gemini):   فهم السياق الكامل

إذا نجح أي مرور → يُوقف السلسلة ويُعيد النتيجة.
إذا فشلت الأربعة → يُطلق حدث MATCH_FAILED.
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
import hashlib
from functools import lru_cache

# [FIX OPT-02] نبني TF-IDF matrix مرة واحدة لكل كتالوج ونُخزّنها في cache
# بدلاً من إعادة البناء 1,000+ مرة (مرة لكل منتج منافس)
@lru_cache(maxsize=4)
def _build_tfidf_matrix(catalog_hash: str, names_tuple: tuple):
    """يبني ويُخزّن TF-IDF matrix للكتالوج. مُخزَّن بـ lru_cache."""
    TfidfVectorizer, _ = _sklearn_tfidf()
    vec    = TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 4))
    matrix = vec.fit_transform(list(names_tuple))
    return vec, matrix


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
_SIZE_RE  = re.compile(
    r"\b(\d+(?:\.\d+)?)\s*(ml|مل|gm|جم|g|oz|fl\.?\s*oz)\b",
    re.I | re.UNICODE
)
_TYPE_RE  = re.compile(r"\b(edp|edt|edc|parfum|cologne|perfume|eau\s+de\s+\w+)\b", re.I)
_NBSP_RE  = re.compile(r"[\u00a0\u200b\u200c\u200d\u202f\ufeff]")

# [LOGICAL-01] جداول تحويل الأرقام العربية والفارسية إلى لاتينية
_AR_DIGIT = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_FA_DIGIT = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")


def normalize(text: str) -> str:
    """تطبيع النص للمقارنة: تصغير + إزالة ترقيم + ضبط مسافات.
    [FIX LOGICAL-01] يحوّل الأرقام العربية/الفارسية إلى لاتينية قبل المعالجة.
    """
    if not text:
        return ""
    text = _NBSP_RE.sub(" ", str(text))
    text = text.translate(_AR_DIGIT).translate(_FA_DIGIT)   # ← FIX LOGICAL-01
    text = text.lower()
    text = re.sub(r"[^\w\s\u0600-\u06ff]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_size(text: str) -> float:
    """استخراج الحجم الرقمي (ml / مل / gm) من نص المنتج.
    [FIX LOGICAL-02] يدعم الآن 'مل' العربية و'جم'.
    """
    # نطبّق normalize أولاً لتحويل ٥٠ → 50 قبل البحث
    m = _SIZE_RE.search(normalize(str(text)))
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
                  name_col: str = "\u0627\u0644\u0645\u0646\u062a\u062c") -> MatchResult:
    """
    يطابق الاسم الاساسي فقط (بعد التطبيع) بـ token_sort_ratio.
    عتبة النجاح: MATCH_PASS1_FUZZY_THRESHOLD (افتراضي 92).

    [FIX LOGICAL-04] reset_index(drop=True) يضمن ان idx من extractOne
    (موضع في قائمة) يتطابق مع iloc — يمنع الخطأ الصامت عند الكتالوج
    المفلتَر (بعد dropna/filter) الذي له index غير متسلسل.
    """
    fuzz, process = _rapidfuzz()
    q_norm = normalize(query)

    # [FIX LOGICAL-04] اجعل iloc و positional index متطابقَين دائما
    cat = catalog.reset_index(drop=True)
    names_norm = cat[name_col].fillna("").apply(normalize).tolist()
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
            candidate=str(cat.iloc[idx][name_col]),
            candidate_row=cat.iloc[idx],
        )
    return MatchResult(pass_number=1, method="pass1_fuzzy")


# ─────────────────────────────────────────────────────────────────────────────
#  المرور الثاني — Weighted Attribute Scoring
# ─────────────────────────────────────────────────────────────────────────────
def _score_row(query_attrs: dict, row: pd.Series) -> float:
    """
    نسخة احتياطية للصف الواحد — تُستخدم فقط خارج match_dataframe.
    داخل _pass2_weighted نستخدم النسخة المُتجهية (vectorized) الأسرع بـ ×100.
    """
    fuzz, _ = _rapidfuzz()
    total_weight = sum(MATCH_WEIGHTS.values())
    score = 0.0

    w = MATCH_WEIGHTS["name"]
    s = fuzz.token_sort_ratio(
        normalize(query_attrs.get("name", "")),
        normalize(str(row.get("المنتج", "") or row.get("منتج_المنافس", "")))
    )
    score += (s / 100) * w

    w = MATCH_WEIGHTS["brand"]
    q_brand = normalize(query_attrs.get("brand", ""))
    r_brand = normalize(str(row.get("الماركة", "")))
    if q_brand and r_brand:
        score += (fuzz.partial_ratio(q_brand, r_brand) / 100) * w
    else:
        score += 0.5 * w

    w = MATCH_WEIGHTS["size"]
    q_size = query_attrs.get("size", 0.0)
    r_size = extract_size(str(row.get("الحجم", "") or row.get("منتج_المنافس", "")))
    if q_size > 0 and r_size > 0:
        score += max(0, 1 - abs(q_size - r_size) / max(q_size, r_size) * 2) * w
    else:
        score += 0.5 * w

    w = MATCH_WEIGHTS["type"]
    q_type = query_attrs.get("type", "")
    r_type = extract_type(str(row.get("النوع", "") or row.get("منتج_المنافس", "")))
    if q_type and r_type:
        score += w if q_type == r_type else 0
    else:
        score += 0.5 * w

    return round(score / total_weight * 100, 2)


def _pass2_weighted(query_attrs: dict, catalog: pd.DataFrame) -> MatchResult:
    """
    [FIX LOGICAL-03] نسخة مُتجهية (vectorized) تُحلّ مشكلة O(n²).
    الأصلية كانت تُكرر 7,604 × 1,013 = 7.7M تكرار Python.
    الآن: عمليات numpy/pandas مُتجهية بالكامل.

    [FIX LOGICAL-04] reset_index(drop=True) يضمن أن iloc و loc متطابقان،
    مما يمنع تعارض الـ index بعد dropna/filter في الكتالوج.

    MATCH_PASS2_CONFIRMED (85) → matched
    MATCH_PASS2_REVIEW    (68) → review
    """
    import numpy as np
    fuzz, _ = _rapidfuzz()
    try:
        from rapidfuzz import process as _rfp
        _cdist = _rfp.cdist
    except ImportError:
        _cdist = None

    total_weight = sum(MATCH_WEIGHTS.values())

    # [FIX LOGICAL-04] reset_index يجعل iloc/loc متطابقَين دائماً
    cat = catalog.reset_index(drop=True)

    # ── الاسم: rapidfuzz.cdist (C-level) بدلاً من Python loop ──────────────
    q_name    = normalize(query_attrs.get("name", ""))
    cat_names = cat["المنتج"].fillna("").apply(normalize).tolist()

    if _cdist is not None:
        # cdist يُعيد matrix — نأخذ الصف الأول (استعلام واحد)
        name_arr = _cdist([q_name], cat_names,
                          scorer=fuzz.token_sort_ratio)[0].astype(float) / 100
    else:
        name_arr = np.array([
            fuzz.token_sort_ratio(q_name, n) / 100 for n in cat_names
        ], dtype=float)

    name_scores = name_arr * MATCH_WEIGHTS["name"]

    # ── الماركة ─────────────────────────────────────────────────────────────
    q_brand  = normalize(query_attrs.get("brand", ""))
    w_brand  = MATCH_WEIGHTS["brand"]
    if q_brand and "الماركة" in cat.columns:
        cat_brands = cat["الماركة"].fillna("").apply(normalize)
        has_brand  = cat_brands.str.len() > 0
        brand_arr  = np.full(len(cat), 0.5 * w_brand, dtype=float)
        if has_brand.any():
            brand_arr[has_brand.values] = np.array([
                fuzz.partial_ratio(q_brand, b) / 100 * w_brand
                for b in cat_brands[has_brand]
            ], dtype=float)
    else:
        brand_arr = np.full(len(cat), 0.5 * w_brand, dtype=float)

    # ── الحجم: vectorized numpy ──────────────────────────────────────────────
    q_size  = float(query_attrs.get("size", 0.0))
    w_size  = MATCH_WEIGHTS["size"]
    size_src = (cat.get("الحجم", pd.Series([""] * len(cat), index=cat.index))
                    .fillna("").astype(str)
                + " "
                + cat["المنتج"].fillna("").astype(str))
    r_sizes = size_src.apply(extract_size).values.astype(float)

    size_arr = np.full(len(cat), 0.5 * w_size, dtype=float)
    if q_size > 0:
        both = r_sizes > 0
        with np.errstate(divide="ignore", invalid="ignore"):
            diff_pct = np.where(both, np.abs(q_size - r_sizes) / np.maximum(q_size, r_sizes), 0)
        size_arr[both]  = np.clip(1 - diff_pct[both] * 2, 0, 1) * w_size
        size_arr[~both] = 0.5 * w_size   # حجم غير معروف → نقاط جزئية

    # ── النوع ────────────────────────────────────────────────────────────────
    q_type  = query_attrs.get("type", "")
    w_type  = MATCH_WEIGHTS["type"]
    type_src = (cat.get("النوع", pd.Series([""] * len(cat), index=cat.index))
                    .fillna("").astype(str)
                + " "
                + cat["المنتج"].fillna("").astype(str))
    r_types  = type_src.apply(extract_type).values

    if q_type:
        type_arr = np.where(r_types == "",    0.5 * w_type,
                   np.where(r_types == q_type, w_type, 0.0))
    else:
        type_arr = np.full(len(cat), 0.5 * w_type, dtype=float)

    # ── التجميع ──────────────────────────────────────────────────────────────
    total_arr  = (name_scores + brand_arr + size_arr + type_arr) / total_weight * 100
    best_iloc  = int(total_arr.argmax())
    best_score = round(float(total_arr[best_iloc]), 2)
    best_row   = cat.iloc[best_iloc]
    best_name  = str(best_row.get("المنتج", "") or "")

    if best_score >= MATCH_PASS2_CONFIRMED:
        return MatchResult(
            matched=True, state=ProductState.MATCHED,
            score=best_score, pass_number=2,
            method="pass2_weighted_vectorized", candidate=best_name,
            candidate_row=best_row,
        )
    if best_score >= MATCH_PASS2_REVIEW:
        return MatchResult(
            matched=True, state=ProductState.REVIEW,
            score=best_score, pass_number=2,
            method="pass2_weighted_vectorized", candidate=best_name,
            candidate_row=best_row,
        )
    return MatchResult(pass_number=2, method="pass2_weighted_vectorized",
                       score=best_score, candidate=best_name)


# ─────────────────────────────────────────────────────────────────────────────
#  المرور الثالث — TF-IDF Cosine Similarity
# ─────────────────────────────────────────────────────────────────────────────
def _pass3_tfidf(query: str, catalog: pd.DataFrame,
                  name_col: str = "المنتج") -> MatchResult:
    """
    يُنشئ TF-IDF matrix للكتالوج ويحسب cosine similarity مع الاستعلام.
    عتبة النجاح: MATCH_PASS3_COSINE_THRESHOLD (0.72) → REVIEW (ليس MATCHED).
    التطابق المؤكد يتطلب المرور الرابع.
    """
    TfidfVectorizer, cosine_similarity = _sklearn_tfidf()

    names = catalog[name_col].fillna("").apply(normalize).tolist()
    if not any(names):
        return MatchResult(pass_number=3, method="pass3_tfidf")

    q_norm = normalize(query)

    try:
        # [FIX OPT-02] نستخدم cached matrix بدلاً من بناء جديد لكل منتج
        names_tuple  = tuple(names)
        catalog_hash = hashlib.md5(str(names_tuple[:20]).encode()).hexdigest()[:16]
        vec, cat_mat = _build_tfidf_matrix(catalog_hash, names_tuple)
        q_vec  = vec.transform([q_norm])
        sims   = cosine_similarity(q_vec, cat_mat).flatten()
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
def _pass4_ai_embedding(query: str, catalog: pd.DataFrame,
                          name_col: str = "المنتج") -> MatchResult:
    """
    يُولّد embedding للاستعلام وأفضل 20 مرشحاً من الكتالوج
    ثم يحسب cosine similarity بينهم.
    يستخدم Gemini text-embedding-004.
    """
    import numpy as np
    from engines.ai_engine import get_product_embedding   # lazy import

    q_vec = get_product_embedding(query)
    if q_vec is None:
        _logger.warning("Pass4: embedding فشل للاستعلام '%s'", query[:50])
        return MatchResult(pass_number=4, method="pass4_embedding_failed")

    # [FIX CRITICAL-01] بدلاً من names[:30] الصلبة، نستخدم TF-IDF pre-screening
    # لاختيار أفضل 50 مرشحاً من كامل الكتالوج (7,604 منتج) ثم Embedding عليهم فقط.
    TfidfVectorizer, cosine_similarity = _sklearn_tfidf()
    all_names  = catalog[name_col].fillna("").apply(normalize).tolist()
    q_norm     = normalize(query)

    try:
        _vec   = TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 4))
        _mat   = _vec.fit_transform(all_names + [q_norm])
        _sims  = cosine_similarity(_mat[-1], _mat[:-1]).flatten()
        # نأخذ أفضل 50 مرشحاً بدلاً من أول 30 موضعاً
        TOP_K        = min(50, len(all_names))
        top_indices  = _sims.argsort()[-TOP_K:][::-1]
    except Exception as _tfidf_exc:
        _logger.warning("Pass4 TF-IDF pre-screen فشل: %s — سنستخدم كامل الكتالوج", _tfidf_exc)
        top_indices = list(range(min(50, len(all_names))))

    names      = catalog[name_col].fillna("").tolist()
    best_score = 0.0
    best_idx   = -1

    for i in top_indices:
        cand_vec = get_product_embedding(names[i])
        if cand_vec is None:
            continue
        sim = float(np.dot(q_vec, cand_vec) /
                    (np.linalg.norm(q_vec) * np.linalg.norm(cand_vec) + 1e-9))
        if sim > best_score:
            best_score = sim
            best_idx   = int(i)

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
    product_key:  str,
    product_name: str,
    catalog:      pd.DataFrame,
    *,
    brand:        str = "",
    size:         float = 0.0,
    ptype:        str = "",
    name_col:     str = "المنتج",
    skip_pass4:   bool = False,
    competitor:   str = "",
    _skip_db_persist: bool = False,   # [FIX CRITICAL-02] يمنع الكتابة الفردية عند الـ batch
) -> MatchResult:
    """
    تشغيل دورة المطابقة الكاملة (4 مرور) لمنتج واحد.

    Parameters
    ----------
    product_key  : str  — معرّف فريد للمنتج (يُستخدم في DB)
    product_name : str  — اسم المنتج كاملاً
    catalog      : pd.DataFrame  — كتالوجنا (يجب أن يحتوي على name_col)
    brand        : str  — الماركة (اختياري)
    size         : float — الحجم بـ ml (اختياري)
    ptype        : str  — النوع EDP/EDT (اختياري)
    name_col     : str  — اسم عمود الاسم في catalog (افتراضي "المنتج")
    skip_pass4   : bool — تجاوز المرور الرابع (AI Embedding)
    competitor   : str  — اسم المنافس (للتسجيل)

    Returns
    -------
    MatchResult
    """
    from utils.db_manager import DB_PATH
    from utils.event_bus import EventType, emit
    from utils.product_state import init_product, transition

    t0 = time.monotonic()

    # تهيئة سجل الحالة
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
        ("p3", lambda: _pass3_tfidf(product_name, catalog, name_col)),
    ]
    if not skip_pass4:
        passes.append(("p4", lambda: _pass4_ai_embedding(product_name, catalog, name_col)))

    final_result = MatchResult()
    attempts     = []

    for pass_label, pass_fn in passes:
        pass_t0 = time.monotonic()
        try:
            res = pass_fn()
        except Exception as exc:
            _logger.exception("Pass %s exception for '%s': %s", pass_label, product_name, exc)
            res = MatchResult(pass_number=int(pass_label[1]), method=f"{pass_label}_error")

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
            break   # وجدنا تطابقاً — نوقف السلسلة

    final_result.attempts     = attempts
    final_result.duration_ms  = int((time.monotonic() - t0) * 1000)

    # ── تحديث قاعدة البيانات ─────────────────────────────────────────────
    if not _skip_db_persist:   # [FIX CRITICAL-02] تجنّب 4000+ connection فردي
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
                "trigger_detail": f"Pass {final_result.pass_number} → {final_result.method} score={final_result.score}",
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
    comp_name_col: str = "منتج_المنافس",
    our_name_col:  str = "المنتج",
    brand_col:     str = "الماركة",
    size_col:      str = "الحجم",
    type_col:      str = "النوع",
    competitor:    str = "",
    skip_pass4:    bool = False,
    progress_cb    = None,
) -> pd.DataFrame:
    """
    تطبيق دورة المطابقة على DataFrame كامل من منتجات المنافس.

    Parameters
    ----------
    competitor_df : pd.DataFrame  — منتجات المنافس
    our_catalog   : pd.DataFrame  — كتالوجنا
    progress_cb   : callable(done, total) — callback اختياري للتقدم

    Returns
    -------
    competitor_df مُضافاً إليه أعمدة:
        match_state | match_score | match_pass | match_method | match_candidate
    """
    total   = len(competitor_df)
    results = []
    # [FIX CRITICAL-02] نجمع كل محاولات DB في قائمة واحدة ونكتبها دفعةً واحدة
    # بدلاً من 4,000+ اتصال مفتوح/مغلق (4 per product × 1000 product)
    all_attempts: list[dict] = []

    # [FIX CRITICAL-03] نُثري DataFrame المنافس بمكوّنات دلالية مُستخرَجة من الاسم
    # تُعوِّض غياب SKU/Barcode وتُصحِّح وزن الماركة والحجم في Pass2
    try:
        from utils.sku_resolver import enrich_competitor_df, make_product_key, parse_product_name
        competitor_df = enrich_competitor_df(competitor_df, name_col=comp_name_col)
        _use_sku_resolver = True
    except Exception as _sku_exc:
        _logger.warning("sku_resolver غير متاح: %s — سيعمل النظام بدون إثراء", _sku_exc)
        _use_sku_resolver = False

    for i, (_, row) in enumerate(competitor_df.iterrows()):
        pname = str(row.get(comp_name_col, "") or "").strip()
        brand = str(row.get(brand_col, "") or "").strip()
        size  = extract_size(str(row.get(size_col, "") or "") + " " + pname)
        ptype = extract_type(str(row.get(type_col, "") or "") + " " + pname)

        # [FIX CRITICAL-03] استخدم fingerprint مستقر كمفتاح بدلاً من الاسم الخام
        if _use_sku_resolver:
            _parsed = parse_product_name(pname)
            pkey    = make_product_key(competitor, _parsed)
            # تحديث brand/size/type من الاستخراج الدلالي إذا كانت فارغة
            brand = brand or _parsed["brand"]
            size  = size  or _parsed["size_ml"]
            ptype = ptype or _parsed["frag_type"]
        else:
            pkey = f"comp_{competitor}_{pname}_{brand}_{int(size)}"[:120]

        res = match_product(
            pkey, pname, our_catalog,
            brand=brand, size=size, ptype=ptype,
            name_col=our_name_col,
            skip_pass4=skip_pass4,
            competitor=competitor,
            _skip_db_persist=True,          # ← لا تكتب DB داخل match_product
        )
        all_attempts.extend(res.attempts)   # اجمع المحاولات
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

    # كتابة واحدة لكل المحاولات بعد انتهاء الحلقة
    if all_attempts:
        _persist_attempts(all_attempts)

    result_df = pd.DataFrame(results, index=competitor_df.index)
    return pd.concat([competitor_df, result_df], axis=1)


# ─────────────────────────────────────────────────────────────────────────────
#  دوال مساعدة
# ─────────────────────────────────────────────────────────────────────────────
def _persist_attempts(attempts: list[dict]) -> None:
    """حفظ سجل محاولات المطابقة في قاعدة البيانات."""
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
