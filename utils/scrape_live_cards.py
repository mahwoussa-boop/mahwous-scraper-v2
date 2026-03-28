"""
تصنيف صفوف ناتج خط التسعير (final_priced_latest) إلى «بطاقات» حسب منطق المقارنة مع المنافس.

يُكمّل الكشط المباشر (حفظ CSV/SQLite) وتشغيل Gemini عبر run_auto_pricing_pipeline_background على دفعات.
"""
from __future__ import annotations

from typing import Dict, Tuple

import pandas as pd

# تسامح نسبي بسيط حول تساوي السعرين
_PRICE_EPS_RATIO = 0.015


def _safe_float(x) -> float | None:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        v = float(x)
        if pd.isna(v):
            return None
        return v
    except (TypeError, ValueError):
        return None


def classify_pricing_row(row: pd.Series) -> str:
    """
    يعيد مفتاح الحاوية:
    missing | review | higher | lower | ok
    """
    act = str(row.get("action_required", "") or "")
    st = str(row.get("status", "") or "")
    ai_st = str(row.get("ai_verification_state", "") or "")
    mscore = _safe_float(row.get("match_score"))

    if "مفقود" in act or "missing" in st.lower():
        return "missing"
    if ai_st == "under_review":
        return "review"
    if mscore is not None and mscore < 50:
        return "review"
    if mscore is not None and 50 <= mscore < 80 and ai_st not in ("verified_by_ai",):
        return "review"

    our = _safe_float(row.get("price"))
    comp = _safe_float(row.get("comp_price"))
    if our is None or comp is None or our <= 0 or comp <= 0:
        return "review"

    lo = our * (1 - _PRICE_EPS_RATIO)
    hi = our * (1 + _PRICE_EPS_RATIO)
    if comp > hi:
        return "higher"
    if comp < lo:
        return "lower"
    return "ok"


BUCKET_META: Dict[str, Tuple[str, str, str]] = {
    "higher": ("🔴 سعر أعلى عند المنافس", "comp_price أعلى من سعرك — المنافس يعرض أغلى.", "#5c1010"),
    "lower": ("🟢 سعر أقل عند المنافس", "comp_price أقل — فرصة تنافس أو مراجعة هامشك.", "#0d3d1f"),
    "ok": ("✅ موافق / مسعر بشكل سليم", "تطابق جيد وسعر قريب من سعر المنافس ضمن الحد المسموح.", "#1a472a"),
    "missing": ("🔍 منتجات مفقودة", "موجودة لدى المنافس أو غير مطابقة بعد التحقق — غير مدرجة لديك.", "#1e3a5f"),
    "review": ("⚠️ تحت المراجعة", "بيانات غير حاسمة أو درجة مطابقة منخفضة — يحتاج تدخل بشري.", "#5c4a10"),
}


def bucket_final_priced_df(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """يقسّم DataFrame إلى قواميس حسب classify_pricing_row."""
    if df is None or df.empty:
        return {k: pd.DataFrame() for k in BUCKET_META}
    work = df.copy()
    work["__bucket"] = work.apply(classify_pricing_row, axis=1)
    out: Dict[str, pd.DataFrame] = {}
    for key in BUCKET_META:
        sub = work.loc[work["__bucket"] == key].drop(columns=["__bucket"], errors="ignore")
        out[key] = sub.reset_index(drop=True)
    return out


def summarize_buckets(buckets: Dict[str, pd.DataFrame]) -> Dict[str, int]:
    return {k: len(v) for k, v in buckets.items()}


_DISPLAY_COLS = [
    "name",
    "sku",
    "price",
    "comp_name",
    "comp_price",
    "comp_url",
    "match_score",
    "action_required",
    "ai_verification_state",
    "ai_verification_reason",
]


def trim_for_display(df: pd.DataFrame, max_rows: int = 80) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    cols = [c for c in _DISPLAY_COLS if c in df.columns]
    if not cols:
        return df.head(max_rows)
    return df[cols].head(max_rows)


def competitor_missing_vs_our_catalog(comp_df: pd.DataFrame, our_df: pd.DataFrame) -> pd.DataFrame:
    """
    صفوف في كشط المنافس غير موجودة في كتالوجنا (SKU + اسم) — تقديري سريع للواجهة.
    """
    if comp_df is None or comp_df.empty or our_df is None or our_df.empty:
        return pd.DataFrame()

    d = comp_df.copy()
    col_map = {
        "name": "الاسم",
        "الاسم": "الاسم",
        "sku": "sku",
        "السعر": "السعر",
        "price": "السعر",
    }
    for a, b in col_map.items():
        if a in d.columns and b not in d.columns:
            d[b] = d[a]
    if "الاسم" not in d.columns:
        return pd.DataFrame()

    our = our_df.copy()
    _our_sku = next(
        (c for c in ("sku", "رقم المنتج", "رمز المنتج sku", "product_id") if c in our.columns),
        None,
    )
    _our_name = next(
        (c for c in ("name", "اسم المنتج", "أسم المنتج", "product_name") if c in our.columns),
        None,
    )
    if not _our_name:
        return pd.DataFrame()

    skus = set()
    if _our_sku:
        skus = set(our[_our_sku].astype(str).str.strip().tolist())
    names = set(our[_our_name].astype(str).str.strip().str.lower().tolist())

    d["__sku"] = d["sku"].astype(str).str.strip() if "sku" in d.columns else ""
    d["__name"] = d["الاسم"].astype(str).str.strip().str.lower()
    in_cat = d["__name"].isin(names)
    if skus:
        in_cat = in_cat | d["__sku"].isin(skus)
    miss = d.loc[~in_cat].copy()
    if miss.empty:
        return pd.DataFrame()
    show_cols = [c for c in ["الاسم", "السعر", "sku", "رابط_المنتج", "comp_url", "url"] if c in miss.columns]
    if not show_cols:
        show_cols = list(miss.columns)[:8]
    return miss[show_cols].head(200)
