"""
utils/salla_exporter.py — مولد ملفات سلة v1.0
===============================================
يولد ملفات CSV متوافقة 100% مع قوالب منصة سلة:
  1. ملف تحديث الأسعار  → SKU + السعر الجديد
  2. ملف المنتجات المفقودة → منتجات المنافسين غير الموجودة عندنا
"""
from __future__ import annotations

import io
import logging
from datetime import datetime
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ── ثوابت ─────────────────────────────────────────────────
SALLA_PRICE_UPDATE_COLS = ["sku", "price"]          # الحد الأدنى لتحديث السعر
SALLA_MISSING_COLS = [                               # أعمدة المنتجات المفقودة
    "name", "brand", "price", "image_url", "comp_url", "competitor"
]


# ── 1. ملف تحديث الأسعار ──────────────────────────────────
def build_price_update_csv(
    df: pd.DataFrame,
    only_approved: bool = True,
    include_current_price: bool = True,
    include_margin: bool = True,
) -> Optional[bytes]:
    """
    يبني CSV لرفع تحديثات الأسعار لسلة.

    Args:
        df: DataFrame يحتوي على: sku, suggested_price, [price], [margin_pct], [strategy]
        only_approved: إذا True، يُصدِّر فقط الصفوف التي strategy != "hold"
        include_current_price: إضافة عمود السعر الحالي للمقارنة
        include_margin: إضافة عمود هامش الربح

    Returns:
        bytes (UTF-8 BOM) أو None عند الخطأ
    """
    if df is None or df.empty:
        logger.warning("salla_exporter: DataFrame فارغ")
        return None

    out = pd.DataFrame()

    # SKU
    if "sku" not in df.columns:
        logger.error("salla_exporter: عمود 'sku' مفقود")
        return None
    out["sku"] = df["sku"].fillna("").astype(str)

    # السعر الجديد
    price_col = "suggested_price" if "suggested_price" in df.columns else "price"
    out["price"] = pd.to_numeric(df[price_col], errors="coerce").round(2)

    # تصفية الصفوف
    if only_approved and "strategy" in df.columns:
        mask = df["strategy"].isin(["undercut", "raise"])
        out = out[mask.values]
        df_filtered = df[mask]
    else:
        df_filtered = df

    # أعمدة إضافية اختيارية
    if include_current_price and "price" in df_filtered.columns:
        out["current_price"] = pd.to_numeric(
            df_filtered["price"], errors="coerce"
        ).round(2).values

    if include_margin and "margin_pct" in df_filtered.columns:
        out["margin_pct"] = pd.to_numeric(
            df_filtered["margin_pct"], errors="coerce"
        ).round(1).values

    # إزالة الصفوف بـ SKU فارغ أو سعر صفري
    out = out[
        (out["sku"].str.strip() != "") &
        (out["price"].fillna(0) > 0)
    ]

    if out.empty:
        logger.info("salla_exporter: لا توجد تحديثات للتصدير")
        return None

    # تصدير UTF-8 BOM (لضمان فتح صحيح في Excel العربي)
    buffer = io.BytesIO()
    out.to_csv(buffer, index=False, encoding="utf-8-sig")
    return buffer.getvalue()


# ── 2. ملف المنتجات المفقودة ─────────────────────────────
def build_missing_products_csv(
    df: pd.DataFrame,
    min_comp_price: float = 0.0,
) -> Optional[bytes]:
    """
    يبني CSV للمنتجات الرابحة عند المنافسين وغير موجودة في كتالوجنا.

    Args:
        df: DataFrame يحتوي على أعمدة المنتجات المفقودة
        min_comp_price: تصفية حسب الحد الأدنى لسعر المنافس

    Returns:
        bytes (UTF-8 BOM) أو None
    """
    if df is None or df.empty:
        return None

    # تصفية حسب action_required إذا وجد
    if "action_required" in df.columns:
        mask = df["action_required"].str.contains("مفقود|missing", case=False, na=False)
        df = df[mask].copy()

    if df.empty:
        return None

    # تصفية السعر
    if min_comp_price > 0 and "comp_price" in df.columns:
        df = df[pd.to_numeric(df["comp_price"], errors="coerce").fillna(0) >= min_comp_price]

    if df.empty:
        return None

    out = pd.DataFrame()
    # الاسم
    for col in ("comp_name", "name_comp", "name"):
        if col in df.columns:
            out["product_name"] = df[col].fillna("").astype(str)
            break
    else:
        out["product_name"] = ""

    # الماركة
    for col in ("comp_brand", "brand"):
        if col in df.columns:
            out["brand"] = df[col].fillna("").astype(str)
            break

    # السعر عند المنافس
    for col in ("comp_price", "price"):
        if col in df.columns:
            out["competitor_price"] = pd.to_numeric(df[col], errors="coerce").round(2)
            break

    # رابط المنتج عند المنافس
    if "comp_url" in df.columns:
        out["competitor_url"] = df["comp_url"].fillna("").astype(str)

    # رابط الصورة
    for col in ("comp_image_url", "image_url"):
        if col in df.columns:
            out["image_url"] = df[col].fillna("").astype(str)
            break

    # اسم المنافس
    if "competitor" in df.columns:
        out["competitor"] = df["competitor"].fillna("").astype(str)

    # تاريخ الاكتشاف
    out["discovered_at"] = datetime.now().strftime("%Y-%m-%d")

    buffer = io.BytesIO()
    out.to_csv(buffer, index=False, encoding="utf-8-sig")
    return buffer.getvalue()


# ── 3. دوال مساعدة للـ UI ─────────────────────────────────
def get_price_update_stats(df: pd.DataFrame) -> dict:
    """إحصائيات سريعة لعرضها قبل التصدير."""
    if df is None or df.empty:
        return {"total": 0, "to_lower": 0, "to_raise": 0, "avg_change_pct": 0}

    if "strategy" not in df.columns:
        return {"total": len(df), "to_lower": 0, "to_raise": 0, "avg_change_pct": 0}

    to_lower = len(df[df["strategy"] == "undercut"])
    to_raise = len(df[df["strategy"] == "raise"])

    avg_change = 0.0
    if "price" in df.columns and "suggested_price" in df.columns:
        price = pd.to_numeric(df["price"], errors="coerce").fillna(0)
        sugg = pd.to_numeric(df["suggested_price"], errors="coerce").fillna(0)
        mask = (price > 0) & (sugg > 0)
        if mask.any():
            avg_change = float(((sugg[mask] - price[mask]) / price[mask] * 100).mean())

    return {
        "total": to_lower + to_raise,
        "to_lower": to_lower,
        "to_raise": to_raise,
        "avg_change_pct": round(avg_change, 1),
    }


def render_salla_export_ui(
    priced_df: pd.DataFrame,
    missing_df: Optional[pd.DataFrame] = None,
) -> None:
    """
    واجهة Streamlit كاملة لتصدير ملفات سلة.
    استدعِ هذه الدالة من داخل صفحة Streamlit.
    """
    import streamlit as st

    st.subheader("📤 تصدير ملفات سلة")

    tab1, tab2 = st.tabs(["💰 تحديث الأسعار", "🔍 المنتجات المفقودة"])

    # ── تاب 1: تحديث الأسعار ──
    with tab1:
        if priced_df is not None and not priced_df.empty:
            stats = get_price_update_stats(priced_df)
            c1, c2, c3 = st.columns(3)
            c1.metric("📊 إجمالي للتحديث", stats["total"])
            c2.metric("⬇️ للخفض", stats["to_lower"])
            c3.metric("⬆️ للرفع", stats["to_raise"])

            if stats["avg_change_pct"] != 0:
                direction = "📉" if stats["avg_change_pct"] < 0 else "📈"
                st.caption(
                    f"{direction} متوسط التغيير: {stats['avg_change_pct']:+.1f}%"
                )

            col_a, col_b = st.columns(2)
            only_approved = col_a.checkbox(
                "تصدير قرارات الخفض/الرفع فقط", value=True, key="salla_only_approved"
            )
            include_margin = col_b.checkbox(
                "إضافة عمود هامش الربح", value=True, key="salla_margin"
            )

            if st.button("🚀 تجهيز ملف تحديث الأسعار", type="primary", key="salla_price_btn"):
                csv_bytes = build_price_update_csv(
                    priced_df,
                    only_approved=only_approved,
                    include_margin=include_margin,
                )
                if csv_bytes:
                    fname = f"salla_price_update_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
                    st.download_button(
                        label=f"⬇️ تحميل {fname}",
                        data=csv_bytes,
                        file_name=fname,
                        mime="text/csv",
                        key="salla_price_dl",
                    )
                    st.success(f"✅ الملف جاهز — {stats['total']} منتج")
                else:
                    st.warning("لا توجد تحديثات للتصدير")
        else:
            st.info("شغّل التحليل أولاً لتظهر البيانات هنا")

    # ── تاب 2: المنتجات المفقودة ──
    with tab2:
        _missing = missing_df if missing_df is not None else priced_df
        if _missing is not None and not _missing.empty:
            min_price = st.number_input(
                "حد أدنى لسعر المنافس (ر.س)",
                min_value=0.0, max_value=500.0, value=50.0, step=10.0,
                key="salla_missing_min_price",
            )
            if st.button("🚀 تجهيز ملف المنتجات المفقودة", type="primary", key="salla_missing_btn"):
                csv_bytes = build_missing_products_csv(_missing, min_comp_price=min_price)
                if csv_bytes:
                    fname = f"salla_missing_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
                    st.download_button(
                        label=f"⬇️ تحميل {fname}",
                        data=csv_bytes,
                        file_name=fname,
                        mime="text/csv",
                        key="salla_missing_dl",
                    )
                    st.success("✅ ملف المنتجات المفقودة جاهز")
                else:
                    st.warning("لا توجد منتجات مفقودة تستوفي الشروط")
        else:
            st.info("شغّل تحليل المنتجات المفقودة أولاً")
