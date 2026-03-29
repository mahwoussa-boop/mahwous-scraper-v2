import html
import math
from typing import Dict, List

import pandas as pd
import streamlit as st

DEFAULT_PLACEHOLDER_IMAGE = (
    "https://placehold.co/800x800/e5e7eb/6b7280?text=%F0%9F%A7%B4+No+Image"
)


def _normalize_action_label(v: str) -> str:
    t = str(v or "").strip().lower()
    if any(k in t for k in ("decrease", "خفض", "lower")):
        return "Decrease Price 📉"
    if any(k in t for k in ("increase", "رفع", "raise")):
        return "Increase Price 📈"
    if any(k in t for k in ("perfect", "approved", "موافق", "ok", "keep")):
        return "Perfect Price ✅"
    return "Review / Other ⚠️"


def normalize_ui_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure missing text and numeric formatting before UI render."""
    if df is None:
        return pd.DataFrame()
    if df.empty:
        return df.copy()

    d = df.copy()
    text_cols = d.select_dtypes(include=["object"]).columns
    for c in text_cols:
        d[c] = d[c].fillna("N/A").astype(str).str.strip()
        d.loc[d[c] == "", c] = "N/A"

    numeric_cols = d.select_dtypes(include=["number"]).columns
    for c in numeric_cols:
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0)
    return d


def _pick_image_url(row: pd.Series) -> str:
    for c in ("image", "image_url", "img", "thumbnail", "comp_image_url"):
        if c in row.index:
            v = row.get(c)
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            s = str(v).strip()
            if s and s.lower() not in {"nan", "none", "n/a"}:
                return s
    return DEFAULT_PLACEHOLDER_IMAGE


def _group_products_for_tabs(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    if "category" in df.columns:
        grp_col = "category"
    elif "brand" in df.columns:
        grp_col = "brand"
    else:
        grp_col = None

    groups: Dict[str, pd.DataFrame] = {}
    if grp_col is not None:
        gseries = df[grp_col].fillna("N/A").astype(str).replace("", "N/A")
        for key, g in df.groupby(gseries):
            groups[str(key)] = g
        return groups

    action_col = "action_required" if "action_required" in df.columns else None
    if action_col is None:
        return {"All Products": df}

    mapped = df[action_col].apply(_normalize_action_label)
    for key, idx in mapped.groupby(mapped).groups.items():
        groups[str(key)] = df.loc[idx]
    return groups


def _aggregate_multi_competitors(df: pd.DataFrame) -> pd.DataFrame:
    """
    يجمع الصفوف المتكررة لنفس المنتج (SKU/الاسم) في صف واحد
    ويضع كل المنافسين داخل قائمة competitors_list.
    """
    if df.empty:
        return df

    d = df.copy()
    if "competitor_name" not in d.columns:
        d["competitor_name"] = "Competitor"
    if "comp_price" not in d.columns:
        d["comp_price"] = 0.0
    if "comp_image_url" not in d.columns:
        d["comp_image_url"] = ""

    d["__product_key__"] = (
        d.get("sku", pd.Series([""] * len(d), index=d.index)).astype(str).str.strip()
    )
    empty_key = d["__product_key__"].isin(["", "N/A", "nan", "None"])
    d.loc[empty_key, "__product_key__"] = (
        d.get("name", pd.Series(["N/A"] * len(d), index=d.index)).astype(str).str.strip()
    )

    rows = []
    for _, g in d.groupby("__product_key__", dropna=False):
        g = g.copy()
        first = g.iloc[0].copy()

        comps = []
        for _, r in g.iterrows():
            cname = str(r.get("competitor_name", "Competitor")).strip() or "Competitor"
            cprice = float(pd.to_numeric(r.get("comp_price", 0), errors="coerce") or 0)
            cimg = str(r.get("comp_image_url", "") or "").strip()
            if not cimg or cimg.lower() in {"nan", "none", "n/a"}:
                cimg = ""
            comps.append({"name": cname, "price": cprice, "comp_image_url": cimg})

        # إزالة التكرار حسب (اسم المنافس + السعر)
        seen = set()
        uniq = []
        for c in comps:
            k = (c["name"], round(float(c["price"]), 4))
            if k not in seen:
                seen.add(k)
                uniq.append(c)

        first["competitors_list"] = uniq
        first["competitors_count"] = len(uniq)
        if uniq:
            first["comp_price"] = min(float(x["price"]) for x in uniq if float(x["price"]) > 0) if any(
                float(x["price"]) > 0 for x in uniq
            ) else float(uniq[0]["price"])
        rows.append(first)

    out = pd.DataFrame(rows).drop(columns=["__product_key__"], errors="ignore")
    return out.reset_index(drop=True)


def render_product_cards(df: pd.DataFrame, items_per_page: int = 15, key_prefix: str = "cards"):
    d = normalize_ui_dataframe(df)
    if d.empty:
        st.warning("⚠️ لا توجد بيانات لعرضها.")
        return
    d = _aggregate_multi_competitors(d)
    if "sent_to_make_keys" not in st.session_state:
        st.session_state["sent_to_make_keys"] = set()

    page_key = f"{key_prefix}_page"
    if page_key not in st.session_state:
        st.session_state[page_key] = 1

    st.markdown("### 🔍 أدوات التحكم والبحث")
    st.caption("المقارنة داخل البطاقات: **Mahwous (سعرنا)** VS **المنافس**")
    col_search, col_filter = st.columns([2, 1])
    with col_search:
        search_query = st.text_input(
            "ابحث عن منتج (بالاسم أو SKU):", "", key=f"{key_prefix}_search"
        ).strip().lower()
    with col_filter:
        filter_options: List[str] = ["الكل"]
        if "action_required" in d.columns:
            vals = sorted([x for x in d["action_required"].dropna().astype(str).unique().tolist() if x != "N/A"])
            filter_options.extend(vals)
        selected_filter = st.selectbox(
            "تصفية حسب الحالة:", filter_options, key=f"{key_prefix}_filter"
        )

    filtered_df = d.copy()
    if search_query:
        name_match = filtered_df.get("name", pd.Series(dtype=str)).astype(str).str.lower().str.contains(search_query, na=False)
        sku_match = filtered_df.get("sku", pd.Series(dtype=str)).astype(str).str.lower().str.contains(search_query, na=False)
        filtered_df = filtered_df[name_match | sku_match]

    if selected_filter != "الكل" and "action_required" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["action_required"].astype(str) == selected_filter]

    total_items = len(filtered_df)
    if total_items == 0:
        st.info("لم يتم العثور على منتجات تطابق بحثك.")
        return

    total_pages = max(1, math.ceil(total_items / items_per_page))
    if st.session_state[page_key] > total_pages:
        st.session_state[page_key] = 1

    start_idx = (st.session_state[page_key] - 1) * items_per_page
    end_idx = start_idx + items_per_page
    page_df = filtered_df.iloc[start_idx:end_idx]

    st.markdown(
        """
        <style>
        .product-card { background-color: #ffffff; border-radius: 12px; box-shadow: 0 4px 8px rgba(0,0,0,0.05); padding: 15px; margin-bottom: 15px; text-align: center; transition: transform 0.2s; border: 1px solid #f0f0f0; height: 100%; display: flex; flex-direction: column; justify-content: space-between; }
        .product-card:hover { transform: translateY(-4px); box-shadow: 0 8px 16px rgba(0,0,0,0.1); border-color: #d1d5db; }
        .product-img { object-fit: cover; height: 200px; width: 100%; border-radius: 8px; margin-bottom: 10px; background: #f3f4f6; }
        .product-title { font-size: 15px; font-weight: 700; color: #1f2937; margin-bottom: 10px; line-height: 1.4; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
        .price-block { display: flex; justify-content: space-between; align-items: center; background-color: #f3f4f6; padding: 12px; border-radius: 8px; margin-bottom: 10px; }
        .price-col { text-align: center; width: 50%; }
        .price-label { font-size: 11px; color: #6b7280; font-weight: 600; text-transform: uppercase;}
        .price-value { font-size: 16px; font-weight: 800; color: #374151; }
        .suggested-value { font-size: 16px; font-weight: 800; color: #059669; }
        .comp-block { padding-top: 10px; border-top: 1px dashed #e5e7eb; font-size: 14px; }
        .comp-higher { color: #dc2626; font-weight: bold; }
        .comp-lower { color: #059669; font-weight: bold; }
        .comp-neutral { color: #6b7280; font-weight: 500; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<p style='text-align: left; color: #6b7280; font-size: 14px;'>نعرض {len(page_df)} من أصل {total_items} منتج</p>",
        unsafe_allow_html=True,
    )

    cols_per_row = 3
    for i in range(0, len(page_df), cols_per_row):
        cols = st.columns(cols_per_row)
        chunk = page_df.iloc[i : i + cols_per_row]
        for index, (_, row) in enumerate(chunk.iterrows()):
            with cols[index]:
                safe_name = html.escape(str(row.get("name", "N/A")))
                img_url = _pick_image_url(row)
                my_price = float(pd.to_numeric(row.get("price", 0), errors="coerce") or 0)
                sug_price = float(pd.to_numeric(row.get("suggested_price", 0), errors="coerce") or 0)
                comp_price = float(pd.to_numeric(row.get("comp_price", 0), errors="coerce") or 0)
                sku = html.escape(str(row.get("sku", "N/A")))
                competitor_name = html.escape(str(row.get("competitor_name", "Competitor")))
                is_missing = bool(row.get("is_missing", False))
                ai_state = str(row.get("ai_verification_state", "") or "").strip().lower()
                ai_conf = int(float(pd.to_numeric(row.get("ai_verification_confidence", 0), errors="coerce") or 0))
                ai_reason = html.escape(str(row.get("ai_verification_reason", "") or "").strip())
                competitors_list = row.get("competitors_list", [])
                if not isinstance(competitors_list, list):
                    competitors_list = []
                if len(competitors_list) > 1:
                    competitor_name = f"{len(competitors_list)} منافسين"

                if comp_price == 0:
                    comp_status = "<span class='comp-neutral'>سعر المنافس: غير متوفر ➖</span>"
                elif comp_price > my_price:
                    comp_status = f"<span class='comp-higher'>سعر المنافس: {comp_price:,.2f} ر.س 🔺 (أعلى من سعرك)</span>"
                elif comp_price < my_price:
                    comp_status = f"<span class='comp-lower'>سعر المنافس: {comp_price:,.2f} ر.س 🔻 (أقل من سعرك)</span>"
                else:
                    comp_status = f"<span class='comp-neutral'>سعر المنافس: {comp_price:,.2f} ر.س ➖ (مطابق)</span>"

                if is_missing:
                    my_price_html = "<div class='price-value' style='color:#6b7280;'>غير متوفر لدينا</div>"
                    comp_status = "<span class='comp-higher'>هذا المنتج موجود عند المنافس وغير موجود في Mahwous</span>"
                else:
                    my_price_html = f"<div class='price-value'>{my_price:,.2f}</div>"

                card_html = f"""
                <div class="product-card">
                    <div>
                        <img src="{img_url}" class="product-img" onerror="this.src='{DEFAULT_PLACEHOLDER_IMAGE}'">
                        <div class="product-title" title="{safe_name}">{safe_name}</div>
                        <div style="font-size:11px; color:#9ca3af; margin-bottom:8px;">SKU: {sku}</div>
                    </div>
                    <div>
                        <div style="font-size:12px; color:#334155; margin-bottom:8px;">
                            <b>Mahwous</b> VS <b>{competitor_name}</b>
                        </div>
                        <div class="price-block">
                            <div class="price-col" style="border-left: 1px solid #e5e7eb;">
                                <div class="price-label">Mahwous Price</div>{my_price_html}
                            </div>
                            <div class="price-col">
                                <div class="price-label">Suggested</div><div class="suggested-value">{sug_price:,.2f}</div>
                            </div>
                        </div>
                        <div class="comp-block">{comp_status}</div>
                    </div>
                </div>
                """
                st.markdown(card_html, unsafe_allow_html=True)
                if is_missing and ai_state in {"missing_candidate", "rejected", "not_match", "unmatched"}:
                    st.markdown(
                        (
                            "<div style='background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;"
                            "padding:10px;margin:8px 0 10px 0;font-size:13px;color:#9a3412;'>"
                            f"<b>تم التحقق بواسطة Gemini: غير مطابق</b> — الثقة: <b>{ai_conf}%</b><br>"
                            f"{ai_reason if ai_reason else 'لا يوجد سبب تفصيلي.'}"
                            "</div>"
                        ),
                        unsafe_allow_html=True,
                    )
                if competitors_list:
                    with st.expander(f"المنافسون لهذا المنتج ({len(competitors_list)})", expanded=False):
                        competitors_list = sorted(
                            competitors_list,
                            key=lambda x: float(pd.to_numeric(x.get("price", 0), errors="coerce") or 0),
                        )
                        rows_html = ""
                        for c in competitors_list:
                            c_name = html.escape(str(c.get("name", "N/A")))
                            c_price = float(pd.to_numeric(c.get("price", 0), errors="coerce") or 0)
                            c_img = str(c.get("comp_image_url", "") or "").strip()
                            if c_img and c_img.lower() not in {"nan", "none", "n/a"}:
                                c_img_html = (
                                    f'<img src="{html.escape(c_img)}" '
                                    'style="width: 45px; height: 45px; object-fit: cover; border-radius: 6px; border: 1px solid #ddd;">'
                                )
                            else:
                                c_img_html = (
                                    '<div style="width:45px; height:45px; background:#eee; text-align:center; '
                                    'line-height:45px; border-radius:6px;">N/A</div>'
                                )
                            rows_html += (
                                "<tr>"
                                f"<td style='padding:6px;'>{c_img_html}</td>"
                                f"<td style='padding:6px;'>{c_name}</td>"
                                f"<td style='padding:6px;'>{c_price:,.2f}</td>"
                                "</tr>"
                            )
                        table_html = f"""
                        <table style="width:100%; border-collapse:collapse; font-size:13px;">
                            <thead>
                                <tr style="background:#f8fafc;">
                                    <th style="text-align:right; padding:6px;">🖼️ الصورة</th>
                                    <th style="text-align:right; padding:6px;">المنافس</th>
                                    <th style="text-align:right; padding:6px;">السعر</th>
                                </tr>
                            </thead>
                            <tbody>
                                {rows_html}
                            </tbody>
                        </table>
                        """
                        st.markdown(table_html, unsafe_allow_html=True)
                row_key = str(row.get("sku", "") or row.get("name", ""))
                already_sent = row_key in st.session_state["sent_to_make_keys"]
                if st.button(
                    "✔️ تمت المزامنة" if already_sent else f"🚀 اعتماد السعر ({sug_price:,.2f})",
                    key=f"{key_prefix}_btn_{sku}_{index}_{i}",
                    width="stretch",
                    disabled=already_sent,
                ):
                    st.session_state["sent_to_make_keys"].add(row_key)
                    st.toast(f"تم إرسال أمر تحديث المنتج {sku} بنجاح!", icon="✅")

    st.markdown("---")
    page_col1, page_col2, page_col3 = st.columns([1, 2, 1])
    with page_col1:
        if st.button(
            "⬅️ الصفحة السابقة",
            disabled=(st.session_state[page_key] == 1),
            width="stretch",
            key=f"{key_prefix}_prev",
        ):
            st.session_state[page_key] -= 1
            st.rerun()
    with page_col2:
        st.markdown(
            f"<div style='text-align: center; padding-top: 8px; font-weight: bold;'>الصفحة {st.session_state[page_key]} من {total_pages}</div>",
            unsafe_allow_html=True,
        )
    with page_col3:
        if st.button(
            "الصفحة التالية ➡️",
            disabled=(st.session_state[page_key] == total_pages),
            width="stretch",
            key=f"{key_prefix}_next",
        ):
            st.session_state[page_key] += 1
            st.rerun()


def render_products_in_tabs(df: pd.DataFrame, key_prefix: str = "tabs") -> None:
    d = normalize_ui_dataframe(df)
    if d.empty:
        st.warning("⚠️ لا توجد بيانات لعرضها.")
        return
    groups = _group_products_for_tabs(d)
    labels = list(groups.keys())
    tabs = st.tabs(labels)
    for i, label in enumerate(labels):
        with tabs[i]:
            render_product_cards(groups[label], key_prefix=f"{key_prefix}_{i}")
