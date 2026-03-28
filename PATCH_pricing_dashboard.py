"""
PATCH: app.py — لوحة التسعير (Pricing Dashboard)

INSTRUCTIONS:
Replace the ENTIRE `elif page == "📊 لوحة التسعير":` section in app.py
with the code below.

WHAT WAS FIXED:
- Bug #1: All code after `if df is not None:` is now properly indented INSIDE the block
- Bug #6: except block now shows actual error details
- Bug #7: All variables (work, comp_csv, mask_*, df_higher, etc.) are now inside the if block
"""

# ════════════════════════════════════════════════
#  1c. لوحة التسعير — رؤى قابلة للتنفيذ
# ════════════════════════════════════════════════
# --- START OF CORRECTED SECTION (paste into app.py) ---

elif page == "📊 لوحة التسعير":
    try:
        st.header("📊 لوحة التسعير")
        st.caption(
            f"رؤى تسعير تعتمد خط أساس ثابت: **{MAIN_STORE_NAME} ({MAIN_STORE_DOMAIN})** مقابل المنافسين."
        )
        db_log("pricing_dashboard", "view")

        if "final_priced_df" not in st.session_state or st.session_state["final_priced_df"] is None:
            _auto_priced_path = os.path.join(os.getcwd(), "data", "final_priced_latest.csv")
            if os.path.exists(_auto_priced_path):
                try:
                    st.session_state["final_priced_df"] = pd.read_csv(_auto_priced_path)
                    st.success("✅ تم تحميل نتائج التسعير التلقائي الجاهزة من الخلفية.")
                except Exception as _e:
                    st.warning(f"تعذر تحميل نتائج التسعير التلقائي: {str(_e)}")
            if st.session_state.get("final_priced_df") is None:
                st.info("لا توجد نتائج تسعير جاهزة بعد. سيتم عرضها تلقائياً عند اكتمال المعالجة الخلفية.")

        df = st.session_state.get("final_priced_df")
        if not isinstance(df, pd.DataFrame):
            st.error("final_priced_df يجب أن يكون DataFrame.")
            df = None
        elif df.empty:
            st.warning("جدول التسعير فارغ.")
            df = None

        # ═══════════════════════════════════════════════════════════════
        #  FIX: Everything below is now INSIDE `if df is not None:`
        # ═══════════════════════════════════════════════════════════════
        if df is not None:
            from utils.ui_components import render_product_cards

            work = df.copy()
            if "sent_to_make_keys" not in st.session_state:
                st.session_state["sent_to_make_keys"] = set()

            for c in ("price", "comp_price", "suggested_price", "match_score"):
                if c not in work.columns:
                    work[c] = 0.0
                work[c] = pd.to_numeric(work[c], errors="coerce").fillna(0.0)

            if "status" not in work.columns:
                work["status"] = ""
            work["status"] = work["status"].fillna("").astype(str)

            group_key_col = "sku" if "sku" in work.columns else "name"
            work["_group_key"] = work[group_key_col].astype(str).fillna("").str.strip()
            work.loc[work["_group_key"].isin(["", "N/A", "nan", "None"]), "_group_key"] = (
                work.get("name", pd.Series(["N/A"] * len(work), index=work.index)).astype(str).str.strip()
            )

            # min_comp_price per product key
            work["min_comp_price"] = work.groupby("_group_key", dropna=False)["comp_price"].transform("min")
            work["min_comp_price"] = pd.to_numeric(work["min_comp_price"], errors="coerce").fillna(0.0)

            # state tracking
            sent_keys = {str(x) for x in st.session_state.get("sent_to_make_keys", set())}
            row_keys = work.get("sku", work.get("name", pd.Series([""] * len(work), index=work.index))).astype(str)
            work.loc[row_keys.isin(sent_keys), "status"] = "sent_to_make"

            rel_diff = (
                (work["price"] - work["min_comp_price"]).abs()
                / work["min_comp_price"].replace(0, pd.NA)
            ).fillna(999.0)
            status_l = work["status"].str.lower()
            valid_comp = work["min_comp_price"] > 0
            mask_verified_missing = status_l.eq("missing_after_verification")
            mask_processed = status_l.eq("sent_to_make")
            mask_review = (~mask_verified_missing) & (status_l.isin({"processing", "under_review"}) | (work["match_score"] < 80))
            mask_higher = (~mask_verified_missing & ~mask_processed & ~mask_review & valid_comp & (work["price"] > work["min_comp_price"]))
            mask_lower = (~mask_verified_missing & ~mask_processed & ~mask_review & valid_comp & (work["price"] < work["min_comp_price"]))
            mask_approved = (~mask_verified_missing & ~mask_processed & ~mask_review & valid_comp & (rel_diff <= 0.02))

            # Missing products
            missing_df = pd.DataFrame()
            comp_csv = os.path.join(os.getcwd(), "data", "competitors_latest.csv")
            if os.path.exists(comp_csv):
                try:
                    comp_raw = pd.read_csv(comp_csv)
                    rename_map = {
                        "الاسم": "name",
                        "السعر": "comp_price",
                        "الماركة": "brand",
                        "رابط_الصورة": "comp_image_url",
                        "رابط_المنتج": "comp_url",
                        "المنافس": "competitor_name",
                    }
                    for ar, en in rename_map.items():
                        if ar in comp_raw.columns and en not in comp_raw.columns:
                            comp_raw[en] = comp_raw[ar]
                    if "name" not in comp_raw.columns:
                        comp_raw["name"] = "N/A"
                    if "sku" not in comp_raw.columns:
                        comp_raw["sku"] = ""
                    if "comp_image_url" not in comp_raw.columns and "image_url" in comp_raw.columns:
                        comp_raw["comp_image_url"] = comp_raw["image_url"]
                    if "competitor_name" not in comp_raw.columns:
                        comp_raw["competitor_name"] = (
                            comp_raw.get("comp_url", pd.Series([""] * len(comp_raw), index=comp_raw.index))
                            .astype(str)
                            .str.extract(r"https?://([^/]+)", expand=False)
                            .fillna("Competitor")
                        )
                    comp_raw["comp_price"] = pd.to_numeric(comp_raw.get("comp_price", 0), errors="coerce").fillna(0.0)
                    comp_raw["sku"] = comp_raw["sku"].astype(str).fillna("").str.strip()
                    comp_raw["name"] = comp_raw["name"].astype(str).fillna("").str.strip()

                    mah_skus = set(work.get("sku", pd.Series(dtype=str)).astype(str).str.strip().tolist())
                    mah_names = set(work.get("name", pd.Series(dtype=str)).astype(str).str.strip().tolist())
                    miss_mask = (~comp_raw["sku"].isin(mah_skus)) & (~comp_raw["name"].isin(mah_names))
                    missing_df = comp_raw.loc[miss_mask].copy()
                    missing_df["price"] = pd.NA
                    missing_df["suggested_price"] = 0.0
                    missing_df["is_missing"] = True
                    missing_df["status"] = "missing"
                except Exception:
                    missing_df = pd.DataFrame()

            df_higher = work[mask_higher].copy()
            df_lower = work[mask_lower].copy()
            df_approved = work[mask_approved].copy()
            df_review = work[mask_review].copy()
            df_processed = work[mask_processed].copy()
            verified_missing_df = work[mask_verified_missing].copy()
            if not verified_missing_df.empty:
                verified_missing_df["is_missing"] = True
                if "comp_name" in verified_missing_df.columns and "competitor_name" not in verified_missing_df.columns:
                    verified_missing_df["competitor_name"] = verified_missing_df["comp_name"]
                if "comp_image_url" in verified_missing_df.columns and "image_url" not in verified_missing_df.columns:
                    verified_missing_df["image_url"] = verified_missing_df["comp_image_url"]
                if missing_df.empty:
                    missing_df = verified_missing_df
                else:
                    missing_df = pd.concat([missing_df, verified_missing_df], ignore_index=True, sort=False)

            # Approve & Sync Prices to Salla
            sync_df = pd.concat([df_higher, df_lower, df_approved], ignore_index=True, sort=False)
            if not sync_df.empty and "sku" in sync_df.columns:
                sync_df = sync_df.drop_duplicates(subset=["sku"], keep="last")
            st.markdown("### 🚀 Approve & Sync Prices to Salla")
            sync_disabled = bool(st.session_state.get("pricing_sync_in_progress", False)) or sync_df.empty
            if st.button(
                f"🚀 اعتماد ومزامنة الأسعار إلى سلة ({len(sync_df)})",
                key="btn_approve_sync_salla_make",
                type="primary",
                disabled=sync_disabled,
                use_container_width=True,
            ):
                st.session_state["pricing_sync_in_progress"] = True
                try:
                    with st.spinner("جاري إرسال البيانات إلى متجر سلة عبر Make.com..."):
                        ok = send_approved_prices_to_make(sync_df)
                    if ok:
                        sent_skus = set(sync_df["sku"].astype(str).tolist()) if "sku" in sync_df.columns else set()
                        base_df = st.session_state.get("final_priced_df")
                        if isinstance(base_df, pd.DataFrame) and not base_df.empty and sent_skus:
                            base_df = base_df.copy()
                            if "status" not in base_df.columns:
                                base_df["status"] = ""
                            _m = base_df["sku"].astype(str).isin(sent_skus) if "sku" in base_df.columns else pd.Series([False] * len(base_df), index=base_df.index)
                            base_df.loc[_m, "status"] = "sent_to_make"
                            st.session_state["final_priced_df"] = base_df
                            st.session_state["sent_to_make_keys"] = st.session_state.get("sent_to_make_keys", set()) | sent_skus
                        st.toast("تم تحديث الأسعار بنجاح! 🚀", icon="✅")
                        st.session_state["pricing_sync_in_progress"] = False
                        st.rerun()
                    else:
                        st.error("فشل إرسال الأسعار إلى Make.com. تحقق من Webhook والاتصال.")
                except Exception as _sync_e:
                    logger.exception("Pricing sync to Make failed")
                    st.error(f"حدث خطأ أثناء المزامنة: {_sync_e}")
                finally:
                    st.session_state["pricing_sync_in_progress"] = False

            tabs = st.tabs(
                [
                    "🔴 سعر أعلى",
                    "🟢 سعر أقل",
                    "✅ موافق عليها",
                    "🔍 منتجات مفقودة",
                    "⚠️ تحت المراجعة",
                    "✔️ تمت المعالجة",
                ]
            )
            with tabs[0]:
                render_product_cards(df_higher, key_prefix="wf_higher")
            with tabs[1]:
                render_product_cards(df_lower, key_prefix="wf_lower")
            with tabs[2]:
                render_product_cards(df_approved, key_prefix="wf_approved")
            with tabs[3]:
                render_product_cards(missing_df, key_prefix="wf_missing")
            with tabs[4]:
                render_product_cards(df_review, key_prefix="wf_review")
            with tabs[5]:
                render_product_cards(df_processed, key_prefix="wf_processed")

        # ═══ END of `if df is not None:` block ═══

    except Exception as e:
        logger.exception("Critical error in pricing dashboard block: %s", e)
        st.error(f"حدث خطأ غير متوقع أثناء تشغيل لوحة التسعير: {type(e).__name__}: {str(e)[:200]}")

# --- END OF CORRECTED SECTION ---
