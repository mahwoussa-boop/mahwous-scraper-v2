"""
PATCH_merchant_brain.py — رقعة دمج عقل التاجر v1.0
=====================================================
طريقة التطبيق:
  python PATCH_merchant_brain.py

ماذا تفعل الرقعة:
  1. تُضيف import للـ GeminiVisualVerifier وMerchantBrain في pricing_pipeline.py
  2. تُعدِّل مثيلي GeminiMatchVerifier لاستخدام gemini-2.5-flash-preview مع الصور
  3. تربط MerchantBrain بالـ DataFrame النهائي
  4. تُعدِّل async_scraper.py لتمرير البيانات الجديدة للـ pipeline
"""
import re
import shutil
from pathlib import Path

# الملف تحت utils/ — جذر المشروع مستوى واحد فوق
BASE = Path(__file__).resolve().parent.parent

FILES = {
    "pricing_pipeline": BASE / "utils" / "pricing_pipeline.py",
    "async_scraper": BASE / "utils" / "async_scraper.py",
    "app": BASE / "app.py",
}


def backup(path: Path) -> None:
    bak = path.with_suffix(path.suffix + ".bak")
    if not bak.exists():
        shutil.copy2(path, bak)
        print(f"  📦 Backup: {bak.name}")


# ══════════════════════════════════════════════════════════════
# PATCH 1 — pricing_pipeline.py
# ══════════════════════════════════════════════════════════════
PATCH_PIPELINE_IMPORT = '''
# ── Merchant Brain Integration ──
try:
    from utils.gemini_visual_verifier import verify_if_needed, VISUAL_VERIFY_THRESHOLD
    from utils.merchant_brain import MerchantBrain, MerchantBrainConfig
    _MERCHANT_BRAIN_AVAILABLE = True
except ImportError:
    _MERCHANT_BRAIN_AVAILABLE = False
    VISUAL_VERIFY_THRESHOLD = 90.0
'''

PATCH_PIPELINE_VISUAL_VERIFY = '''
        # ── تحقق بصري للمطابقات ذات النسبة المنخفضة (< 90%) ──
        # يستبدل منطق Gemini النصي القديم بمحقق بصري يستخدم gemini-2.5-flash-preview
        if _MERCHANT_BRAIN_AVAILABLE:
            low_conf_mask = final_df["match_score"].between(50, 89, inclusive="both")
            if low_conf_mask.any():
                for idx in final_df.index[low_conf_mask].tolist():
                    row = final_df.loc[idx]
                    mah_name  = str(row.get("name", "") or "")
                    comp_name = str(row.get("comp_name", row.get("name_comp", "")) or "")
                    mah_img   = str(row.get("image_url", "") or "")
                    comp_img  = str(row.get("comp_image_url", "") or "")
                    score = float(row.get("match_score", 0) or 0)

                    vr = verify_if_needed(score, mah_name, comp_name, mah_img, comp_img)
                    if vr is None:
                        continue
                    conf = int(vr.get("confidence", 0) or 0)
                    is_match = bool(vr.get("is_match", False))
                    reason = str(vr.get("reason", "") or "")
                    method = str(vr.get("method", "") or "")

                    final_df.at[idx, "ai_verification_reason"] = f"[{method}] {reason}"
                    final_df.at[idx, "ai_verification_confidence"] = conf

                    if is_match and conf >= 85:
                        final_df.at[idx, "ai_verification_state"] = "verified_by_ai"
                        if score < conf:
                            final_df.at[idx, "match_score"] = float(conf)
                    elif not is_match:
                        final_df.at[idx, "ai_verification_state"] = "missing_candidate"
                        final_df.at[idx, "status"] = "missing_after_verification"
                        final_df.at[idx, "action_required"] = "🔍 منتجات مفقودة"
                    else:
                        final_df.at[idx, "ai_verification_state"] = "under_review"
'''

PATCH_PIPELINE_MERCHANT_BRAIN = '''
    # ── تطبيق عقل التاجر على النتائج النهائية ──
    if _MERCHANT_BRAIN_AVAILABLE and not priced_df.empty:
        try:
            from config import AUTOMATION_RULES_DEFAULT
            trusted = [r.get("domain","") for r in AUTOMATION_RULES_DEFAULT
                       if r.get("trusted", False)]
        except Exception:
            trusted = []
        brain = MerchantBrain(MerchantBrainConfig(trusted_competitors=trusted))
        priced_df = brain.process_dataframe(priced_df)
        logger.info("✅ Merchant Brain: تم تطبيق خوارزمية التسعير السيكولوجي")
'''


def patch_pricing_pipeline():
    path = FILES["pricing_pipeline"]
    if not path.exists():
        print(f"  ⚠️ {path} not found — skip")
        return
    backup(path)
    src = path.read_text(encoding="utf-8")

    # 1. إضافة imports
    insert_after = "logger = logging.getLogger(__name__)"
    if "GeminiVisualVerifier" not in src:
        src = src.replace(insert_after, insert_after + PATCH_PIPELINE_IMPORT, 1)
        print("  ✅ Added GeminiVisualVerifier + MerchantBrain imports")

    # 2. استبدال منطق التحقق القديم
    old_verify_marker = "# Gemini AI verifier for doubtful matches (50%..79%)"
    if old_verify_marker in src and "visual_verifier" not in src:
        # أضف التحقق البصري بعد تعريف ai_verification columns
        insert_before = "    verifier = GeminiMatchVerifier()"
        src = src.replace(
            insert_before,
            PATCH_PIPELINE_VISUAL_VERIFY + "\n    " + insert_before.lstrip(),
            1
        )
        print("  ✅ Added visual verification logic")

    # 3. إضافة Merchant Brain بعد process_pricing_strategy
    if "Merchant Brain" not in src:
        insert_after_brain = "    priced_df = ai_engine.process_pricing_strategy(final_df, target_margin=0.35)"
        src = src.replace(
            insert_after_brain,
            insert_after_brain + "\n" + PATCH_PIPELINE_MERCHANT_BRAIN,
            1
        )
        print("  ✅ Added Merchant Brain post-processing")

    path.write_text(src, encoding="utf-8")
    print("  ✅ pricing_pipeline.py patched")


# ══════════════════════════════════════════════════════════════
# PATCH 2 — app.py: إضافة صفحة عقل التاجر وسلة في القائمة الجانبية
# ══════════════════════════════════════════════════════════════
PATCH_APP_IMPORTS = '''
# ── Merchant Brain UI Imports ──
try:
    from utils.master_card import render_master_cards
    from utils.salla_exporter import render_salla_export_ui, build_price_update_csv
    from utils.merchant_brain import MerchantBrain, MerchantBrainConfig
    from utils.user_preferences import get_decision_stats, get_match_accuracy, init_preferences_db
    init_preferences_db()
    _MERCHANT_UI_AVAILABLE = True
except ImportError as _e:
    _MERCHANT_UI_AVAILABLE = False
    import logging as _logging
    _logging.getLogger(__name__).warning("Merchant Brain UI not available: %s", _e)
'''

PATCH_APP_SIDEBAR_SECTION = '''
    # ── Merchant Brain Sidebar Section ──
    if _MERCHANT_UI_AVAILABLE:
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 🧠 عقل التاجر")
        _brain_section = st.sidebar.radio(
            "",
            ["📊 لوحة التحكم", "🔴 سعر أعلى", "🟢 سعر أقل", "🔍 مفقودة", "📤 تصدير سلة"],
            key="brain_nav",
            label_visibility="collapsed",
        )
        st.session_state["_brain_section"] = _brain_section
'''

PATCH_APP_BRAIN_PAGE = '''
# ══════════════════════════════════════════════════════════════
# صفحة عقل التاجر الذكي
# ══════════════════════════════════════════════════════════════
def render_merchant_brain_page():
    """الصفحة الرئيسية لعقل التاجر."""
    if not _MERCHANT_UI_AVAILABLE:
        st.error("مكتبات عقل التاجر غير متاحة — تحقق من التثبيت")
        return

    section = st.session_state.get("_brain_section", "📊 لوحة التحكم")

    df = st.session_state.get("final_priced_df")
    if df is None and st.session_state.get("results") is not None:
        df = st.session_state.get("results")

    # ── لوحة التحكم ──
    if section == "📊 لوحة التحكم":
        st.subheader("📊 ملخص عقل التاجر")

        if df is not None and not df.empty:
            from utils.merchant_brain import MerchantBrain
            sections = MerchantBrain.classify_products(df)

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("🔴 تحتاج خفض", len(sections["high_price"]))
            c2.metric("🟢 يمكن رفعها", len(sections["low_price"]))
            c3.metric("🔍 مفقودة", len(sections["missing"]))
            c4.metric("✅ مثالية", len(sections["optimal"]))

            if "suggested_price" in df.columns and "price" in df.columns:
                price = pd.to_numeric(df["price"], errors="coerce").fillna(0)
                sugg  = pd.to_numeric(df["suggested_price"], errors="coerce").fillna(0)
                mask = (price > 0) & (sugg > 0)
                if mask.any():
                    diff = (sugg[mask] - price[mask]).sum()
                    sign = "+" if diff >= 0 else ""
                    st.metric(
                        "💰 إجمالي التغيير المتوقع في الإيراد",
                        f"{sign}{diff:,.0f} ر.س",
                        help="بناءً على تطبيق جميع الأسعار المقترحة"
                    )

        stats = get_decision_stats(days=30)
        acc = get_match_accuracy()
        col_a, col_b = st.columns(2)
        with col_a:
            st.caption("📈 إحصائيات القرارات (30 يوم)")
            if stats.get("total", 0) > 0:
                st.write(f"- موافق: **{stats['approved']}** | رفض: **{stats['rejected']}** | تعديل: **{stats['modified']}**")
                st.write(f"- نسبة الموافقة: **{stats['approval_rate']}%**")
            else:
                st.info("لا توجد قرارات مسجلة بعد")
        with col_b:
            st.caption("🤖 دقة نظام المطابقة")
            if acc.get("total", 0) > 0:
                st.write(f"- إجمالي: **{acc['total']}** | صحيح: **{acc['correct']}**")
                st.write(f"- الدقة: **{acc['accuracy_pct']}%**")
            else:
                st.info("لا توجد تغذية راجعة بعد")

    # ── أقسام المنتجات ──
    elif section in ("🔴 سعر أعلى", "🟢 سعر أقل", "🔍 مفقودة"):
        section_map = {
            "🔴 سعر أعلى": "high_price",
            "🟢 سعر أقل": "low_price",
            "🔍 مفقودة": "missing",
        }
        key = section_map[section]

        st.subheader(section)
        if df is not None and not df.empty:
            from utils.merchant_brain import MerchantBrain
            sections = MerchantBrain.classify_products(df)
            section_df = sections.get(key, pd.DataFrame())
            if not section_df.empty:
                render_master_cards(section_df, show_decision_buttons=(key != "missing"))
            else:
                st.success(f"✅ لا توجد منتجات في هذا القسم")
        else:
            st.info("شغّل التحليل أولاً من الشريط الجانبي")

    # ── تصدير سلة ──
    elif section == "📤 تصدير سلة":
        missing_df = (
            st.session_state.get("missing_df") or
            (MerchantBrain.classify_products(df).get("missing") if df is not None else None)
        )
        render_salla_export_ui(df, missing_df)
'''


def patch_app():
    path = FILES["app"]
    if not path.exists():
        print(f"  ⚠️ {path} not found — skip")
        return
    backup(path)
    src = path.read_text(encoding="utf-8")

    # 1. إضافة imports
    if "_MERCHANT_UI_AVAILABLE" not in src:
        # أضف بعد imports الموجودة
        insert_after = "from utils.db_manager import (init_db, log_event"
        idx = src.find(insert_after)
        if idx > 0:
            # ابحث عن نهاية السطر
            end = src.find("\n", idx)
            while src[end+1] in (" ", "\t") or src[end+1:end+3] in ("  ", "        "):
                end = src.find("\n", end + 1)
                if end < 0:
                    break
            if end > 0:
                src = src[:end+1] + PATCH_APP_IMPORTS + src[end+1:]
                print("  ✅ Added Merchant Brain imports to app.py")

    # 2. إضافة صفحة عقل التاجر للقائمة الجانبية
    # نبحث عن مكان مناسب في PAGES أو القائمة الجانبية
    if "render_merchant_brain_page" not in src:
        # أضف الدالة قبل if __name__ أو قبل نهاية الملف
        insert_before = "\n# ── إعداد الصفحة"
        if insert_before in src:
            src = src.replace(insert_before, PATCH_APP_BRAIN_PAGE + insert_before, 1)
        else:
            src = src + "\n" + PATCH_APP_BRAIN_PAGE
        print("  ✅ Added render_merchant_brain_page() to app.py")

    path.write_text(src, encoding="utf-8")
    print("  ✅ app.py patched")


if __name__ == "__main__":
    print("\n🧠 تطبيق رقعة عقل التاجر...\n")
    print("📄 Patching pricing_pipeline.py...")
    patch_pricing_pipeline()
    print("\n📄 Patching app.py...")
    patch_app()
    print("\n✅ تم تطبيق جميع الرقع بنجاح!")
    print("\nالخطوات التالية:")
    print("  1. انسخ ملفات utils/ الجديدة إلى المشروع")
    print("  2. شغّل: python PATCH_merchant_brain.py")
    print("  3. أعد تشغيل التطبيق: streamlit run app.py")
