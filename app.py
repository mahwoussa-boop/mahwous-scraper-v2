"""
app.py - نظام التسعير الذكي مهووس v26.1
✅ معالجة خلفية مع حفظ تلقائي
✅ جداول مقارنة بصرية في كل الأقسام
✅ أزرار AI + قرارات لكل منتج
✅ بحث أسعار السوق والمنافسين
✅ بحث mahwous.com للمنتجات المفقودة
✅ تحديث تلقائي للأسعار عند إعادة رفع المنافس
✅ تصدير Make لكل منتج وللمجموعات
✅ Gemini Chat مباشر
✅ فلاتر ذكية في كل قسم
✅ تاريخ جميل لكل العمليات
✅ محرك أتمتة ذكي مع قواعد تسعير قابلة للتخصيص (v26.0)
✅ لوحة تحكم الأتمتة متصلة بالتنقل (v26.0)
"""
import streamlit as st
import pandas as pd
import asyncio
import os
import threading
import time
import uuid
import logging
from datetime import datetime

try:
    from streamlit.runtime.scriptrunner import add_script_run_ctx
except ImportError:
    try:
        from streamlit.scriptrunner import add_script_run_ctx
    except ImportError:
        def add_script_run_ctx(t): return t

from config import *
import config as _config
_config.refresh_gemini_keys()

# Fix for Bug #2: Undefined variable _FR in AI Page
_FR = "https://www.fragranticarabia.com"

from styles import get_styles, stat_card, vs_card, comp_strip, miss_card, get_sidebar_toggle_js
from engines.engine import (read_file, run_full_analysis, find_missing_products,
                             extract_brand, extract_size, extract_type, is_sample,
                             guess_default_columns, apply_column_mapping)
from engines.ai_engine import (call_ai, gemini_chat, chat_with_ai,
                                verify_match, analyze_product,
                                bulk_verify, suggest_price,
                                search_market_price, search_mahwous,
                                check_duplicate, process_paste,
                                fetch_fragrantica_info, fetch_product_images,
                                generate_mahwous_description,
                                analyze_paste, reclassify_review_items,
                                ai_deep_analysis)
from engines.automation import (AutomationEngine, ScheduledSearchManager,
                                 auto_push_decisions, auto_process_review_items,
                                 process_confirmed_batch, safety_check_decisions,
                                 safe_push_decisions,
                                 log_automation_decision, get_automation_log,
                                 get_automation_stats)
from utils.db_manager import check_strict_duplicate
from utils.helpers import (apply_filters, get_filter_options, export_to_excel,
                            export_multiple_sheets, parse_pasted_text,
                            safe_float, format_price, format_diff, make_columns_unique)
try:
    from utils.make_helper import (send_price_updates, send_new_products,
                                    send_missing_products, send_single_product,
                                    verify_webhook_connection, export_to_make_format,
                                    send_batch_smart, build_pricing_sync_payload,
                                    bulk_sync_pricing_recommendations,
                                    is_pricing_webhook_configured,
                                    send_approved_prices_to_make)
except Exception:
    # Fallback for environments running older cached make_helper versions.
    from utils import make_helper as _mkh

    send_price_updates = _mkh.send_price_updates
    send_new_products = _mkh.send_new_products
    send_missing_products = _mkh.send_missing_products
    send_single_product = _mkh.send_single_product
    verify_webhook_connection = _mkh.verify_webhook_connection
    export_to_make_format = _mkh.export_to_make_format
    send_batch_smart = _mkh.send_batch_smart
    build_pricing_sync_payload = _mkh.build_pricing_sync_payload
    bulk_sync_pricing_recommendations = _mkh.bulk_sync_pricing_recommendations
    is_pricing_webhook_configured = _mkh.is_pricing_webhook_configured
    send_approved_prices_to_make = getattr(_mkh, "send_approved_prices_to_make", None)

    if send_approved_prices_to_make is None:
        def send_approved_prices_to_make(df):
            return False
from utils.competitor_manager import render_competitor_scrape_page
from utils.db_manager import (init_db, log_event, log_decision,
                               log_analysis, get_events, get_decisions,
                               get_analysis_history, upsert_price_history,
                               get_price_history, get_price_changes,
                               save_job_progress, get_job_progress, get_last_job,
                               clear_missing_from_last_job,
                               save_hidden_product, get_hidden_product_keys,
                               init_db_v26, upsert_our_catalog, upsert_comp_catalog,
                               save_processed, get_processed, undo_processed,
                               get_processed_keys, migrate_db_v26)

INTERNAL_STORE_PATH = os.path.join("data", "mahwous_catalog.csv")

# ── إعداد الصفحة ──────────────────────────
logger = logging.getLogger(__name__)
st.set_option("client.showErrorDetails", False)
st.set_page_config(page_title=APP_TITLE, page_icon=APP_ICON,
                   layout="wide", initial_sidebar_state="expanded")
st.markdown(get_styles(), unsafe_allow_html=True)
st.markdown(get_sidebar_toggle_js(), unsafe_allow_html=True)
st.markdown(
    "<h1 style='margin:0 0 8px 0;color:#00C853;'>Mahwous.com - Smart Pricing Dashboard</h1>",
    unsafe_allow_html=True,
)
if BASELINE_LOCKED:
    _dom = normalize_domain(MAIN_STORE_DOMAIN)
    if _dom != "mahwous.com":
        st.error("Baseline guard: MAIN_STORE_DOMAIN must remain mahwous.com")
        st.stop()
if "db_initialized" not in st.session_state:
    try:
        init_db()
        init_db_v26()
        migrate_db_v26()  # v26.0 — ترحيل آمن (idempotent)
        st.session_state["db_initialized"] = True
    except Exception as e:
        st.error(f"Database Initialization Error: {e}")

# ── Session State ─────────────────────────
_defaults = {
    "results": None, "missing_df": None, "analysis_df": None,
    "chat_history": [], "job_id": None, "job_running": False,
    "decisions_pending": {},   # {product_name: action}
    "our_df": None, "comp_dfs": None,  # حفظ الملفات للمنتجات المفقودة
    "store_df": None,  # نسخة داخلية من كتالوج متجر مهووس
    "store_autoloaded": False,
    "hidden_products": set(),  # منتجات أُرسلت لـ Make أو أُزيلت
    "selected_products": {},   # {prefix: [list of row dicts]} — القرارات المجمعة بالـ Checkboxes
    "final_priced_df": None,   # نتيجة خط أنابيب التسعير (مطابقة + كاشط + مقترح سعر)
    "pricing_sync_in_progress": False,
}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# تحميل المنتجات المخفية من قاعدة البيانات عند كل تشغيل
_db_hidden = get_hidden_product_keys()
st.session_state.hidden_products = st.session_state.hidden_products | _db_hidden


def _autoload_internal_store_catalog(show_message: bool = False) -> None:
    """تحميل كتالوج متجر مهووس الداخلي تلقائياً وربطه بـ our_df/store_df."""
    _store_df = st.session_state.get("store_df")
    _our_df = st.session_state.get("our_df")
    if isinstance(_store_df, pd.DataFrame) and not _store_df.empty:
        if not (isinstance(_our_df, pd.DataFrame) and not _our_df.empty):
            st.session_state.our_df = _store_df
        if show_message and st.session_state.get("store_autoloaded", False):
            st.success("✅ تم تحميل ملف متجر مهووس الأساسي تلقائياً من النظام.")
        return
    if isinstance(_our_df, pd.DataFrame) and not _our_df.empty:
        st.session_state.store_df = _our_df
        return
    if os.path.exists(INTERNAL_STORE_PATH):
        try:
            _loaded = pd.read_csv(INTERNAL_STORE_PATH)
            if isinstance(_loaded, pd.DataFrame) and not _loaded.empty:
                st.session_state.store_df = _loaded
                st.session_state.our_df = _loaded
                st.session_state.store_autoloaded = True
                if show_message:
                    st.success("✅ تم تحميل ملف متجر مهووس الأساسي تلقائياً من النظام.")
        except Exception as _e:
            st.warning(f"تعذر تحميل ملف متجر مهووس الداخلي: {_e}")

# ════════════════════════════════════════════════
#  دوال المعالجة — يجب تعريفها قبل استخدامها
# ════════════════════════════════════════════════
def _split_results(df):
    """تقسيم نتائج التحليل على الأقسام بأمان تام"""
    def _contains(col, txt):
        try:
            return df[col].str.contains(txt, na=False, regex=False)
        except Exception:
            return pd.Series([False] * len(df))
    return {
        "price_raise": df[_contains("القرار", "أعلى")].reset_index(drop=True),
        "price_lower": df[_contains("القرار", "أقل")].reset_index(drop=True),
        "approved":    df[_contains("القرار", "موافق")].reset_index(drop=True),
        "review":      df[_contains("القرار", "مراجعة")].reset_index(drop=True),
        "all":         df,
    }


def _match_score_col(df: pd.DataFrame) -> str | None:
    """اسم عمود نسبة التطابق (إنجليزي حالياً أو عربي من جلسات/JSON قديمة)."""
    if df is None or df.empty:
        return None
    if "match_score" in df.columns:
        return "match_score"
    if "نسبة_التطابق" in df.columns:
        return "نسبة_التطابق"
    return None


def _safe_results_for_json(results_list):
    """تحويل النتائج لصيغة آمنة للحفظ في JSON/SQLite — يحول القوائم المتداخلة"""
    safe = []
    for r in results_list:
        row = {}
        for k, v in (r.items() if isinstance(r, dict) else {}):
            if isinstance(v, list):
                # تحويل قوائم المنافسين لنص JSON
                try:
                    import json as _j
                    row[k] = _j.dumps(v, ensure_ascii=False, default=str)
                except Exception:
                    row[k] = str(v)
            elif pd.isna(v) if isinstance(v, float) else False:
                row[k] = 0
            else:
                row[k] = v
        safe.append(row)
    return safe


def _first_display_image_url(img_result):
    """يستخرج أول رابط صورة حقيقي من نتيجة fetch_product_images (ليست صفحة بحث)."""
    if not img_result or not isinstance(img_result, dict):
        return ""
    for im in img_result.get("images") or []:
        if not isinstance(im, dict):
            continue
        if im.get("is_search"):
            continue
        u = str(im.get("url") or "").strip()
        if u.startswith("http") and any(ext in u.lower() for ext in (".jpg", ".png", ".webp", ".jpeg")):
            return u
    return ""


def _image_url_from_row(row: pd.Series, *keys: str) -> str:
    """أول رابط صورة صالح من أعمدة الصف (ملف المنتجات أو كشط المنافسين)."""
    for k in keys:
        if k not in row.index:
            continue
        v = row.get(k)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = str(v).strip()
        if s.startswith("http"):
            return s
    return ""


def _build_cards_df(source_df: pd.DataFrame, section_label: str) -> pd.DataFrame:
    """تهيئة DataFrame موحّد لعرض البطاقات التبويبية."""
    if source_df is None or source_df.empty:
        return pd.DataFrame()
    d = source_df.copy()

    def _pick(cols, default="N/A"):
        for c in cols:
            if c in d.columns:
                return d[c]
        return pd.Series([default] * len(d), index=d.index)

    out = pd.DataFrame(index=d.index)
    out["name"] = _pick(["المنتج", "منتج_المنافس", "product_name", "name"])
    out["sku"] = _pick(["معرف_المنتج", "معرف_المنافس", "product_id", "sku", "product_key"])
    out["price"] = pd.to_numeric(_pick(["السعر", "price", "old_price"], 0), errors="coerce").fillna(0)
    out["comp_price"] = pd.to_numeric(
        _pick(["سعر_المنافس", "comp_price", "new_price"], 0), errors="coerce"
    ).fillna(0)
    out["suggested_price"] = pd.to_numeric(
        _pick(["السعر_المقترح", "suggested_price", "new_price", "price"], 0), errors="coerce"
    ).fillna(0)
    out["brand"] = _pick(["الماركة", "brand"], "N/A")
    out["category"] = _pick(["النوع", "category"], section_label)
    out["competitor_name"] = _pick(["المنافس", "competitor", "comp_name"], "Competitor")
    out["image_url"] = _pick(
        ["image_url", "comp_image_url", "رابط_الصورة", "thumbnail", "image"],
        "",
    )
    out["action_required"] = _pick(["action_required", "القرار", "action"], section_label)
    return out


def _restore_results_from_json(results_list):
    """استعادة النتائج من JSON — يحول نصوص القوائم لقوائم فعلية"""
    import json as _j
    restored = []
    for r in results_list:
        row = dict(r) if isinstance(r, dict) else {}
        for k in ["جميع_المنافسين", "جميع المنافسين"]:
            v = row.get(k)
            if isinstance(v, str):
                try:
                    row[k] = _j.loads(v)
                except Exception:
                    row[k] = []
            elif v is None:
                row[k] = []
        restored.append(row)
    return restored


def _auto_refresh_results_from_background_files() -> bool:
    """
    يوزّع المنتجات تلقائياً من ملفات الخلفية الجاهزة (بدون رفع يدوي):
    - data/final_priced_latest.csv
    - data/competitors_latest.csv (لاستخراج مفقودات إضافية)
    """
    priced_path = os.path.join(os.getcwd(), "data", "final_priced_latest.csv")
    comp_csv = os.path.join(os.getcwd(), "data", "competitors_latest.csv")
    if not os.path.exists(priced_path):
        # fallback: وزّع على الأقل المفقودات من ملف المنافسين حتى لا تبقى الأقسام صفراً
        if os.path.exists(comp_csv):
            try:
                comp_raw = pd.read_csv(comp_csv)
                if comp_raw.empty:
                    return False
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
                    comp_raw["name"] = ""
                if "sku" not in comp_raw.columns:
                    comp_raw["sku"] = ""
                comp_raw["is_missing"] = True
                comp_raw["status"] = "missing"
                st.session_state.results = {
                    "price_raise": pd.DataFrame(),
                    "price_lower": pd.DataFrame(),
                    "approved": pd.DataFrame(),
                    "review": pd.DataFrame(),
                    "missing": comp_raw,
                    "all": comp_raw,
                }
                st.session_state.analysis_df = comp_raw.copy()
                return True
            except Exception:
                return False
        return False
    try:
        work = pd.read_csv(priced_path)
        if work is None or work.empty:
            return False
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
        work["min_comp_price"] = work.groupby("_group_key", dropna=False)["comp_price"].transform("min")
        work["min_comp_price"] = pd.to_numeric(work["min_comp_price"], errors="coerce").fillna(0.0)

        rel_diff = (
            (work["price"] - work["min_comp_price"]).abs()
            / work["min_comp_price"].replace(0, pd.NA)
        ).fillna(999.0)
        status_l = work["status"].str.lower()
        valid_comp = work["min_comp_price"] > 0
        mask_missing = status_l.eq("missing_after_verification")
        mask_processed = status_l.eq("sent_to_make")
        mask_review = (~mask_missing) & (status_l.isin({"processing", "under_review"}) | (work["match_score"] < 80))
        mask_higher = (~mask_missing & ~mask_processed & ~mask_review & valid_comp & (work["price"] > work["min_comp_price"]))
        mask_lower = (~mask_missing & ~mask_processed & ~mask_review & valid_comp & (work["price"] < work["min_comp_price"]))
        mask_approved = (~mask_missing & ~mask_processed & ~mask_review & valid_comp & (rel_diff <= 0.02))

        missing_df = work[mask_missing].copy()
        if os.path.exists(comp_csv):
            comp_raw = pd.read_csv(comp_csv)
            rename_map = {"الاسم": "name", "sku": "sku"}
            for k, v in rename_map.items():
                if k in comp_raw.columns and v not in comp_raw.columns:
                    comp_raw[v] = comp_raw[k]
            if "name" not in comp_raw.columns:
                comp_raw["name"] = ""
            if "sku" not in comp_raw.columns:
                comp_raw["sku"] = ""
            mah_skus = set(work.get("sku", pd.Series(dtype=str)).astype(str).str.strip().tolist())
            mah_names = set(work.get("name", pd.Series(dtype=str)).astype(str).str.strip().tolist())
            miss_mask = (~comp_raw["sku"].astype(str).str.strip().isin(mah_skus)) & (
                ~comp_raw["name"].astype(str).str.strip().isin(mah_names)
            )
            more_missing = comp_raw.loc[miss_mask].copy()
            if not more_missing.empty:
                more_missing["is_missing"] = True
                missing_df = pd.concat([missing_df, more_missing], ignore_index=True, sort=False)

        auto_results = {
            "price_raise": work[mask_higher].copy(),
            "price_lower": work[mask_lower].copy(),
            "approved": work[mask_approved].copy(),
            "review": work[mask_review].copy(),
            "missing": missing_df,
            "all": work.copy(),
        }
        st.session_state.results = auto_results
        st.session_state.analysis_df = work.copy()
        st.session_state.final_priced_df = work.copy()
        return True
    except Exception:
        return False


# ── تحميل تلقائي للنتائج المحفوظة عند فتح التطبيق ──
# ملاحظة: لا تستخدم `if results:` لأن [] تُعتبر False وتمنع الاستعادة رغم أن الحفظ نجح
if st.session_state.results is None and not st.session_state.job_running:
    _auto_job = get_last_job()
    if _auto_job and _auto_job["status"] == "done" and _auto_job.get("results") is not None:
        _auto_records = _restore_results_from_json(_auto_job["results"])
        _auto_df = pd.DataFrame(_auto_records)
        _auto_miss = pd.DataFrame(_auto_job.get("missing", [])) if _auto_job.get("missing") else pd.DataFrame()
        _auto_r = _split_results(_auto_df)
        _auto_r["missing"] = _auto_miss
        st.session_state.results     = _auto_r
        st.session_state.analysis_df = _auto_df
        st.session_state.job_id      = _auto_job.get("job_id")
    else:
        _auto_refresh_results_from_background_files()


# ── دوال مساعدة ───────────────────────────
def db_log(page, action, details=""):
    try: log_event(page, action, details)
    except: pass

def ts_badge(ts_str=""):
    """شارة تاريخ مصغرة جميلة"""
    if not ts_str:
        ts_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    return f'<span style="font-size:.65rem;color:#555;background:#1a1a2e;padding:1px 6px;border-radius:8px;margin-right:4px">🕐 {ts_str}</span>'

def decision_badge(action):
    colors = {
        "approved": ("#00C853", "✅ موافق"),
        "deferred": ("#FFD600", "⏸️ مؤجل"),
        "removed":  ("#FF1744", "🗑️ محذوف"),
    }
    c, label = colors.get(action, ("#666", action))
    return f'<span style="font-size:.7rem;color:{c};font-weight:700">{label}</span>'


def _run_analysis_background(job_id, our_df, comp_dfs, our_file_name, comp_names):
    """تعمل في thread منفصل — تحفظ النتائج كل 10 منتجات مع حماية شاملة من الأخطاء"""
    total     = len(our_df)
    processed = 0
    _last_save = [0]  # آخر عدد تم حفظه (mutable لـ closure)

    def progress_cb(pct, current_results):
        nonlocal processed
        processed = int(pct * total)
        # حفظ كل 25 منتجاً أو عند الاكتمال (تقليل ضغط SQLite)
        if processed - _last_save[0] >= 25 or processed >= total:
            _last_save[0] = processed
            try:
                safe_res = _safe_results_for_json(current_results)
                save_job_progress(
                    job_id, total, processed,
                    safe_res,
                    "running",
                    our_file_name, comp_names
                )
            except Exception as _save_err:
                # لا نوقف المعالجة بسبب خطأ حفظ جزئي
                import traceback
                traceback.print_exc()

    analysis_df = pd.DataFrame()
    missing_df  = pd.DataFrame()

    # ── المرحلة 1: التحليل الرئيسي ──────────────────────────────────
    try:
        analysis_df = run_full_analysis(
            our_df, comp_dfs,
            progress_callback=progress_cb
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        # حفظ ما تم تحليله حتى الآن كنتائج جزئية
        save_job_progress(
            job_id, total, processed,
            [], f"error: تحليل المقارنة فشل — {str(e)[:200]}",
            our_file_name, comp_names
        )
        return

    # ── المرحلة 2: حفظ تاريخ الأسعار (لا يوقف المعالجة إذا فشل) ────
    try:
        for _, row in analysis_df.iterrows():
            if safe_float(row.get("match_score", 0)) > 0:
                upsert_price_history(
                    str(row.get("المنتج",       "")),
                    str(row.get("المنافس",       "")),
                    safe_float(row.get("سعر_المنافس", 0)),
                    safe_float(row.get("السعر",       0)),
                    safe_float(row.get("الفرق",        0)),
                    safe_float(row.get("match_score", 0)),
                    str(row.get("القرار",         ""))
                )
    except Exception:
        pass  # تاريخ الأسعار ثانوي — لا نوقف المعالجة

    # ── المرحلة 3: المنتجات المفقودة (منفصلة عن التحليل) ────────────
    try:
        missing_df = find_missing_products(our_df, comp_dfs)
    except Exception as e:
        import traceback
        traceback.print_exc()
        missing_df = pd.DataFrame()  # فشلت المفقودة لكن النتائج الرئيسية محفوظة

    # ── المرحلة 4: الحفظ النهائي ────────────────────────────────────
    try:
        safe_records = _safe_results_for_json(analysis_df.to_dict("records"))
        safe_missing = missing_df.to_dict("records") if not missing_df.empty else []

        save_job_progress(
            job_id, total, total,
            safe_records,
            "done",
            our_file_name, comp_names,
            missing=safe_missing
        )
        log_analysis(
            our_file_name, comp_names, total,
            int((analysis_df.get("match_score", pd.Series(dtype=float)) > 0).sum()),
            len(missing_df)
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        # محاولة أخيرة — حفظ بدون missing
        try:
            save_job_progress(
                job_id, total, total,
                _safe_results_for_json(analysis_df.to_dict("records")),
                "done",
                our_file_name, comp_names,
                missing=[]
            )
        except Exception:
            save_job_progress(
                job_id, total, processed,
                [], f"error: فشل الحفظ النهائي — {str(e)[:200]}",
                our_file_name, comp_names
            )


# ════════════════════════════════════════════════
#  مكوّن جدول المقارنة البصري (مشترك)
# ════════════════════════════════════════════════
def render_pro_table(df, prefix, section_type="update", show_search=True):
    """
    جدول احترافي بصري مع:
    - فلاتر ذكية
    - أزرار AI + قرار لكل منتج
    - تصدير Make
    - Pagination
    """
    if df is None or df.empty:
        st.info("لا توجد منتجات")
        return

    # ── فلاتر ─────────────────────────────────
    opts = get_filter_options(df)
    with st.expander("🔍 فلاتر متقدمة", expanded=False):
        c1, c2, c3, c4 = st.columns(4)
        search   = c1.text_input("🔎 بحث",    key=f"{prefix}_s")
        brand_f  = c2.selectbox("🏷️ الماركة", opts["brands"],      key=f"{prefix}_b")
        comp_f   = c3.selectbox("🏪 المنافس", opts["competitors"], key=f"{prefix}_c")
        type_f   = c4.selectbox("🧴 النوع",   opts["types"],       key=f"{prefix}_t")
        c5, c6, c7 = st.columns(3)
        match_min  = c5.slider("أقل تطابق%", 0, 100, 0, key=f"{prefix}_m")
        price_min  = c6.number_input("سعر من", 0.0, key=f"{prefix}_p1")
        price_max  = c7.number_input("سعر لـ", 0.0, key=f"{prefix}_p2")

    filters = {
        "search": search, "brand": brand_f, "competitor": comp_f,
        "type": type_f,
        "match_min": match_min if match_min > 0 else None,
        "price_min": price_min if price_min > 0 else 0.0,
        "price_max": price_max if price_max > 0 else None,
    }
    filtered = apply_filters(df, filters)

    # ── شريط الأدوات ───────────────────────────
    # تهيئة قائمة المنتجات المحددة لهذا القسم إن لم تكن موجودة
    if prefix not in st.session_state.selected_products:
        st.session_state.selected_products[prefix] = []

    ac1, ac2, ac3, ac4, ac5, ac6 = st.columns(6)
    with ac1:
        _exdf = filtered.copy()
        if "جميع المنافسين" in _exdf.columns: _exdf = _exdf.drop(columns=["جميع المنافسين"])
        if "جميع_المنافسين" in _exdf.columns: _exdf = _exdf.drop(columns=["جميع_المنافسين"])
        excel_data = export_to_excel(_exdf, prefix)
        st.download_button("📥 Excel", data=excel_data,
            file_name=f"{prefix}_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{prefix}_xl")
    with ac2:
        _csdf = filtered.copy()
        if "جميع المنافسين" in _csdf.columns: _csdf = _csdf.drop(columns=["جميع المنافسين"])
        if "جميع_المنافسين" in _csdf.columns: _csdf = _csdf.drop(columns=["جميع_المنافسين"])
        _csv_bytes = _csdf.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button("📄 CSV", data=_csv_bytes,
            file_name=f"{prefix}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv", key=f"{prefix}_csv")
    with ac3:
        _bulk_labels = {"raise": "🤖 تحليل ذكي — خفض (محدد أو 20)",
                        "lower": "🤖 تحليل ذكي — رفع (محدد أو 20)",
                        "review": "🤖 تحقق جماعي (محدد أو 20)",
                        "approved": "🤖 مراجعة (محدد أو 20)"}
        if st.button(_bulk_labels.get(prefix, "🤖 AI جماعي (محدد أو 20)"), key=f"{prefix}_bulk"):
            _sel_rows = st.session_state.selected_products.get(prefix, [])
            if _sel_rows:
                _bulk_df = pd.DataFrame(_sel_rows)
            else:
                _bulk_df = filtered.head(20)
            with st.spinner(f"🤖 AI يحلل {len(_bulk_df)} منتجاً..."):
                _section_map = {"raise": "price_raise", "lower": "price_lower",
                                "review": "review", "approved": "approved"}
                items = [{
                    "our": str(r.get("المنتج", "")),
                    "comp": str(r.get("منتج_المنافس", "")),
                    "our_price": safe_float(r.get("السعر", 0)),
                    "comp_price": safe_float(r.get("سعر_المنافس", 0))
                } for _, r in _bulk_df.iterrows()]
                res = bulk_verify(items, _section_map.get(prefix, "general"))
                st.markdown(f'<div class="ai-box">{res["response"]}</div>',
                            unsafe_allow_html=True)
    with ac4:
        if st.button("📤 إرسال كل لـ Make", key=f"{prefix}_make_all"):
            products = export_to_make_format(filtered, section_type)
            if section_type in ("missing", "new"):
                res = send_new_products(products)
            else:
                res = send_price_updates(products)
            if res["success"]:
                st.success(res["message"])
                # v26: سجّل كل منتج في processed_products
                for _i, (_idx, _r) in enumerate(filtered.iterrows()):
                    _pname = str(_r.get("المنتج", _r.get("منتج_المنافس", "")))
                    _pkey  = f"{prefix}_{_pname}_{_i}"
                    _pid_r = str(_r.get("معرف_المنتج", _r.get("معرف_المنافس", "")))
                    _comp  = str(_r.get("المنافس",""))
                    _op    = safe_float(_r.get("السعر", _r.get("سعر_المنافس", 0)))
                    _np    = safe_float(_r.get("سعر_المنافس", _r.get("السعر", 0)))
                    st.session_state.hidden_products.add(_pkey)
                    save_hidden_product(_pkey, _pname, "sent_to_make_bulk")
                    save_processed(_pkey, _pname, _comp, "send_price",
                                   old_price=_op, new_price=_np,
                                   product_id=_pid_r,
                                   notes=f"إرسال جماعي ← {prefix}")
                st.rerun()
            else:
                st.error(res["message"])
    with ac5:
        # جمع القرارات المعلقة وإرسالها
        pending = {k: v for k, v in st.session_state.decisions_pending.items()
                   if v["action"] in ["approved", "deferred", "removed"]}
        if pending and st.button(f"📦 ترحيل {len(pending)} قرار → Make", key=f"{prefix}_send_decisions"):
            to_send = [{"name": k, "action": v["action"], "reason": v.get("reason", "")}
                       for k, v in pending.items()]
            res = send_price_updates(to_send)
            st.success(f"✅ تم إرسال {len(to_send)} قرار لـ Make")
            # v26: سجّل القرارات المعلقة في processed_products
            for k, v in pending.items():
                _pkey = f"decision_{k}"
                _act  = v.get("action","approved")
                save_processed(_pkey, k, v.get("competitor",""), _act,
                               old_price=safe_float(v.get("our_price",0)),
                               new_price=safe_float(v.get("comp_price",0)),
                               notes=f"قرار معلق → Make | {v.get('reason','')}")
            st.session_state.decisions_pending = {}
            st.rerun()
    with ac6:
        # ── زر إرسال المنتجات المحددة بالـ Checkbox ──
        _sel = st.session_state.selected_products.get(prefix, [])
        _sel_count = len(_sel)
        if _sel_count > 0:
            if st.button(f"📤 إرسال المحدد ({_sel_count}) لـ Make", key=f"{prefix}_send_selected",
                         type="primary"):
                _sel_products = export_to_make_format(
                    pd.DataFrame(_sel), section_type
                )
                if section_type in ("missing", "new"):
                    _sel_res = send_new_products(_sel_products)
                else:
                    _sel_res = send_price_updates(_sel_products)
                if _sel_res["success"]:
                    st.success(_sel_res["message"])
                    for _si, _sr in enumerate(_sel):
                        _spname = str(_sr.get("المنتج", _sr.get("منتج_المنافس", "")))
                        _spkey  = f"{prefix}_{_spname}_{_si}"
                        _spid   = str(_sr.get("معرف_المنتج", _sr.get("معرف_المنافس", "")))
                        _scomp  = str(_sr.get("المنافس", ""))
                        _sop    = safe_float(_sr.get("السعر", _sr.get("سعر_المنافس", 0)))
                        _snp    = safe_float(_sr.get("سعر_المنافس", _sr.get("السعر", 0)))
                        st.session_state.hidden_products.add(_spkey)
                        save_hidden_product(_spkey, _spname, "sent_to_make_selected")
                        save_processed(_spkey, _spname, _scomp, "send_price",
                                       old_price=_sop, new_price=_snp,
                                       product_id=_spid,
                                       notes=f"إرسال محدد بـ Checkbox ← {prefix}")
                    st.session_state.selected_products[prefix] = []
                    st.rerun()
                else:
                    st.error(_sel_res["message"])
        else:
            st.caption("☑️ حدد منتجات\nبالـ Checkbox أدناه")

    st.caption(f"عرض {len(filtered)} من {len(df)} منتج — {datetime.now().strftime('%H:%M:%S')}")
    st.caption(
        "🖼️ **بطاقة VS:** يسار = **منتجنا** (صورة من ملفك أو `image_url` إن وُجدت)، يمين = **لدى المنافس** "
        "(من كشط المنافسين `comp_image_url` / `رابط_الصورة` في الدمج، أو بحث تلقائي)."
    )

    # ── Pagination ─────────────────────────────
    PAGE_SIZE = 25
    total_pages = max(1, (len(filtered) + PAGE_SIZE - 1) // PAGE_SIZE)
    if total_pages > 1:
        page_num = st.number_input("الصفحة", 1, total_pages, 1, key=f"{prefix}_pg")
    else:
        page_num = 1
    start = (page_num - 1) * PAGE_SIZE
    page_df = filtered.iloc[start:start + PAGE_SIZE]

       # ── الجدول البصري ─────────────────────
    for idx, row in page_df.iterrows():
        our_name   = str(row.get("المنتج", "—"))
        # تخطي المنتجات التي أُرسلت لـ Make أو أُزيلت
        _hide_key = f"{prefix}_{our_name}_{idx}"
        if _hide_key in st.session_state.hidden_products:
            continue
        comp_name  = str(row.get("منتج_المنافس", "—"))
        our_price  = safe_float(row.get("السعر", 0))
        comp_price = safe_float(row.get("سعر_المنافس", 0))
        diff       = safe_float(row.get("الفرق", our_price - comp_price))
        match_pct  = safe_float(row.get("match_score", 0))
        comp_src   = str(row.get("المنافس", ""))
        brand      = str(row.get("الماركة", ""))
        size       = row.get("الحجم", "")
        ptype      = str(row.get("النوع", ""))
        risk       = str(row.get("الخطورة", ""))
        decision   = str(row.get("القرار", ""))
        ts_now     = datetime.now().strftime("%Y-%m-%d %H:%M")

        # ── Checkbox لتحديد المنتج للإرسال الجماعي ──
        _cb_key = f"cb_{prefix}_{idx}"
        _row_dict = row.to_dict()
        _is_selected = any(
            r.get("المنتج") == our_name and str(r.get("المنافس","")) == comp_src
            for r in st.session_state.selected_products.get(prefix, [])
        )
        _chk_col, _card_col = st.columns([0.04, 0.96])
        with _chk_col:
            _checked = st.checkbox("", value=_is_selected, key=_cb_key,
                                   label_visibility="collapsed")
        # مزامنة الاختيار مع session_state
        _sel_list = st.session_state.selected_products.setdefault(prefix, [])
        _already = any(
            r.get("المنتج") == our_name and str(r.get("المنافس","")) == comp_src
            for r in _sel_list
        )
        if _checked and not _already:
            _sel_list.append(_row_dict)
        elif not _checked and _already:
            st.session_state.selected_products[prefix] = [
                r for r in _sel_list
                if not (r.get("المنتج") == our_name and str(r.get("المنافس","")) == comp_src)
            ]

        # سحب رقم المنتج من جميع الأعمدة المحتملة
        _pid_raw = (
            row.get("معرف_المنتج", "") or
            row.get("product_id", "") or
            row.get("رقم المنتج", "") or
            row.get("رقم_المنتج", "") or
            row.get("معرف المنتج", "") or ""
        )
        _pid_str = ""
        if _pid_raw and str(_pid_raw) not in ("", "nan", "None", "0"):
            try: _pid_str = str(int(float(str(_pid_raw))))
            except: _pid_str = str(_pid_raw)

        # ── صور: أولوية لأعمدة الصف (ملف + كشط)، ثم بحث تلقائي ──
        _our_from_row = _image_url_from_row(
            row,
            "image_url",
            "صورة المنتج",
            "رابط_الصورة",
            "صورة",
            "رابط الصورة",
        )
        _comp_from_row = _image_url_from_row(
            row,
            "comp_image_url",
            "صورة_المنافس",
            "رابط_صورة_المنافس",
            "رابط صورة المنافس",
        )

        _img_cache_key = f"img_{our_name}_{brand}"
        if _our_from_row:
            _product_image_url = _our_from_row
        else:
            if _img_cache_key not in st.session_state:
                try:
                    _img_result = fetch_product_images(our_name, brand)
                    st.session_state[_img_cache_key] = _first_display_image_url(_img_result)
                except Exception:
                    st.session_state[_img_cache_key] = ""
            _product_image_url = st.session_state.get(_img_cache_key, "")

        _comp_img_key = f"img_comp_{comp_name}_{brand}_{comp_src}"
        if _comp_from_row:
            _comp_image_url = _comp_from_row
        else:
            if _comp_img_key not in st.session_state:
                try:
                    _cr = fetch_product_images(comp_name, brand)
                    st.session_state[_comp_img_key] = _first_display_image_url(_cr)
                except Exception:
                    st.session_state[_comp_img_key] = ""
            _comp_image_url = st.session_state.get(_comp_img_key, "")

        if comp_price and comp_price > 0:
            _diff_pct_val = (our_price - comp_price) / comp_price * 100.0
        else:
            _diff_pct_val = 0.0
        _diff_pct_str = f"{_diff_pct_val:+.1f}"

        with _card_col:
          # بطاقة VS مع صورتين ونسبة الفارق السعري
          st.markdown(vs_card(
              our_name, our_price, comp_name, comp_price,
              _diff_pct_str, diff,
              our_image_url=_product_image_url,
              comp_image_url=_comp_image_url,
              comp_src=comp_src, pid_str=_pid_str,
          ), unsafe_allow_html=True)

        # شريط المعلومات
        match_color = ("#00C853" if match_pct >= 90
                       else "#FFD600" if match_pct >= 70 else "#FF1744")
        risk_html = ""
        if risk:
            rc = {"حرج": "#FF1744", "عالي": "#FF1744", "متوسط": "#FFD600", "منخفض": "#00C853", "عادي": "#00C853"}.get(risk.replace("🔴 ","").replace("🟡 ","").replace("🟢 ",""), "#888")
            risk_html = f'<span style="color:{rc};font-size:.75rem;font-weight:700">⚡{risk}</span>'

        # تاريخ آخر تغيير سعر
        ph = get_price_history(our_name, comp_src, limit=2)
        price_change_html = ""
        if len(ph) >= 2:
            old_p = ph[1]["price"]
            chg = ph[0]["price"] - old_p
            chg_c = "#FF1744" if chg > 0 else "#00C853"
            price_change_html = f'<span style="color:{chg_c};font-size:.7rem">{"▲" if chg>0 else "▼"}{abs(chg):.0f} منذ {ph[1]["date"]}</span>'

        # قرار معلق؟
        pend = st.session_state.decisions_pending.get(our_name, {})
        pend_html = decision_badge(pend.get("action", "")) if pend else ""

        st.markdown(f"""
        <div style="display:flex;justify-content:space-between;align-items:center;
                    padding:3px 12px;font-size:.8rem;flex-wrap:wrap;gap:4px;">
          <span>🏷️ <b>{brand}</b> {size} {ptype}</span>
          <span>تطابق: <b style="color:{match_color}">{match_pct:.0f}%</b></span>
          {risk_html}
          {price_change_html}
          {pend_html}
          {ts_badge(ts_now)}
        </div>""", unsafe_allow_html=True)

        # شريط المنافسين المصغر — يعرض كل المنافسين بأسعارهم
        all_comps = row.get("جميع_المنافسين", row.get("جميع المنافسين", []))
        if isinstance(all_comps, list) and len(all_comps) > 0:
            st.markdown(comp_strip(all_comps), unsafe_allow_html=True)

        # ── أزرار لكل منتج ─────────────────────
        b1, b2, b3, b4, b5, b6, b7, b8, b9 = st.columns([1, 1, 1, 1, 1, 1, 1, 1, 1])

        with b1:  # AI تحقق ذكي — يُصحح القسم
            _ai_label = {"raise": "🤖 هل نخفض؟", "lower": "🤖 هل نرفع؟",
                         "review": "🤖 هل يطابق؟", "approved": "🤖 تحقق"}.get(prefix, "🤖 تحقق")
            if st.button(_ai_label, key=f"v_{prefix}_{idx}"):
                with st.spinner("🤖 AI يحلل ويتحقق..."):
                    r = verify_match(our_name, comp_name, our_price, comp_price)
                    if r.get("success"):
                        icon = "✅" if r.get("match") else "❌"
                        conf = r.get("confidence", 0)
                        reason = r.get("reason","")[:200]
                        correct_sec = r.get("correct_section","")
                        suggested_price = r.get("suggested_price", 0)

                        # تحديد القسم الحالي من prefix
                        current_sec_map = {
                            "raise": "🔴 سعر أعلى",
                            "lower": "🟢 سعر أقل",
                            "approved": "✅ موافق",
                            "review": "⚠️ تحت المراجعة"
                        }
                        current_sec = current_sec_map.get(prefix, "")

                        # هل AI يوافق على القسم الحالي؟
                        section_ok = True
                        if correct_sec and current_sec:
                            # مقارنة مبسطة
                            if ("اعلى" in correct_sec or "أعلى" in correct_sec) and prefix != "raise":
                                section_ok = False
                            elif ("اقل" in correct_sec or "أقل" in correct_sec) and prefix != "lower":
                                section_ok = False
                            elif "موافق" in correct_sec and prefix != "approved":
                                section_ok = False
                            elif ("مفقود" in correct_sec or "🔵" in correct_sec) and r.get("match") == False:
                                section_ok = False

                        if r.get("match"):
                            # مطابقة صحيحة — عرض نتيجة السعر
                            diff_info = ""
                            if prefix == "raise":
                                diff_info = f"\n\n💡 **توصية:** {'خفض السعر' if diff > 20 else 'إبقاء السعر'}"
                            elif prefix == "lower":
                                diff_info = f"\n\n💡 **توصية:** {'رفع السعر' if abs(diff) > 20 else 'إبقاء السعر'}"
                            if suggested_price > 0:
                                diff_info += f"\n💰 **السعر المقترح: {suggested_price:,.0f} ر.س**"

                            st.success(f"{icon} **تطابق {conf}%** — المطابقة صحيحة\n\n{reason}{diff_info}")

                            if not section_ok:
                                st.warning(f"⚠️ AI يرى أن هذا المنتج يجب أن يكون في قسم: **{correct_sec}**")
                        else:
                            # مطابقة خاطئة — تنبيه
                            st.error(f"{icon} **المطابقة خاطئة** ({conf}%)\n\n{reason}")
                            st.warning("🔵 هذا المنتج يجب أن يكون في **المنتجات المفقودة**")
                    else:
                        st.error("فشل AI")

        with b2:  # بحث سعر السوق ذكي
            _mkt_label = {"raise": "🌐 سعر عادل؟", "lower": "🌐 فرصة رفع؟"}.get(prefix, "🌐 سوق")
            if st.button(_mkt_label, key=f"mkt_{prefix}_{idx}"):
                with st.spinner("🌐 يبحث في السوق السعودي..."):
                    r = search_market_price(our_name, our_price)
                    if r.get("success"):
                        mp  = r.get("market_price", 0)
                        rng = r.get("price_range", {})
                        rec = r.get("recommendation", "")[:250]
                        web_ctx = r.get("web_context","")
                        comps = r.get("competitors", [])
                        conf = r.get("confidence", 0)

                        _verdict = ""
                        if prefix == "raise" and mp > 0:
                            _verdict = "✅ سعرنا ضمن السوق" if our_price <= mp * 1.1 else "⚠️ سعرنا أعلى من السوق — يُنصح بالخفض"
                        elif prefix == "lower" and mp > 0:
                            _gap = mp - our_price
                            _verdict = f"💰 فرصة رفع ~{_gap:.0f} ر.س" if _gap > 10 else "✅ سعرنا قريب من السوق"

                        _comps_txt = ""
                        if comps:
                            _comps_txt = "\n\n**منافسون:**\n" + "\n".join(
                                f"• {c.get('name','')}: {c.get('price',0):,.0f} ر.س" for c in comps[:3]
                            )

                        _price_range = f"{rng.get('min',0):.0f}–{rng.get('max',0):.0f}" if rng else "—"
                        st.info(
                            f"💹 **سعر السوق: {mp:,.0f} ر.س** ({_price_range} ر.س)\n\n"
                            f"{rec}{_comps_txt}\n\n{'**' + _verdict + '**' if _verdict else ''}"
                        )
                        if web_ctx:
                            with st.expander("🔍 مصادر البحث"):
                                st.caption(web_ctx)
                    else:
                        st.warning("تعذر البحث في السوق")

        with b3:  # موافق
            if st.button("✅ موافق", key=f"ok_{prefix}_{idx}"):
                st.session_state.decisions_pending[our_name] = {
                    "action": "approved", "reason": "موافقة يدوية",
                    "our_price": our_price, "comp_price": comp_price,
                    "diff": diff, "competitor": comp_src,
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                log_decision(our_name, prefix, "approved",
                             "موافقة يدوية", our_price, comp_price, diff, comp_src)
                _hk3 = f"{prefix}_{our_name}_{idx}"
                st.session_state.hidden_products.add(_hk3)
                save_hidden_product(_hk3, our_name, "approved")
                save_processed(_hk3, our_name, comp_src, "approved",
                               old_price=our_price, new_price=our_price,
                               product_id=str(row.get("معرف_المنتج","")),
                               notes=f"موافق من {prefix} | منافس: {comp_src}")
                st.rerun()

        with b4:  # تأجيل
            if st.button("⏸️ تأجيل", key=f"df_{prefix}_{idx}"):
                st.session_state.decisions_pending[our_name] = {
                    "action": "deferred", "reason": "تأجيل",
                    "our_price": our_price, "comp_price": comp_price,
                    "diff": diff, "competitor": comp_src,
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                log_decision(our_name, prefix, "deferred",
                             "تأجيل", our_price, comp_price, diff, comp_src)
                st.warning("⏸️")

        with b5:  # إزالة
            if st.button("🗑️ إزالة", key=f"rm_{prefix}_{idx}"):
                st.session_state.decisions_pending[our_name] = {
                    "action": "removed", "reason": "إزالة",
                    "our_price": our_price, "comp_price": comp_price,
                    "diff": diff, "competitor": comp_src,
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                log_decision(our_name, prefix, "removed",
                             "إزالة", our_price, comp_price, diff, comp_src)
                _hk = f"{prefix}_{our_name}_{idx}"
                st.session_state.hidden_products.add(_hk)
                save_hidden_product(_hk, our_name, "removed")
                save_processed(_hk, our_name, comp_src, "removed",
                               old_price=our_price, new_price=our_price,
                               product_id=str(row.get("معرف_المنتج","")),
                               notes=f"إزالة من {prefix}")
                st.rerun()

        with b6:  # سعر يدوي
            _auto_price = round(comp_price - 1, 2) if comp_price > 0 else our_price
            _custom_price = st.number_input(
                "سعر", value=_auto_price, min_value=0.0,
                step=1.0, key=f"cp_{prefix}_{idx}",
                label_visibility="collapsed"
            )

        with b7:  # تصدير Make
            if st.button("📤 Make", key=f"mk_{prefix}_{idx}"):
                # سحب رقم المنتج من جميع الأعمدة المحتملة
                _pid_raw = (
                    row.get("معرف_المنتج", "") or
                    row.get("product_id", "") or
                    row.get("رقم المنتج", "") or
                    row.get("رقم_المنتج", "") or
                    row.get("معرف المنتج", "") or ""
                )
                # تحويل float إلى int (مثل 1081786650.0 → 1081786650)
                try:
                    _fv = float(_pid_raw)
                    _pid = str(int(_fv)) if _fv == int(_fv) else str(_pid_raw)
                except (ValueError, TypeError):
                    _pid = str(_pid_raw).strip()
                if _pid in ("nan", "None", "NaN", ""): _pid = ""
                _final_price = _custom_price if _custom_price > 0 else _auto_price
                res = send_single_product({
                    "product_id": _pid,
                    "name": our_name, "price": _final_price,
                    "comp_name": comp_name, "comp_price": comp_price,
                    "diff": diff, "decision": decision, "competitor": comp_src
                })
                if res["success"]:
                    _hk = f"{prefix}_{our_name}_{idx}"
                    st.session_state.hidden_products.add(_hk)
                    save_hidden_product(_hk, our_name, "sent_to_make")
                    save_processed(_hk, our_name, comp_src, "send_price",
                                   old_price=our_price, new_price=_final_price,
                                   product_id=_pid,
                                   notes=f"Make ← {prefix} | منافس: {comp_src} | {comp_price:.0f}→{_final_price:.0f}ر.س")
                    st.rerun()

        with b8:  # تحقق AI — يُصحح القسم
            if st.button("🔍 تحقق", key=f"vrf_{prefix}_{idx}"):
                with st.spinner("🤖 يتحقق..."):
                    _vr2 = verify_match(our_name, comp_name, our_price, comp_price)
                    if _vr2.get("success"):
                        _mc2 = "✅ متطابق" if _vr2.get("match") else "❌ غير متطابق"
                        _conf2 = _vr2.get("confidence",0)
                        _sec2 = _vr2.get("correct_section","")
                        _reason2 = _vr2.get("reason","")[:150]
                        st.markdown(f"{_mc2} {_conf2}%\n\n{_reason2}")
                        if _sec2 and not _vr2.get("match"):
                            st.warning(f"يجب نقله → **{_sec2}**")

        with b9:  # تاريخ السعر
            if st.button("📈 تاريخ", key=f"ph_{prefix}_{idx}"):
                history = get_price_history(our_name, comp_src)
                if history:
                    rows_h = [f"📅 {h['date']}: {h['price']:,.0f} ر.س" for h in history[:5]]
                    st.info("\n".join(rows_h))
                else:
                    st.info("لا يوجد تاريخ بعد")

        st.markdown('<hr style="border:none;border-top:1px solid #1a1a2e;margin:6px 0">', unsafe_allow_html=True)


# ════════════════════════════════════════════════
#  الشريط الجانبي
# ════════════════════════════════════════════════
with st.sidebar:
    st.markdown(f"## {APP_ICON} {APP_TITLE}")
    st.caption(f"الإصدار {APP_VERSION}")

    # حالة AI — تشخيص مفصل
    ai_ok = bool(GEMINI_API_KEYS)
    if ai_ok:
        ai_color = "#00C853"
        ai_label = f"🤖 Gemini ✅ ({len(GEMINI_API_KEYS)} مفتاح)"
    else:
        ai_color = "#FF1744"
        ai_label = "🔴 AI غير متصل — أضف مفتاح Gemini"

    st.markdown(
        f'<div style="background:{ai_color}22;border:1px solid {ai_color};'
        f'border-radius:6px;padding:6px;text-align:center;color:{ai_color};'
        f'font-weight:700;font-size:.85rem">{ai_label}</div>',
        unsafe_allow_html=True
    )

    # زر تشخيص سريع
    if not ai_ok:
        st.caption(
            "ضع **`GEMINI_API_KEY`** في Railway Variables أو في `.streamlit/secrets.toml` محلياً. "
            "يُقبل أيضاً **`GOOGLE_API_KEY`** كاسم بديل."
        )
        if st.button("🔍 تشخيص المشكلة", key="diag_btn"):
            import os
            st.write("**المتغيرات البيئية (أسماء فقط):**")
            for nm in ("GEMINI_API_KEY", "GEMINI_API_KEYS", "GOOGLE_API_KEY", "GEMINI_KEY_1"):
                st.write(f"  `{nm}`: {'موجود' if os.environ.get(nm) else '—'}")
            st.write("**الـ secrets المتاحة:**")
            try:
                available = list(st.secrets.keys())
                for k in available:
                    val = str(st.secrets[k])
                    masked = val[:8] + "..." if len(val) > 8 else val
                    st.write(f"  `{k}` = `{masked}`")
            except Exception as e:
                st.error(f"خطأ: {e}")
            # محاولة مباشرة
            for key_name in ["GEMINI_API_KEYS","GEMINI_API_KEY","GEMINI_KEY_1","GOOGLE_API_KEY"]:
                try:
                    v = st.secrets[key_name]
                    st.success(f"✅ وجدت {key_name} = {str(v)[:20]}...")
                except:
                    st.warning(f"❌ {key_name} غير موجود")

    # حالة المعالجة — تحديث حي مع auto-rerun
    if st.session_state.job_id:
        job = get_job_progress(st.session_state.job_id)
        if job:
            if job["status"] == "running":
                pct = job["processed"] / max(job["total"], 1)
                st.progress(min(pct, 0.99),
                            f"⚙️ {job['processed']}/{job['total']} منتج")
                # تحديث تلقائي كل 4 ثوانٍ بدون إعادة تشغيل الكود كاملاً
                try:
                    from streamlit_autorefresh import st_autorefresh
                    st_autorefresh(interval=4000, key="progress_refresh")
                except ImportError:
                    # fallback: rerun عادي إذا لم تكن المكتبة موجودة
                    time.sleep(4)
                    st.rerun()
            elif job["status"] == "done":
                # اكتمل — حمّل النتائج (يشمل [] إذا كان التحليل فارغاً) عند أول اكتمال أو إن لم تُحمَّل بعد
                _should_load = (
                    job.get("results") is not None
                    and (st.session_state.job_running or st.session_state.results is None)
                )
                if _should_load:
                    _restored = _restore_results_from_json(job["results"])
                    df_all = pd.DataFrame(_restored)
                    missing_df = pd.DataFrame(job.get("missing", [])) if job.get("missing") else pd.DataFrame()
                    _r = _split_results(df_all)
                    _r["missing"] = missing_df
                    st.session_state.results     = _r
                    st.session_state.analysis_df = df_all
                st.session_state.job_running = False
                if _should_load:
                    st.balloons()
                    st.rerun()
            elif job["status"].startswith("error"):
                st.error(f"❌ فشل: {job['status'][7:80]}")
                st.session_state.job_running = False

    # إزالة أي تكرار محتمل في الأقسام (حماية واجهة)
    _sections_unique = []
    for _s in SECTIONS:
        if _s not in _sections_unique:
            _sections_unique.append(_s)
    page = st.radio("الأقسام", _sections_unique, label_visibility="collapsed")
    # توافق رجعي: دمج "لوحة التسعير" داخل "لوحة التحكم"
    if page == "📊 لوحة التسعير":
        page = "📊 لوحة التحكم"

    st.markdown(
        '<p style="font-size:0.85rem;margin:8px 0;line-height:1.4;">'
        "🏢 <b>كشط المنافسين:</b> افتح القسم <b>🏢 كشط المنافسين</b> لإضافة روابط Sitemap "
        "وتشغيل الجلب مع عرض وحفظ تدريجي."
        "</p>",
        unsafe_allow_html=True,
    )

    st.markdown("---")
    if st.session_state.results:
        r = st.session_state.results
        st.markdown("**📊 ملخص:**")
        st.caption(
            "🔍 **مفقود** = منتج يظهر عند المنافس وقد لا يكون في كتالوجنا — **ليس خطأ في البرنامج**. "
            "الألوان أدناه = **مدى ثقة المحرك** في اقتراح المطابقة."
        )
        for key, icon, label in [
            ("price_raise","🔴","أعلى"), ("price_lower","🟢","أقل"),
            ("approved","✅","موافق"), ("missing","🔍","مفقود"),
            ("review","⚠️","مراجعة")
        ]:
            cnt = len(r.get(key, pd.DataFrame()))
            st.caption(f"{icon} {label}: **{cnt}**")
        if len(r.get("missing", pd.DataFrame())) > 0:
            if st.button("🗑️ مسح المفقودات من الملخص (للتجربة)", key="btn_clear_missing_sidebar", use_container_width=True):
                st.session_state.results["missing"] = pd.DataFrame()
                clear_missing_from_last_job()
                st.rerun()
        # ملخص الثقة للمفقودات (ليست «أخطاء» — جودة اقتراح المطابقة)
        _miss_df = r.get("missing", pd.DataFrame())
        if not _miss_df.empty and "مستوى_الثقة" in _miss_df.columns:
            _gc = len(_miss_df[_miss_df["مستوى_الثقة"] == "green"])
            _yc = len(_miss_df[_miss_df["مستوى_الثقة"] == "yellow"])
            _rc = len(_miss_df[_miss_df["مستوى_الثقة"] == "red"])
            st.caption("ثقة اقتراح المفقود:")
            st.markdown(
                f'<div style="background:#1a1a2e;border-radius:6px;padding:6px;margin-top:4px;font-size:.75rem">'
                f'🟢 ثقة قوية: <b>{_gc}</b> &nbsp; '
                f'🟡 ثقة متوسطة: <b>{_yc}</b> &nbsp; '
                f'🔴 مشكوك: <b>{_rc}</b></div>',
                unsafe_allow_html=True)
        _priced = sum(len(r.get(k, pd.DataFrame())) for k in ("price_raise", "price_lower", "approved", "review"))
        if _priced == 0 and not _miss_df.empty:
            st.info(
                "**لماذا المفقود فقط؟** قسم «مفقود» = منتجات **عند المنافس** قد لا تكون في كتالوجنا. "
                "أقسام سعر أعلى/أقل/موافق = مقارنة **منتجاتنا** مع المنافس؛ إذا كانت كلها **0** فغالباً "
                "لا تطابق كافٍ (أسماء مختلفة، ترميز CSV، أو أعمدة غير معروفة). "
                "جرّب: حفظ CSV بـ **UTF-8**، وتأكد أن **حد الصفوف = 0** لمعالجة الملف كاملاً، ورفع ملفات متطابقة الأعمدة."
            )

    # قرارات معلقة
    pending_cnt = len(st.session_state.decisions_pending)
    if pending_cnt:
        st.markdown(f'<div style="background:#FF174422;border:1px solid #FF1744;'
                    f'border-radius:6px;padding:6px;text-align:center;color:#FF1744;'
                    f'font-size:.8rem">📦 {pending_cnt} قرار معلق</div>',
                    unsafe_allow_html=True)


# ── توقيع الملف المرفوع + تعيين الأعمدة (قبل سلسلة if page) ──
_NO_ID_LABEL = "— بدون معرّف —"
_PEEK_ROWS = 400


def _upload_file_sig(uf):
    if uf is None:
        return ""
    try:
        return f"{uf.name}_{len(uf.getvalue())}"
    except Exception:
        return str(getattr(uf, "name", "x"))


def _col_select_index(cols, default):
    if not cols:
        return 0
    if default and default in cols:
        return cols.index(default)
    return 0


def display_pricing_insights(final_df: pd.DataFrame) -> None:
    """
    مقاييس لوحة الرؤى:
    - إجمالي زيادة الربح المحتملة: مجموع (المقترح − الحالي) عندما المقترح أعلى.
    - تنبيه المنافسة: عدد المنتجات الأغلى من سعر المنافس (مع سعر منافس صالح).
    - منتجات رابحة: أسعارنا أقل من المنافس (أفضل سعر في السوق ضمن البيانات).
    """
    if final_df is None or final_df.empty:
        st.warning("لا توجد بيانات لحساب المؤشرات.")
        return
    df = final_df.copy()
    for c in ("price", "comp_price", "suggested_price"):
        if c not in df.columns:
            df[c] = 0.0
        else:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    valid_comp = df["comp_price"] > 0
    over_priced = int((valid_comp & (df["price"] > df["comp_price"])).sum())
    under_priced = int((valid_comp & (df["price"] < df["comp_price"])).sum())
    profit_opt = float(
        (df["suggested_price"] - df["price"]).where(df["suggested_price"] > df["price"], 0.0).sum()
    )
    col1, col2, col3 = st.columns(3)
    col1.metric("منتجات أغلى من المنافس", over_priced, delta_color="inverse")
    col2.metric("منتجات أرخص من المنافس", under_priced)
    col3.metric("فرصة ربح إضافية (ريال)", f"{profit_opt:,.2f}")


def render_smart_decision_table(df: pd.DataFrame) -> None:
    """جدول قرار مع تلوين: أحمر إذا سعرنا أعلى من المنافس بأكثر من 10٪، أخضر إذا كنا الأرخص."""
    if df is None or df.empty:
        return
    d = df.copy()
    for c in ("price", "comp_price", "suggested_price"):
        if c not in d.columns:
            d[c] = 0.0
        else:
            d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0.0)
    d["Potential Profit Change"] = d["suggested_price"] - d["price"]
    base_cols = ["name", "sku", "price", "comp_price", "suggested_price", "Potential Profit Change"]
    show_cols = [c for c in base_cols if c in d.columns]
    for c in ("match_score", "action_required", "comp_name"):
        if c in d.columns and c not in show_cols:
            show_cols.append(c)
    show_df = d[show_cols].copy()

    def _row_style(row: pd.Series):
        p = float(row.get("price", 0) or 0)
        c = float(row.get("comp_price", 0) or 0)
        css = ""
        if c > 0 and p > c * 1.10:
            css = "background-color: #fecaca; color: #7f1d1d"
        elif c > 0 and p < c:
            css = "background-color: #bbf7d0; color: #14532d"
        return [css] * len(row)

    st.markdown("### 📋 جدول القرار الذكي")
    st.dataframe(
        show_df.style.apply(_row_style, axis=1),
        use_container_width=True,
        hide_index=True,
    )


# ════════════════════════════════════════════════
#  1. لوحة التحكم
# ════════════════════════════════════════════════
if page == "📊 لوحة التحكم":
    st.header("📊 لوحة التحكم")
    st.caption("تم دمج عرض لوحة التسعير داخل لوحة التحكم لتفادي التكرار في القائمة الجانبية.")
    db_log("dashboard", "view")
    # تحديث تلقائي مباشر من ملفات الخلفية كل 12 ثانية
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=12000, key="dashboard_auto_refresh_background")
    except Exception:
        pass
    _auto_refresh_results_from_background_files()

    # ── بانر صحة النظام ────────────────────────────────────────────────────
    from engines.ai_engine import get_last_errors
    _errors = get_last_errors()
    if _errors:
        with st.expander(f"⚠️ {len(_errors)} خطأ أخير في AI — اضغط للتفاصيل", expanded=False):
            for e in _errors:
                st.code(e, language=None)
    else:
        st.success("✅ محركات AI تعمل بدون أخطاء", icon="🤖")

    # تغييرات الأسعار
    changes = get_price_changes(7)
    if changes:
        st.markdown("#### 🔔 تغييرات أسعار آخر 7 أيام")
        c_df = pd.DataFrame(changes)
        st.dataframe(c_df[["product_name","competitor","old_price","new_price",
                            "price_diff","new_date"]].rename(columns={
            "product_name": "المنتج", "competitor": "المنافس",
            "old_price": "السعر السابق", "new_price": "السعر الجديد",
            "price_diff": "التغيير", "new_date": "التاريخ"
        }).head(200), use_container_width=True, height=200)
        st.markdown("---")

    if st.session_state.results:
        r = st.session_state.results
        cols = st.columns(5)
        data = [
            ("🔴","سعر أعلى",  len(r.get("price_raise", pd.DataFrame())), COLORS["raise"]),
            ("🟢","سعر أقل",   len(r.get("price_lower", pd.DataFrame())), COLORS["lower"]),
            ("✅","موافق",     len(r.get("approved", pd.DataFrame())),     COLORS["approved"]),
            ("🔍","مفقود",     len(r.get("missing", pd.DataFrame())),      COLORS["missing"]),
            ("⚠️","مراجعة",   len(r.get("review", pd.DataFrame())),       COLORS["review"]),
        ]
        for col, (icon, label, val, color) in zip(cols, data):
            col.markdown(stat_card(icon, label, val, color), unsafe_allow_html=True)
        st.caption(
            "**توضيح:** «مفقود» يعني منتجاً عند المنافس قد لا يكون لديكم في الملف — **وليس خطأ تشغيلاً**. "
            "شريط الألوان التالي يوضّح **قوة ثقة المحرك** في اقتراح المطابقة لتلك المفقودات."
        )

        # ملخص الثقة للمفقودات في لوحة التحكم
        _miss_dash = r.get("missing", pd.DataFrame())
        if not _miss_dash.empty and "مستوى_الثقة" in _miss_dash.columns:
            _g = len(_miss_dash[_miss_dash["مستوى_الثقة"] == "green"])
            _y = len(_miss_dash[_miss_dash["مستوى_الثقة"] == "yellow"])
            _rd = len(_miss_dash[_miss_dash["مستوى_الثقة"] == "red"])
            st.markdown(
                f'<div style="display:flex;gap:12px;justify-content:center;padding:8px;'
                f'background:#1a1a2e;border-radius:8px;margin:8px 0">'
                f'<span style="color:#00C853">🟢 ثقة قوية: <b>{_g}</b></span>'
                f'<span style="color:#FFD600">🟡 ثقة متوسطة: <b>{_y}</b></span>'
                f'<span style="color:#FF1744">🔴 مشكوك: <b>{_rd}</b></span>'
                f'</div>', unsafe_allow_html=True)

        st.markdown("---")
        cc1, cc2 = st.columns(2)
        with cc1:
            sheets = {}
            for key, name in [("price_raise","سعر_أعلى"),("price_lower","سعر_أقل"),
                               ("approved","موافق"),("missing","مفقود"),("review","مراجعة")]:
                if key in r and not r[key].empty:
                    df_ex = r[key].copy()
                    if "جميع المنافسين" in df_ex.columns:
                        df_ex = df_ex.drop(columns=["جميع المنافسين"])
                    sheets[name] = df_ex
            if sheets:
                excel_all = export_multiple_sheets(sheets)
                st.download_button("📥 تصدير كل الأقسام Excel",
                    data=excel_all, file_name="mahwous_all.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        with cc2:
            if st.button("📤 إرسال كل شيء لـ Make (دفعات ذكية)"):
                _prog_all = st.progress(0, text="جاري الإرسال...")
                _status_all = st.empty()
                _sent_total = 0
                _fail_total = 0
                _sections = [
                    ("price_raise", "raise", "update", "🔴 سعر أعلى"),
                    ("price_lower", "lower", "update", "🟢 سعر أقل"),
                    ("approved",    "approved", "update", "✅ موافق"),
                    ("missing",     "missing", "new", "🔍 مفقودة"),
                ]
                for _si, (_key, _sec, _btype, _label) in enumerate(_sections):
                    if _key in r and not r[_key].empty:
                        _p = export_to_make_format(r[_key], _sec)
                        _res = send_batch_smart(_p, batch_type=_btype, batch_size=20, max_retries=3)
                        _sent_total += _res.get("sent", 0)
                        _fail_total += _res.get("failed", 0)
                        _status_all.caption(f"{_label}: ✅ {_res.get('sent',0)} | ❌ {_res.get('failed',0)}")
                    _prog_all.progress((_si + 1) / len(_sections), text=f"جاري: {_label}")
                _prog_all.progress(1.0, text="اكتمل")
                st.success(f"✅ تم إرسال {_sent_total} منتج لـ Make!" + (f" (فشل {_fail_total})" if _fail_total else ""))
    else:
        # استئناف آخر job؟
        last = get_last_job()
        if last and last["status"] == "done" and last.get("results") is not None:
            st.info(f"💾 يوجد تحليل محفوظ من {last.get('updated_at','')}")
            if st.button("🔄 استعادة النتائج المحفوظة"):
                _restored_last = _restore_results_from_json(last["results"])
                df_all = pd.DataFrame(_restored_last)
                missing_df = pd.DataFrame(last.get("missing", [])) if last.get("missing") else pd.DataFrame()
                _r = _split_results(df_all)
                _r["missing"] = missing_df
                st.session_state.results     = _r
                st.session_state.analysis_df = df_all
                st.rerun()
        else:
            st.info("👈 ارفع ملفاتك من قسم 'رفع الملفات'")


# ════════════════════════════════════════════════
#  1b. كشط المنافسين (Sitemap + CSV حي)
# ════════════════════════════════════════════════
elif page == "🏢 كشط المنافسين":
    db_log("competitor_scrape", "view")
    _autoload_internal_store_catalog(show_message=True)
    our_df = getattr(st.session_state, "our_df", None)
    has_store = isinstance(our_df, pd.DataFrame) and not our_df.empty
    if has_store:
        with st.expander("🛍️ جدول متجر مهووس (الأساس للمقارنة) - اضغط للتحقق", expanded=False):
            st.info(
                f"✅ تم تحميل {len(our_df)} منتج من متجر مهووس بنجاح. "
                "ستتم مقارنة المسحوبات بهذا الجدول."
            )
            _preferred_cols = [
                "أسم المنتج",
                "اسم المنتج",
                "سعر المنتج",
                "السعر",
                "رمز المنتج sku",
                "رقم المنتج",
                "sku",
                "الماركة",
                "brand",
            ]
            _show_cols = [c for c in _preferred_cols if c in our_df.columns]
            if _show_cols:
                # إزالة التكرار مع الحفاظ على ترتيب العرض
                # Make columns unique before displaying to avoid ValueError
                display_df = make_columns_unique(our_df[_show_cols].copy())
                st.dataframe(display_df, use_container_width=True)
            else:
                # Make columns unique before displaying to avoid ValueError
                display_df = make_columns_unique(our_df.copy())
                st.dataframe(display_df, use_container_width=True)
    render_competitor_scrape_page()


# ════════════════════════════════════════════════
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

            # min_comp_price per product key (strict funnel baseline against cheapest competitor)
            work["min_comp_price"] = work.groupby("_group_key", dropna=False)["comp_price"].transform("min")
            work["min_comp_price"] = pd.to_numeric(work["min_comp_price"], errors="coerce").fillna(0.0)

            # state tracking: move to processed after sync click in card
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

            # Missing products: in competitors feed but not in Mahwous catalog/SKU
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

                # Approve & Sync Prices to Salla (Make.com) with lock/spinner/state updates
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
    except Exception as e:
        logger.exception("Critical error in pricing dashboard block: %s", e)
        st.error(f"حدث خطأ غير متوقع أثناء تشغيل لوحة التسعير: {type(e).__name__}: {str(e)[:200]}")


# ════════════════════════════════════════════════
#  2. رفع الملفات
# ════════════════════════════════════════════════
elif page == "📂 رفع الملفات":
    st.header("📂 رفع الملفات")
    db_log("upload", "view")
    _autoload_internal_store_catalog(show_message=True)

    with st.expander("⚙️ تحديث قاعدة بيانات متجر مهووس (للنظام الداخلي)", expanded=False):
        st.caption("ارفع ملف CSV/Excel واحد ليتم حفظه داخلياً واستخدامه تلقائياً في كل تشغيل.")
        internal_store_file = st.file_uploader(
            "📦 تحديث ملف متجر مهووس الداخلي",
            type=["csv", "xlsx", "xls"],
            key="internal_store_file",
        )
        if internal_store_file is not None:
            try:
                os.makedirs(os.path.dirname(INTERNAL_STORE_PATH), exist_ok=True)
                _internal_df, _internal_err = read_file(internal_store_file)
                if _internal_err:
                    st.error(f"❌ فشل قراءة الملف الداخلي: {_internal_err}")
                else:
                    # نوحّد الحفظ بصيغة CSV حتى يبقى التحميل التلقائي ثابتاً مهما كان الملف المرفوع.
                    _internal_df.to_csv(INTERNAL_STORE_PATH, index=False, encoding="utf-8-sig")
                    st.session_state.store_df = _internal_df
                    st.session_state.our_df = _internal_df
                    st.session_state.store_autoloaded = True
                    st.success("تم تحديث قاعدة بيانات متجر مهووس الداخلية بنجاح! سيتم استخدام هذا الملف تلقائياً من الآن فصاعداً.")
            except Exception as _save_e:
                st.error(f"❌ تعذر تحديث الملف الداخلي: {_save_e}")

    our_file   = st.file_uploader("📦 ملف منتجاتنا (CSV/Excel)",
                                   type=["csv","xlsx","xls"], key="our_file")
    comp_files = st.file_uploader("🏪 ملفات المنافسين (متعدد)",
                                   type=["csv","xlsx","xls"],
                                   accept_multiple_files=True, key="comp_files")

    col_opt1, col_opt2 = st.columns(2)
    with col_opt1:
        bg_mode  = st.checkbox("⚡ معالجة خلفية (يمكنك التنقل أثناء التحليل)", value=True)
    with col_opt2:
        max_rows = st.number_input(
            "حد الصفوف لملف منتجاتنا فقط (0=كل)",
            min_value=0,
            max_value=500_000,
            value=0,
            step=500,
            help="ضع 0 لمعالجة آلاف الصفوف دفعة واحدة. يقتصر الحد على ملف «منتجاتنا» فقط.",
        )

    # ── تعيين أعمدة (قائمة منسدلة مصغّرة داخل expander) ──
    if our_file and comp_files:
        our_peek, our_peek_err = read_file(our_file, preview_rows=_PEEK_ROWS)
        if our_peek_err:
            st.warning(f"⚠️ معاينة أعمدة منتجاتنا: {our_peek_err}")
        elif our_peek is not None and len(our_peek.columns) > 0:
            _osig = _upload_file_sig(our_file)
            _oc = [str(c) for c in our_peek.columns]
            _dn, _dp, _di = guess_default_columns(our_peek)
            _id_opts = [_NO_ID_LABEL] + _oc
            _idi = 0
            if _di and _di in _oc:
                _idi = _oc.index(_di) + 1
            with st.expander("📋 تعيين الأعمدة — تحقق يدوي (منسدل)", expanded=False):
                st.caption(
                    "يُعاد تسمية المختار داخلياً إلى: **اسم المنتج**، **السعر**، و**رقم المنتج** (إن وُجد)."
                )
                st.caption(
                    "**المعرّف (اختياري):** عمود يثبت هوية الصنف — مثل رقم المنتج، SKU، الباركود، أو كود ERP. "
                    "يُحسّن مطابقة الكتالوج عند إعادة رفع الملف. **إن لم يوجد** في الجدول، اختر «بدون معرّف»."
                )
                u1, u2, u3 = st.columns(3)
                with u1:
                    _sn = st.selectbox(
                        "منتجاتنا · الاسم",
                        _oc,
                        index=_col_select_index(_oc, _dn),
                        key=f"map_our_n_{_osig}",
                        help="عمود اسم المنتج الظاهر للعميل.",
                    )
                with u2:
                    _sp = st.selectbox(
                        "منتجاتنا · السعر",
                        _oc,
                        index=_col_select_index(_oc, _dp),
                        help="عمود السعر الرقمي (ر.س أو ما يعادله).",
                    )
                with u3:
                    _si = st.selectbox(
                        "منتجاتنا · المعرّف",
                        _id_opts,
                        index=_idi,
                        key=f"map_our_i_{_osig}",
                        help="SKU / رقم منتج / باركود — أو «بدون معرّف».",
                    )
                st.session_state["_colmap_our"] = (
                    _sn,
                    _sp,
                    None if _si == _NO_ID_LABEL else _si,
                )
                st.session_state["_colmap_comp"] = {}
                for _ci, _cf in enumerate(comp_files):
                    _cdf, _ce = read_file(_cf, preview_rows=_PEEK_ROWS)
                    if _ce or _cdf is None or len(_cdf.columns) == 0:
                        st.caption(f"🏪 `{_cf.name}` — تعذر قراءة المعاينة: {_ce or 'لا أعمدة'}")
                        continue
                    _cc = [str(c) for c in _cdf.columns]
                    _cn, _cp, _cid = guess_default_columns(_cdf)
                    _csig = _upload_file_sig(_cf)
                    _cid_opts = [_NO_ID_LABEL] + _cc
                    _cidi = 0
                    if _cid and _cid in _cc:
                        _cidi = _cc.index(_cid) + 1
                    st.markdown(f"**🏪 {_cf.name}**")
                    k1, k2, k3 = st.columns(3)
                    with k1:
                        _cnm = st.selectbox(
                            "الاسم",
                            _cc,
                            index=_col_select_index(_cc, _cn),
                            key=f"map_c{_ci}_n_{_csig}",
                            help="اسم المنتج عند المنافس.",
                        )
                    with k2:
                        _cpr = st.selectbox(
                            "السعر",
                            _cc,
                            index=_col_select_index(_cc, _cp),
                            key=f"map_c{_ci}_p_{_csig}",
                            help="سعر المنافس (رقم).",
                        )
                    with k3:
                        _cidv = st.selectbox(
                            "المعرّف",
                            _cid_opts,
                            index=_cidi,
                            key=f"map_c{_ci}_i_{_csig}",
                            help="اختياري — كود/SKU إن وُجد؛ وإلا «بدون معرّف».",
                        )
                    st.session_state["_colmap_comp"][_csig] = (
                        _cnm,
                        _cpr,
                        None if _cidv == _NO_ID_LABEL else _cidv,
                    )

    _store_df = st.session_state.get("store_df")
    has_internal_store = isinstance(_store_df, pd.DataFrame) and not _store_df.empty

    if st.button("🚀 بدء التحليل", type="primary"):
        if (our_file or has_internal_store) and comp_files:
            if our_file:
                our_df, err = read_file(our_file)
                if err:
                    st.error(f"❌ {err}")
                    our_df = None
            else:
                our_df, err = _store_df.copy(), None

            if our_df is not None:
                _omap = st.session_state.get("_colmap_our")
                _cmap = st.session_state.get("_colmap_comp", {})
                if _omap and our_file:
                    _on, _op, _oi = _omap
                    our_df = apply_column_mapping(our_df, _on, _op, _oi)
                else:
                    _gdn, _gdp, _gdi = guess_default_columns(our_df)
                    our_df = apply_column_mapping(our_df, _gdn, _gdp, _gdi)
                if max_rows > 0:
                    our_df = our_df.head(int(max_rows))

                comp_dfs = {}
                for cf in comp_files:
                    cdf, cerr = read_file(cf)
                    if cerr:
                        st.warning(f"⚠️ {cf.name}: {cerr}")
                    else:
                        _cs = _upload_file_sig(cf)
                        _trip = _cmap.get(_cs)
                        if _trip:
                            _pn, _pp, _pid = _trip
                            cdf = apply_column_mapping(cdf, _pn, _pp, _pid)
                        else:
                            _gn, _gp, _gi = guess_default_columns(cdf)
                            cdf = apply_column_mapping(cdf, _gn, _gp, _gi)
                        comp_dfs[cf.name] = cdf

                if comp_dfs:
                    _comp_rows = sum(len(x) for x in comp_dfs.values())
                    st.caption(
                        f"📄 **منتجاتنا:** {len(our_df)} صف — **المنافسون:** {_comp_rows} صفاً عبر {len(comp_dfs)} ملفاً"
                    )
                    # ── v26: upsert كتالوج يومي بدون تكرار ──────────
                    with st.spinner("📦 تحديث الكتالوج اليومي..."):
                        r_our  = upsert_our_catalog(our_df,
                            name_col="اسم المنتج", id_col="رقم المنتج", price_col="السعر")
                        r_comp = upsert_comp_catalog(comp_dfs)
                        st.caption(f"✅ كتالوجنا: {r_our['inserted']} جديد / {r_our['updated']} تحديث | "
                                   f"المنافسين: {r_comp['new_products']} جديد")
                    # ─────────────────────────────────────────────────
                    st.session_state.our_df = our_df
                    st.session_state.comp_dfs = comp_dfs
                    job_id = str(uuid.uuid4())[:8]
                    st.session_state.job_id = job_id
                    comp_names = ",".join(comp_dfs.keys())

                    if bg_mode:
                        # ── خلفية ──
                        t = threading.Thread(
                            target=_run_analysis_background,
                            args=(job_id, our_df, comp_dfs,
                                  our_file.name, comp_names),
                            daemon=True
                        )
                        # ربط الثريد بسياق Streamlit — يمنع توقف المعالجة
                        add_script_run_ctx(t)
                        t.start()
                        st.session_state.job_running = True
                        st.success(f"✅ بدأ التحليل في الخلفية (Job: {job_id})")
                        # انتقل فوراً للوحة التحكم لمتابعة التقدم بشريط حي
                        st.rerun()
                    else:
                        # ── مباشر ──
                        prog = st.progress(0, "جاري التحليل...")
                        def upd(p, _r=None): prog.progress(min(float(p), 0.99), f"{float(p)*100:.0f}%")
                        df_all = run_full_analysis(our_df, comp_dfs, progress_callback=upd)
                        missing_df = find_missing_products(our_df, comp_dfs)

                        for _, row in df_all.iterrows():
                            if row.get("match_score", 0) > 0:
                                upsert_price_history(
                                    str(row.get("المنتج","")), str(row.get("المنافس","")),
                                    safe_float(row.get("سعر_المنافس",0)),
                                    safe_float(row.get("السعر",0)),
                                    safe_float(row.get("الفرق",0)),
                                    safe_float(row.get("match_score",0)),
                                    str(row.get("القرار",""))
                                )

                        _r = _split_results(df_all)
                        _r["missing"] = missing_df
                        st.session_state.results     = _r
                        st.session_state.analysis_df = df_all
                        _our_source_name = our_file.name if our_file else os.path.basename(INTERNAL_STORE_PATH)
                        log_analysis(_our_source_name, comp_names, len(our_df),
                                     int((df_all.get("match_score", pd.Series(dtype=float)) > 0).sum()),
                                     len(missing_df))
                        prog.progress(1.0, "✅ اكتمل!")
                        st.balloons()
                        st.rerun()
        else:
            st.warning("⚠️ ارفع ملف منافس واحد على الأقل، وتأكد من توفر ملف متجر مهووس (رفع يدوي أو داخلي تلقائي).")


# ════════════════════════════════════════════════
#  3. سعر أعلى
# ════════════════════════════════════════════════
elif page == "🔴 سعر أعلى":
    st.header("🔴 منتجات سعرنا أعلى — فرصة خفض")
    st.caption(
        f"🏪 **Baseline المعتمد:** `{MAIN_STORE_NAME}` ({MAIN_STORE_DOMAIN}) — "
        "كل المقارنات هنا هي **Competitor vs Mahwous** فقط."
    )
    db_log("price_raise", "view")
    if st.session_state.results and "price_raise" in st.session_state.results:
        df = st.session_state.results["price_raise"]
        if not df.empty:
            st.error(f"⚠️ {len(df)} منتج سعرنا أعلى من المنافسين")
            # AI تدريب لهذا القسم
            with st.expander("🤖 نصيحة AI لهذا القسم", expanded=False):
                if st.button("📡 احصل على تحليل شامل للقسم", key="ai_section_raise"):
                    with st.spinner("🤖 AI يحلل البيانات الفعلية..."):
                        _top = df.nlargest(min(15, len(df)), "الفرق") if "الفرق" in df.columns else df.head(15)
                        _lines = "\n".join(
                            f"- {r.get('المنتج','')}: سعرنا {safe_float(r.get('السعر',0)):.0f} | المنافس ({r.get('المنافس','')}) {safe_float(r.get('سعر_المنافس',0)):.0f} | فرق +{safe_float(r.get('الفرق',0)):.0f}"
                            for _, r in _top.iterrows())
                        _avg_diff = safe_float(df["الفرق"].mean()) if "الفرق" in df.columns else 0
                        _prompt = (f"المرجع الأساسي للمقارنة: {MAIN_STORE_NAME} ({MAIN_STORE_DOMAIN}).\n"
                                   f"عندي {len(df)} منتج سعر متجرنا ({MAIN_STORE_NAME}) أعلى من المنافسين.\n"
                                   f"متوسط الفرق: {_avg_diff:.0f} ر.س\n"
                                   f"أعلى 15 فرق:\n{_lines}\n\n"
                                   f"أعطني:\n1. أي المنتجات يجب خفض سعرها فوراً (فرق>30)؟\n"
                                   f"2. أي المنتجات يمكن إبقاؤها (فرق<10)؟\n"
                                   f"3. استراتيجية تسعير مخصصة لكل ماركة")
                        r = call_ai(_prompt, "price_raise")
                        st.markdown(f'<div class="ai-box">{r["response"]}</div>', unsafe_allow_html=True)
            from utils.ui_components import render_products_in_tabs

            st.markdown("### 🗂️ عرض تبويبي (بطاقات)")
            render_products_in_tabs(_build_cards_df(df, "Decrease Price 📉"), key_prefix="raise_tabs")
            with st.expander("🧰 العرض المتقدم (الأدوات الكاملة)", expanded=False):
                render_pro_table(df, "raise", "raise")
        else:
            st.success("✅ ممتاز! لا توجد منتجات بسعر أعلى")
    else:
        st.info("ارفع الملفات أولاً")


# ════════════════════════════════════════════════
#  4. سعر أقل
# ════════════════════════════════════════════════
elif page == "🟢 سعر أقل":
    st.header("🟢 منتجات سعرنا أقل — فرصة رفع")
    st.caption(
        f"المقارنة هنا ثابتة: **{MAIN_STORE_NAME} ({MAIN_STORE_DOMAIN})** مقابل المنافسين."
    )
    db_log("price_lower", "view")
    if st.session_state.results and "price_lower" in st.session_state.results:
        df = st.session_state.results["price_lower"]
        if not df.empty:
            st.info(f"💰 {len(df)} منتج يمكن رفع سعره لزيادة الهامش")
            with st.expander("🤖 نصيحة AI لهذا القسم", expanded=False):
                if st.button("📡 استراتيجية رفع الأسعار", key="ai_section_lower"):
                    with st.spinner("🤖 AI يحلل فرص الربح..."):
                        _top = df.nsmallest(min(15, len(df)), "الفرق") if "الفرق" in df.columns else df.head(15)
                        _lines = "\n".join(
                            f"- {r.get('المنتج','')}: سعرنا {safe_float(r.get('السعر',0)):.0f} | المنافس ({r.get('المنافس','')}) {safe_float(r.get('سعر_المنافس',0)):.0f} | فرق {safe_float(r.get('الفرق',0)):.0f}"
                            for _, r in _top.iterrows())
                        _total_lost = safe_float(df["الفرق"].sum()) if "الفرق" in df.columns else 0
                        _prompt = (f"المرجع الأساسي للمقارنة: {MAIN_STORE_NAME} ({MAIN_STORE_DOMAIN}).\n"
                                   f"عندي {len(df)} منتج سعر متجرنا ({MAIN_STORE_NAME}) أقل من المنافسين.\n"
                                   f"إجمالي الأرباح الضائعة: {abs(_total_lost):.0f} ر.س\n"
                                   f"أكبر 15 فرصة ربح:\n{_lines}\n\n"
                                   f"أعطني:\n1. أي المنتجات يمكن رفع سعرها فوراً (فرق>50)؟\n"
                                   f"2. أي المنتجات نرفعها تدريجياً (فرق 10-50)؟\n"
                                   f"3. كم الربح المتوقع إذا رفعنا الأسعار؟")
                        r = call_ai(_prompt, "price_lower")
                        st.markdown(f'<div class="ai-box">{r["response"]}</div>', unsafe_allow_html=True)
            from utils.ui_components import render_products_in_tabs

            st.markdown("### 🗂️ عرض تبويبي (بطاقات)")
            render_products_in_tabs(_build_cards_df(df, "Increase Price 📈"), key_prefix="lower_tabs")
            with st.expander("🧰 العرض المتقدم (الأدوات الكاملة)", expanded=False):
                render_pro_table(df, "lower", "lower")
        else:
            st.info("لا توجد منتجات")
    else:
        st.info("ارفع الملفات أولاً")


# ════════════════════════════════════════════════
#  5. موافق عليها
# ════════════════════════════════════════════════
elif page == "✅ موافق عليها":
    st.header("✅ منتجات موافق عليها")
    st.caption(
        f"الأسعار متوازنة بالنسبة لمرجعنا الأساسي **{MAIN_STORE_NAME} ({MAIN_STORE_DOMAIN})**."
    )
    db_log("approved", "view")
    if st.session_state.results and "approved" in st.session_state.results:
        df = st.session_state.results["approved"]
        if not df.empty:
            st.success(f"✅ {len(df)} منتج بأسعار تنافسية مناسبة")
            from utils.ui_components import render_products_in_tabs

            st.markdown("### 🗂️ عرض تبويبي (بطاقات)")
            render_products_in_tabs(_build_cards_df(df, "Perfect Price ✅"), key_prefix="approved_tabs")
            with st.expander("🧰 العرض المتقدم (الأدوات الكاملة)", expanded=False):
                render_pro_table(df, "approved", "approved")
        else:
            st.info("لا توجد منتجات موافق عليها")
    else:
        st.info("ارفع الملفات أولاً")


# ════════════════════════════════════════════════
#  6. منتجات مفقودة — v26 مع كشف التستر/الأساسي
# ════════════════════════════════════════════════
elif page == "🔍 منتجات مفقودة":
    st.header("🔍 منتجات المنافسين غير الموجودة عندنا")
    st.caption(
        f"منتجات تظهر عند المنافسين ولا تطابق كتالوج **{MAIN_STORE_NAME} ({MAIN_STORE_DOMAIN})** بعد."
    )
    db_log("missing", "view")

    if st.session_state.results and "missing" in st.session_state.results:
        df = st.session_state.results["missing"]
        if df is not None and not df.empty:
            from utils.ui_components import render_products_in_tabs

            st.markdown("### 🗂️ عرض تبويبي (بطاقات المفقودات)")
            render_products_in_tabs(_build_cards_df(df, "Review / Other ⚠️"), key_prefix="missing_tabs")

            # ── إحصاءات سريعة ──────────────────────────────────────────────
            total_miss   = len(df)
            has_tester   = df["نوع_متاح"].str.contains("تستر", na=False).sum()    if "نوع_متاح" in df.columns else 0
            has_base     = df["نوع_متاح"].str.contains("العطر الأساسي", na=False).sum() if "نوع_متاح" in df.columns else 0
            pure_missing = total_miss - has_tester - has_base

            c1,c2,c3,c4 = st.columns(4)
            c1.metric("🔍 مفقود فعلاً",    pure_missing)
            c2.metric("🏷️ يوجد تستر",      has_tester)
            c3.metric("✅ يوجد الأساسي",   has_base)
            c4.metric("📦 إجمالي المنافسين", total_miss)

            # ── تحليل AI الأولويات ────────────────────────────────────────
            with st.expander("🤖 تحليل AI — أولويات الإضافة", expanded=False):
                if st.button("📡 تحليل الأولويات", key="ai_missing_section"):
                    with st.spinner("🤖 AI يحلل أولويات الإضافة..."):
                        _pure = df[df["نوع_متاح"].str.strip() == ""] if "نوع_متاح" in df.columns else df
                        _brands = _pure["الماركة"].value_counts().head(10).to_dict() if "الماركة" in _pure.columns else {}
                        _summary = " | ".join(f"{b}:{c}" for b,c in _brands.items()) if _brands else "غير محدد"
                        _lines   = "\n".join(
                            f"- {r.get('منتج_المنافس','')}: {safe_float(r.get('سعر_المنافس',0)):.0f}ر.س ({r.get('الماركة','')}) — {r.get('المنافس','')}"
                            for _, r in _pure.head(20).iterrows())
                        _prompt = (
                            f"المرجع الأساسي: {MAIN_STORE_NAME} ({MAIN_STORE_DOMAIN}).\n"
                            f"لديّ {len(_pure)} منتج مفقود فعلاً مقارنة بكتالوج {MAIN_STORE_NAME} (بدون التستر/الأساسي المتاح).\n"
                            f"توزيع الماركات: {_summary}\nعينة:\n{_lines}\n\n"
                            "أعطني:\n1. ترتيب أولويات الإضافة (عالية/متوسطة/منخفضة) مع السبب\n"
                            "2. أي الماركات الأكثر ربحية؟\n"
                            "3. سعر مقترح (أقل من المنافس بـ5-10 ر.س)\n"
                            "4. منتجات لا تستحق الإضافة — ولماذا؟"
                        )
                        r_ai = call_ai(_prompt, "missing")
                        resp = r_ai["response"] if r_ai["success"] else "❌ فشل AI"
                        # تنظيف JSON من المخرجات
                        import re as _re
                        resp = _re.sub(r'```json.*?```', '', resp, flags=_re.DOTALL)
                        resp = _re.sub(r'```.*?```', '', resp, flags=_re.DOTALL)
                        st.markdown(f'<div class="ai-box">{resp}</div>', unsafe_allow_html=True)

            # ── فلاتر ─────────────────────────────────────────────────────
            opts = get_filter_options(df)
            with st.expander("🔍 فلاتر", expanded=False):
                c1,c2,c3,c4,c5 = st.columns(5)
                search   = c1.text_input("🔎 بحث", key="miss_s")
                brand_f  = c2.selectbox("الماركة", opts["brands"], key="miss_b")
                comp_f   = c3.selectbox("المنافس", opts["competitors"], key="miss_c")
                variant_f= c4.selectbox("النوع",
                    ["الكل","مفقود فعلاً","يوجد تستر","يوجد الأساسي"], key="miss_v")
                conf_f   = c5.selectbox("الثقة",
                    ["الكل","🟢 ثقة قوية","🟡 ثقة متوسطة","🔴 مشكوك"], key="miss_conf_f")

            filtered = df.copy()
            if search:
                filtered = filtered[filtered.apply(lambda r: search.lower() in str(r.values).lower(), axis=1)]
            if brand_f != "الكل" and "الماركة" in filtered.columns:
                filtered = filtered[filtered["الماركة"].str.contains(brand_f, case=False, na=False, regex=False)]
            if comp_f != "الكل" and "المنافس" in filtered.columns:
                filtered = filtered[filtered["المنافس"].str.contains(comp_f, case=False, na=False, regex=False)]
            if variant_f == "مفقود فعلاً" and "نوع_متاح" in filtered.columns:
                filtered = filtered[filtered["نوع_متاح"].str.strip() == ""]
            elif variant_f == "يوجد تستر" and "نوع_متاح" in filtered.columns:
                filtered = filtered[filtered["نوع_متاح"].str.contains("تستر", na=False)]
            elif variant_f == "يوجد الأساسي" and "نوع_متاح" in filtered.columns:
                filtered = filtered[filtered["نوع_متاح"].str.contains("الأساسي", na=False)]
            # فلتر الثقة
            if conf_f != "الكل" and "مستوى_الثقة" in filtered.columns:
                _conf_map = {"🟢 ثقة قوية": "green", "🟡 ثقة متوسطة": "yellow", "🔴 مشكوك": "red"}
                _cv = _conf_map.get(conf_f, "")
                if _cv:
                    filtered = filtered[filtered["مستوى_الثقة"] == _cv]

            # ── ترتيب حسب الثقة (الأكثر ثقة أولاً) ─────────────────────
            if "مستوى_الثقة" in filtered.columns:
                _conf_order = {"green": 0, "yellow": 1, "red": 2}
                filtered = filtered.assign(
                    _conf_sort=filtered["مستوى_الثقة"].map(_conf_order).fillna(3)
                ).sort_values("_conf_sort").drop(columns=["_conf_sort"])

            # ── تصدير ─────────────────────────────────────────────────────
            cc1,cc2,cc3 = st.columns(3)
            with cc1:
                excel_m = export_to_excel(filtered, "مفقودة")
                st.download_button("📥 Excel", data=excel_m, file_name="missing.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="miss_dl")
            with cc2:
                _csv_m = filtered.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                st.download_button("📄 CSV", data=_csv_m, file_name="missing.csv", mime="text/csv", key="miss_csv")
            with cc3:
                # ── خيارات الإرسال الذكي ─────────────────────────────
                _conf_opts = {"🟢 ثقة قوية فقط": "green", "🟡 ثقة متوسطة فقط": "yellow", "🔵 الكل": ""}
                _conf_sel = st.selectbox("مستوى الثقة", list(_conf_opts.keys()), key="miss_conf_sel")
                _conf_val = _conf_opts[_conf_sel]
                if st.button("📤 إرسال بدفعات ذكية لـ Make", key="miss_make_all"):
                    # فلتر المفقودة الفعلية فقط (بدون التستر/الأساسي المتاح)
                    _to_send = filtered[filtered["نوع_متاح"].str.strip() == ""] if "نوع_متاح" in filtered.columns else filtered
                    products = export_to_make_format(_to_send, "missing")
                    # إضافة مستوى الثقة لكل منتج
                    for _ip, _pr_row in enumerate(products):
                        if _ip < len(_to_send):
                            _pr_row["مستوى_الثقة"] = str(_to_send.iloc[_ip].get("مستوى_الثقة", "green"))
                    _prog_bar = st.progress(0, text="جاري الإرسال...")
                    _status_txt = st.empty()
                    def _miss_progress(sent, failed, total, cur_name):
                        pct = (sent + failed) / max(total, 1)
                        _prog_bar.progress(min(pct, 1.0), text=f"إرسال: {sent}/{total} | {cur_name}")
                        _status_txt.caption(f"✅ {sent} | ❌ {failed} | الإجمالي {total}")
                    res = send_batch_smart(products, batch_type="new",
                                           batch_size=20, max_retries=3,
                                           progress_cb=_miss_progress,
                                           confidence_filter=_conf_val)
                    _prog_bar.progress(1.0, text="اكتمل")
                    if res["success"]:
                        st.success(res["message"])
                        # v26: احفظ في قائمة المعالجة
                        for _, _pr in _to_send.iterrows():
                            _pk = f"miss_{str(_pr.get('منتج_المنافس',''))[:30]}_{str(_pr.get('المنافس',''))}"
                            save_processed(_pk, str(_pr.get('منتج_المنافس','')),
                                         str(_pr.get('المنافس','')), "send_missing",
                                         new_price=safe_float(_pr.get('سعر_المنافس',0)))
                    else:
                        st.error(res["message"])
                    if res.get("errors"):
                        with st.expander(f"❌ منتجات فشلت ({len(res['errors'])})"):
                            for _en in res["errors"]:
                                st.caption(f"• {_en}")

            st.caption(f"{len(filtered)} منتج — {datetime.now().strftime('%Y-%m-%d %H:%M')}")

            # ── عرض المنتجات ──────────────────────────────────────────────
            PAGE_SIZE = 20
            total_p = len(filtered)
            tp = max(1, (total_p + PAGE_SIZE - 1) // PAGE_SIZE)
            pn = st.number_input("الصفحة", 1, tp, 1, key="miss_pg") if tp > 1 else 1
            page_df = filtered.iloc[(pn-1)*PAGE_SIZE : pn*PAGE_SIZE]

            for idx, row in page_df.iterrows():
                name  = str(row.get("منتج_المنافس", ""))
                _miss_key = f"missing_{name}_{idx}"
                if _miss_key in st.session_state.hidden_products:
                    continue

                price           = safe_float(row.get("سعر_المنافس", 0))
                brand           = str(row.get("الماركة", ""))
                comp            = str(row.get("المنافس", ""))
                size            = str(row.get("الحجم", ""))
                ptype           = str(row.get("النوع", ""))
                note            = str(row.get("ملاحظة", ""))
                # استخراج معرف المنتج (SKU/الكود)
                _miss_pid_raw = (
                    row.get("معرف_المنافس", "") or
                    row.get("product_id", "") or
                    row.get("رقم المنتج", "") or
                    row.get("رقم_المنتج", "") or
                    row.get("SKU", "") or
                    row.get("sku", "") or
                    row.get("الكود", "") or
                    row.get("كود", "") or
                    row.get("الباركود", "") or ""
                )
                _miss_pid = ""
                if _miss_pid_raw and str(_miss_pid_raw) not in ("", "nan", "None", "0", "NaN"):
                    try: _miss_pid = str(int(float(str(_miss_pid_raw))))
                    except: _miss_pid = str(_miss_pid_raw).strip()
                variant_label   = str(row.get("نوع_متاح", ""))
                variant_product = str(row.get("منتج_متاح", ""))
                variant_score   = safe_float(row.get("نسبة_التشابه", 0))
                is_tester_flag  = bool(row.get("هو_تستر", False))
                conf_level      = str(row.get("مستوى_الثقة", "green"))
                conf_score      = safe_float(row.get("درجة_التشابه", 0))
                suggested_price = round(price - 1, 2) if price > 0 else 0

                _is_similar = "⚠️" in note
                _has_variant= bool(variant_label and variant_label.strip())
                _is_tester_type = "تستر" in variant_label if _has_variant else False

                # ── لون البطاقة حسب الحالة ────────────────────────────
                if _has_variant and _is_tester_type:
                    _border = "#ff980055"; _badge_bg = "#ff9800"
                elif _has_variant:
                    _border = "#4caf5055"; _badge_bg = "#4caf50"
                elif _is_similar:
                    _border = "#ff572255"; _badge_bg = "#ff5722"
                else:
                    _border = "#007bff44"; _badge_bg = "#007bff"

                # ── بادج النوع المتاح ──────────────────────────────────
                _variant_html = ""
                if _has_variant:
                    _variant_html = f"""
                    <div style="margin-top:6px;padding:5px 10px;border-radius:6px;
                                background:{_badge_bg}22;border:1px solid {_badge_bg}88;
                                font-size:.78rem;color:{_badge_bg};font-weight:700">
                        {variant_label}
                        <span style="font-weight:400;color:#aaa;margin-right:6px">
                            ({variant_score:.0f}%) → {variant_product[:50]}
                        </span>
                    </div>"""

                # ── بادج تستر ─────────────────────────────────────────
                _tester_badge = ""
                if is_tester_flag:
                    _tester_badge = '<span style="font-size:.68rem;padding:2px 7px;border-radius:10px;background:#9c27b022;color:#ce93d8;margin-right:6px">🏷️ تستر</span>'

                st.markdown(miss_card(
                    name=name, price=price, brand=brand, size=size,
                    ptype=ptype, comp=comp, suggested_price=suggested_price,
                    note=note if _is_similar else "",
                    variant_html=_variant_html, tester_badge=_tester_badge,
                    border_color=_border,
                    confidence_level=conf_level, confidence_score=conf_score,
                    product_id=_miss_pid
                ), unsafe_allow_html=True)

                # ── الأزرار — صف 1 ────────────────────────────────────
                b1,b2,b3,b4 = st.columns(4)

                with b1:
                    if st.button("🖼️ صور المنتج", key=f"imgs_{idx}"):
                        with st.spinner("🔍 يبحث عن صور..."):
                            img_result = fetch_product_images(name, brand)
                            images = img_result.get("images", [])
                            frag_url = img_result.get("fragrantica_url","")
                            if images:
                                img_cols = st.columns(min(len(images),3))
                                for ci, img_data in enumerate(images[:3]):
                                    url = img_data.get("url",""); src = img_data.get("source","")
                                    is_search = img_data.get("is_search", False)
                                    with img_cols[ci]:
                                        if not is_search and url.startswith("http") and any(
                                            ext in url.lower() for ext in [".jpg",".png",".webp",".jpeg"]):
                                            try:    st.image(url, caption=f"📸 {src}", use_container_width=True)
                                            except: st.markdown(f"[🔗 {src}]({url})")
                                        else:
                                            st.markdown(f"[🔍 ابحث في {src}]({url})")
                                if frag_url:
                                    st.markdown(f"[🔗 Fragrantica Arabia]({frag_url})")
                            else:
                                st.warning("لم يتم العثور على صور")

                with b2:
                    if st.button("🌸 مكونات", key=f"notes_{idx}"):
                        with st.spinner("يجلب من Fragrantica Arabia..."):
                            fi = fetch_fragrantica_info(name)
                            if fi.get("success"):
                                top  = ", ".join(fi.get("top_notes",[])[:5])
                                mid  = ", ".join(fi.get("middle_notes",[])[:5])
                                base = ", ".join(fi.get("base_notes",[])[:5])
                                st.markdown(f"""
**🌸 هرم العطر:**
- **القمة:** {top or "—"}
- **القلب:** {mid or "—"}
- **القاعدة:** {base or "—"}
- **الماركة:** {fi.get('brand','—')} | **السنة:** {fi.get('year','—')} | **العائلة:** {fi.get('fragrance_family','—')}""")
                                if fi.get("fragrantica_url"):
                                    st.markdown(f"[🔗 Fragrantica Arabia]({fi['fragrantica_url']})")
                                st.session_state[f"frag_info_{idx}"] = fi
                            else:
                                st.warning("لم يتم العثور على بيانات")

                with b3:
                    if st.button("🔎 تحقق مهووس", key=f"mhw_{idx}"):
                        with st.spinner("يبحث في mahwous.com..."):
                            r_m = search_mahwous(name)
                            if r_m.get("success"):
                                avail = "✅ متوفر" if r_m.get("likely_available") else "❌ غير متوفر"
                                resp_text = str(r_m.get("reason",""))[:200]
                                # تنظيف JSON
                                import re as _re
                                resp_text = _re.sub(r'\{.*?\}', '', resp_text, flags=_re.DOTALL)
                                st.info(f"{avail} | أولوية: **{r_m.get('add_recommendation','—')}**\n{resp_text}")
                            else:
                                st.warning("تعذر البحث")

                with b4:
                    if st.button("💹 سعر السوق", key=f"mkt_m_{idx}"):
                        with st.spinner("🌐 يبحث في السوق..."):
                            r_s = search_market_price(name, price)
                            if r_s.get("success"):
                                mp  = r_s.get("market_price", 0)
                                rng = r_s.get("price_range", {})
                                rec = str(r_s.get("recommendation",""))[:200]
                                # تنظيف JSON من الرد
                                import re as _re
                                rec = _re.sub(r'```.*?```','', rec, flags=_re.DOTALL).strip()
                                mn  = rng.get("min",0); mx = rng.get("max",0)
                                _gap = mp - price if mp > price else 0
                                st.markdown(f"""
<div style="background:#0e1a2e;border:1px solid #4fc3f744;border-radius:8px;padding:10px;">
  <div style="font-weight:700;color:#4fc3f7">💹 سعر السوق: {mp:,.0f} ر.س</div>
  <div style="color:#888;font-size:.8rem">النطاق: {mn:,.0f} – {mx:,.0f} ر.س</div>
  {"<div style='color:#4caf50;font-size:.82rem'>💰 هامش: ~" + f"{_gap:,.0f} ر.س</div>" if _gap > 10 else ""}
  <div style="color:#aaa;font-size:.82rem;margin-top:6px">{rec}</div>
</div>""", unsafe_allow_html=True)

                # ── الأزرار — صف 2 ────────────────────────────────────
                st.markdown('<div style="margin-top:6px"></div>', unsafe_allow_html=True)
                b5,b6,b7,b8 = st.columns(4)

                with b5:
                    if st.button("✍️ خبير الوصف", key=f"expert_{idx}", type="primary"):
                        with st.spinner("🤖 خبير مهووس يكتب الوصف الكامل..."):
                            fi_cached = st.session_state.get(f"frag_info_{idx}")
                            if not fi_cached:
                                fi_cached = fetch_fragrantica_info(name)
                                st.session_state[f"frag_info_{idx}"] = fi_cached
                            desc = generate_mahwous_description(name, suggested_price, fi_cached)
                            # تنظيف أي JSON عارض
                            import re as _re
                            desc = _re.sub(r'```json.*?```','', desc, flags=_re.DOTALL)
                            st.session_state[f"desc_{idx}"] = desc
                            st.success("✅ الوصف جاهز!")

                    if f"desc_{idx}" in st.session_state:
                        with st.expander("📄 الوصف الكامل — خبير مهووس", expanded=True):
                            edited_desc = st.text_area(
                                "راجع وعدّل الوصف قبل الإرسال:",
                                value=st.session_state[f"desc_{idx}"],
                                height=400,
                                key=f"desc_edit_{idx}"
                            )
                            st.session_state[f"desc_{idx}"] = edited_desc
                            _wc = len(edited_desc.split())
                            _col = "#4caf50" if _wc >= 1000 else "#ff9800"
                            st.markdown(f'<span style="color:{_col};font-size:.8rem">📊 {_wc} كلمة</span>', unsafe_allow_html=True)

                with b6:
                    _has_desc = f"desc_{idx}" in st.session_state
                    _make_lbl = "📤 إرسال Make + وصف" if _has_desc else "📤 إرسال Make"
                    if st.button(_make_lbl, key=f"mk_m_{idx}", type="primary" if _has_desc else "secondary"):
                        _desc_send  = st.session_state.get(f"desc_{idx}","")
                        _fi_send    = st.session_state.get(f"frag_info_{idx}",{})
                        _img_url    = _fi_send.get("image_url","") if _fi_send else ""
                        _size_val   = extract_size(name)
                        _size_str   = f"{int(_size_val)}ml" if _size_val else size
                        # إرسال مباشر سواء كان هناك وصف أم لا
                        with st.spinner("📤 يُرسل لـ Make..."):
                            res = send_new_products([{
                                "أسم المنتج":  name,
                                "سعر المنتج":  suggested_price,
                                "brand":       brand,
                                "الوصف":       _desc_send,
                                "image_url":   _img_url,
                                "الحجم":       _size_str,
                                "النوع":       ptype,
                                "المنافس":     comp,
                                "سعر_المنافس": price,
                            }])
                        if res["success"]:
                            _wc = len(_desc_send.split()) if _desc_send else 0
                            _wc_msg = f" — وصف {_wc} كلمة" if _wc > 0 else ""
                            st.success(f"✅ {res['message']}{_wc_msg}")
                            _mk = f"missing_{name}_{idx}"
                            st.session_state.hidden_products.add(_mk)
                            save_hidden_product(_mk, name, "sent_to_make")
                            save_processed(_mk, name, comp, "send_missing",
                                           new_price=suggested_price,
                                           notes=f"إضافة جديدة" + (f" + وصف {_wc} كلمة" if _wc > 0 else ""))
                            for k in [f"desc_{idx}",f"frag_info_{idx}"]:
                                if k in st.session_state: del st.session_state[k]
                            st.rerun()
                        else:
                            st.error(res["message"])

                with b7:
                    if st.button("🔍 فحص تكرار", key=f"dup_{idx}"):
                        with st.spinner("فحص قاعدة البيانات + AI..."):
                            # ── المستوى 1: فحص صارم من قاعدة البيانات ──
                            db_check = check_strict_duplicate(
                                product_name=name,
                                sku=str(row.get("رقم_المنتج", "") or row.get("SKU", "")),
                                brand=str(row.get("العلامة_التجارية", "") or ""),
                                catalog="our"
                            )
                            if db_check["is_duplicate"]:
                                st.error(
                                    f"🚫 **مكرر في قاعدة البيانات** "
                                    f"(طريقة: {db_check['method']})\n"
                                    f"موجود مسبقاً: **{db_check['existing_name']}**"
                                )
                            else:
                                st.success("✅ غير مكرر في قاعدة البيانات")
                                # ── المستوى 2: تحقق AI من المنتجات الأخرى ──
                                our_prods = []
                                if st.session_state.analysis_df is not None:
                                    our_prods = st.session_state.analysis_df.get(
                                        "المنتج", pd.Series()
                                    ).tolist()[:50]
                                if our_prods:
                                    r_dup = check_duplicate(name, our_prods)
                                    import re as _re
                                    _dup_resp = str(r_dup.get("response",""))[:300]
                                    _dup_resp = _re.sub(r'```.*?```','', _dup_resp, flags=_re.DOTALL).strip()
                                    _dup_resp = _re.sub(r'\{[^}]{0,200}\}','[بيانات]', _dup_resp)
                                    st.info(f"🤖 تحليل AI: {_dup_resp}" if r_dup.get("success") else "")

                with b8:
                    if st.button("🗑️ تجاهل", key=f"ign_{idx}"):
                        log_decision(name,"missing","ignored","تجاهل",0,price,-price,comp)
                        _ign = f"missing_{name}_{idx}"
                        st.session_state.hidden_products.add(_ign)
                        save_hidden_product(_ign, name, "ignored")
                        save_processed(_ign, name, comp, "ignored",
                                       new_price=price,
                                       notes="تجاهل من قسم المفقودة")
                        st.rerun()

                st.markdown('<hr style="border:none;border-top:1px solid #0d1a2e;margin:8px 0">', unsafe_allow_html=True)
        else:
            st.success("✅ لا توجد منتجات مفقودة!")
    else:
        st.info("ارفع الملفات أولاً")
# ════════════════════════════════════════════════
#  7. تحت المراجعة — v26 مقارنة جنباً إلى جنب
# ════════════════════════════════════════════════
elif page == "⚠️ تحت المراجعة":
    st.header("⚠️ منتجات تحت المراجعة — مطابقة غير مؤكدة")
    st.caption(
        f"راجع المطابقة قبل القرار — خط الأساس الإجباري هو **{MAIN_STORE_NAME} ({MAIN_STORE_DOMAIN})**."
    )
    db_log("review", "view")

    if st.session_state.results and "review" in st.session_state.results:
        df = st.session_state.results["review"]
        if df is not None and not df.empty:
            from utils.ui_components import render_products_in_tabs

            st.markdown("### 🗂️ عرض تبويبي (بطاقات المراجعة)")
            render_products_in_tabs(_build_cards_df(df, "Review / Other ⚠️"), key_prefix="review_tabs")

            st.warning(f"⚠️ {len(df)} منتج بمطابقة غير مؤكدة — يحتاج مراجعة بشرية أو AI")

            # ── تصنيف تلقائي بـ AI ────────────────────────────────────────
            col_r1, col_r2 = st.columns([2, 1])
            with col_r1:
                if st.button("🤖 إعادة تصنيف بالذكاء الاصطناعي", type="primary", key="reclassify_review"):
                    with st.spinner("🤖 AI يعيد تصنيف المنتجات..."):
                        _items_rc = []
                        for _, rr in df.head(30).iterrows():
                            _items_rc.append({
                                "our":       str(rr.get("المنتج","")),
                                "comp":      str(rr.get("منتج_المنافس","")),
                                "our_price": safe_float(rr.get("السعر",0)),
                                "comp_price":safe_float(rr.get("سعر_المنافس",0)),
                            })
                        _rc_results = reclassify_review_items(_items_rc)
                        if _rc_results:
                            _moved = 0
                            for rc in _rc_results:
                                _sec = rc.get("section","")
                                if _sec and "مراجعة" not in _sec and rc.get("confidence",0) >= 95:
                                    _moved += 1
                            st.success(f"✅ AI نقل {_moved} منتج إلى قسمه الصحيح")
                        else:
                            st.warning("لم يتمكن AI من إعادة التصنيف")
            with col_r2:
                excel_rv = export_to_excel(df, "مراجعة")
                st.download_button("📥 Excel", data=excel_rv, file_name="review.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="rv_dl")

            # ── فلتر بحث ──────────────────────────────────────────────────
            search_rv = st.text_input("🔎 بحث في المنتجات", key="rv_search")
            df_rv = df.copy()
            if search_rv:
                df_rv = df_rv[df_rv.apply(lambda r: search_rv.lower() in str(r.values).lower(), axis=1)]

            st.caption(f"{len(df_rv)} منتج للمراجعة")

            # ── عرض المقارنة جنباً إلى جنب ────────────────────────────────
            PAGE_SIZE = 15
            tp = max(1, (len(df_rv) + PAGE_SIZE - 1) // PAGE_SIZE)
            pn = st.number_input("الصفحة", 1, tp, 1, key="rv_pg") if tp > 1 else 1
            page_rv = df_rv.iloc[(pn-1)*PAGE_SIZE : pn*PAGE_SIZE]

            for idx, row in page_rv.iterrows():
                our_name   = str(row.get("المنتج",""))
                comp_name  = str(row.get("منتج_المنافس","—"))
                our_price  = safe_float(row.get("السعر",0))
                comp_price = safe_float(row.get("سعر_المنافس",0))
                score      = safe_float(row.get("match_score",0))
                brand      = str(row.get("الماركة",""))
                size       = str(row.get("الحجم",""))
                comp_name_s= str(row.get("المنافس",""))
                diff       = our_price - comp_price

                _rv_key = f"review_{our_name}_{idx}"
                if _rv_key in st.session_state.hidden_products:
                    continue

                # لون الثقة
                _score_color = "#4caf50" if score >= 85 else "#ff9800" if score >= 70 else "#f44336"
                _diff_color  = "#f44336" if diff > 10 else "#4caf50" if diff < -10 else "#888"
                _diff_label  = f"+{diff:.0f}" if diff > 0 else f"{diff:.0f}"

                # ── بطاقة المقارنة ─────────────────────────────────────
                st.markdown(f"""
                <div style="border:1px solid #ff980055;border-radius:10px;padding:12px;
                            margin:6px 0;background:linear-gradient(135deg,#0a1628,#0e1a30);">
                  <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
                    <span style="font-size:.75rem;color:#888">🏷️ {brand} | 📏 {size}</span>
                    <span style="font-size:.75rem;padding:2px 8px;border-radius:10px;
                                 background:{_score_color}22;color:{_score_color};font-weight:700">
                      نسبة المطابقة: {score:.0f}%
                    </span>
                  </div>
                  <div style="display:grid;grid-template-columns:1fr 60px 1fr;gap:8px;align-items:center">
                    <!-- منتجنا -->
                    <div style="background:#0d2040;border-radius:8px;padding:10px;border:1px solid #4fc3f733">
                      <div style="font-size:.65rem;color:#4fc3f7;margin-bottom:4px">📦 منتجنا</div>
                      <div style="font-weight:700;color:#fff;font-size:.88rem">{our_name[:60]}</div>
                      <div style="font-size:1.1rem;font-weight:900;color:#4caf50;margin-top:6px">{our_price:,.0f} ر.س</div>
                    </div>
                    <!-- الفرق -->
                    <div style="text-align:center">
                      <div style="font-size:1.2rem;color:{_diff_color};font-weight:900">{_diff_label}</div>
                      <div style="font-size:.6rem;color:#555">ر.س</div>
                    </div>
                    <!-- منتج المنافس -->
                    <div style="background:#1a0d20;border-radius:8px;padding:10px;border:1px solid #ff572233">
                      <div style="font-size:.65rem;color:#ff5722;margin-bottom:4px">🏪 {comp_name_s}</div>
                      <div style="font-weight:700;color:#fff;font-size:.88rem">{comp_name[:60]}</div>
                      <div style="font-size:1.1rem;font-weight:900;color:#ff9800;margin-top:6px">{comp_price:,.0f} ر.س</div>
                    </div>
                  </div>
                </div>""", unsafe_allow_html=True)

                # ── أزرار المراجعة ─────────────────────────────────────
                ba,bb,bc,bd,be = st.columns(5)

                with ba:
                    if st.button("🤖 تحقق AI", key=f"rv_verify_{idx}"):
                        with st.spinner("..."):
                            r_v = verify_match(our_name, comp_name, our_price, comp_price)
                            if r_v.get("success"):
                                conf = r_v.get("confidence",0)
                                match = r_v.get("match", False)
                                reason = str(r_v.get("reason",""))[:200]
                                # تنظيف JSON
                                import re as _re
                                reason = _re.sub(r'```.*?```','', reason, flags=_re.DOTALL)
                                reason = _re.sub(r'\{[^}]{0,200}\}','', reason).strip()
                                _lbl = "✅ نفس المنتج" if match else "❌ مختلف"
                                st.info(f"**{_lbl}** ({conf}%)\n{reason[:150]}")
                            else:
                                st.warning("فشل التحقق")

                with bb:
                    if st.button("✅ موافق", key=f"rv_approve_{idx}"):
                        log_decision(our_name,"review","approved","موافق",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "approved_from_review")
                        save_processed(_rv_key, our_name, comp_name_s, "approved",
                                       old_price=our_price, new_price=our_price,
                                       notes="موافق من تحت المراجعة")
                        st.rerun()

                with bc:
                    if st.button("🔴 سعر أعلى", key=f"rv_raise_{idx}"):
                        log_decision(our_name,"review","price_raise","سعر أعلى",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "moved_price_raise")
                        save_processed(_rv_key, our_name, comp_name_s, "send_price",
                                       old_price=our_price, new_price=comp_price - 1 if comp_price > 0 else our_price,
                                       notes="نُقل من المراجعة → سعر أعلى")
                        st.rerun()

                with bd:
                    if st.button("🔵 مفقود", key=f"rv_missing_{idx}"):
                        log_decision(our_name,"review","missing","مفقود",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "moved_missing")
                        save_processed(_rv_key, our_name, comp_name_s, "send_missing",
                                       new_price=comp_price,
                                       notes="نُقل من المراجعة → مفقود")
                        st.rerun()

                with be:
                    if st.button("🗑️ تجاهل", key=f"rv_ign_{idx}"):
                        log_decision(our_name,"review","ignored","تجاهل",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "ignored_review")
                        save_processed(_rv_key, our_name, comp_name_s, "ignored",
                                       old_price=our_price,
                                       notes="تجاهل من تحت المراجعة")
                        st.rerun()

                st.markdown('<hr style="border:none;border-top:1px solid #0d1a2e;margin:6px 0">',
                            unsafe_allow_html=True)
        else:
            st.success("✅ لا توجد منتجات تحت المراجعة!")
    else:
        st.info("ارفع الملفات أولاً")
# ════════════════════════════════════════════════
#  8. الذكاء الاصطناعي — Gemini مباشر
# ════════════════════════════════════════════════

# ════════════════════════════════════════════════
#  7b. تمت المعالجة — v26
# ════════════════════════════════════════════════
elif page == "✔️ تمت المعالجة":
    st.header("✔️ المنتجات المعالجة")
    st.caption(
        f"سجل الإجراءات في مسار **{MAIN_STORE_NAME} ({MAIN_STORE_DOMAIN})** مقابل المنافسين."
    )
    db_log("processed", "view")

    processed = get_processed(limit=500)
    if not processed:
        st.info("📭 لا توجد منتجات معالجة بعد")
    else:
        df_proc = pd.DataFrame(processed)
        from utils.ui_components import render_products_in_tabs

        st.markdown("### 🗂️ عرض تبويبي (بطاقات المعالجة)")
        render_products_in_tabs(_build_cards_df(df_proc, "Review / Other ⚠️"), key_prefix="processed_tabs")

        # إحصاء
        actions = df_proc["action"].value_counts()
        cols_p = st.columns(len(actions) + 1)
        for i, (act, cnt) in enumerate(actions.items()):
            icon = {"send_price":"💰","send_missing":"📦","approved":"✅","removed":"🗑️"}.get(act,"📌")
            cols_p[i].metric(f"{icon} {act}", cnt)
        cols_p[-1].metric("📦 الإجمالي", len(df_proc))

        # فلتر
        act_filter = st.selectbox("نوع الإجراء", ["الكل"] + list(actions.index))
        show_df = df_proc if act_filter == "الكل" else df_proc[df_proc["action"] == act_filter]

        st.markdown("---")

        for _, row in show_df.iterrows():
            p_key  = str(row.get("product_key",""))
            p_name = str(row.get("product_name",""))
            p_act  = str(row.get("action",""))
            p_ts   = str(row.get("timestamp",""))
            p_price_old = safe_float(row.get("old_price",0))
            p_price_new = safe_float(row.get("new_price",0))
            p_notes = str(row.get("notes",""))
            p_comp  = str(row.get("competitor",""))

            icon_map = {"send_price":"💰","send_missing":"📦","approved":"✅","removed":"🗑️"}
            icon = icon_map.get(p_act, "📌")

            col_a, col_b = st.columns([5, 1])
            with col_a:
                price_info = ""
                if p_price_old > 0 and p_price_new > 0:
                    price_info = f" | {p_price_old:.0f} → {p_price_new:.0f} ر.س"
                elif p_price_new > 0:
                    price_info = f" | {p_price_new:.0f} ر.س"
                _notes_html = ("<br><span style='color:#aaa;font-size:.73rem'>" + p_notes[:80] + "</span>") if p_notes else ""
                st.markdown(
                    f'<div style="padding:6px 10px;border-radius:6px;background:#0a1628;'
                    f'border:1px solid #1a2a44;font-size:.85rem">'
                    f'<span style="color:#888;font-size:.75rem">{p_ts[:16]}</span> &nbsp;'
                    f'{icon} <b style="color:#4fc3f7">{p_name[:60]}</b>'
                    f'<span style="color:#888"> — {p_act}{price_info}</span>'
                    f'{_notes_html}</div>',
                    unsafe_allow_html=True
                )
            with col_b:
                if st.button("↩️ تراجع", key=f"undo_{p_key}"):
                    undo_processed(p_key)
                    # أعد للقائمة النشطة
                    if p_key in st.session_state.hidden_products:
                        st.session_state.hidden_products.discard(p_key)
                    st.success(f"✅ تم التراجع: {p_name[:40]}")
                    st.rerun()

        # تصدير
        st.markdown("---")
        csv_proc = df_proc.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button("📥 تصدير CSV", data=csv_proc,
                           file_name="processed_products.csv", mime="text/csv")


elif page == "🤖 الذكاء الصناعي":
    db_log("ai", "view")

    # ── شريط الحالة ──
    if GEMINI_API_KEYS:
        st.markdown(f'''<div style="background:linear-gradient(90deg,#051505,#030d1f);
            border:1px solid #00C853;border-radius:10px;padding:10px 18px;
            margin-bottom:12px;display:flex;align-items:center;gap:10px;">
          <div style="width:10px;height:10px;border-radius:50%;background:#00C853;
                      box-shadow:0 0 8px #00C853;animation:pulse 2s infinite"></div>
          <span style="color:#00C853;font-weight:800;font-size:1rem">Gemini Flash — متصل مباشرة</span>
          <span style="color:#555;font-size:.78rem"> | {len(GEMINI_API_KEYS)} مفاتيح | {GEMINI_MODEL}</span>
        </div>''', unsafe_allow_html=True)
    else:
        st.error("❌ Gemini غير متصل — أضف GEMINI_API_KEYS في Streamlit Secrets")

    # ── سياق البيانات ──
    _ctx = []
    if st.session_state.results:
        _r = st.session_state.results
        _ctx = [
            f"المنتجات الكلية: {len(_r.get('all', pd.DataFrame()))}",
            f"سعر أعلى: {len(_r.get('price_raise', pd.DataFrame()))}",
            f"سعر أقل: {len(_r.get('price_lower', pd.DataFrame()))}",
            f"موافق: {len(_r.get('approved', pd.DataFrame()))}",
            f"مراجعة: {len(_r.get('review', pd.DataFrame()))}",
            f"مفقود: {len(_r.get('missing', pd.DataFrame()))}",
        ]
    _ctx_str = " | ".join(_ctx) if _ctx else "لم يتم تحليل بيانات بعد"

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "💬 دردشة مباشرة", "📋 لصق وتحليل", "🔍 تحقق منتج", "💹 بحث سوق", "📊 أوامر مجمعة"
    ])

    # ═══ TAB 1: دردشة Gemini مباشرة ═══════════
    with tab1:
        st.caption(f"📊 البيانات: {_ctx_str}")

        # صندوق المحادثة
        _chat_h = 430
        _msgs_html = ""
        if not st.session_state.chat_history:
            _msgs_html = """<div style="text-align:center;padding:60px 20px;color:#333">
              <div style="font-size:3rem">🤖</div>
              <div style="color:#666;margin-top:10px;font-size:1rem">Gemini Flash جاهز للمساعدة</div>
              <div style="color:#444;margin-top:6px;font-size:.82rem">
                اسأل عن الأسعار · المنتجات · توصيات التسعير · تحليل المنافسين
              </div>
            </div>"""
        else:
            for h in st.session_state.chat_history[-15:]:
                _msgs_html += f"""
                <div style="display:flex;justify-content:flex-end;margin:5px 0">
                  <div style="background:#1e1e3f;color:#B8B4FF;padding:8px 14px;
                              border-radius:14px 14px 2px 14px;max-width:82%;font-size:.88rem;
                              line-height:1.5">{h['user']}</div>
                </div>
                <div style="display:flex;justify-content:flex-start;margin:4px 0 10px 0">
                  <div style="background:#080f1e;border:1px solid #1a3050;color:#d0d0d0;
                              padding:10px 14px;border-radius:14px 14px 14px 2px;
                              max-width:88%;font-size:.88rem;line-height:1.65">
                    <span style="color:#00C853;font-size:.65rem;font-weight:700">
                      ● {h.get('source','Gemini')} · {h.get('ts','')}</span><br>
                    {h['ai'].replace(chr(10),'<br>')}
                  </div>
                </div>"""

        st.markdown(
            f'''<div style="background:#050b14;border:1px solid #1a3050;border-radius:12px;
                padding:14px;height:{_chat_h}px;overflow-y:auto;direction:rtl">
              {_msgs_html}
            </div>''', unsafe_allow_html=True)

        # إدخال
        _mc1, _mc2 = st.columns([5, 1])
        with _mc1:
            _user_in = st.text_input("", key="gem_in",
                placeholder="اسأل Gemini — عن المنتجات، الأسعار، التوصيات...",
                label_visibility="collapsed")
        with _mc2:
            _send = st.button("➤ إرسال", key="gem_send", type="primary", use_container_width=True)

        # أزرار سريعة
        _qc = st.columns(4)
        _quick = None
        _quick_labels = [
            ("📉 أولويات الخفض", "بناءً على البيانات المحملة أعطني أولويات خفض الأسعار مع الأرقام"),
            ("📈 فرص الرفع", "حلّل فرص رفع الأسعار وأعطني توصية مرتبة"),
            ("🔍 أولويات المفقودات", "حلّل المنتجات المفقودة وأعطني أولويات الإضافة"),
            ("📊 ملخص شامل", f"أعطني ملخصاً تنفيذياً: {_ctx_str}"),
        ]
        for i, (lbl, q) in enumerate(_quick_labels):
            with _qc[i]:
                if st.button(lbl, key=f"q{i}", use_container_width=True):
                    _quick = q

        _msg_to_send = _quick or (_user_in if _send and _user_in else None)
        if _msg_to_send:
            _full = f"سياق البيانات: {_ctx_str}\n\n{_msg_to_send}"
            with st.spinner("🤖 Gemini يفكر..."):
                _res = gemini_chat(_full, st.session_state.chat_history)
            if _res["success"]:
                st.session_state.chat_history.append({
                    "user": _msg_to_send, "ai": _res["response"],
                    "source": _res.get("source","Gemini"),
                    "ts": datetime.now().strftime("%H:%M")
                })
                st.rerun()
            else:
                st.error(_res["response"])

        _dc1, _dc2 = st.columns([4,1])
        with _dc2:
            if st.session_state.chat_history:
                if st.button("🗑️ مسح", key="clr_chat"):
                    st.session_state.chat_history = []
                    st.rerun()

    # ═══ TAB 2: لصق وتحليل ══════════════════════
    with tab2:
        st.markdown("**الصق منتجات أو بيانات أو أوامر — Gemini سيحللها فوراً:**")

        _paste = st.text_area(
            "الصق هنا:",
            height=200, key="paste_box",
            placeholder="""يمكنك لصق:
• قائمة منتجات من Excel (Ctrl+C ثم Ctrl+V)
• أوامر: "خفّض كل منتج فرقه أكثر من 30 ريال"
• CSV مباشرة
• أي نص تريد تحليله""")

        _pc1, _pc2 = st.columns(2)
        with _pc1:
            if st.button("🤖 تحليل بـ Gemini", key="paste_go", type="primary", use_container_width=True):
                if _paste:
                    # إضافة سياق البيانات الحالية
                    _ctx_data = ""
                    if st.session_state.results:
                        _r2 = st.session_state.results
                        _all = _r2.get("all", pd.DataFrame())
                        if not _all.empty and len(_all) > 0:
                            cols = [c for c in ["المنتج","السعر","منتج_المنافس","سعر_المنافس","القرار"] if c in _all.columns]
                            if cols:
                                _ctx_data = "\n\nعينة من بيانات التطبيق:\n" + _all[cols].head(15).to_string(index=False)
                    with st.spinner("🤖 Gemini يحلل..."):
                        _pr = analyze_paste(_paste, _ctx_data)
                    st.markdown(f'<div class="ai-box">{_pr["response"]}</div>', unsafe_allow_html=True)
        with _pc2:
            if st.button("📊 تحويل لجدول", key="paste_table", use_container_width=True):
                if _paste:
                    try:
                        import io as _io
                        _df_p = pd.read_csv(_io.StringIO(_paste), sep=None, engine='python')
                        st.dataframe(_df_p.head(200), use_container_width=True)
                        _csv_p = _df_p.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                        st.download_button("📄 تحميل CSV", data=_csv_p,
                            file_name="pasted.csv", mime="text/csv", key="paste_dl")
                    except:
                        st.warning("تعذر التحويل لجدول — جرب تنسيق CSV أو TSV")

    # ═══ TAB 3: تحقق منتج ══════════════════════
    with tab3:
        st.markdown("**تحقق من تطابق منتجين بدقة 100%:**")
        _vc1, _vc2 = st.columns(2)
        _vp1 = _vc1.text_input("🏷️ منتجنا:", key="v_our", placeholder="Dior Sauvage EDP 100ml")
        _vp2 = _vc2.text_input("🏪 المنافس:", key="v_comp", placeholder="ديور سوفاج بارفان 100 مل")
        _vc3, _vc4 = st.columns(2)
        _vpr1 = _vc3.number_input("💰 سعرنا:", 0.0, key="v_p1")
        _vpr2 = _vc4.number_input("💰 سعر المنافس:", 0.0, key="v_p2")
        if st.button("🔍 تحقق الآن", key="vbtn", type="primary"):
            if _vp1 and _vp2:
                with st.spinner("🤖 AI يتحقق..."):
                    _vr = verify_match(_vp1, _vp2, _vpr1, _vpr2)
                if _vr["success"]:
                    _mc = "#00C853" if _vr.get("match") else "#FF1744"
                    _ml = "✅ متطابقان" if _vr.get("match") else "❌ غير متطابقان"
                    st.markdown(f'''<div style="background:{_mc}22;border:1px solid {_mc};
                        border-radius:8px;padding:12px;margin:8px 0">
                      <div style="color:{_mc};font-weight:800;font-size:1.1rem">{_ml}</div>
                      <div style="color:#aaa;margin-top:4px">ثقة: <b>{_vr.get("confidence",0)}%</b></div>
                      <div style="color:#888;font-size:.88rem;margin-top:6px">{_vr.get("reason","")}</div>
                    </div>''', unsafe_allow_html=True)
                    if _vr.get("suggestion"):
                        st.info(f"💡 {_vr['suggestion']}")
                else:
                    st.error("فشل الاتصال")

    # ═══ TAB 4: بحث السوق ══════════════════════
    with tab4:
        st.markdown("**ابحث عن سعر السوق الحقيقي لأي منتج:**")
        _ms1, _ms2 = st.columns([3,1])
        with _ms1:
            _mprod = st.text_input("🔎 اسم المنتج:", key="mkt_prod",
                                    placeholder="Dior Sauvage EDP 100ml")
        with _ms2:
            _mcur = st.number_input("💰 سعرنا:", 0.0, key="mkt_price")

        if st.button("🌐 ابحث في السوق", key="mkt_btn", type="primary"):
            if _mprod:
                with st.spinner("🌐 Gemini يبحث في السوق..."):
                    _mr = search_market_price(_mprod, _mcur)
                if _mr.get("success"):
                    _mp = _mr.get("market_price", 0)
                    _rng = _mr.get("price_range", {})
                    _comps = _mr.get("competitors", [])
                    _rec = _mr.get("recommendation","")
                    _diff_v = _mp - _mcur if _mcur > 0 else 0
                    _diff_c = "#00C853" if _diff_v > 0 else "#FF1744" if _diff_v < 0 else "#888"

                    _src1, _src2 = st.columns(2)
                    with _src1:
                        st.metric("💹 سعر السوق", f"{_mp:,.0f} ر.س",
                                  delta=f"{_diff_v:+.0f} ر.س" if _mcur > 0 else None)
                    with _src2:
                        _mn = _rng.get("min",0); _mx = _rng.get("max",0)
                        st.metric("📊 نطاق السعر", f"{_mn:,.0f} - {_mx:,.0f} ر.س")

                    if _comps:
                        st.markdown("**🏪 منافسون في السوق:**")
                        for _c in _comps[:5]:
                            _cpv = float(_c.get("price",0))
                            _dv = _cpv - _mcur if _mcur > 0 else 0
                            st.markdown(
                                f"• **{_c.get('name','')}**: {_cpv:,.0f} ر.س "
                                f"({'أعلى' if _dv>0 else 'أقل'} بـ {abs(_dv):.0f}ر.س)" if _dv != 0 else
                                f"• **{_c.get('name','')}**: {_cpv:,.0f} ر.س"
                            )
                    if _rec:
                        st.markdown(f'<div class="ai-box">💡 {_rec}</div>', unsafe_allow_html=True)

        # صورة المنتج من Fragrantica
        with st.expander("🖼️ صورة ومكونات من Fragrantica Arabia", expanded=False):
            _fprod = st.text_input("اسم العطر:", key="frag_prod",
                                    placeholder="Dior Sauvage EDP")
            if st.button("🔍 ابحث في Fragrantica", key="frag_btn"):
                if _fprod:
                    with st.spinner("يجلب من Fragrantica Arabia..."):
                        _fi = fetch_fragrantica_info(_fprod)
                    if _fi.get("success"):
                        _fic1, _fic2 = st.columns([1,2])
                        with _fic1:
                            _img_url = _fi.get("image_url","")
                            if _img_url and _img_url.startswith("http"):
                                st.image(_img_url, width=200, caption=_fprod)
                            else:
                                st.markdown(f"[🔗 Fragrantica Arabia]({_FR}/search/?query={_fprod.replace(' ','+')})")
                        with _fic2:
                            _top = ", ".join(_fi.get("top_notes",[])[:5])
                            _mid = ", ".join(_fi.get("middle_notes",[])[:5])
                            _base = ", ".join(_fi.get("base_notes",[])[:5])
                            st.markdown(f"""
🌸 **القمة:** {_top or "—"}
💐 **القلب:** {_mid or "—"}
🌿 **القاعدة:** {_base or "—"}
📝 **{_fi.get('description_ar','')}**""")
                        if _fi.get("fragrantica_url"):
                            st.markdown(f"[🌐 صفحة العطر في Fragrantica]({_fi['fragrantica_url']})")
                    else:
                        st.info("لم يتم العثور على بيانات — تحقق من اسم العطر")

    # ═══ TAB 5: أوامر مجمعة ════════════════════
    with tab5:
        st.markdown("**نفّذ أوامر مجمعة على بياناتك:**")
        st.caption(f"📊 البيانات: {_ctx_str}")

        _cmd_section = st.selectbox(
            "اختر القسم:", ["الكل", "سعر أعلى", "سعر أقل", "موافق", "مراجعة", "مفقود"],
            key="cmd_sec"
        )
        _cmd_text = st.text_area(
            "الأمر أو السؤال:", height=120, key="cmd_area",
            placeholder="""أمثلة:
• حلّل المنتجات التي فرقها أكثر من 30 ريال وأعطني توصية
• رتّب المنتجات حسب الأولوية
• ما المنتجات التي تحتاج خفض سعر فوري؟
• أعطني ملخص مقارنة مع المنافسين"""
        )

        if st.button("⚡ تنفيذ الأمر", key="cmd_run", type="primary"):
            if _cmd_text and st.session_state.results:
                _sec_map = {
                    "سعر أعلى":"price_raise","سعر أقل":"price_lower",
                    "موافق":"approved","مراجعة":"review","مفقود":"missing"
                }
                _df_sec = None
                if _cmd_section != "الكل":
                    _k = _sec_map.get(_cmd_section)
                    _df_sec = st.session_state.results.get(_k, pd.DataFrame())
                else:
                    _df_sec = st.session_state.results.get("all", pd.DataFrame())

                if _df_sec is not None and not _df_sec.empty:
                    _cols = [c for c in ["المنتج","السعر","منتج_المنافس","سعر_المنافس","القرار","الفرق"] if c in _df_sec.columns]
                    _sample = _df_sec[_cols].head(25).to_string(index=False) if _cols else ""
                    _full_cmd = f"""البيانات ({_cmd_section}) - {len(_df_sec)} منتج:
{_sample}

الأمر: {_cmd_text}"""
                    with st.spinner("⚡ Gemini ينفذ الأمر..."):
                        _cr = call_ai(_full_cmd, "general")
                    st.markdown(f'<div class="ai-box">{_cr["response"]}</div>', unsafe_allow_html=True)
                else:
                    with st.spinner("🤖"):
                        _cr = call_ai(f"{_ctx_str}\n\n{_cmd_text}", "general")
                    st.markdown(f'<div class="ai-box">{_cr["response"]}</div>', unsafe_allow_html=True)
            elif _cmd_text:
                with st.spinner("🤖"):
                    _cr = call_ai(_cmd_text, "general")
                st.markdown(f'<div class="ai-box">{_cr["response"]}</div>', unsafe_allow_html=True)


# ════════════════════════════════════════════════
#  9. أتمتة Make
# ════════════════════════════════════════════════
elif page == "⚡ أتمتة Make":
    st.header("⚡ أتمتة Make.com")
    db_log("make", "view")

    tab1, tab2, tab3 = st.tabs(["🔗 حالة الاتصال", "📤 إرسال", "📦 القرارات المعلقة"])

    with tab1:
        if st.button("🔍 فحص الاتصال"):
            with st.spinner("..."):
                results = verify_webhook_connection()
                for name, r in results.items():
                    if name != "all_connected":
                        color = "🟢" if r["success"] else "🔴"
                        st.markdown(f"{color} **{name}:** {r['message']}")
                if results.get("all_connected"):
                    st.success("✅ جميع الاتصالات تعمل")

    with tab2:
        if st.session_state.results:
            wh = st.selectbox("نوع الإرسال", ["سعر أعلى (تخفيض)","سعر أقل (رفع)","موافق عليها","مفقودة"])
            key_map = {
                "سعر أعلى (تخفيض)": "price_raise",
                "سعر أقل (رفع)":    "price_lower",
                "موافق عليها":      "approved",
                "مفقودة":           "missing",
            }
            section_type_map = {
                "price_raise": "raise",
                "price_lower": "lower",
                "approved":    "approved",
                "missing":     "missing",
            }
            sec_key  = key_map[wh]
            sec_type = section_type_map[sec_key]
            df_s     = st.session_state.results.get(sec_key, pd.DataFrame())

            if not df_s.empty:
                # معاينة ما سيُرسل
                _prev_cols = ["المنتج","السعر","سعر_المنافس","الماركة"]
                _prev_cols = [c for c in _prev_cols if c in df_s.columns]
                if _prev_cols:
                    st.dataframe(df_s[_prev_cols].head(10), use_container_width=True)

                products = export_to_make_format(df_s, sec_type)
                _sendable = [p for p in products if p.get("name") and p.get("price",0) > 0]
                st.info(f"سيتم إرسال {len(_sendable)} منتج → Make (Payload: product_id + name + price)")

                if st.button("📤 إرسال الآن", type="primary"):
                    if sec_type == "missing":
                        res = send_missing_products(_sendable)
                    else:
                        res = send_price_updates(_sendable)
                    st.success(res["message"]) if res["success"] else st.error(res["message"])
            else:
                st.info("لا توجد بيانات في هذا القسم")

    with tab3:
        pending = st.session_state.decisions_pending
        if pending:
            st.info(f"📦 {len(pending)} قرار معلق")
            df_p = pd.DataFrame([
                {"المنتج": k, "القرار": v["action"],
                 "وقت القرار": v.get("ts",""), "المنافس": v.get("competitor","")}
                for k, v in pending.items()
            ])
            st.dataframe(df_p.head(200), use_container_width=True)

            c1, c2 = st.columns(2)
            with c1:
                if st.button("📤 إرسال كل القرارات لـ Make"):
                    to_send = [{"name": k, **v} for k, v in pending.items()]
                    res = send_price_updates(to_send)
                    st.success(res["message"])
                    st.session_state.decisions_pending = {}
                    st.rerun()
            with c2:
                if st.button("🗑️ مسح القرارات"):
                    st.session_state.decisions_pending = {}
                    st.rerun()
        else:
            st.info("لا توجد قرارات معلقة")


# ════════════════════════════════════════════════
#  10. الإعدادات
# ════════════════════════════════════════════════
elif page == "⚙️ الإعدادات":
    st.header("⚙️ الإعدادات")
    db_log("settings", "view")

    tab1, tab2, tab3 = st.tabs(["🔑 المفاتيح", "⚙️ المطابقة", "📜 السجل"])

    with tab1:
        # ── الحالة الحالية ────────────────────────────────────────────────
        gemini_s = f"✅ {len(GEMINI_API_KEYS)} مفتاح" if GEMINI_API_KEYS else "❌ لا توجد مفاتيح"
        or_s     = "✅ مفعل" if OPENROUTER_API_KEY else "❌ غير موجود"
        co_s     = "✅ مفعل" if COHERE_API_KEY else "❌ غير موجود"
        st.info(f"Gemini API: {gemini_s}")
        st.info(f"OpenRouter: {or_s}")
        st.info(f"Cohere:     {co_s}")
        st.info(f"Webhook أسعار:   {'✅' if WEBHOOK_UPDATE_PRICES else '❌'}")
        st.info(f"Webhook منتجات:  {'✅' if WEBHOOK_NEW_PRODUCTS else '❌'}")

        st.markdown("---")

        # ── تشخيص شامل ───────────────────────────────────────────────────
        st.subheader("🔬 تشخيص AI")
        st.caption("يختبر الاتصال الفعلي بكل مزود ويُظهر الخطأ الحقيقي")

        if st.button("🔬 تشخيص شامل لجميع المزودين", type="primary"):
            with st.spinner("يختبر الاتصال بـ Gemini, OpenRouter, Cohere..."):
                from engines.ai_engine import diagnose_ai_providers
                diag = diagnose_ai_providers()

            # ── نتائج Gemini ──────────────────────────────────────────────
            st.markdown("**Gemini API:**")
            any_gemini_ok = False
            for g in diag.get("gemini", []):
                status = g["status"]
                if "✅" in status:
                    st.success(f"مفتاح {g['key']}: {status}")
                    any_gemini_ok = True
                elif "⚠️" in status:
                    st.warning(f"مفتاح {g['key']}: {status}")
                else:
                    st.error(f"مفتاح {g['key']}: {status}")

            # ── نتائج OpenRouter ──────────────────────────────────────────
            or_res = diag.get("openrouter","")
            st.markdown("**OpenRouter:**")
            if "✅" in or_res: st.success(or_res)
            elif "⚠️" in or_res: st.warning(or_res)
            else: st.error(or_res)

            # ── نتائج Cohere ──────────────────────────────────────────────
            co_res = diag.get("cohere","")
            st.markdown("**Cohere:**")
            if "✅" in co_res: st.success(co_res)
            elif "⚠️" in co_res: st.warning(co_res)
            else: st.error(co_res)

            # ── تحليل وتوصية ─────────────────────────────────────────────
            or_ok = "✅" in or_res
            co_ok = "✅" in co_res

            st.markdown("---")
            if any_gemini_ok or or_ok or co_ok:
                working = []
                if any_gemini_ok: working.append("Gemini")
                if or_ok: working.append("OpenRouter")
                if co_ok: working.append("Cohere")
                st.success(f"✅ AI يعمل عبر: {' + '.join(working)}")
            else:
                st.error("❌ جميع المزودين فاشلون")
                # تحليل السبب
                _all_errs = [g["status"] for g in diag.get("gemini",[]) if "❌" in g.get("status","")]
                if any("اتصال" in e or "ConnectionError" in e or "Pool" in e for e in _all_errs + [or_res, co_res]):
                    st.warning("""
**🔴 السبب المحتمل: Streamlit Cloud يحجب الطلبات الخارجية**

الحل: في صفحة تطبيقك على Streamlit Cloud:
1. اذهب إلى ⚙️ Settings → General
2. ابحث عن **"Network"** أو **"Egress"**
3. تأكد أن Outbound connections مسموح بها

أو جرب نشر التطبيق على **Railway** بدلاً من Streamlit Cloud.
                    """)
                elif any("403" in e or "IP" in e for e in _all_errs):
                    st.warning("🔴 مفاتيح Gemini محظورة من IP هذا الخادم — جرب OpenRouter")
                elif any("401" in e for e in _all_errs + [or_res, co_res]):
                    st.warning("🔴 مفتاح غير صحيح — تحقق من المفاتيح في Secrets")

        st.markdown("---")

        # ── سجل الأخطاء الأخيرة ──────────────────────────────────────────
        st.subheader("📋 آخر أخطاء AI")
        from engines.ai_engine import get_last_errors
        errs = get_last_errors()
        if errs:
            for e in errs:
                st.code(e, language=None)
        else:
            st.caption("لا أخطاء مسجلة بعد — جرب أي زر AI ثم ارجع هنا")

        st.markdown("---")

        # ── اختبار سريع ──────────────────────────────────────────────────
        if st.button("🧪 اختبار سريع"):
            with st.spinner("يتصل بـ AI..."):
                r = call_ai("أجب بكلمة واحدة فقط: يعمل", "general")
            if r["success"]:
                st.success(f"✅ AI يعمل عبر {r['source']}: {r['response'][:80]}")
            else:
                st.error("❌ فشل — اضغط 'تشخيص شامل' لمعرفة السبب الدقيق")
                from engines.ai_engine import get_last_errors
                for e in get_last_errors()[:5]:
                    st.code(e, language=None)

    with tab2:
        st.info(f"حد التطابق الأدنى: {MIN_MATCH_SCORE}%")
        st.info(f"حد التطابق العالي: {HIGH_MATCH_SCORE}%")
        st.info(f"هامش فرق السعر: {PRICE_DIFF_THRESHOLD} ر.س")

    with tab3:
        decisions = get_decisions(limit=30)
        if decisions:
            df_dec = pd.DataFrame(decisions)
            st.dataframe(df_dec[["timestamp","product_name","old_status",
                                  "new_status","reason","competitor"]].rename(columns={
                "timestamp":"التاريخ","product_name":"المنتج",
                "old_status":"من","new_status":"إلى",
                "reason":"السبب","competitor":"المنافس"
            }).head(200), use_container_width=True)
        else:
            st.info("لا توجد قرارات مسجلة")

    st.markdown("---")
    st.info(
        "🏢 **كشط المنافسين و Sitemap:** انتقل إلى القسم **🏢 كشط المنافسين** في الشريط الجانبي "
        "لإضافة الروابط وتشغيل الجلب مع عرض وحفظ تدريجي."
    )


# ════════════════════════════════════════════════
#  11. السجل
# ════════════════════════════════════════════════
elif page == "📜 السجل":
    st.header("📜 السجل الكامل")
    db_log("log", "view")

    tab1, tab2, tab3 = st.tabs(["📊 التحليلات", "💰 تغييرات الأسعار", "📝 الأحداث"])

    with tab1:
        history = get_analysis_history(20)
        if history:
            df_h = pd.DataFrame(history)
            st.dataframe(df_h[["timestamp","our_file","comp_file",
                                "total_products","matched","missing"]].rename(columns={
                "timestamp":"التاريخ","our_file":"ملف منتجاتنا",
                "comp_file":"ملف المنافس","total_products":"الإجمالي",
                "matched":"متطابق","missing":"مفقود"
            }).head(200), use_container_width=True)
        else:
            st.info("لا يوجد تاريخ")

    with tab2:
        days = st.slider("آخر X يوم", 1, 30, 7)
        changes = get_price_changes(days)
        if changes:
            df_c = pd.DataFrame(changes)
            st.dataframe(df_c.rename(columns={
                "product_name":"المنتج","competitor":"المنافس",
                "old_price":"السعر السابق","new_price":"السعر الجديد",
                "price_diff":"التغيير","new_date":"تاريخ التغيير"
            }).head(200), use_container_width=True)
        else:
            st.info(f"لا توجد تغييرات في آخر {days} يوم")

    with tab3:
        events = get_events(limit=50)
        if events:
            df_e = pd.DataFrame(events)
            st.dataframe(df_e[["timestamp","page","event_type","details"]].rename(columns={
                "timestamp":"التاريخ","page":"الصفحة",
                "event_type":"الحدث","details":"التفاصيل"
            }).head(200), use_container_width=True)
        else:
            st.info("لا توجد أحداث")

# ════════════════════════════════════════════════
#  13. الأتمتة الذكية (v26.0 — متصل بالتنقل)
# ════════════════════════════════════════════════
elif page == "🔄 الأتمتة الذكية":
    st.header("🔄 الأتمتة الذكية — محرك القرارات التلقائية")
    db_log("automation", "view")

    # ── إنشاء محرك الأتمتة ──
    if "auto_engine" not in st.session_state:
        st.session_state.auto_engine = AutomationEngine()
    if "search_manager" not in st.session_state:
        st.session_state.search_manager = ScheduledSearchManager()

    engine = st.session_state.auto_engine
    search_mgr = st.session_state.search_manager

    tab_a1, tab_a2, tab_a3, tab_a4, tab_a5 = st.tabs([
        "🤖 تشغيل الأتمتة", "⚙️ قواعد التسعير",
        "🔍 البحث الدوري", "📊 سجل القرارات", "🛡️ شبكة الأمان"
    ])

    # ── تاب 1: تشغيل الأتمتة ──
    with tab_a1:
        st.subheader("تطبيق القواعد التلقائية على نتائج التحليل")

        if st.session_state.results and st.session_state.analysis_df is not None:
            adf = st.session_state.analysis_df
            _msc = _match_score_col(adf)
            if _msc is None:
                st.warning("لا يوجد عمود **match_score** أو **نسبة_التطابق** في نتائج التحليل — أعد تشغيل التحليل من «رفع الملفات».")
                matched_df = pd.DataFrame()
            else:
                matched_df = adf[adf[_msc].apply(lambda x: safe_float(x)) >= 85].copy()
            st.info(f"📦 {len(matched_df)} منتج مؤكد المطابقة جاهز للتقييم التلقائي")

            col_a, col_b = st.columns(2)
            with col_a:
                auto_push_enabled = st.checkbox(
                    "إرسال تلقائي لـ Make.com بعد الأتمتة",
                    value=False, key="auto_push_check",
                    help="تأكد من إعداد Webhook قبل التفعيل"
                )
                if st.button("🚀 تشغيل الأتمتة على كل المنتجات المؤكدة",
                             type="primary", key="run_auto"):
                    prog_bar  = st.progress(0, text="⚙️ جاري تقييم المنتجات...")
                    prog_text = st.empty()
                    _prog_state = {"p": 0}

                    def _progress_cb(processed, total):
                        pct = processed / total if total > 0 else 0
                        prog_bar.progress(pct,
                            text=f"⚙️ جاري التقييم: {processed}/{total} منتج...")
                        _prog_state["p"] = processed

                    engine.clear_log()
                    result = process_confirmed_batch(
                        matched_df,
                        push_to_make=auto_push_enabled,
                        progress_callback=_progress_cb
                    )
                    prog_bar.progress(1.0, text="✅ اكتملت المعالجة!")
                    st.session_state._auto_decisions = result.get("decisions", [])

                    decisions = result.get("decisions", [])
                    summary   = result.get("summary", {})

                    if decisions:
                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("📦 إجمالي المُقيَّمة", result.get("total_evaluated", 0))
                        c2.metric("⬇️ خفض سعر",  summary.get("lower", 0))
                        c3.metric("⬆️ رفع سعر",  summary.get("raise", 0))
                        c4.metric("✅ إبقاء",      summary.get("keep", 0))

                        net = summary.get("net_impact", 0)
                        if net > 0:
                            st.success(f"💰 الأثر المالي: +{net:.0f} ر.س (ربح إضافي متوقع)")
                        elif net < 0:
                            st.warning(f"📉 الأثر المالي: {net:.0f} ر.س (خفض لتحقيق التنافسية)")

                        if auto_push_enabled:
                            pushed = result.get("pushed", 0)
                            st.info(f"📡 {pushed} تحديث أُرسل إلى Make.com تلقائياً")

                        # جدول القرارات مع فلتر
                        dec_df = pd.DataFrame(decisions)
                        action_filter = st.selectbox(
                            "فلتر الإجراءات",
                            ["الكل", "lower_price", "raise_price", "keep_price"],
                            key="auto_action_filter"
                        )
                        show_df = dec_df if action_filter == "الكل" else                                   dec_df[dec_df["action"] == action_filter]

                        display_cols = ["product_name", "action", "old_price",
                                        "new_price", "comp_price", "match_score", "reason"]
                        available = [c for c in display_cols if c in show_df.columns]
                        st.dataframe(show_df[available].rename(columns={
                            "product_name": "المنتج", "action": "الإجراء",
                            "old_price": "السعر الحالي", "new_price": "السعر الجديد",
                            "comp_price": "سعر المنافس",
                            "match_score": "تطابق%", "reason": "السبب"
                        }), use_container_width=True, height=400)

                        # تصدير
                        _auto_excel = result["decisions"]
                        try:
                            import io, openpyxl
                            _out = io.BytesIO()
                            pd.DataFrame(_auto_excel).to_excel(_out, index=False)
                            st.download_button("📥 تحميل قرارات الأتمتة Excel",
                                data=_out.getvalue(),
                                file_name="automation_decisions.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="auto_dl_excel")
                        except Exception:
                            pass
                    else:
                        st.info("لم يتم اتخاذ أي قرارات — جميع الأسعار ضمن الهامش المقبول")

            with col_b:
                auto_decisions = st.session_state.get("_auto_decisions", [])
                push_eligible = [d for d in auto_decisions
                                 if d.get("action") in ("lower_price", "raise_price")
                                 and d.get("product_id")]
                if push_eligible:
                    # ── فحص Safety Net أولاً ──────────────────────────
                    _sc = safety_check_decisions(push_eligible)
                    _safe_c    = _sc["summary"]["safe_count"]
                    _blocked_c = _sc["summary"]["blocked_count"]

                    if _blocked_c > 0:
                        st.warning(
                            f"⛔ **{_blocked_c} قرار محجوب** بسبب انخفاض/ارتفاع مفرط "
                            f"(> 30%) — يحتاج موافقة يدوية"
                        )
                        with st.expander("👁 عرض القرارات المحجوبة", expanded=False):
                            for bd in _sc["blocked"]:
                                st.error(
                                    f"**{bd.get('product_name','؟')}** | "
                                    f"{bd.get('old_price',0):.0f} ← {bd.get('new_price',0):.0f} ر.س | "
                                    f"{bd.get('_block_reason','')}"
                                )

                    st.info(f"📤 **{_safe_c} قرار آمن** جاهز للإرسال إلى Make.com/سلة")

                    col_pb1, col_pb2 = st.columns(2)
                    with col_pb1:
                        if _safe_c > 0 and st.button(
                            f"📤 إرسال {_safe_c} قرار آمن → Make.com",
                            key="push_auto", type="primary"
                        ):
                            with st.spinner("🔒 فحص الأمان + إرسال..."):
                                push_res = safe_push_decisions(auto_decisions)
                            if push_res.get("success"):
                                st.success(push_res["message"])
                            else:
                                st.warning(push_res["message"])

                    with col_pb2:
                        if _blocked_c > 0 and st.button(
                            f"⚠️ إرسال {_blocked_c} محجوب بقوة",
                            key="push_force",
                            help="تجاوز Safety Net — للخبراء فقط"
                        ):
                            with st.spinner("يُرسل جميع القرارات بدون فلتر..."):
                                push_res2 = auto_push_decisions(push_eligible)
                            if push_res2.get("success"):
                                st.success(push_res2["message"])
                            else:
                                st.error(push_res2["message"])
                else:
                    st.caption("لا توجد قرارات جاهزة للإرسال — شغّل الأتمتة أولاً")

        else:
            st.warning("⚠️ لا توجد نتائج تحليل — ارفع الملفات أولاً من صفحة 'رفع الملفات'")

        # ── معالجة قسم المراجعة تلقائياً ──
        st.divider()
        st.subheader("🔄 معالجة قسم المراجعة تلقائياً")
        st.caption("يستخدم AI للتحقق المزدوج من المطابقات غير المؤكدة")

        if st.session_state.results and "review" in st.session_state.results:
            rev_df = st.session_state.results.get("review", pd.DataFrame())
            if not rev_df.empty:
                st.info(f"📋 {len(rev_df)} منتج تحت المراجعة")
                batch_size = st.slider(
                    "عدد المنتجات للمعالجة دفعة واحدة",
                    min_value=10, max_value=min(100, len(rev_df)),
                    value=min(30, len(rev_df)), step=10,
                    key="review_batch_size",
                    help="AI يحتاج وقتاً لكل منتج — ابدأ بـ 30"
                )
                st.caption(
                    "⚠️ الحد الأدنى للتأكيد: **95% ثقة** — "
                    "ما دون ذلك يبقى للمراجعة اليدوية"
                )
                if st.button("🤖 تحقق AI مزدوج لقسم المراجعة",
                             key="auto_review", type="primary"):
                    _rev_prog = st.progress(0, text="🤖 AI يتحقق من المطابقات...")
                    batch_df = rev_df.head(batch_size)
                    with st.spinner(f"🔍 يتحقق من {len(batch_df)} منتج بحد ثقة 95%..."):
                        confirmed = auto_process_review_items(
                            batch_df,
                            confidence_threshold=95.0
                        )
                    _rev_prog.progress(1.0, text="✅ اكتمل التحقق")

                    rejected_count = len(batch_df) - len(confirmed)
                    if not confirmed.empty:
                        st.success(
                            f"✅ تأكيد: **{len(confirmed)}** منتج | "
                            f"⏸️ للمراجعة اليدوية: **{rejected_count}** منتج"
                        )
                        # عرض المؤكدة مع نسبة الثقة
                        show_cols = [c for c in
                            ["المنتج", "منتج_المنافس", "القرار",
                             "_verification_confidence"]
                            if c in confirmed.columns]
                        st.dataframe(
                            confirmed[show_cols].rename(columns={
                                "_verification_confidence": "ثقة AI%"
                            }),
                            use_container_width=True
                        )
                    else:
                        st.info(
                            f"لم يتم تأكيد أي مطابقة من {len(batch_df)} منتج "
                            f"— نسبة الثقة < 95% لجميعها، تحتاج مراجعة يدوية"
                        )
            else:
                st.success("لا توجد منتجات تحت المراجعة")

    # ── تاب 2: قواعد التسعير ──
    with tab_a2:
        st.subheader("⚙️ قواعد التسعير النشطة")
        st.caption("القواعد تُطبّق بالترتيب — أول قاعدة تنطبق تُنفَّذ")

        for i, rule in enumerate(engine.rules):
            with st.expander(f"{'✅' if rule.enabled else '⬜'} {rule.name}", expanded=False):
                st.write(f"**الإجراء:** {rule.action}")
                st.write(f"**حد التطابق الأدنى:** {rule.min_match_score}%")
                for k, v in rule.params.items():
                    if k not in ("name", "enabled", "action", "min_match_score", "condition"):
                        st.write(f"**{k}:** {v}")

        st.divider()
        st.subheader("📝 تخصيص القواعد")
        st.caption("يمكنك تعديل القواعد من ملف config.py → AUTOMATION_RULES_DEFAULT")
        st.code("""
# مثال: إضافة قاعدة جديدة في config.py
AUTOMATION_RULES_DEFAULT.append({
    "name": "خفض عدواني",
    "enabled": True,
    "action": "undercut",
    "min_diff": 5,
    "undercut_amount": 2,
    "min_match_score": 95,
    "max_loss_pct": 10,
})
        """, language="python")

    # ── تاب 3: البحث الدوري ──
    with tab_a3:
        st.subheader("🔍 البحث الدوري عن أسعار المنافسين")

        c1, c2 = st.columns(2)
        c1.metric("⏱️ البحث القادم", search_mgr.time_until_next())
        c2.metric("📊 آخر نتائج", f"{len(search_mgr.last_results)} منتج")

        if st.session_state.analysis_df is not None:
            scan_count = st.slider("عدد المنتجات للمسح", 5, 50, 15, key="scan_n")
            if st.button("🔍 مسح السوق الآن", type="primary", key="scan_now"):
                with st.spinner(f"يبحث عن أسعار {scan_count} منتج في السوق..."):
                    scan_results = search_mgr.run_scan(st.session_state.analysis_df, scan_count)
                if scan_results:
                    st.success(f"✅ تم مسح {len(scan_results)} منتج بنجاح")
                    for sr in scan_results[:10]:
                        md = sr.get("market_data", {})
                        rec = md.get("recommendation", md.get("market_price", "—"))
                        st.markdown(f"**{sr['product']}** — سعرنا: {sr['our_price']:.0f} | السوق: {rec}")
                else:
                    st.warning("لم يتم العثور على نتائج — تحقق من اتصال AI")
        else:
            st.warning("ارفع ملفات التحليل أولاً")

    # ── تاب 4: سجل القرارات ──
    with tab_a4:
        st.subheader("📊 سجل قرارات الأتمتة")
        days_filter = st.selectbox("الفترة", [7, 14, 30], index=0, key="auto_log_days")

        stats = get_automation_stats(days_filter)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("إجمالي", stats["total"])
        c2.metric("خفض", stats["lower"])
        c3.metric("رفع", stats["raise"])
        c4.metric("أُرسل لـ Make", stats["pushed"])

        log_data = get_automation_log(limit=100)
        if log_data:
            log_df = pd.DataFrame(log_data)
            display = ["timestamp", "product_name", "action", "old_price",
                        "new_price", "competitor", "match_score", "pushed_to_make"]
            available = [c for c in display if c in log_df.columns]
            st.dataframe(log_df[available].rename(columns={
                "timestamp": "التاريخ", "product_name": "المنتج",
                "action": "الإجراء", "old_price": "السعر القديم",
                "new_price": "السعر الجديد", "competitor": "المنافس",
                "match_score": "التطابق%", "pushed_to_make": "أُرسل؟"
            }), use_container_width=True)
        else:
            st.info("لا توجد قرارات مسجلة بعد — شغّل الأتمتة من التاب الأول")

    # ── تاب 5: شبكة الأمان ──
    with tab_a5:
        st.subheader("🛡️ إعدادات شبكة الأمان (Safety Net)")
        st.caption(
            "شبكة الأمان تمنع الإرسال التلقائي لأي تغيير سعري غير منطقي "
            "قد ينتج عن خطأ في بيانات المنافس."
        )

        col_s1, col_s2, col_s3 = st.columns(3)
        with col_s1:
            max_drop = st.number_input(
                "⬇️ أقصى انخفاض مسموح (%)",
                min_value=5.0, max_value=80.0, value=30.0, step=5.0,
                key="safety_max_drop",
                help="إذا انخفض السعر بأكثر من هذه النسبة → يُحجب ويُرسل للمراجعة"
            )
        with col_s2:
            max_raise = st.number_input(
                "⬆️ أقصى ارتفاع مسموح (%)",
                min_value=5.0, max_value=100.0, value=50.0, step=5.0,
                key="safety_max_raise",
                help="إذا ارتفع السعر بأكثر من هذه النسبة → يُحجب ويُرسل للمراجعة"
            )
        with col_s3:
            min_price = st.number_input(
                "💰 أقل سعر مطلق (ريال)",
                min_value=1.0, max_value=100.0, value=10.0, step=1.0,
                key="safety_min_price",
                help="لا يمكن لأي سعر أن يُرسل إذا كان أقل من هذا الحد"
            )

        st.divider()
        st.markdown("#### 🧪 اختبر شبكة الأمان على القرارات الحالية")

        auto_decisions = st.session_state.get("_auto_decisions", [])
        push_eligible  = [d for d in auto_decisions
                          if d.get("action") in ("lower_price", "raise_price")]

        if push_eligible:
            if st.button("🔍 فحص القرارات الحالية", key="test_safety"):
                from engines.automation import safety_check_decisions, SAFETY_MAX_DROP_PCT, SAFETY_MAX_RAISE_PCT, SAFETY_ABS_MIN_PRICE
                # Override defaults with UI values
                import engines.automation as _auto_mod
                _auto_mod.SAFETY_MAX_DROP_PCT  = max_drop
                _auto_mod.SAFETY_MAX_RAISE_PCT = max_raise
                _auto_mod.SAFETY_ABS_MIN_PRICE = min_price

                checked = safety_check_decisions(push_eligible)
                s = checked["summary"]

                col_r1, col_r2, col_r3 = st.columns(3)
                col_r1.metric("إجمالي القرارات",  s["total"])
                col_r2.metric("✅ قرارات آمنة",   s["safe_count"])
                col_r3.metric("⛔ محجوبة",         s["blocked_count"])

                if checked["blocked"]:
                    st.warning("القرارات المحجوبة — يجب مراجعتها يدوياً:")
                    for bd in checked["blocked"]:
                        st.error(
                            f"**{bd.get('product_name','؟')}** | "
                            f"{bd.get('old_price',0):.0f} ر.س ← {bd.get('new_price',0):.0f} ر.س | "
                            f"⛔ {bd.get('_block_reason','')}"
                        )
                else:
                    st.success("✅ جميع القرارات آمنة ضمن الحدود المحددة")
        else:
            st.info("شغّل الأتمتة أولاً من التاب الأول لتظهر القرارات هنا")

        st.divider()
        with st.expander("📖 كيف تعمل شبكة الأمان؟", expanded=False):
            st.markdown("""
**مثال:**
- منتج سعره **500 ر.س** وسعر المنافس **300 ر.س**
- الانخفاض = 40% > 30% → **محجوب** ← قد يكون خطأ عند المنافس

**ماذا يحدث بعد الحجب؟**
1. القرار لا يُرسل إلى Make.com تلقائياً
2. يظهر في قائمة "القرارات المحجوبة" مع السبب
3. يمكنك مراجعته يدوياً والموافقة عليه أو رفضه
4. زر "إرسال بقوة" متاح للخبراء فقط عند الحاجة

**التوصية:**
- ابدأ بـ 30% انخفاض و 50% ارتفاع
- بعد شهر من الاستخدام يمكنك رفعها لـ 40% و 60%
            """)

