import json
import os
import threading
import subprocess
import sys
import time
import sqlite3
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from config import MAIN_STORE_DOMAIN, MAIN_STORE_NAME, is_main_store_domain
from utils.sitemap_resolve import resolve_store_to_sitemap_url

_SCRAPER_PROGRESS = os.path.join("data", "scraper_progress.json")
STOP_FLAG_PATH = os.path.join("data", "scraper_stop.flag")

COMPETITORS_FILE = "data/competitors_list.json"
# مرجع متجرنا (للعرض — لا يُكشط كمنافس من هذه القائمة)
PRIMARY_STORE_SITEMAP = "https://mahwous.com/sitemap.xml"
PRIMARY_STORE_LABEL = "مهووس — متجرنا"


def load_competitors():
    if not os.path.exists('data'):
        os.makedirs('data')
    if not os.path.exists(COMPETITORS_FILE):
        with open(COMPETITORS_FILE, 'w', encoding='utf-8') as f:
            json.dump([], f)
        return []
    try:
        with open(COMPETITORS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError:
        return []


def save_competitors(competitors_list):
    if not os.path.exists('data'):
        os.makedirs('data')
    with open(COMPETITORS_FILE, 'w', encoding='utf-8') as f:
        json.dump(competitors_list, f, ensure_ascii=False, indent=4)


def render_competitor_management_ui():
    st.markdown("## 🏢 إدارة روابط المنافسين (Sitemaps)")
    with st.expander("ℹ️ تعليمات الإضافة", expanded=False):
        st.info(
            "أدخل **رابط المتجر** (مثل `https://mahwous.com/`) أو **رابط Sitemap مباشر**. "
            "التطبيق يستنتج تلقائياً ملف الـ sitemap الصحيح من `robots.txt` أو المسارات الشائعة."
        )

    competitors = load_competitors()
    # تطبيع العناصر (قد تكون نصوص sitemap أو dict من قائمة المنافسين الموسعة)
    normalized_rows = []
    normalized_urls = []
    for i, item in enumerate(competitors):
        if isinstance(item, dict):
            name = str(item.get("name", f"Competitor {i+1}") or f"Competitor {i+1}")
            domain = str(item.get("domain", "") or "").strip()
            url = domain
            if domain and not domain.endswith("/"):
                domain += "/"
            if domain and "sitemap" not in domain.lower():
                url = f"{domain}sitemap.xml"
            normalized_rows.append({"name": name, "domain": str(item.get("domain", "") or ""), "sitemap": url})
            normalized_urls.append(url)
        else:
            url = str(item or "").strip()
            normalized_rows.append({"name": f"Competitor {i+1}", "domain": "", "sitemap": url})
            normalized_urls.append(url)

    with st.form("add_competitor_form", clear_on_submit=True):
        col1, col2 = st.columns([3, 1])
        with col1:
            new_url = st.text_input(
                "رابط المتجر أو Sitemap:",
                placeholder="https://example.com/",
            )
        with col2:
            st.write("")
            st.write("")
            submitted = st.form_submit_button("➕ إضافة", use_container_width=True)

        if submitted:
            if not (new_url and new_url.strip()):
                st.error("الرجاء إدخال رابط.")
            else:
                if is_main_store_domain(new_url.strip()):
                    st.error(
                        f"❌ لا يمكن إضافة `{MAIN_STORE_NAME}` ({MAIN_STORE_DOMAIN}) كمنافس. "
                        "هو baseline الأساسي للمقارنة."
                    )
                    st.stop()
                resolved, msg = resolve_store_to_sitemap_url(new_url.strip())
                if not resolved:
                    st.error(msg)
                elif resolved in normalized_urls:
                    st.warning("هذا الرابط (مُسنّداً) مضاف مسبقاً.")
                else:
                    competitors.append(resolved)  # keep legacy-compatible format for scraper
                    save_competitors(competitors)
                    st.success(f"تمت الإضافة بنجاح! {msg}")
                    st.rerun()

    with st.expander(f"📋 قائمة المنافسين الحالية (منسدلة) — {len(competitors)}", expanded=False):
        if not competitors:
            st.warning("لم تقم بإضافة أي منافسين بعد. ابدأ بإضافة 7 منافسين كاختبار.")
        else:
            df_view = pd.DataFrame(normalized_rows)
            # ترتيب أعمدة العرض ليكون ثابتاً وواضحاً
            keep_cols = [c for c in ["name", "domain", "sitemap"] if c in df_view.columns]
            df_view = df_view[keep_cols] if keep_cols else df_view
            st.dataframe(df_view, use_container_width=True, hide_index=True)
            st.markdown("#### 🗑️ حذف منافس")
            options = [f"{r['name']} — {r['sitemap']}" for r in normalized_rows]
            pick = st.selectbox(
                "اختر المنافس للحذف:",
                options=options,
                index=0,
                key="competitor_delete_pick",
            )
            del_idx = options.index(pick) if options else -1
            if st.button("🗑️ حذف المحدد", key="competitor_delete_btn", use_container_width=True):
                if del_idx >= 0:
                    competitors.pop(del_idx)
                    save_competitors(competitors)
                    st.success("تم الحذف بنجاح.")
                    st.rerun()

    # لا نعرض القائمة خارج الـ expander لتفادي أي مخرجات خام/متكررة.


def render_competitor_scrape_page():  # noqa: C901
    """صفحة كاملة: إدارة روابط المنافسين + كشط مع حفظ وعرض تدريجي."""
    st.header("🏢 كشط المنافسين")
    st.caption(
        f"**{PRIMARY_STORE_LABEL}** = مرجع ملف منتجاتك (رفع من «📂 رفع الملفات») — "
        f"Sitemap المرجعي: `{PRIMARY_STORE_SITEMAP}`. أدناه **روابط كشط المنافسين فقط**."
    )
    st.success(
        "المنافسون المقترحون: **عالم جيفنشي** · **خبير العطور** · **سارا ميك أب** — "
        "يُكشطون إلى `competitors_latest.csv` لاستخدامها في المقارنة و**بطاقات VS** في الأقسام."
    )

    render_competitor_management_ui()

    st.markdown("---")
    st.subheader("🤖 تشغيل محرك الكشط وعرض النتائج")
    st.info(
        "يُجلب أحدث أسعار المنافسين من روابط الـ Sitemap أعلاه. **الكشط يعمل في الخلفية**؛ "
        "سترى تقدّم الطلبات وعدد الصفوف المحفوظة أثناء العمل، ثم الملخص عند الانتهاء."
    )

    prog_running = False
    prog: dict = {}
    if os.path.exists(_SCRAPER_PROGRESS):
        try:
            with open(_SCRAPER_PROGRESS, "r", encoding="utf-8") as _pf:
                prog = json.load(_pf)
            prog_running = bool(prog.get("running"))
        except Exception:
            pass

    if prog_running:
        try:
            from streamlit_autorefresh import st_autorefresh

            st_autorefresh(interval=4000, key="competitor_scrape_autorefresh")
        except ImportError:
            pass
        st.warning("⏳ جاري سحب البيانات… يُحدَّث العرض كل بضع ثوانٍ حتى يكتمل.")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric(
                "تقدّم الطلبات",
                f"{prog.get('urls_processed', 0):,} / {max(prog.get('urls_total', 0), 1):,}",
            )
        with c2:
            st.metric("صفوف محفوظة في CSV", f"{prog.get('rows_in_csv', 0):,}")
        with c3:
            st.caption(f"Sitemap: `{prog.get('current_sitemap', '—')}`")

    # ── مراقبة + إيقاف تلقائي عند التعطل ─────────────────────────────────
    with st.expander("🛑 مراقبة الكشط (Live) + إيقاف إذا خرب", expanded=False):
        stop_flag = os.path.exists(STOP_FLAG_PATH)
        phase = str(prog.get("phase", "process") or "process").lower()

        auto_stop_if_stalled = st.checkbox(
            "إيقاف تلقائي إذا توقف التقدم (بدون تراكم/تعليق)",
            value=False,
        )
        stall_minutes = st.number_input(
            "بعد كم دقيقة بدون تقدم يعتبر توقف",
            min_value=1,
            max_value=120,
            value=60,
            step=1,
        )

        if st.button("🛑 إيقاف الآن", use_container_width=True, disabled=stop_flag):
            os.makedirs("data", exist_ok=True)
            with open(STOP_FLAG_PATH, "w", encoding="utf-8") as f:
                f.write(str(datetime.now(timezone.utc).isoformat()))
            st.success("✅ تم طلب إيقاف الخدمة. انتظر 10-30 ثانية.")
            st.rerun()

        if stop_flag:
            st.error("🛑 تم ضبط `scraper_stop.flag` — الخدمة ستتوقف بعد اكتمال الدورة الحالية.")
        else:
            st.success("✅ الخدمة تعمل (إذا كان الكشط متاحًا في الخلفية).")

        current_processed = int(prog.get("urls_processed", 0) or 0)
        now_ts = time.time()

        last_processed = st.session_state.get("scraper_last_urls_processed", None)
        last_change_time = st.session_state.get("scraper_last_processed_change_at", None)
        stalled = False

        if last_processed is None:
            st.session_state["scraper_last_urls_processed"] = current_processed
            st.session_state["scraper_last_processed_change_at"] = now_ts
        else:
            if current_processed != last_processed:
                st.session_state["scraper_last_urls_processed"] = current_processed
                st.session_state["scraper_last_processed_change_at"] = now_ts
            else:
                if last_change_time is not None and (now_ts - last_change_time) > stall_minutes * 60:
                    stalled = True

        # أثناء مرحلة جلب الـ sitemap (`phase=sync`) قد لا يتحرك `urls_processed`.
        # لذلك نتجنب اعتبارها "تعطل" حتى لا نوقف الخدمة خطأ.
        if phase != "process":
            stalled = False

        if stalled and auto_stop_if_stalled and not stop_flag:
            os.makedirs("data", exist_ok=True)
            with open(STOP_FLAG_PATH, "w", encoding="utf-8") as f:
                f.write(str(datetime.now(timezone.utc).isoformat()))
            st.error("🛑 تم الإيقاف تلقائياً: لا يوجد تقدم خلال الفترة المحددة.")
            st.rerun()

        colh1, colh2, colh3 = st.columns(3)
        with colh1:
            st.metric("urls_processed", f"{current_processed:,}")
        with colh2:
            st.metric("stalled", "نعم" if stalled else "لا")
        with colh3:
            st.metric("stall_limit", f"{stall_minutes} دقيقة")

        # عرض عدّادات الطابور كـ جدول سريع
        q_df = pd.DataFrame()
        try:
            db_path = os.path.join(os.getcwd(), "data", "scraper_state.db")
            if os.path.exists(db_path):
                conn = sqlite3.connect(db_path)
                q_rows = conn.execute(
                    "SELECT status, COUNT(*) AS cnt FROM url_queue GROUP BY status"
                ).fetchall()
                conn.close()
                q_df = pd.DataFrame(q_rows, columns=["status", "count"])
        except Exception:
            q_df = pd.DataFrame()
        if not q_df.empty:
            st.dataframe(q_df, use_container_width=True, hide_index=True)

    # تحقق من وجود كتالوج مهووس قبل السماح بالكشط
    our_df = getattr(st.session_state, "our_df", None)
    has_store = isinstance(our_df, pd.DataFrame) and not our_df.empty
    if not has_store:
        st.error("⚠️ يرجى رفع ملف متجر مهووس الأساسي أولاً لتتم المقارنة التلقائية.")

    col_btn, col_live = st.columns([1, 2])
    with col_btn:
        start_disabled = prog_running or not has_store or os.path.exists(STOP_FLAG_PATH)
        if st.button(
            "🚀 بدء جلب بيانات المنافسين الآن",
            use_container_width=True,
            disabled=start_disabled,
            key="btn_start_scrape_page",
        ):
            # تشغيل الكاشط في عملية خلفية غير حاجبة
            cmd = [sys.executable, os.path.join(os.getcwd(), "run_background_worker.py")]
            try:
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                st.session_state["scraper_pid"] = proc.pid
            except Exception as e:
                st.error(f"تعذر تشغيل الكاشط الخلفي: {e}")
            else:
                st.success("✅ تم تشغيل محرك الكشط في الخلفية. سيتم تحديث النتائج تلقائياً أدناه.")

    # عرض مباشر للمنتجات الجديدة/المفقودة مقارنة بكتالوج مهووس
    live_placeholder = col_live.empty()
    status_placeholder = col_live.empty()

    def _live_auto_compare_once() -> None:
        data_path = os.path.join(os.getcwd(), "data", "competitors_latest.csv")
        if not (has_store and os.path.exists(data_path)):
            return
        try:
            df_comp = pd.read_csv(data_path)
        except Exception:
            return
        if df_comp.empty:
            return

        d = df_comp.copy()
        # توحيد الأعمدة
        col_map = {
            "name": "الاسم",
            "price": "السعر",
            "brand": "الماركة",
            "image_url": "رابط_الصورة",
            "comp_image_url": "رابط_الصورة",
            "comp_url": "رابط_المنتج",
            "url": "رابط_المنتج",
        }
        for en, ar in col_map.items():
            if en in d.columns and ar not in d.columns:
                d[ar] = d[en]
        if "sku" not in d.columns:
            d["sku"] = ""

        # مجموعات كتالوج مهووس (SKU + name)
        our = our_df.copy() if has_store else pd.DataFrame()
        _our_sku_col = next(
            (c for c in ("sku", "رقم المنتج", "رمز المنتج sku", "product_id") if c in our.columns),
            None,
        )
        _our_name_col = next(
            (c for c in ("name", "اسم المنتج", "أسم المنتج", "product_name") if c in our.columns),
            None,
        )
        our_skus = set(
            our.get(_our_sku_col, pd.Series(dtype=str)).astype(str).str.strip().tolist()
        )
        our_names = set(
            our.get(_our_name_col, pd.Series(dtype=str)).astype(str).str.strip().tolist()
        )

        d["__sku"] = d["sku"].astype(str).str.strip()
        d["__name"] = d["الاسم"].astype(str).str.strip()
        missing_mask = (~d["__sku"].isin(our_skus)) & (~d["__name"].isin(our_names))
        missing_df = d.loc[missing_mask].copy()
        if missing_df.empty:
            status_placeholder.info("لا توجد منتجات مفقودة جديدة حالياً مقارنة بكتالوج مهووس.")
            return

        table_cols = ["الاسم", "السعر", "الماركة", "رابط_الصورة", "رابط_المنتج", "sku"]
        for c in table_cols:
            if c not in missing_df.columns:
                missing_df[c] = ""
        missing_df = missing_df[table_cols]
        status_placeholder.success(
            f"🔍 منتجات مفقودة/جديدة حالياً مقارنة بكتالوج مهووس: {len(missing_df)}"
        )
        live_placeholder.dataframe(
            missing_df,
            use_container_width=True,
            hide_index=True,
        )

    # Polling خفيف كل بضع ثواني عند فتح الصفحة (بدون حظر واجهة المستخدم كلياً)
    # ملاحظة: Streamlit سيعيد تشغيل الكود عند أي تفاعل، لذلك يكفي تشغيل المراقبة مرة واحدة لكل تحميل.
    if has_store:
        _live_auto_compare_once()

    meta_path = os.path.join(os.getcwd(), "data", "scraper_last_run.json")
    if os.path.exists(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as _mf:
                sm = json.load(_mf)
            st.markdown("### 📈 ملخص أداء آخر كشط")
            st.caption(
                f"آخر تحديث (UTC): `{sm.get('finished_at', '—')}` · الحالة: **{sm.get('status', '—')}**"
            )
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.metric("روابط في الطابور", f"{sm.get('urls_queued', 0):,}")
            with c2:
                st.metric("صفوف في CSV", f"{sm.get('rows_written_csv', 0):,}")
            with c3:
                st.metric("نسبة النجاح", f"{sm.get('success_rate_pct', 0.0):.1f}%")
            with c4:
                st.metric("المدة (ث)", f"{sm.get('duration_seconds', 0):.1f}")
            c5, c6, c7 = st.columns(3)
            with c5:
                st.metric("قبل إزالة التكرار", f"{sm.get('rows_extracted_before_dedupe', 0):,}")
            with c6:
                st.metric("طلبات فاشلة (استثناء)", f"{sm.get('fetch_exceptions', 0):,}")
            with c7:
                st.metric("بدون استخراج (فراغ)", f"{sm.get('parse_null', 0):,}")
            diag = sm.get("sitemap_diagnostics") or []
            if diag:
                with st.expander("🔎 تشخيص روابط الـ Sitemap (حالة HTTP وأخطاء الجلب)", expanded=False):
                    st.dataframe(pd.DataFrame(diag), use_container_width=True, hide_index=True)
                    st.caption(
                        "إذا ظهرت حالة **410 Gone** أو **404** فالرابط لم يعد متاحاً على الخادم — "
                        "استبدله برابط sitemap حديث من المتجر (أو من لوحة تحكم سلة/زد)."
                    )
        except Exception:
            pass

    st.markdown("### 📊 البيانات المسحوبة من المنافسين")
    data_path = os.path.join(os.getcwd(), "data", "competitors_latest.csv")
    if os.path.exists(data_path):
        try:
            df_comp = pd.read_csv(data_path)
            if df_comp.empty:
                st.warning(
                    "⚠️ الملف موجود لكنه فارغ. تحقق من الـ Sitemap أو انتظر أول دفعة بعد بدء الكشط."
                )
            else:
                st.success(
                    f"✅ **{len(df_comp)}** صف محفوظ — يُحدَّث أثناء الكشط إن كان يعملاً."
                )
                # توحيد الأعمدة لعرض جدولي واضح بالصيغة العربية المطلوبة
                d = df_comp.copy()
                col_map = {
                    "name": "الاسم",
                    "price": "السعر",
                    "brand": "الماركة",
                    "image_url": "رابط_الصورة",
                    "comp_image_url": "رابط_الصورة",
                    "comp_url": "رابط_المنتج",
                    "url": "رابط_المنتج",
                }
                for en, ar in col_map.items():
                    if en in d.columns and ar not in d.columns:
                        d[ar] = d[en]
                if "sku" not in d.columns:
                    d["sku"] = ""

                table_cols = ["الاسم", "السعر", "الماركة", "رابط_الصورة", "رابط_المنتج", "sku"]
                for c in table_cols:
                    if c not in d.columns:
                        d[c] = ""
                d = d[table_cols]
                st.dataframe(d, use_container_width=True, height=400, hide_index=True)
                st.download_button(
                    "📥 تنزيل CSV",
                    data=df_comp.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                    file_name="competitors_latest.csv",
                    mime="text/csv",
                    key="dl_competitors_csv_page",
                )
        except Exception as e:
            st.error(f"❌ حدث خطأ في قراءة ملف البيانات: {str(e)}")
    else:
        st.info(
            "لا يوجد ملف بعد. اضغط **بدء جلب** أعلاه — سيُنشأ `competitors_latest.csv` ويُملأ تدريجياً."
        )
