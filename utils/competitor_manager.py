import json
import os
import threading
import subprocess
import sys
import time
import sqlite3
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

from config import MAIN_STORE_DOMAIN, MAIN_STORE_NAME, is_main_store_domain
from utils.scrape_live_cards import (
    BUCKET_META,
    bucket_final_priced_df,
    competitor_missing_vs_our_catalog,
    summarize_buckets,
    trim_for_display,
)
from utils.sitemap_resolve import resolve_store_to_sitemap_url

_SCRAPER_PROGRESS = os.path.join("data", "scraper_progress.json")
STOP_FLAG_PATH = os.path.join("data", "scraper_stop.flag")

COMPETITORS_FILE = "data/competitors_list.json"
# مرجع متجرنا (للعرض — لا يُكشط كمنافس من هذه القائمة)
PRIMARY_STORE_SITEMAP = "https://mahwous.com/sitemap.xml"
PRIMARY_STORE_LABEL = "مهووس — متجرنا"


def _parse_progress_started_at(prog: dict):
    raw = prog.get("started_at") if isinstance(prog, dict) else None
    if not raw:
        return None
    try:
        s = str(raw).replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _progress_looks_stuck(prog: dict) -> bool:
    """جلسة كشط توقفت دون تحديث الملف (إغلاق عملية، تعطل، إلخ)."""
    if not isinstance(prog, dict) or not prog.get("running"):
        return False
    started = _parse_progress_started_at(prog)
    if started is None:
        return True
    age = datetime.now(timezone.utc) - started
    if age.total_seconds() > 6 * 3600:
        return True
    if age.total_seconds() > 45 * 60:
        if int(prog.get("urls_processed") or 0) == 0 and int(prog.get("urls_total") or 0) == 0:
            return True
    return False


def _product_state_row_count() -> int | None:
    """عدد صفوف المنتجات الناجحة في SQLite (أدق من JSON أحياناً)."""
    try:
        db_path = os.path.join(os.getcwd(), "data", "scraper_state.db")
        if not os.path.exists(db_path):
            return None
        conn = sqlite3.connect(db_path)
        r = conn.execute("SELECT COUNT(1) FROM product_state").fetchone()
        conn.close()
        return int(r[0]) if r else 0
    except Exception:
        return None


def _live_queue_counts_from_db() -> dict | None:
    """عدادات الطابور الحية من SQLite (أدق من JSON أثناء تنفيذ دفعة طويلة)."""
    try:
        db_path = os.path.join(os.getcwd(), "data", "scraper_state.db")
        if not os.path.exists(db_path):
            return None
        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            "SELECT status, COUNT(1) FROM url_queue GROUP BY status"
        ).fetchall()
        conn.close()
        q = {"pending": 0, "completed": 0, "failed": 0}
        for st, cnt in rows:
            if st in q:
                q[st] = int(cnt)
        total = q["pending"] + q["completed"] + q["failed"]
        handled = q["completed"] + q["failed"]
        return {
            "urls_total": total,
            "urls_processed": handled,
            "urls_completed": q["completed"],
            "urls_failed": q["failed"],
            "urls_pending": q["pending"],
        }
    except Exception:
        return None


def _reset_scraper_progress_and_stop_flag() -> None:
    os.makedirs("data", exist_ok=True)
    payload = {
        "running": False,
        "started_at": None,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "last_error": None,
        "urls_total": 0,
        "urls_processed": 0,
        "rows_in_csv": 0,
        "current_sitemap": None,
        "mode": "idle",
    }
    try:
        with open(_SCRAPER_PROGRESS, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=4)
    except Exception:
        pass
    try:
        if os.path.exists(STOP_FLAG_PATH):
            os.remove(STOP_FLAG_PATH)
    except Exception:
        pass


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
    st.caption(
        "**مراقب الواجهة:** التحديث الفوري هنا يعتمد على ملف `data/scraper_progress.json` + "
        "`streamlit-autorefresh` (بدون WebSockets). للمزامنة الحقيقية عبر الشبكة يمكن لاحقاً "
        "إضافة WebSocket أو Firebase كمكوّن منفصل."
    )

    prog_running = False
    prog: dict = {}
    progress_stuck = False
    stop_flag_present = os.path.exists(STOP_FLAG_PATH)
    if os.path.exists(_SCRAPER_PROGRESS):
        try:
            with open(_SCRAPER_PROGRESS, "r", encoding="utf-8") as _pf:
                prog = json.load(_pf)
            progress_stuck = _progress_looks_stuck(prog)
            prog_running = bool(prog.get("running")) and not progress_stuck
        except Exception:
            pass

    user_stopped_reported = (
        str(prog.get("mode", "")).lower() == "stopped_by_flag"
        or str(prog.get("last_error", "")) == "stopped_by_user_flag"
    )

    if progress_stuck:
        st.warning(
            "⚠️ **حالة كشط عالقة** من جلسة سابقة (العملية انتهت دون تحديث الملف). "
            "اضغط الزر ثم أعد «بدء جلب بيانات المنافسين»."
        )
        if st.button("🔄 إصلاح الحالة العالقة", key="btn_reset_stuck_scraper"):
            _reset_scraper_progress_and_stop_flag()
            st.success("تمت إعادة التعيين.")
            st.rerun()

    if prog_running:
        try:
            from streamlit_autorefresh import st_autorefresh

            st_autorefresh(interval=2500, key="competitor_scrape_autorefresh")
        except ImportError:
            pass
        st.warning(
            "⏳ جاري سحب البيانات… يُحدَّث العرض كل ~2.5 ثانية. "
            "**أول دفعة** قد تستغرق عدة دقائق (تأخير بين الطلبات + مهلة الشبكة)."
        )
        if stop_flag_present:
            st.error(
                "🛑 **طلب إيقاف مفعّل** (`scraper_stop.flag`) — ستتوقف الخدمة بعد انتهاء الدفعة الحالية."
            )
        live_q = _live_queue_counts_from_db()
        disp = live_q if live_q else {
            "urls_total": max(int(prog.get("urls_total", 0) or 0), 1),
            "urls_processed": int(prog.get("urls_processed", 0) or 0),
            "urls_completed": int(prog.get("urls_completed", 0) or 0),
            "urls_failed": int(prog.get("urls_failed", 0) or 0),
            "urls_pending": int(prog.get("urls_pending", 0) or 0),
        }
        total_urls = max(int(disp["urls_total"]), 1)
        done_urls = min(int(disp["urls_processed"]), total_urls)
        frac = done_urls / total_urls
        st.progress(min(1.0, frac))
        st.caption(
            f"**تقدّم الطابور:** تمت محاولة **{done_urls:,} / {total_urls:,}** رابط "
            f"(≈ **{frac * 100:.1f}%**). "
            "**نجاح الاستخراج فقط** يضيف صفاً إلى `product_state` وCSV — الفشل (حظر/parse) لا يزيد الصفوف."
        )
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric(
                "روابط مُعالَجة (نجاح + فشل)",
                f"{disp['urls_processed']:,} / {max(disp['urls_total'], 1):,}",
            )
        with c2:
            _pc = _product_state_row_count()
            _rows_shown = _pc if _pc is not None else int(prog.get("rows_in_csv", 0) or 0)
            st.metric("صفوف ناجحة (SQLite → CSV)", f"{_rows_shown:,}")
        with c3:
            st.caption(
                f"✅ مكتمل: **{disp['urls_completed']:,}** · "
                f"❌ فشل: **{disp['urls_failed']:,}** · "
                f"⏳ بالانتظار: **{disp['urls_pending']:,}**"
            )
            st.caption(f"Sitemap: `{prog.get('current_sitemap', '—')}` · phase: `{prog.get('phase', '—')}`")
    elif user_stopped_reported or (stop_flag_present and not prog_running):
        st.error(
            "🛑 **الكشط متوقف** — إما بأمر المستخدم أو بملف الإيقاف. "
            "احذف `scraper_stop.flag` إن أردت المتابعة، ثم أعد «بدء جلب بيانات المنافسين»."
        )
    elif not progress_stuck and prog and not prog.get("running") and prog.get("finished_at"):
        st.success(
            "✅ **آخر دورة كشط انتهت** (حسب `scraper_progress.json`). "
            "يمكنك مراجعة الملخص والبطاقات أدناه."
        )

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

        _live = _live_queue_counts_from_db()
        current_processed = int(
            (_live or {}).get("urls_processed", prog.get("urls_processed", 0)) or 0
        )
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

    # كتالوج مهووس: الجلسة أولاً، ثم SQLite (our_catalog) إن وُجد
    our_df = getattr(st.session_state, "our_df", None)
    has_store = isinstance(our_df, pd.DataFrame) and not our_df.empty
    if not has_store:
        try:
            from utils.pricing_pipeline import _load_our_catalog_df

            _odb = _load_our_catalog_df()
            if _odb is not None and not _odb.empty:
                our_df = _odb
                has_store = True
                st.success(
                    "✅ **تُحمَّل منتجاتكم من قاعدة SQLite** (`our_catalog`) — "
                    "أقسام التحليل والمقارنة تعمل دون رفع ملف في هذه الجلسة."
                )
        except Exception:
            pass
    if not has_store:
        st.info(
            "ℹ️ **بلا كتالوج في الجلسة أو في القاعدة:** يمكنك تشغيل الكشط؛ "
            "لتفعيل **أقسام التسعير** (أعلى/أقل/موافق/مفقود) ارفع ملفاً من «📂 رفع الملفات» "
            "أو عُدْ بكتالوج سابق ليُملأ `our_catalog` في SQLite."
        )

    if os.path.exists(STOP_FLAG_PATH):
        st.warning(
            "⏹️ يوجد ملف **إيقاف الكشط** (`scraper_stop.flag`) — "
            "وضع **الخدمة المستمرة** يتوقف عند رؤيته. احذفه قبل أن يُكمِل الكشط دفعات جديدة."
        )
        if st.button("🗑️ حذف ملف الإيقاف الآن", key="btn_remove_stop_flag_only"):
            try:
                os.remove(STOP_FLAG_PATH)
                st.success("تم حذف `scraper_stop.flag`.")
                st.rerun()
            except OSError as e:
                st.error(f"تعذر الحذف: {e}")

    col_btn, col_live = st.columns([1, 2])
    with col_btn:
        start_disabled = prog_running
        if st.button(
            "🚀 بدء جلب بيانات المنافسين الآن",
            use_container_width=True,
            disabled=start_disabled,
            key="btn_start_scrape_page",
        ):
            try:
                if os.path.exists(STOP_FLAG_PATH):
                    os.remove(STOP_FLAG_PATH)
            except OSError as e:
                st.error(f"تعذر إزالة ملف الإيقاف: {e}")
                st.stop()
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

    st.markdown("### 📡 تدفق الكشط اللحظي (ذاكرة مشتركة)")
    st.caption(
        "كل منتج يُكشط بنجاح يُدفع هنا **قبل** اكتمال تصدير `competitors_latest.csv`. "
        "الفشل: سبب مختصر لكل رابط في الجدول — وفّر `SCRAPER_LOG_EACH_FAILURE=1` لتسجيل كل فشل في سجل النشاط أيضاً."
    )
    try:
        from streamlit import fragment as st_fragment
        from utils.scrape_live_buffer import snapshot_failures_for_ui, snapshot_products_for_ui

        @st_fragment(run_every=timedelta(seconds=2))
        def _scrape_buffer_fragment():
            prods = snapshot_products_for_ui(50)
            fails = snapshot_failures_for_ui(40)
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**✅ آخر نجاحات (الذاكرة)**")
                if prods:
                    st.dataframe(pd.DataFrame(prods), use_container_width=True, hide_index=True)
                else:
                    st.caption("لا صفوف بعد — مع أول رابط ناجح يظهر الجدول فوراً.")
            with c2:
                st.markdown("**📛 آخر فشل + السبب**")
                if fails:
                    st.dataframe(pd.DataFrame(fails), use_container_width=True, hide_index=True)
                else:
                    st.caption("—")

        _scrape_buffer_fragment()
    except Exception:
        try:
            from utils.scrape_live_buffer import snapshot_failures_for_ui, snapshot_products_for_ui

            prods = snapshot_products_for_ui(50)
            fails = snapshot_failures_for_ui(40)
            st.dataframe(
                pd.DataFrame(prods) if prods else pd.DataFrame({"msg": ["لا بيانات"]}),
                use_container_width=True,
                hide_index=True,
            )
            st.dataframe(
                pd.DataFrame(fails) if fails else pd.DataFrame({"msg": ["لا فشول مسجلة"]}),
                use_container_width=True,
                hide_index=True,
            )
        except Exception:
            st.caption("تعذّر تحميل مخزن الكشط الحي.")

    db_path_q = os.path.join(os.getcwd(), "data", "scraper_state.db")
    if os.path.isfile(db_path_q):
        with st.expander("🔎 آخر فشل من قاعدة الطابور (url_queue)", expanded=False):
            try:
                conn = sqlite3.connect(db_path_q)
                fail_rows = conn.execute(
                    """
                    SELECT url, last_error, attempt_count, updated_at
                    FROM url_queue
                    WHERE status='failed' AND IFNULL(last_error,'') != ''
                    ORDER BY updated_at DESC
                    LIMIT 80
                    """
                ).fetchall()
                conn.close()
                if fail_rows:
                    st.dataframe(
                        pd.DataFrame(
                            fail_rows,
                            columns=["url", "last_error", "attempt_count", "updated_at"],
                        ),
                        use_container_width=True,
                        hide_index=True,
                    )
                else:
                    st.caption("لا صفوف فاشلة مع سبب مخزّن حالياً.")
            except Exception as e:
                st.caption(f"تعذّر القراءة: {e}")

    st.markdown("---")
    st.subheader("🧭 تدفق البيانات وبطاقات التحليل")
    st.markdown(
        """
| المرحلة | الوصف |
|---------|--------|
| **1. حفظ مباشر** | كل رابط يُعالَج يُخزَّن في SQLite (`product_state` / `url_queue`) ثم يُصدَّر إلى `competitors_latest.csv`. |
| **2. Gemini + المطابقة** | بعد دُفعات التحديث يُشغَّل خط التسعير التلقائي: `SmartMatcher` + `GeminiMatchVerifier` للحالات الضبابية + محرك التسعير. |
| **3. تصنيف البطاقات** | يُعاد تجميع صفوف `final_priced_latest.csv` حسب السعر والثقة (الجدول أدناه). |
"""
    )
    final_priced_path = os.path.join(os.getcwd(), "data", "final_priced_latest.csv")
    final_meta_path = os.path.join(os.getcwd(), "data", "final_priced_latest_meta.json")
    meta_txt = ""
    if os.path.isfile(final_meta_path):
        try:
            with open(final_meta_path, "r", encoding="utf-8") as _mf:
                _fm = json.load(_mf)
            meta_txt = (
                f"آخر توليد (UTC): `{_fm.get('generated_at_utc', '—')}` · "
                f"صفوف: **{_fm.get('rows', 0)}** · السبب: `{_fm.get('reason', '—')}`"
            )
        except Exception:
            pass
    if meta_txt:
        st.caption(meta_txt)

    if os.path.isfile(final_priced_path):
        try:
            df_fp = pd.read_csv(final_priced_path, encoding="utf-8-sig")
        except Exception as e:
            st.warning(f"تعذّر قراءة نتيجة التحليل: {e}")
            df_fp = pd.DataFrame()
        if df_fp is not None and not df_fp.empty:
            buckets = bucket_final_priced_df(df_fp)
            counts = summarize_buckets(buckets)
            comp_path_quick = os.path.join(os.getcwd(), "data", "competitors_latest.csv")
            if os.path.isfile(comp_path_quick):
                try:
                    _ncomp = len(pd.read_csv(comp_path_quick, encoding="utf-8-sig", usecols=[0]))
                except Exception:
                    _ncomp = 0
                if _ncomp > max(len(df_fp), 1) * 2:
                    st.caption(
                        f"💡 **أقسام التسعير** تعرض صفوف **المطابَكة** فقط ({len(df_fp)} صف في `final_priced_latest.csv`)، "
                        f"بينما كشط المنافس يضم **~{_ncomp}** صفاً — الباقي في الجدول «البيانات المسحوبة» أسفل الصفحة."
                    )
            bcols = st.columns(5)
            bucket_order = ["higher", "lower", "ok", "missing", "review"]
            short_lbl = {
                "higher": "🔴 أعلى",
                "lower": "🟢 أقل",
                "ok": "✅ سليم",
                "missing": "🔍 مفقود",
                "review": "⚠️ مراجعة",
            }
            for i, bid in enumerate(bucket_order):
                _title, hint, _color = BUCKET_META[bid]
                with bcols[i]:
                    st.metric(short_lbl[bid], f"{counts.get(bid, 0):,}")
                    st.caption(hint[:72] + "…" if len(hint) > 72 else hint)
            tab_labels = [BUCKET_META[b][0] for b in bucket_order]
            tabs = st.tabs(tab_labels)
            for tab, bid in zip(tabs, bucket_order):
                with tab:
                    title, desc, _c = BUCKET_META[bid]
                    st.markdown(f"**{title}** — {desc}")
                    show = trim_for_display(buckets.get(bid, pd.DataFrame()))
                    if show.empty:
                        st.info(
                            "لا صفوف في هذا القسم — إما لا توجد مطابقة بهذه الحالة، "
                            "أو خط التسعير لم يُحدّث بعد. راجع الجدول «البيانات المسحوبة من المنافسين» أسفل الصفحة."
                        )
                    else:
                        st.dataframe(show, use_container_width=True, hide_index=True)
            if has_store and os.path.isfile(os.path.join(os.getcwd(), "data", "competitors_latest.csv")):
                try:
                    df_c = pd.read_csv(os.path.join(os.getcwd(), "data", "competitors_latest.csv"), encoding="utf-8-sig")
                    csv_miss = competitor_missing_vs_our_catalog(df_c, our_df)
                except Exception:
                    csv_miss = pd.DataFrame()
                if csv_miss is not None and not csv_miss.empty:
                    with st.expander("🔍 تقدير سريع: مفقودات من CSV الكاشط مقابل كتالوج الجلسة", expanded=False):
                        st.caption(
                            "يستكمل قسم «منتجات مفقودة» في خط التسعير؛ مفيد قبل اكتمال تشغيل Gemini."
                        )
                        st.dataframe(csv_miss, use_container_width=True, hide_index=True)
        else:
            st.info("ملف `final_priced_latest.csv` فارغ — انتظر اكتمال دفعة كشط ثم خط التسعير التلقائي.")
    else:
        st.warning(
            "لا يوجد `data/final_priced_latest.csv` بعد. "
            "يُنشأ تلقائياً عند نجاح **خط التسعير الخلفي** بعد تحديثات الكشط (مع فترة تهدئة ~دقيقتين بين التشغيلات)."
        )

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
                _sm_rows = _product_state_row_count()
                st.metric(
                    "صفوف في CSV",
                    f"{_sm_rows if _sm_rows is not None else int(sm.get('rows_written_csv', 0) or 0):,}",
                )
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
