"""
مكوّنات العرض الحية لأقسام التسعير (بدون تبويبات داخل الصفحة — التنقل عبر الشريط الجانبي فقط).

ربط أسماء الأقسام في config → حاويات SQLite في live_price_store:
- 🔴 سعر أعلى (سعرنا أعلى من المنافس) ↔ bucket «lower» (سعر المنافس أقل)
- 🟢 سعر أقل (سعرنا أقل) ↔ bucket «higher»
- ✅ موافق عليها ↔ «ok»
- 🔍 منتجات مفقودة ↔ «missing»
- ⚠️ تحت المراجعة ↔ «review»
"""
from __future__ import annotations

import datetime
import html
from typing import Callable, Dict, List, Optional

import streamlit as st

from utils.live_price_store import (
    count_by_bucket,
    get_cards_for_bucket,
    get_recent_logs,
    init_live_db,
    sync_from_final_priced_csv,
)
from utils.scrape_live_buffer import merge_cards_for_bucket, pricing_preview_age_seconds
from utils.live_price_worker import (
    is_watcher_alive,
    start_live_file_watcher,
    stop_live_file_watcher,
)
from utils.ui_components import DEFAULT_PLACEHOLDER_IMAGE

# أقسام يُفعَّل لها الشريط الجانبي + الـ fragments
LIVE_RESULT_PAGES = frozenset(
    {
        "🔴 سعر أعلى",
        "🟢 سعر أقل",
        "✅ موافق عليها",
        "🔍 منتجات مفقودة",
        "⚠️ تحت المراجعة",
    }
)

PAGE_TO_BUCKET: Dict[str, str] = {
    "🔴 سعر أعلى": "lower",
    "🟢 سعر أقل": "higher",
    "✅ موافق عليها": "ok",
    "🔍 منتجات مفقودة": "missing",
    "⚠️ تحت المراجعة": "review",
}

_FALLBACK_IMG = (
    "data:image/png;base64,"
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
)

_LIVE_CSS = """
<style>
.lp-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 14px;
  align-items: stretch;
}
.lp-card {
  background: linear-gradient(160deg, #1e293b 0%, #0f172a 100%);
  border: 1px solid #334155;
  border-radius: 14px;
  padding: 12px;
  box-shadow: 0 4px 14px rgba(0,0,0,0.25);
  transition: border-color 0.2s ease, transform 0.15s ease;
}
.lp-card:hover {
  border-color: #64748b;
  transform: translateY(-1px);
}
.lp-thumbs {
  display: flex;
  gap: 8px;
  margin-bottom: 10px;
  justify-content: space-between;
}
.lp-img {
  width: 96px;
  height: 96px;
  object-fit: cover;
  border-radius: 10px;
  border: 1px solid #475569;
  background: #0b1220;
}
.lp-title {
  color: #f1f5f9;
  font-weight: 600;
  font-size: 0.95rem;
  line-height: 1.35;
  min-height: 2.6em;
  margin-bottom: 6px;
}
.lp-meta {
  color: #cbd5e1;
  font-size: 0.8rem;
  line-height: 1.45;
}
.lp-price-our { color: #38bdf8; font-weight: 700; }
.lp-price-comp { color: #fbbf24; font-weight: 700; }
.lp-badge {
  display: inline-block;
  margin-top: 6px;
  padding: 2px 8px;
  border-radius: 999px;
  font-size: 0.72rem;
  background: #334155;
  color: #e2e8f0;
}
.lp-log-box {
  max-height: 200px;
  overflow-y: auto;
  font-size: 0.76rem;
  background: #0f172a;
  border: 1px solid #334155;
  border-radius: 8px;
  padding: 8px;
  color: #cbd5e1;
  line-height: 1.35;
}
</style>
"""


def _esc(s: str) -> str:
    return html.escape(str(s or ""), quote=True)


def _fmt_money(v: float) -> str:
    try:
        return f"{float(v):,.2f}"
    except Exception:
        return "—"


def _cards_html(rows: List[dict]) -> str:
    if not rows:
        return '<p class="lp-meta">لا توجد بطاقات في هذا القسم بعد — نفّذ مزامنة CSV أو انتظر خط التسعير التلقائي.</p>'
    ph = _esc(DEFAULT_PLACEHOLDER_IMAGE)
    fb = _esc(_FALLBACK_IMG)
    parts = ['<div class="lp-grid">']
    for r in rows:
        i1 = str(r.get("image_our") or "").strip()
        i2 = str(r.get("image_comp") or "").strip()
        src1 = _esc(i1) if i1 else ph
        src2 = _esc(i2) if i2 else ph
        title = _esc(r.get("product_name") or r.get("comp_name") or "—")
        cname = _esc(r.get("comp_name") or "")
        curl = _esc(r.get("comp_url") or "#")
        ms = r.get("match_score")
        ms_txt = f"{float(ms):.0f}%" if ms is not None else "—"
        ai = _esc(r.get("ai_state") or "")
        parts.append(
            f"""
<div class="lp-card">
  <div class="lp-thumbs">
    <img class="lp-img" src="{src1}" loading="lazy" alt="متجرنا"
         onerror="this.onerror=null;this.src='{fb}';" />
    <img class="lp-img" src="{src2}" loading="lazy" alt="المنافس"
         onerror="this.onerror=null;this.src='{fb}';" />
  </div>
  <div class="lp-title">{title}</div>
  <div class="lp-meta">
    <span class="lp-price-our">سعرنا: {_fmt_money(r.get('price', 0))}</span>
    &nbsp;·&nbsp;
    <span class="lp-price-comp">المنافس: {_fmt_money(r.get('comp_price', 0))}</span>
  </div>
  <div class="lp-meta">مطابقة: {ms_txt}</div>
  {f'<div class="lp-meta">منافس: {cname}</div>' if cname else ''}
  <a class="lp-meta" href="{curl}" target="_blank" rel="noopener">رابط المنتج ←</a>
  {f'<span class="lp-badge">AI: {ai}</span>' if ai else ''}
</div>
"""
        )
    parts.append("</div>")
    return "\n".join(parts)


def _rolling_log_compact() -> None:
    st.markdown("##### 📜 سجل العمليات المتحرك")
    logs = get_recent_logs(28)
    if not logs:
        st.caption("لا إدخالات بعد.")
        return
    blocks = []
    for lg in logs[-28:]:
        ts = str(lg.get("ts", ""))[:19].replace("T", " ")
        msg = html.escape(str(lg.get("message", ""))[:200])
        blocks.append(
            f'<div style="margin-bottom:5px;border-bottom:1px solid #1e293b;padding-bottom:5px">'
            f'<span style="color:#64748b">{html.escape(ts)}</span><br/>{msg}</div>'
        )
    st.markdown(
        _LIVE_CSS + '<div class="lp-log-box">' + "\n".join(blocks) + "</div>",
        unsafe_allow_html=True,
    )


def _live_card_cap() -> int:
    return int(st.session_state.get("live_card_cap", 80))


def _live_bucket_body(bucket: str, page_label: str) -> None:
    init_live_db()
    st.markdown(_LIVE_CSS, unsafe_allow_html=True)
    n = count_by_bucket().get(bucket, 0)
    cap = _live_card_cap()
    age = pricing_preview_age_seconds()
    age_txt = f"معاينة ذاكرة التسعير: منذ **{age:.0f}** ث" if age >= 0 else "لا معاينة ذاكرة بعد"
    st.caption(
        f"**بطاقات حية** — {page_label} — SQLite: **{n}** | {age_txt}. "
        "تُدمج أولاً بطاقات **معاينة الذاكرة** (آخر تشغيل لخط التسعير) ثم قاعدة اللوحة."
    )
    sqlite_rows = get_cards_for_bucket(bucket, limit=cap)
    rows = merge_cards_for_bucket(bucket, sqlite_rows, cap)
    st.markdown(_cards_html(rows), unsafe_allow_html=True)
    st.markdown("---")
    _rolling_log_compact()


# خمس دوال fragment ثابتة الهوية (مطلوب لاستقرار Streamlit)
try:
    from streamlit import fragment as _st_fragment

    _frag_deco = _st_fragment(run_every=datetime.timedelta(seconds=1.5))
except Exception:
    _frag_deco = None

if _frag_deco is not None:

    @_frag_deco
    def _live_frag_raise():
        _live_bucket_body("lower", "🔴 سعر أعلى")

    @_frag_deco
    def _live_frag_lower():
        _live_bucket_body("higher", "🟢 سعر أقل")

    @_frag_deco
    def _live_frag_ok():
        _live_bucket_body("ok", "✅ موافق عليها")

    @_frag_deco
    def _live_frag_missing():
        _live_bucket_body("missing", "🔍 منتجات مفقودة")

    @_frag_deco
    def _live_frag_review():
        _live_bucket_body("review", "⚠️ تحت المراجعة")
else:
    _live_frag_raise = None  # type: ignore
    _live_frag_lower = None  # type: ignore
    _live_frag_ok = None  # type: ignore
    _live_frag_missing = None  # type: ignore
    _live_frag_review = None  # type: ignore


_FRAG_BY_PAGE: Dict[str, Callable[[], None]] = {}
if _frag_deco is not None:
    _FRAG_BY_PAGE = {
        "🔴 سعر أعلى": _live_frag_raise,
        "🟢 سعر أقل": _live_frag_lower,
        "✅ موافق عليها": _live_frag_ok,
        "🔍 منتجات مفقودة": _live_frag_missing,
        "⚠️ تحت المراجعة": _live_frag_review,
    }


def render_live_sidebar_controls() -> None:
    """يُستدعى من app.py داخل st.sidebar عندما page ∈ LIVE_RESULT_PAGES."""
    st.markdown("---")
    st.markdown("##### ⚡ لوحة حية (SQLite)")
    st.caption("مزامنة من `final_priced_latest.csv` + معاينة الذاكرة — تحديث البطاقات كل ~1.5 ث.")
    st.slider(
        "حد بطاقات العرض للقسم الحالي",
        min_value=20,
        max_value=200,
        value=80,
        key="live_card_cap",
    )
    if st.button("🔄 مزامنة الآن", width="stretch", key="live_sync_sidebar_btn"):
        if sync_from_final_priced_csv():
            st.success("تمت المزامنة.")
        else:
            st.warning("تعذّرت المزامنة — تحقق من الملف.")
        st.rerun()
    w = st.toggle(
        "مراقب ملف الخلفية (~5 ث)",
        key="live_watcher_sidebar",
    )
    if w:
        start_live_file_watcher(poll_seconds=5.0)
    else:
        stop_live_file_watcher()
    st.caption(f"المراقب: **{'يعمل' if is_watcher_alive() else 'متوقف'}**")


def run_live_section(page: str) -> None:
    """
    يُستدعى من قسم app.py المطابق بعد العنوان الثابت.
    يشغّل fragment مخصصاً لهذا القسم فقط، أو بديلاً autorefresh.
    """
    if page not in PAGE_TO_BUCKET:
        return
    st.markdown("### 📡 عرض حي (بطاقات)")
    fn = _FRAG_BY_PAGE.get(page)
    if fn is not None:
        fn()
    else:
        bucket = PAGE_TO_BUCKET[page]
        _live_bucket_body(bucket, page)
        try:
            from streamlit_autorefresh import st_autorefresh

            st_autorefresh(interval=1600, key=f"live_ar_{PAGE_TO_BUCKET[page]}")
        except ImportError:
            st.caption("ثبّت streamlit>=1.33 للتحديث التلقائي عبر fragment.")
