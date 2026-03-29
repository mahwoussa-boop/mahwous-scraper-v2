"""
utils/master_card.py — البطاقة الموحدة (The Master Card) v1.0
===============================================================
عرض منتجنا مقابل المنافسين في بطاقة موحدة احترافية بدلاً من صفوف مكررة.
تعرض:
 • منتجنا (اسم + سعر + تكلفة + صورة) في اليسار
 • المنافس المتصدر (أقل سعر) في اليمين
 • قائمة منسدلة لبقية المنافسين
 • أزرار قرارات الذكاء الاصطناعي + موافق/رفض
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
import pandas as pd

logger = logging.getLogger(__name__)

# ── CSS للبطاقة الموحدة ────────────────────────────────────
MASTER_CARD_CSS = """
<style>
.master-card {
    background: #1a1a2e;
    border: 1px solid #333;
    border-radius: 12px;
    padding: 16px;
    margin-bottom: 16px;
    direction: rtl;
}
.master-card.threat { border-color: #ef4444; box-shadow: 0 0 8px rgba(239,68,68,0.3); }
.master-card.opportunity { border-color: #10b981; box-shadow: 0 0 8px rgba(16,185,129,0.3); }
.master-card.hold { border-color: #555; }

.mc-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;
}
.mc-title { font-size: 1.05em; font-weight: 700; color: #e2e8f0; }
.mc-badge {
    font-size: 0.75em; padding: 3px 10px; border-radius: 20px;
    font-weight: 600; letter-spacing: 0.5px;
}
.badge-undercut { background: #7f1d1d; color: #fca5a5; }
.badge-raise    { background: #064e3b; color: #6ee7b7; }
.badge-hold     { background: #374151; color: #9ca3af; }
.badge-warning  { background: #78350f; color: #fde68a; }

.mc-vs-grid {
    display: grid;
    grid-template-columns: 1fr 40px 1fr;
    gap: 8px;
    align-items: center;
}
.mc-side {
    background: #0f172a;
    border-radius: 8px;
    padding: 12px;
    text-align: center;
}
.mc-side.ours { border: 1px solid #3b82f6; }
.mc-side.comp { border: 1px solid #f59e0b; }
.mc-side img {
    width: 70px; height: 70px; object-fit: contain;
    border-radius: 6px; margin-bottom: 6px;
}
.mc-side .product-name {
    font-size: 0.82em; color: #94a3b8;
    margin-bottom: 4px; max-height: 2.4em; overflow: hidden;
}
.mc-side .price-big {
    font-size: 1.4em; font-weight: 800;
}
.mc-side .price-big.ours-price { color: #60a5fa; }
.mc-side .price-big.comp-price { color: #fbbf24; }
.mc-side .cost-label {
    font-size: 0.72em; color: #6b7280; margin-top: 2px;
}
.mc-vs-label {
    font-size: 1.1em; font-weight: 900; color: #7c3aed;
    text-align: center;
}
.mc-suggested {
    background: #0f172a;
    border: 1px dashed #10b981;
    border-radius: 8px;
    padding: 10px 14px;
    margin-top: 10px;
    display: flex;
    justify-content: space-between;
    align-items: center;
}
.mc-suggested .label { font-size: 0.8em; color: #6b7280; }
.mc-suggested .value { font-size: 1.3em; font-weight: 700; color: #34d399; }
.mc-notes { font-size: 0.75em; color: #f59e0b; margin-top: 6px; }
.mc-comp-strip {
    background: #0f172a;
    border-radius: 6px;
    padding: 6px 10px;
    margin: 3px 0;
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 0.8em;
    color: #94a3b8;
}
.mc-comp-strip .comp-price-sm { color: #fbbf24; font-weight: 600; }
.mc-ai-badge {
    display: inline-block;
    font-size: 0.72em; padding: 2px 8px;
    border-radius: 10px;
    margin-right: 4px;
}
.ai-verified { background: #1e3a2f; color: #6ee7b7; }
.ai-review   { background: #3b2a10; color: #fde68a; }
.ai-missing  { background: #3b1f1f; color: #fca5a5; }
</style>
"""


# ── دوال بناء HTML ─────────────────────────────────────────
def _img_html(url: str, alt: str = "") -> str:
    if url and str(url).startswith("http"):
        return f'<img src="{url}" alt="{alt}" onerror="this.style.display=\'none\'">'
    return f'<div style="width:70px;height:70px;background:#222;border-radius:6px;display:inline-flex;align-items:center;justify-content:center;color:#555">📦</div>'


def _strategy_badge(strategy: str) -> str:
    labels = {
        "undercut": ("⬇️ يتطلب خفض", "badge-undercut"),
        "raise": ("⬆️ يمكن رفعه", "badge-raise"),
        "hold": ("✅ مثالي", "badge-hold"),
        "below_cost_warning": ("⚠️ تحت التكلفة", "badge-warning"),
    }
    text, cls = labels.get(strategy, ("—", "badge-hold"))
    return f'<span class="mc-badge {cls}">{text}</span>'


def _ai_state_badge(state: str) -> str:
    if state == "verified_by_ai":
        return '<span class="mc-ai-badge ai-verified">🤖 Gemini ✓</span>'
    elif state == "under_review":
        return '<span class="mc-ai-badge ai-review">🔍 مراجعة</span>'
    elif state == "missing_candidate":
        return '<span class="mc-ai-badge ai-missing">❓ مفقود</span>'
    return ""


def build_master_card_html(
    product_name: str,
    our_price: float,
    cost_price: float,
    our_image: str,
    comp_name: str,
    comp_price: float,
    comp_image: str,
    comp_url: str,
    suggested_price: float,
    strategy: str,
    margin_pct: float,
    margin_safe: bool,
    ai_state: str = "",
    alert_type: str = "none",
    notes: str = "",
    all_competitors: Optional[List[Dict]] = None,
) -> str:
    """بناء HTML للبطاقة الموحدة."""

    card_class = "master-card"
    if alert_type == "threat":
        card_class += " threat"
    elif alert_type == "opportunity":
        card_class += " opportunity"
    else:
        card_class += " hold"

    price_diff = our_price - comp_price if comp_price > 0 else 0
    diff_sign = "+" if price_diff > 0 else ""
    diff_color = "#ef4444" if price_diff > 0 else "#10b981"

    margin_color = "#10b981" if margin_safe else "#ef4444"

    # ── بناء HTML ──
    html = f"""
{MASTER_CARD_CSS}
<div class="{card_class}">
  <div class="mc-header">
    <div class="mc-title">{product_name[:60]}</div>
    <div style="display:flex;gap:6px;align-items:center">
      {_ai_state_badge(ai_state)}
      {_strategy_badge(strategy)}
    </div>
  </div>

  <div class="mc-vs-grid">
    <div class="mc-side ours">
      {_img_html(our_image, "منتجنا")}
      <div class="product-name">🏪 متجرنا</div>
      <div class="price-big ours-price">{our_price:.2f} ر.س</div>
      {"" if cost_price <= 0 else f'<div class="cost-label">تكلفة: {cost_price:.2f} | هامش: <span style="color:{margin_color}">{margin_pct:.1f}%</span></div>'}
    </div>

    <div class="mc-vs-label">VS</div>

    <div class="mc-side comp">
      {_img_html(comp_image, "المنافس")}
      <div class="product-name">🏢 <a href="{comp_url}" target="_blank" style="color:#94a3b8;text-decoration:none">{comp_name[:35]}</a></div>
      <div class="price-big comp-price">{comp_price:.2f} ر.س</div>
      <div class="cost-label" style="color:{diff_color}">فارق: {diff_sign}{price_diff:.2f} ر.س</div>
    </div>
  </div>

  <div class="mc-suggested">
    <div><span class="label">💡 السعر المقترح</span></div>
    <div class="value">{suggested_price:.2f} ر.س</div>
  </div>

  {f'<div class="mc-notes">{notes}</div>' if notes else ""}
</div>
"""

    # قائمة بقية المنافسين
    if all_competitors and len(all_competitors) > 1:
        rest = [c for c in all_competitors if str(c.get("comp_url", "")) != comp_url]
        if rest:
            strips = ""
            for c in rest[:5]:
                c_price = float(c.get("comp_price", 0) or 0)
                c_name = str(c.get("comp_name", "") or "")[:30]
                c_url = str(c.get("comp_url", "") or "")
                strips += f"""
<div class="mc-comp-strip">
  <span><a href="{c_url}" target="_blank" style="color:#94a3b8;text-decoration:none">🔗 {c_name}</a></span>
  <span class="comp-price-sm">{c_price:.2f} ر.س</span>
</div>"""
            html += f"""
<details style="margin-top:6px">
<summary style="cursor:pointer;font-size:0.8em;color:#6b7280;padding:4px 0">
  ▼ {len(rest)} منافس آخر
</summary>
<div style="padding:4px 0">
{strips}
</div>
</details>"""

    return html


# ── الواجهة الرئيسية ──────────────────────────────────────
def render_master_cards(
    df: pd.DataFrame,
    show_decision_buttons: bool = True,
    max_cards: int = 50,
) -> None:
    """
    عرض البطاقات الموحدة لكل منتج في Streamlit.

    يُجمِّع منتجات متعددة بنفس SKU في بطاقة واحدة
    ويعرض المنافس الأرخص في الواجهة الرئيسية.
    """
    import streamlit as st
    from utils.user_preferences import render_decision_buttons

    if df is None or df.empty:
        st.info("لا توجد بيانات للعرض")
        return

    # تجميع حسب SKU
    sku_col = "sku" if "sku" in df.columns else df.columns[0]
    groups = df.groupby(sku_col, sort=False)

    shown = 0
    for sku, group in groups:
        if shown >= max_cards:
            st.caption(f"... و{len(groups) - max_cards} منتج آخر")
            break

        # اختر المنافس الأرخص
        if "comp_price" in group.columns:
            group_sorted = group.sort_values(
                by="comp_price",
                key=lambda x: pd.to_numeric(x, errors="coerce").fillna(999999)
            )
        else:
            group_sorted = group

        primary = group_sorted.iloc[0]

        # استخراج القيم
        name = str(primary.get("name", primary.get("product_name", sku)))
        our_price = float(primary.get("price", 0) or 0)
        cost = float(primary.get("cost", 0) or 0)
        our_img = str(primary.get("image_url", "") or "")
        comp_name = str(primary.get("comp_name", "") or "")
        comp_price = float(primary.get("comp_price", 0) or 0)
        comp_img = str(primary.get("comp_image_url", "") or "")
        comp_url = str(primary.get("comp_url", "") or "")
        suggested = float(primary.get("suggested_price", our_price) or our_price)
        strategy = str(primary.get("strategy", "hold") or "hold")
        margin = float(primary.get("margin_pct", 0) or 0)
        margin_safe = bool(primary.get("margin_safe", True))
        ai_state = str(primary.get("ai_verification_state", "") or "")
        alert = str(primary.get("alert_type", "none") or "none")
        notes = str(primary.get("notes", "") or "")

        all_competitors = group_sorted.to_dict("records")

        card_html = build_master_card_html(
            product_name=name,
            our_price=our_price,
            cost_price=cost,
            our_image=our_img,
            comp_name=comp_name,
            comp_price=comp_price,
            comp_image=comp_img,
            comp_url=comp_url,
            suggested_price=suggested,
            strategy=strategy,
            margin_pct=margin,
            margin_safe=margin_safe,
            ai_state=ai_state,
            alert_type=alert,
            notes=notes,
            all_competitors=all_competitors,
        )

        st.markdown(card_html, unsafe_allow_html=True)

        if show_decision_buttons:
            render_decision_buttons(
                sku=str(sku),
                product_name=name,
                our_price=our_price,
                comp_price=comp_price,
                suggested=suggested,
                strategy=strategy,
                competitor=comp_name,
                match_score=float(primary.get("match_score", 0) or 0),
                key_prefix=f"mc_{shown}",
            )
            st.divider()

        shown += 1
