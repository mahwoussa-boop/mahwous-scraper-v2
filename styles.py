import html
import streamlit as st

def get_styles():
    return """
    <style>
    [data-testid="stAppViewContainer"] {
        background-color: #0e1117;
        color: #ffffff;
    }
    .stat-card {
        background: #1a1a2e;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #30363d;
        text-align: center;
        margin-bottom: 10px;
    }
    .vs-card {
        background: #161b22;
        padding: 20px;
        border-radius: 12px;
        border: 1px solid #30363d;
        margin-bottom: 15px;
    }
    .comp-strip {
        display: flex;
        gap: 10px;
        overflow-x: auto;
        padding: 10px 0;
    }
    .miss-card {
        background: #1c1c1c;
        padding: 15px;
        border-radius: 10px;
        border-left: 5px solid #ff4b4b;
        margin-bottom: 10px;
    }
    </style>
    """

def get_sidebar_toggle_js():
    return "<script>console.log('Sidebar toggle JS loaded');</script>"

def stat_card(icon, label, val, color):
    return f"""
    <div class="stat-card" style="border-top: 3px solid {color}">
        <div style="font-size: 1.5rem;">{icon}</div>
        <div style="font-size: 0.8rem; color: #8b949e;">{label}</div>
        <div style="font-size: 1.2rem; font-weight: bold; color: {color};">{val}</div>
    </div>
    """

def vs_card(our_name, our_price, comp_name, comp_price, diff_pct, diff_val,
             our_image_url=None, comp_image_url=None, comp_src="", pid_str=""):
    """بطاقة مقارنة بصرية مزدوجة (منتجنا vs المنافس) — ألوان متناسقة مع الوضع الداكن."""
    on = html.escape(str(our_name))
    cn = html.escape(str(comp_name))
    cs = html.escape(str(comp_src or ""))
    ps = html.escape(str(pid_str or ""))
    dps = html.escape(str(diff_pct))

    try:
        dv = float(diff_val)
    except (TypeError, ValueError):
        dv = 0.0
    # نفس منطق البطاقة السابقة: أخضر عندما لا يزيد سعرنا عن المنافس (الفرق ≤ 0)
    accent = "#00C853" if dv <= 0 else "#FF1744"

    def _img_block(url, placeholder):
        u = (url or "").strip()
        if u and u not in ("None", "nan"):
            return (
                f'<img src="{html.escape(u)}" '
                'style="width:60px;height:60px;border-radius:8px;object-fit:cover;'
                'border:1px solid #30363d;flex-shrink:0;" '
                'onerror="this.style.display=\'none\'">'
            )
        return (
            f'<div style="width:60px;height:60px;border-radius:8px;background:#21262d;'
            f'color:#8b949e;font-size:0.65rem;display:flex;align-items:center;'
            f'justify-content:center;text-align:center;padding:4px;">{placeholder}</div>'
        )

    our_img = _img_block(our_image_url, "بدون<br>صورة")
    comp_img = _img_block(comp_image_url, "بدون<br>صورة")

    pid_line = (
        f'<div style="font-size:0.75rem;color:#8b949e;margin-top:4px;">معرّف: {ps}</div>'
        if ps else ""
    )
    src_line = (
        f'<div style="font-size:0.75rem;color:#8b949e;margin-top:4px;">{cs}</div>'
        if cs else ""
    )

    return f"""
    <div class="vs-card" style="padding:12px;margin-bottom:10px;">
        <div style="display:flex;justify-content:space-between;align-items:stretch;
                    gap:8px;background:#161b22;border:1px solid #30363d;
                    border-radius:8px;padding:10px;">
            <div style="flex:1;text-align:right;padding-left:8px;min-width:0;">
                <div style="display:flex;align-items:center;gap:10px;justify-content:flex-end;">
                    <div style="min-width:0;">
                        <strong style="color:#e6edf3;">متجرنا</strong>
                        <span style="color:#c9d1d9;">{on}</span><br>
                        <b style="color:#58a6ff;">{our_price} ر.س</b>
                        {pid_line}
                    </div>
                    {our_img}
                </div>
            </div>
            <div style="padding:0 10px;text-align:center;border-left:1px solid #30363d;
                        border-right:1px solid #30363d;display:flex;flex-direction:column;
                        justify-content:center;min-width:72px;">
                <strong style="color:{accent};font-size:15px;" dir="ltr">{dps}%</strong>
                <span style="color:#8b949e;font-size:11px;">الفارق</span>
                <span style="color:{accent};font-size:11px;margin-top:4px;" dir="ltr">{dv:.0f} ر.س</span>
            </div>
            <div style="flex:1;text-align:left;padding-right:8px;min-width:0;">
                <div style="display:flex;align-items:center;gap:10px;justify-content:flex-start;">
                    {comp_img}
                    <div style="min-width:0;">
                        <strong style="color:#e6edf3;">المنافس</strong>
                        <span style="color:#c9d1d9;">{cn}</span><br>
                        <b style="color:{accent};">{comp_price} ر.س</b>
                        {src_line}
                    </div>
                </div>
            </div>
        </div>
    </div>
    """

def comp_strip(all_comps):
    html = '<div class="comp-strip">'
    for comp in all_comps:
        html += f'<div style="background: #21262d; padding: 5px 10px; border-radius: 15px; font-size: 0.8rem;">{comp}</div>'
    html += '</div>'
    return html

def miss_card(name, price, brand, size, ptype, comp, suggested_price, note, variant_html, tester_badge, border_color, confidence_level, confidence_score, product_id):
    _cl = str(confidence_level or "").lower()
    try:
        _csf = float(confidence_score)
    except (TypeError, ValueError):
        _csf = 0.0
    # لا تعرض «ثقة قوية» مع 0% — يحدث عند صفوف مُستوردة من CSV بلا أعمدة تحليل عربية
    if _csf <= 0.0:
        _conf_ar = "غير مُقيَّم (لا درجة مطابقة بعد)"
        _pct_txt = "—"
    else:
        _conf_ar = {"green": "ثقة قوية", "yellow": "ثقة متوسطة", "red": "مشكوك"}.get(
            _cl, str(confidence_level or "—")
        )
        _pct_txt = f"{_csf:.1f}%"
    return f"""
    <div class="miss-card" style="border-left-color: {border_color}">
        <div style="display: flex; justify-content: space-between;">
            <div>
                <div style="font-weight: bold; font-size: 1.1rem;">{tester_badge} {name} ({product_id})</div>
                <div style="color: #8b949e; font-size: 0.85rem;">{brand} | {size} | {ptype}</div>
            </div>
            <div style="text-align: right;">
                <div style="font-size: 1.2rem; font-weight: bold;">{price} ر.س</div>
                <div style="color: #00C853; font-size: 0.9rem;">المقترح: {suggested_price} ر.س</div>
            </div>
        </div>
        <div style="margin-top: 8px; font-size: 0.85rem; color: #8b949e;">
            المنافس: {comp} | درجة ثقة المطابقة: {_conf_ar} ({_pct_txt})
        </div>
        {variant_html}
        {f'<div style="margin-top: 5px; color: #ffd600; font-style: italic;">{note}</div>' if note else ''}
    </div>
    """
