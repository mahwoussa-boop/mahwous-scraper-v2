"""
دوال مساعدة خالصة (بدون واجهة ولا session_state) — تجهيز البيانات والنصوص.
"""
import json
import re
from datetime import datetime

import pandas as pd

# أول رابط صورة http(s) — يتوقف عند الفاصلة (إنجليزي/عربي) حتى لا يُلتقط رابطان في src واحد
_FIRST_HTTP_IMAGE_URL = re.compile(
    r"https?://[^\s<>\"\'\,\u060c؛;]+?"
    r"\.(?:webp|jpg|jpeg|png|gif|avif)"
    r"(?:\?[^\s<>\"\'\,\u060c؛;]*)?",
    re.I,
)
# تسلسل شائع في سلة/إكسيل: ...jpg,https://...
_AFTER_EXT_COMMA_HTTP = re.compile(
    r"\.(?:webp|jpg|jpeg|png|gif|avif)\s*[,،]\s*https?://",
    re.I,
)


def _looks_like_several_image_urls(s: str) -> bool:
    """True فقط عندما يُرجّح أن النص يضم أكثر من رابط (لا نلمس رابط المنافس بفاصلة داخل ?query)."""
    if not s or ("http://" not in s and "https://" not in s):
        return False
    n = s.count("http://") + s.count("https://")
    if n > 1:
        return True
    return bool(_AFTER_EXT_COMMA_HTTP.search(s))

# حقول وسائط قد تُحفظ كـ NaN — لا تُستبدل بالصفر
_MEDIA_KEYS_EMPTY_ON_NA = frozenset({
    "صورة_منتجنا", "رابط_منتجنا", "صورة_المنتج", "رابط_المنتج",
    "رابط_المنافس",
    "صورة المنتج", "رابط المنتج", "صوره المنتج", "الرابط", "رابط",
})


def first_image_url_string(s: str) -> str:
    """
    عندما تُخزّن عدة روابط صور في خلية واحدة مفصولة بفاصلة (شائع في تصدير سلة/إكسيل)،
    أرجع أول رابط http يبدو ملف صورة — حتى يعمل <img src> والمتصفح.

    إن كان الرابط واحداً (مثل صور المنافسين مع فاصلة داخل ?query)، يُعاد كما هو دون تقسيم.
    """
    s = (s or "").strip()
    if not s:
        return ""
    if "http://" not in s and "https://" not in s:
        return s.split()[0]
    if not _looks_like_several_image_urls(s):
        return s.split()[0]
    m = _FIRST_HTTP_IMAGE_URL.search(s)
    if m:
        return m.group(0).strip().rstrip(".,;)]\"'")
    # احتياط: فواصل غير إنجليزية أو نص بدون امتداد واضح في الـ regex
    norm = s.replace("\u060c", ",").replace("،", ",").replace("؛", ";").replace("\n", ",")
    for sep in (",", ";"):
        if sep not in norm:
            continue
        for part in norm.split(sep):
            part = part.strip()
            if not part.startswith("http"):
                continue
            base = part.split("?")[0].lower()
            if any(base.endswith(ext) for ext in (".webp", ".jpg", ".jpeg", ".png", ".gif", ".avif")):
                return part.split()[0]
        for part in norm.split(sep):
            part = part.strip()
            if part.startswith("http"):
                return part.split()[0]
    return s.split()[0]


def _strip_media_val(v):
    if v is None:
        return ""
    try:
        if isinstance(v, float) and pd.isna(v):
            return ""
        if pd.isna(v) and not isinstance(v, (list, dict, str)):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none", "0", "<na>"):
        return ""
    return s


def normalize_result_media_keys(row: dict) -> None:
    """يوحّد صورة/رابط منتجنا تحت المفتاحين المعتمدين في الواجهة والمحرك."""
    if not row:
        return
    if not _strip_media_val(row.get("صورة_منتجنا")):
        for alt in ("صورة_المنتج", "صورة المنتج", "صوره المنتج"):
            if alt in row:
                v = _strip_media_val(row.get(alt))
                if v:
                    row["صورة_منتجنا"] = v
                    break
    if not _strip_media_val(row.get("رابط_منتجنا")):
        for alt in ("رابط_المنتج", "رابط المنتج", "الرابط", "رابط"):
            if alt in row:
                v = _strip_media_val(row.get(alt))
                if v:
                    row["رابط_منتجنا"] = v
                    break


def row_media_urls_from_analysis(row) -> tuple:
    """
    صورة منتجنا + صورة المنافس الرئيسي من صف نتيجة (Series أو dict).
    يعتمد على مفتاحي صورة_منتجنا وجميع_المنافسين بعد التطبيع.
    """
    if row is None:
        return ("", "")
    d = row.to_dict() if hasattr(row, "to_dict") else dict(row)
    normalize_result_media_keys(d)
    our_img = first_image_url_string(str(d.get("صورة_منتجنا", "") or "").strip())
    comp_img = ""
    all_c = d.get("جميع_المنافسين", d.get("جميع المنافسين", [])) or []
    if isinstance(all_c, str):
        try:
            all_c = json.loads(all_c)
        except Exception:
            all_c = []
    if not isinstance(all_c, list):
        all_c = []
    comp_name = str(d.get("منتج_المنافس", "—"))
    for c in all_c:
        if str(c.get("name", "")).strip() == str(comp_name).strip():
            comp_img = first_image_url_string(str(c.get("image_url") or c.get("thumb") or "").strip())
            break
    if not comp_img and all_c:
        comp_img = first_image_url_string(str(all_c[0].get("image_url") or all_c[0].get("thumb") or "").strip())
    return (our_img, comp_img)


def our_product_url_from_row(row) -> str:
    """رابط صفحة منتجنا — بعد تطبيع أسماء الأعمدة (رابط_منتجنا / رابط_المنتج / …)."""
    if row is None:
        return ""
    d = row.to_dict() if hasattr(row, "to_dict") else dict(row)
    normalize_result_media_keys(d)
    u = _strip_media_val(d.get("رابط_منتجنا"))
    if not u.startswith("http"):
        return ""
    return u.split()[0]


def competitor_product_url_from_row(row) -> str:
    """رابط صفحة المنتج عند المنافس — أعمدة النتيجة أو جميع_المنافسين أو أسماء مثل abs-size href."""
    if row is None:
        return ""
    d = row.to_dict() if hasattr(row, "to_dict") else dict(row)
    for k in ("رابط_المنافس", "رابط المنافس", "competitor_url"):
        v = _strip_media_val(d.get(k))
        if v.startswith("http"):
            return v.split()[0]
    comp_name = str(d.get("منتج_المنافس", "—"))
    all_c = d.get("جميع_المنافسين", d.get("جميع المنافسين", [])) or []
    if isinstance(all_c, str):
        try:
            all_c = json.loads(all_c)
        except Exception:
            all_c = []
    if isinstance(all_c, list):
        for c in all_c:
            if str(c.get("name", "")).strip() == str(comp_name).strip():
                u = str(c.get("product_url") or c.get("url") or "").strip()
                if u.startswith("http"):
                    return u.split()[0]
        if all_c:
            u = str(all_c[0].get("product_url") or all_c[0].get("url") or "").strip()
            if u.startswith("http"):
                return u.split()[0]
    for k, v in d.items():
        sk = str(k).lower()
        if k in ("رابط_منتجنا", "رابط منتجنا") or "منتجنا" in sk:
            continue
        if "صورة" in str(k) and "وصف" not in str(k) and "href" not in sk:
            continue
        if any(x in sk for x in ("href", "رابط", "link", "url")):
            s = _strip_media_val(v)
            if s.startswith("http"):
                return s.split()[0]
    # أحياناً يُخزَّن رابط صفحة المنتج بالخطأ في عمود الاسم (مثل تصدير المنافس)
    vnm = _strip_media_val(d.get("منتج_المنافس"))
    if vnm.startswith("http"):
        return vnm.split()[0]
    return ""


def safe_results_for_json(results_list):
    """تحويل النتائج لصيغة آمنة للحفظ في JSON/SQLite — يحول القوائم المتداخلة."""
    safe = []
    for r in results_list:
        row = {}
        for k, v in (r.items() if isinstance(r, dict) else {}):
            if isinstance(v, list):
                try:
                    row[k] = json.dumps(v, ensure_ascii=False, default=str)
                except Exception:
                    row[k] = str(v)
            else:
                try:
                    if v is not None and not isinstance(v, (list, dict)) and pd.isna(v):
                        row[k] = "" if k in _MEDIA_KEYS_EMPTY_ON_NA else 0
                        continue
                except (TypeError, ValueError):
                    pass
                row[k] = v
        safe.append(row)
    return safe


def restore_results_from_json(results_list):
    """استعادة النتائج من JSON — يحول نصوص القوائم لقوائم فعلية."""
    restored = []
    for r in results_list:
        row = dict(r) if isinstance(r, dict) else {}
        for k in ["جميع_المنافسين", "جميع المنافسين"]:
            v = row.get(k)
            if isinstance(v, str):
                try:
                    row[k] = json.loads(v)
                except Exception:
                    row[k] = []
            elif v is None:
                row[k] = []
        normalize_result_media_keys(row)
        restored.append(row)
    return restored


def ts_badge(ts_str=""):
    """شارة تاريخ مصغرة (HTML)."""
    if not ts_str:
        ts_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    return (
        f'<span style="font-size:.65rem;color:#555;background:#1a1a2e;'
        f'padding:1px 6px;border-radius:8px;margin-right:4px">🕐 {ts_str}</span>'
    )


def decision_badge(action):
    """شارة قرار معلّق (HTML)."""
    colors = {
        "approved": ("#00C853", "✅ موافق"),
        "deferred": ("#FFD600", "⏸️ مؤجل"),
        "removed": ("#FF1744", "🗑️ محذوف"),
    }
    c, label = colors.get(action, ("#666", action))
    return f'<span style="font-size:.7rem;color:{c};font-weight:700">{label}</span>'


def pid_from_row(row, col):
    """استخراج معرف المنتج من صف pandas بشكل آمن."""
    if not col or col not in row.index:
        return ""
    v = row.get(col, "")
    if v is None or str(v) in ("nan", "None", "", "NaN"):
        return ""
    try:
        fv = float(v)
        if fv == int(fv):
            return str(int(fv))
    except (ValueError, TypeError):
        pass
    return str(v).strip()
