"""
utils/data_sanitizer.py — طبقة تنظيف ومعالجة البيانات (Data Sanitization Layer)
v1.0 - القضاء على أخطاء التسمية والترجمات العشوائية وتداخل اللغات والبيانات المفقودة.
"""
from __future__ import annotations
import re
import pandas as pd

# ══════════════════════════════════════════════════════════════════════════════
# 1. قاموس توحيد المصطلحات
# ══════════════════════════════════════════════════════════════════════════════
_CONCENTRATION_RULES = [
    # الإنجليزية الكاملة أولاً
    (r"\bEau\s+[Dd]e\s+Parfum\b",    "أو دو بارفيوم"),
    (r"\bEDP\b",                       "أو دو بارفيوم"),
    (r"\bEau\s+[Dd]e\s+Toilette\b",  "أو دو تواليت"),
    (r"\bEDT\b",                       "أو دو تواليت"),
    (r"\bEau\s+[Dd]e\s+Cologne\b",   "أو دو كولون"),
    (r"\bEDC\b",                       "أو دو كولون"),
    (r"\bExtrait\b",                   "إكسترا دو بارفيم"),
    (r"\bParfum\b",                    "بارفيم"),
    # توحيد الصياغات العربية الخاطئة
    (r"\bأو\s+دو\s+بارفان\b",        "أو دو بارفيوم"),
    (r"\bاو\s+دو\s+بارفان\b",        "أو دو بارفيوم"),
    (r"\bاو\s+دي\s+بارفان\b",        "أو دو بارفيوم"),
    (r"\bاو\s+دي\s+بارفيوم\b",       "أو دو بارفيوم"),
    (r"\bاو\s+دي\s+تواليت\b",        "أو دو تواليت"),
    # بارفان/بارفيوم المنفردة
    (r"(?<!أو دو )\bبارفيوم\b",      "أو دو بارفيوم"),
    (r"(?<!أو دو )\bبارفان\b",       "أو دو بارفيوم"),
    # حذف "Eau de" المعلقة
    (r"\bEau\s+de\s*$",              ""),
    (r"\bEau\s+de\s+(?=[^A-Za-z])", ""),
    (r"\bEau\s+de\b",               ""),
]

_GENDER_RULES = [
    (r"\bللنساء\b",         "للنساء"),
    (r"\bللرجال\b",         "للرجال"),
    (r"\bللجنسين\b",        "للجنسين"),
    (r"\bنسائي\b",          "للنساء"),
    (r"\bرجالي\b",          "للرجال"),
    (r"\bfor\s+Women\b",    "للنساء"),
    (r"\bWomen\b",          "للنساء"),
    (r"\bWoman\b",          "للنساء"),
    (r"\bPour\s+Femme\b",   "للنساء"),
    (r"\bFemme\b",          "للنساء"),
    (r"\bFeminine\b",       "للنساء"),
    (r"\bfor\s+Men\b",      "للرجال"),
    (r"\bMen\b",            "للرجال"),
    (r"\bMan\b",            "للرجال"),
    (r"\bPour\s+Homme\b",   "للرجال"),
    (r"\bHomme\b",          "للرجال"),
    (r"\bUnisex\b",         "للجنسين"),
    (r"\bFor\s+All\b",      "للجنسين"),
]

_ENGLISH_NOISE_RE = re.compile(
    r"\b(?:Eau|de|for|EDP|EDT|EDC|ml|mL|Parfum|Toilette|Cologne|Extrait|"
    r"Spray|Natural|Intense|Limited|Edition|Collector|Collection|"
    r"Original|Authentic|Gift|Set)\b",
    re.IGNORECASE,
)


def standardize_terms(text: str) -> str:
    """الدالة 1: توحيد مصطلحات التركيز والجنس."""
    if not text:
        return text
    result = str(text)
    for pattern, replacement in _CONCENTRATION_RULES:
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
    for pattern, replacement in _GENDER_RULES:
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
    return re.sub(r"\s{2,}", " ", result).strip()


def sanitize_description_terms(html_or_md: str) -> str:
    """الدالة 5: تنظيف مصطلحات الوصف مع الحفاظ على وسوم HTML."""
    if not html_or_md:
        return html_or_md
    parts = re.split(r"(<[^>]+>)", html_or_md)
    return "".join(
        part if (part.startswith("<") and part.endswith(">")) else standardize_terms(part)
        for part in parts
    )


# ══════════════════════════════════════════════════════════════════════════════
# 2. المطابقة المرنة للماركات
# ══════════════════════════════════════════════════════════════════════════════

def _normalize_for_match(s: str) -> str:
    s = str(s or "").lower().strip()
    s = re.sub(r"[^\w\u0600-\u06FF\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def get_brand_arabic_name(
    brand_input: str,
    store_brands: "list[str] | pd.DataFrame",
    brand_col: str = "اسم الماركة",
) -> str:
    """
    الدالة 2: مطابقة مرنة للماركات.
    يبحث في ملف ماركات مهووس بخمس مراحل: تطابق مباشر، جزء إنجليزي،
    كلمات جزئية، contains، نص منظّف.
    يرجع الاسم الرسمي الكامل مثل 'جريس | Gres' أو '' إذا لم يُوجد.
    """
    bv = str(brand_input or "").strip()
    if not bv:
        return ""

    if isinstance(store_brands, pd.DataFrame):
        brands_list = [
            str(x).strip()
            for x in store_brands[brand_col].dropna().tolist()
            if str(x).strip()
        ]
    else:
        brands_list = [str(x).strip() for x in store_brands if str(x).strip()]

    if not brands_list:
        return ""

    bv_norm = _normalize_for_match(bv)
    bv_en   = (bv.split("|")[-1].strip() if "|" in bv else bv).lower().strip()

    # 1. تطابق مباشر
    if bv in brands_list:
        return bv
    for sb in brands_list:
        if sb.lower() == bv.lower():
            return sb

    # 2. تطابق الجزء الإنجليزي (بعد |)
    for sb in brands_list:
        parts = [p.strip() for p in re.split(r"[|]", sb)]
        for part in parts:
            if part.lower() == bv_en:
                return sb
            if bv_en and bv_en in part.lower().split():
                return sb

    # 3. بحث بـ contains (case-insensitive)
    bv_safe = re.escape(bv_en)
    for sb in brands_list:
        if re.search(rf"\b{bv_safe}\b", sb, re.IGNORECASE):
            return sb

    # 4. مطابقة جزئية بالنص المنظّف
    for sb in brands_list:
        sb_norm = _normalize_for_match(sb)
        if bv_norm and (bv_norm in sb_norm or sb_norm in bv_norm):
            return sb

    return ""


def get_brand_display_name(full_brand_label: str) -> str:
    """من 'جريس | Gres' يرجع 'جريس' (الجزء العربي للعرض في العنوان)."""
    s = str(full_brand_label or "").strip()
    if "|" in s:
        parts = [p.strip() for p in s.split("|")]
        ar_parts = [p for p in parts if re.search(r"[\u0600-\u06FF]", p)]
        return ar_parts[0] if ar_parts else parts[0]
    return s


# ══════════════════════════════════════════════════════════════════════════════
# 3. فاحص القيم المفقودة
# ══════════════════════════════════════════════════════════════════════════════

def extract_size_ml(text: str) -> str:
    """الدالة 6: استخراج الحجم بالمل. يرجع '100 مل' أو ''."""
    if not text:
        return ""
    m = re.search(r"(\d{2,4})\s*مل\b", str(text))
    if m:
        return f"{m.group(1)} مل"
    m = re.search(r"(\d{2,4})\s*(?:ml|mL|ML)\b", str(text))
    if m:
        return f"{m.group(1)} مل"
    return ""


def _extract_concentration(text: str) -> str:
    t = str(text or "").lower()
    if re.search(r"\bextrait\b", t):                          return "إكسترا دو بارفيم"
    if re.search(r"\b(eau\s+de\s+parfum|edp)\b", t):         return "أو دو بارفيوم"
    if re.search(r"\b(eau\s+de\s+toilette|edt)\b", t):       return "أو دو تواليت"
    if re.search(r"\b(eau\s+de\s+cologne|edc)\b", t):        return "أو دو كولون"
    if re.search(r"\bparfum\b", t):                           return "بارفيم"
    return ""


def _extract_gender(text: str) -> str:
    t = str(text or "").lower()
    if re.search(r"\b(women|pour\s+femme|للنساء|نسائي|femme|for\s+women)\b", t): return "للنساء"
    if re.search(r"\b(men|pour\s+homme|للرجال|رجالي|homme|for\s+men)\b", t):    return "للرجال"
    if re.search(r"\b(unisex|للجنسين|for\s+all)\b", t):                         return "للجنسين"
    return ""


def validate_product_data(product_dict: dict) -> dict:
    """
    الدالة 3: فحص القيم المطلوبة قبل توليد العنوان أو الوصف.

    يرجع: { status, missing, warnings, clean }
    status = "OK" | "Missing Data" | "Warning"
    إذا كان الحجم مفقوداً → status="Missing Data" → توقف وعلامة ⚠️
    """
    missing, warnings, clean = [], [], {}
    raw_name = str(product_dict.get("name") or "").strip()
    clean["name"] = raw_name

    # الماركة
    brand = str(product_dict.get("brand") or "").strip()
    if not brand or brand.lower() in ("nan", "none", ""):
        missing.append("الماركة")
    clean["brand"] = brand

    # الحجم
    size_raw = str(product_dict.get("size") or "").strip()
    if size_raw and re.search(r"\d", size_raw):
        clean["size"] = size_raw if "مل" in size_raw else f"{size_raw} مل"
    else:
        extracted = extract_size_ml(raw_name)
        if extracted:
            clean["size"] = extracted
            warnings.append(f"الحجم استُخرج تلقائياً من الاسم: {extracted}")
        else:
            missing.append("الحجم")
            clean["size"] = ""

    # التركيز
    conc = str(product_dict.get("concentration") or "").strip()
    if not conc or conc.lower() in ("nan", "none", ""):
        conc_ex = _extract_concentration(raw_name)
        if conc_ex:
            clean["concentration"] = conc_ex
            warnings.append(f"التركيز استُخرج من الاسم: {conc_ex}")
        else:
            missing.append("التركيز")
            clean["concentration"] = ""
    else:
        clean["concentration"] = standardize_terms(conc)

    # الجنس
    gender = str(product_dict.get("gender") or "").strip()
    gender_std = standardize_terms(gender)
    if gender_std not in ("للنساء", "للرجال", "للجنسين"):
        gen_ex = _extract_gender(raw_name)
        if gen_ex:
            clean["gender"] = gen_ex
            warnings.append(f"الجنس استُخرج من الاسم: {gen_ex}")
        else:
            missing.append("الجنس")
            clean["gender"] = ""
    else:
        clean["gender"] = gender_std

    for key in ("arabic_perfume_name", "year", "designer", "family"):
        if product_dict.get(key):
            clean[key] = str(product_dict[key]).strip()

    status = "Missing Data" if missing else ("Warning" if warnings else "OK")
    return {"status": status, "missing": missing, "warnings": warnings, "clean": clean}


# ══════════════════════════════════════════════════════════════════════════════
# 4. دالة بناء العنوان الصارم
# ══════════════════════════════════════════════════════════════════════════════

def sanitize_product_name_enterprise(name: str) -> str:
    """تنظيف اسم المنتج وتوحيده (Enterprise v2.0)."""
    if not name or not isinstance(name, str): return ""
    # إزالة التشكيل
    name = "".join(c for c in unicodedata.normalize("NFD", name) if unicodedata.category(c) != "Mn")
    # إزالة الرموز الغريبة والمسافات الزائدة
    name = re.sub(r"[^\w\s\.\-\|\u0600-\u06FF]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    # توحيد همزات الألف
    for variant in ("أ", "إ", "آ", "ٱ"):
        name = name.replace(variant, "ا")
    return name

def clean_price_enterprise(price_raw: Any) -> float:
    """تحويل السعر الخام إلى رقم عشري نظيف (Enterprise v2.0)."""
    if price_raw is None: return 0.0
    s = str(price_raw).lower()
    # إزالة العملات
    s = re.sub(r"(sar|ر\.س|sr|ريال|ر\.س\.)", "", s)
    # استخراج أول رقم عشري
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if m:
        try: return float(m.group(1))
        except ValueError: return 0.0
    return 0.0

def validate_url_enterprise(url: str) -> bool:
    """التحقق من صحة الرابط (Enterprise v2.0)."""
    if not url: return False
    pattern = re.compile(r'^(?:http|ftp)s?://(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|localhost|\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(?::\d+)?(?:/?|[/?]\S+)$', re.IGNORECASE)
    return bool(re.match(pattern, url))

def build_arabic_product_title(
    product_type: str = "عطر",
    arabic_perfume_name: str = "",
    brand_arabic: str = "",
    concentration: str = "",
    size: str = "",
    gender: str = "",
    max_length: int = 220,
) -> str:
    """
    الدالة 4: بناء عنوان عربي صارم خالٍ من الإنجليزية.

    الهيكل: [نوع] + [اسم العطر] + [الماركة] + [التركيز] + [الحجم] + [الجنس]
    مثال: عطر إيفوريا سبرينغ تيمبتيشن كالفن كلاين أو دو بارفيوم 100 مل للنساء
    """
    conc_clean   = standardize_terms(str(concentration or "").strip())
    gender_clean = standardize_terms(str(gender or "").strip())
    size_clean   = str(size or "").strip()
    if size_clean and "مل" not in size_clean and re.match(r"^\d+$", size_clean):
        size_clean = f"{size_clean} مل"

    pieces = [
        str(product_type or "عطر").strip(),
        str(arabic_perfume_name or "").strip(),
        str(brand_arabic or "").strip(),
        conc_clean,
        size_clean,
        gender_clean,
    ]
    title = " ".join(p for p in pieces if p)
    title = _ENGLISH_NOISE_RE.sub(" ", title)
    return re.sub(r"\s{2,}", " ", title).strip()[:max_length]


def build_title_from_raw(
    raw_name: str,
    brand_input: str,
    store_brands: "list[str] | pd.DataFrame",
    product_type: str = "عطر",
    arabic_perfume_name: str = "",
    gender_hint: str = "",
    brand_col: str = "اسم الماركة",
) -> dict:
    """دالة 7 — شاملة: اسم خام → عنوان نظيف + حالة الصحة."""
    size   = extract_size_ml(raw_name)
    conc   = standardize_terms(_extract_concentration(raw_name))
    gender = standardize_terms(_extract_gender(gender_hint) or _extract_gender(raw_name))

    brand_label   = get_brand_arabic_name(brand_input, store_brands, brand_col)
    brand_display = get_brand_display_name(brand_label) if brand_label else brand_input.strip()

    validation = validate_product_data({
        "name": raw_name, "brand": brand_label or brand_input,
        "size": size, "concentration": conc, "gender": gender,
    })

    title = build_arabic_product_title(
        product_type=product_type,
        arabic_perfume_name=(arabic_perfume_name or "").strip(),
        brand_arabic=brand_display,
        concentration=validation["clean"].get("concentration", conc),
        size=validation["clean"].get("size", size),
        gender=validation["clean"].get("gender", gender),
    )

    return {
        "title":         title,
        "brand_label":   brand_label,
        "brand_display": brand_display,
        "concentration": validation["clean"].get("concentration", conc),
        "size":          validation["clean"].get("size", size),
        "gender":        validation["clean"].get("gender", gender),
        "status":        validation["status"],
        "missing":       validation["missing"],
        "warnings":      validation["warnings"],
    }


# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 68)
    print("اختبار طبقة التنظيف — Data Sanitization Layer")
    print("=" * 68)

    print("\n[1] standardize_terms:")
    for c in ["أو دو بارفان", "Eau de Parfum", "EDP for Women",
              "Pour Homme EDT", "Eau de", "بارفان", "نسائي", "رجالي"]:
        print(f"  {c!r:40} → {standardize_terms(c)!r}")

    print("\n[2] extract_size_ml:")
    for c in ["Gres Cabotine Gold EDP 100ml",
              "عطر Calvin Klein CK One Shock Eau de Toilette أو دو تواليت",
              "100 مل للنساء", "50mL Spray"]:
        print(f"  {extract_size_ml(c)!r:10} ← {c!r}")

    brands_test = ["جريس | Gres", "كالفن كلاين | Calvin Klein", "جيفنشي | Givenchy"]
    print("\n[3] get_brand_arabic_name:")
    for bt in ["Gres", "GRES", "Calvin Klein", "Givenchy", "Unknown"]:
        r = get_brand_arabic_name(bt, brands_test)
        print(f"  {bt!r:20} → {r!r:32} display={get_brand_display_name(r)!r}")

    print("\n[4] validate — CK One Shock (حجم مفقود):")
    v = validate_product_data({
        "name": "Calvin Klein CK One Shock Eau de Toilette",
        "brand": "Calvin Klein", "size": "", "concentration": "", "gender": "للرجال"
    })
    print(f"  status={v['status']}  missing={v['missing']}")

    print("\n[5] build_arabic_product_title:")
    for tc in [
        dict(arabic_perfume_name="كابوتين جولد", brand_arabic="جريس",
             concentration="أو دو تواليت", size="100 مل", gender="للنساء"),
        dict(arabic_perfume_name="إيفوريا سبرينغ تيمبتيشن", brand_arabic="كالفن كلاين",
             concentration="أو دو بارفيوم", size="100 مل", gender="للنساء"),
        dict(arabic_perfume_name="سي كيه ون شوك", brand_arabic="كالفن كلاين",
             concentration="أو دو تواليت", size="100 مل", gender="للرجال"),
    ]:
        print(f"  → {build_arabic_product_title(**tc)}")
    print("\n✅ اكتملت الاختبارات")
