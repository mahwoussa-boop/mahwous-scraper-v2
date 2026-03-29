"""
config.py - الإعدادات المركزية v26.1
المفاتيح محمية عبر Streamlit Secrets
نظام تدوير المفاتيح الذكي (Key Rotation) لضمان استمرارية العمل
"""
import json as _json
import os as _os

# ===== معلومات التطبيق =====
APP_TITLE   = "نظام التسعير الذكي - مهووس"
APP_NAME    = APP_TITLE
APP_VERSION = "v26.1"
APP_ICON    = "🧪"
GEMINI_MODEL = "gemini-1.5-pro"     # تم التحديث لدقة مطابقة أعلى 99.9%

# ══════════════════════════════════════════════
#  Baseline Store (CRITICAL)
# ══════════════════════════════════════════════
MAIN_STORE_DOMAIN = "mahwous.com"
MAIN_STORE_NAME = "Mahwous"
TARGET_MARGIN = 0.35
BASELINE_LOCKED = True


def normalize_domain(value: str) -> str:
    v = str(value or "").strip().lower()
    v = v.replace("https://", "").replace("http://", "")
    v = v.lstrip("www.")
    v = v.split("/")[0]
    return v


def is_main_store_domain(value: str) -> bool:
    return normalize_domain(value) == normalize_domain(MAIN_STORE_DOMAIN)

# ══════════════════════════════════════════════
#  قراءة Secrets بطريقة آمنة 100%
#  تدعم 3 أساليب Streamlit
# ══════════════════════════════════════════════
def _s(key, default=""):
    """
    يقرأ Secret بـ 3 طرق — آمن تماماً في مرحلة Build:
    1. os.environ              Railway Environment Variables (يعمل في Build والتشغيل)
    2. st.secrets[key]         Streamlit Cloud / محلي (.streamlit/secrets.toml)
    3. default                 القيمة الافتراضية
    """
    # 1. os.environ أولاً — يعمل في Build والتشغيل على Railway
    v = _os.environ.get(key, "")
    if v:
        return v
    # 2. st.secrets — عند تشغيل streamlit run (لا نعتمد على runtime.exists()؛ أحياناً يكون False عند أول تحميل لـ config)
    try:
        import sys
        if "streamlit" in sys.modules:
            import streamlit as _st
            if hasattr(_st, "secrets"):
                v = _st.secrets.get(key, None)
                if v is not None:
                    return str(v) if not isinstance(v, (list, dict)) else v
    except Exception:
        pass
    return default


def _parse_gemini_keys():
    """
    يجمع مفاتيح Gemini من أي صيغة:
    • GEMINI_API_KEYS = '["key1","key2","key3"]'  (JSON string)
    • GEMINI_API_KEYS = ["key1","key2"]            (TOML array)
    • GEMINI_API_KEY  = "key1"                     (مفتاح واحد)
    • GEMINI_KEY_1 / GEMINI_KEY_2 / ...           (مفاتيح منفصلة)
    """
    keys = []

    # ─── المحاولة 1: GEMINI_API_KEYS (JSON string أو TOML array) ───
    raw = _s("GEMINI_API_KEYS", "")

    if isinstance(raw, list):
        # TOML array مباشرة
        keys = [k for k in raw if k and isinstance(k, str)]
    elif raw and isinstance(raw, str):
        raw = raw.strip()
        # قد تكون JSON string
        if raw.startswith('['):
            try:
                parsed = _json.loads(raw)
                if isinstance(parsed, list):
                    keys = [k for k in parsed if k]
            except Exception:
                # ربما string بدون quotes صحيحة → نظفها
                clean = raw.strip("[]").replace('"','').replace("'",'')
                keys = [k.strip() for k in clean.split(',') if k.strip()]
        elif raw:
            keys = [raw]

    # ─── المحاولة 2: GEMINI_API_KEY (مفتاح واحد) ───
    single = _s("GEMINI_API_KEY", "")
    if single and single not in keys:
        keys.append(single)

    # ─── المحاولة 3: مفاتيح منفصلة ───
    for n in ["GEMINI_KEY_1","GEMINI_KEY_2","GEMINI_KEY_3",
              "GEMINI_KEY_4","GEMINI_KEY_5","GEMINI_KEY_6","GEMINI_KEY_7","GEMINI_KEY_8","GEMINI_KEY_9","GEMINI_KEY_10"]:
        k = _s(n, "")
        if k and k not in keys:
            keys.append(k)

    # ─── المحاولة 4: أسماء شائعة الخطأ في Railway (مفتاح Google AI / Gemini) ───
    if not keys:
        for alias in ("GOOGLE_API_KEY", "GOOGLE_GENAI_API_KEY", "GENAI_API_KEY"):
            k = _s(alias, "")
            if k and isinstance(k, str) and k.strip() and k.strip() not in keys:
                keys.append(k.strip())
                break

    # تنظيف نهائي: إزالة المفاتيح الفارغة أو القصيرة
    keys = [k.strip() for k in keys if k and len(k) > 20]
    return keys


def _parse_openrouter_keys():
    """
    يجمع مفاتيح OpenRouter من أي صيغة
    """
    keys = []
    raw = _s("OPENROUTER_API_KEYS", "")
    
    if isinstance(raw, list):
        keys = [k for k in raw if k and isinstance(k, str)]
    elif raw and isinstance(raw, str):
        raw = raw.strip()
        if raw.startswith('['):
            try:
                parsed = _json.loads(raw)
                if isinstance(parsed, list):
                    keys = [k for k in parsed if k]
            except Exception:
                clean = raw.strip("[]").replace('"','').replace("'",'')  
                keys = [k.strip() for k in clean.split(',') if k.strip()]
        elif raw:
            keys = [raw]
    
    single = _s("OPENROUTER_API_KEY", "")
    if single and single not in keys:
        keys.append(single)
    
    for n in ["OPENROUTER_KEY_1","OPENROUTER_KEY_2","OPENROUTER_KEY_3"]:
        k = _s(n, "")
        if k and k not in keys:
            keys.append(k)
    
    keys = [k.strip() for k in keys if k and len(k) > 20]
    return keys


def _parse_cohere_keys():
    """
    يجمع مفاتيح Cohere من أي صيغة
    """
    keys = []
    raw = _s("COHERE_API_KEYS", "")
    
    if isinstance(raw, list):
        keys = [k for k in raw if k and isinstance(k, str)]
    elif raw and isinstance(raw, str):
        raw = raw.strip()
        if raw.startswith('['):
            try:
                parsed = _json.loads(raw)
                if isinstance(parsed, list):
                    keys = [k for k in parsed if k]
            except Exception:
                clean = raw.strip("[]").replace('"','').replace("'",'')  
                keys = [k.strip() for k in clean.split(',') if k.strip()]
        elif raw:
            keys = [raw]
    
    single = _s("COHERE_API_KEY", "")
    if single and single not in keys:
        keys.append(single)
    
    for n in ["COHERE_KEY_1","COHERE_KEY_2","COHERE_KEY_3"]:
        k = _s(n, "")
        if k and k not in keys:
            keys.append(k)
    
    keys = [k.strip() for k in keys if k and len(k) > 20]
    return keys


# ══════════════════════════════════════════════
#  المفاتيح الفعلية (قائمة واحدة مُحدَّثة in-place لتعمل مع from config import *)
# ══════════════════════════════════════════════
GEMINI_API_KEYS = []
GEMINI_API_KEYS.extend(_parse_gemini_keys())
GEMINI_API_KEY = GEMINI_API_KEYS[0] if GEMINI_API_KEYS else ""


def refresh_gemini_keys():
    """إعادة قراءة المفاتيح من البيئة و Streamlit secrets بعد تهيئة التطبيق."""
    global GEMINI_API_KEY
    new_keys = _parse_gemini_keys()
    GEMINI_API_KEYS.clear()
    GEMINI_API_KEYS.extend(new_keys)
    GEMINI_API_KEY = GEMINI_API_KEYS[0] if GEMINI_API_KEYS else ""
OPENROUTER_API_KEYS = _parse_openrouter_keys()
OPENROUTER_API_KEY = OPENROUTER_API_KEYS[0] if OPENROUTER_API_KEYS else ""
COHERE_API_KEYS    = _parse_cohere_keys()
COHERE_API_KEY     = COHERE_API_KEYS[0] if COHERE_API_KEYS else ""
EXTRA_API_KEY      = _s("EXTRA_API_KEY")

# ══════════════════════════════════════════════════
#  إعدادات نظام تدوير المفاتيح (Key Rotation)
# ══════════════════════════════════════════════════
KEY_ROTATION_ENABLED = True  # تفعيل نظام تدوير المفاتيح
KEY_ROTATION_ON_429 = True   # تبديل المفتاح تلقائياً عند خطأ 429 (Rate Limit)
KEY_ROTATION_STRATEGY = "round_robin"  # استراتيجية: round_robin أو random

# ══════════════════════════════════════════════
#  Make Webhooks
# ══════════════════════════════════════════════
WEBHOOK_UPDATE_PRICES = (_s("WEBHOOK_UPDATE_PRICES") or "").strip()
WEBHOOK_NEW_PRODUCTS = (_s("WEBHOOK_NEW_PRODUCTS") or "").strip()

# ══════════════════════════════════════════════
#  ألوان
# ══════════════════════════════════════════════
COLORS = {
    "raise": "#dc3545", "lower": "#ffc107", "approved": "#28a745",
    "missing": "#007bff", "review": "#ff9800", "primary": "#6C63FF",
}

# ══════════════════════════════════════════════
#  إعدادات المطابقة
# ══════════════════════════════════════════════
MATCH_THRESHOLD    = 85
HIGH_CONFIDENCE    = 95
REVIEW_THRESHOLD   = 75
PRICE_TOLERANCE    = 5
MIN_MATCH_SCORE    = MATCH_THRESHOLD
HIGH_MATCH_SCORE   = HIGH_CONFIDENCE
PRICE_DIFF_THRESHOLD = PRICE_TOLERANCE

# ══════════════════════════════════════════════
#  فلاتر المنتجات
# ══════════════════════════════════════════════
REJECT_KEYWORDS = [
    "sample","عينة","عينه","decant","تقسيم","تقسيمة",
    "split","miniature","0.5ml","1ml","2ml","3ml",
]
TESTER_KEYWORDS = ["tester","تستر","تيستر"]
SET_KEYWORDS    = ["set","gift set","طقم","مجموعة","coffret"]

# ══════════════════════════════════════════════
#  العلامات التجارية
# ══════════════════════════════════════════════
KNOWN_BRANDS = [
    "Dior","Chanel","Gucci","Tom Ford","Versace","Armani","YSL","Prada",
    "Burberry","Givenchy","Hermes","Creed","Montblanc","Calvin Klein",
    "Hugo Boss","Dolce & Gabbana","Valentino","Bvlgari","Cartier","Lancome",
    "Jo Malone","Amouage","Rasasi","Lattafa","Arabian Oud","Ajmal",
    "Al Haramain","Afnan","Armaf","Nishane","Xerjoff","Parfums de Marly",
    "Initio","Byredo","Le Labo","Mancera","Montale","Kilian","Roja",
    "Carolina Herrera","Jean Paul Gaultier","Narciso Rodriguez",
    "Paco Rabanne","Mugler","Chloe","Coach","Michael Kors","Ralph Lauren",
    "Maison Margiela","Memo Paris","Penhaligons","Serge Lutens","Diptyque",
    "Frederic Malle","Francis Kurkdjian","Floris","Clive Christian",
    "Ormonde Jayne","Zoologist","Tauer","Lush","The Different Company",
    "Missoni","Juicy Couture","Moschino","Dunhill","Bentley","Jaguar",
    "Boucheron","Chopard","Elie Saab","Escada","Ferragamo","Fendi",
    "Kenzo","Lacoste","Loewe","Rochas","Roberto Cavalli","Tiffany",
    "Van Cleef","Azzaro",    "Lanvin","لانفين","Gres","Grès","جريس",
    "Cabotine","كابوتين",
    "Banana Republic","Benetton","Bottega Veneta",
    "Celine","Dsquared2","Ed Hardy","Elizabeth Arden","Ermenegildo Zegna",
    "Swiss Arabian","Ard Al Zaafaran","Nabeel","Asdaaf","Maison Alhambra",
    "لطافة","العربية للعود","رصاصي","أجمل","الحرمين","أرماف",
    "أمواج","كريد","توم فورد","ديور","شانيل","غوتشي","برادا",
    "ميسوني","جوسي كوتور","موسكينو","دانهيل","بنتلي",
    "كينزو","لاكوست","فندي","ايلي صعب","ازارو",
    "Guerlain","Givenchy","Sisley","Issey Miyake","Davidoff","Mexx",
]

# ══════════════════════════════════════════════
#  استبدالات التطبيع
# ══════════════════════════════════════════════
WORD_REPLACEMENTS = {
    'او دو بارفان':'edp','أو دو بارفان':'edp','او دي بارفان':'edp',
    'او دو تواليت':'edt','أو دو تواليت':'edt','او دي تواليت':'edt',
    'مل':'ml','ملي':'ml',
    'سوفاج':'sauvage','ديور':'dior','شانيل':'chanel',
    'توم فورد':'tom ford','أرماني':'armani','غيرلان':'guerlain',
}

# ══════════════════════════════════════════════
#  إعدادات الأتمتة الذكية v26.0
# ══════════════════════════════════════════════
AUTOMATION_RULES_DEFAULT = [
    {
        "name": "خفض السعر تلقائياً",
        "enabled": True,
        "condition": "our_price > comp_price",
        "min_diff": 10,       # فرق أدنى بالريال لتفعيل القاعدة
        "action": "undercut",  # خفض ليصبح أقل من المنافس
        "undercut_amount": 1,  # أقل بكم ريال
        "min_match_score": 90, # حد أدنى لنسبة التطابق
        "max_loss_pct": 15,    # أقصى نسبة خسارة مقبولة من سعر التكلفة
    },
    {
        "name": "رفع السعر عند فرصة ربح",
        "enabled": True,
        "condition": "our_price < comp_price",
        "min_diff": 15,
        "action": "raise_to_match",
        "margin_below": 5,     # أقل من المنافس بكم ريال
        "min_match_score": 90,
    },
    {
        "name": "إبقاء السعر إذا تنافسي",
        "enabled": True,
        "condition": "abs(our_price - comp_price) <= threshold",
        "threshold": 10,
        "action": "keep",
        "min_match_score": 85,
    },
]

# جدولة البحث الدوري (بالدقائق)
AUTO_SEARCH_INTERVAL_MINUTES = 60 * 6   # كل 6 ساعات
AUTO_PUSH_TO_MAKE = False               # إرسال تلقائي لـ Make.com (يتطلب تفعيل يدوي)
AUTO_DECISION_CONFIDENCE = 95           # حد الثقة للقرار التلقائي (95% لمنع الخسائر)

# ══════════════════════════════════════════════
#  أقسام التطبيق (v26.0 — مع لوحة الأتمتة)
# ══════════════════════════════════════════════
SECTIONS = [
    "📊 لوحة التحكم",
    "📂 رفع الملفات",
    "🏢 كشط المنافسين",
    "🔴 سعر أعلى",
    "🟢 سعر أقل",
    "✅ موافق عليها",
    "⚠️ تحت المراجعة",
    "🔍 منتجات مفقودة",
    "✔️ تمت المعالجة",
    "🤖 الذكاء الصناعي",
    "⚡ أتمتة Make",
    "🔄 الأتمتة الذكية",
    "⚙️ الإعدادات",
    "📜 السجل",
]
SIDEBAR_SECTIONS = SECTIONS
PAGES_PER_TABLE  = 25
DB_PATH          = "perfume_pricing.db"
