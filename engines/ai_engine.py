"""
engines/ai_engine.py v26.0 — خبير مهووس الكامل
════════════════════════════════════════════════
✅ تسجيل الأخطاء الحقيقية (لا يبتلعها)
✅ تشخيص ذاتي لكل مزود AI
✅ خبير وصف منتجات مهووس الكامل (SEO + GEO)
✅ جلب صور المنتج من Fragrantica + Google
✅ بحث ويب DuckDuckGo + Gemini Grounding
✅ تحقق AI يُصحّح القسم الخاطئ
✅ تصنيف تلقائي لقسم "تحت المراجعة"
✅ v26.0: بحث أشمل في المتاجر السعودية مع تحليل JSON دقيق
"""
import requests, json, re, time, traceback
from config import GEMINI_API_KEYS, OPENROUTER_API_KEY, COHERE_API_KEY

_GM  = "gemini-2.0-flash"  # ← النموذج المستقر الموصى به
_GU  = f"https://generativelanguage.googleapis.com/v1beta/models/{_GM}:generateContent"
_OR  = "https://openrouter.ai/api/v1/chat/completions"
_CO  = "https://api.cohere.ai/v1/generate"

# ── سجل الأخطاء الأخيرة (يُعرض في صفحة التشخيص) ─────────────────────────
_LAST_ERRORS: list = []

def _log_err(source: str, msg: str):
    global _LAST_ERRORS
    entry = f"[{source}] {msg}"
    _LAST_ERRORS = ([entry] + _LAST_ERRORS)[:10]  # آخر 10 أخطاء

def get_last_errors() -> list:
    return _LAST_ERRORS.copy()

# ── تشخيص شامل لجميع مزودي AI ─────────────────────────────────────────────
def diagnose_ai_providers() -> dict:
    """
    يختبر كل مزود ويُعيد تقريراً مفصلاً بالأخطاء الحقيقية.
    يُستدعى من صفحة الإعدادات.
    """
    results = {}

    # ── Gemini ────────────────────────────────────────────────────────────
    gemini_results = []
    for i, key in enumerate(GEMINI_API_KEYS or []):
        if not key:
            gemini_results.append({"key": i+1, "status": "❌ مفتاح فارغ"})
            continue
        try:
            payload = {
                "contents": [{"parts": [{"text": "test"}]}],
                "generationConfig": {"maxOutputTokens": 5}
            }
            r = requests.post(f"{_GU}?key={key}", json=payload, timeout=15)
            if r.status_code == 200:
                gemini_results.append({"key": i+1, "status": "✅ يعمل"})
            elif r.status_code == 400:
                try: msg = r.json().get("error",{}).get("message","Bad Request")
                except: msg = r.text[:100]
                gemini_results.append({"key": i+1, "status": f"❌ 400 — {msg[:80]}"})
            elif r.status_code == 403:
                gemini_results.append({"key": i+1, "status": "❌ 403 — مفتاح غير مصرح أو IP محظور"})
            elif r.status_code == 429:
                gemini_results.append({"key": i+1, "status": "⚠️ 429 — تجاوز الحد (Rate Limit)"})
            elif r.status_code == 404:
                gemini_results.append({"key": i+1, "status": f"❌ 404 — النموذج {_GM} غير متاح"})
            else:
                try: msg = r.json().get("error",{}).get("message","")
                except: msg = r.text[:100]
                gemini_results.append({"key": i+1, "status": f"❌ {r.status_code} — {msg[:80]}"})
        except requests.exceptions.ConnectionError as e:
            gemini_results.append({"key": i+1, "status": f"❌ لا يوجد اتصال بالإنترنت أو جدار حماية: {str(e)[:60]}"})
        except requests.exceptions.Timeout:
            gemini_results.append({"key": i+1, "status": "❌ انتهت المهلة (Timeout 15s)"})
        except Exception as e:
            gemini_results.append({"key": i+1, "status": f"❌ خطأ: {str(e)[:80]}"})
    results["gemini"] = gemini_results

    # ── OpenRouter ────────────────────────────────────────────────────────
    if OPENROUTER_API_KEY:
        try:
            r = requests.post(_OR, json={
                "model": "google/gemini-2.0-flash",  # ← مستقر
                "messages": [{"role":"user","content":"test"}],
                "max_tokens": 5
            }, headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "HTTP-Referer": "https://mahwous.com"
            }, timeout=15)
            if r.status_code == 200:
                results["openrouter"] = "✅ يعمل"
            elif r.status_code == 401:
                results["openrouter"] = "❌ 401 — مفتاح OpenRouter غير صحيح"
            elif r.status_code == 402:
                results["openrouter"] = "❌ 402 — رصيد OpenRouter منتهٍ"
            elif r.status_code == 429:
                results["openrouter"] = "⚠️ 429 — تجاوز الحد"
            else:
                try: msg = r.json().get("error",{}).get("message","")
                except: msg = r.text[:100]
                results["openrouter"] = f"❌ {r.status_code} — {msg[:80]}"
        except requests.exceptions.ConnectionError:
            results["openrouter"] = "❌ لا اتصال بـ openrouter.ai — قد يكون محظوراً"
        except requests.exceptions.Timeout:
            results["openrouter"] = "❌ Timeout"
        except Exception as e:
            results["openrouter"] = f"❌ {str(e)[:80]}"
    else:
        results["openrouter"] = "⚠️ مفتاح غير موجود"

    # ── Cohere ────────────────────────────────────────────────────────────
    if COHERE_API_KEY:
        try:
            r = requests.post("https://api.cohere.com/v2/chat", json={
                "model": "command-a-03-2025",
                "messages": [{"role": "user", "content": "test"}],
            }, headers={
                "Authorization": f"Bearer {COHERE_API_KEY}",
                "Content-Type": "application/json",
            }, timeout=15)
            if r.status_code == 200:
                results["cohere"] = "✅ يعمل (command-a-03-2025)"
            elif r.status_code == 401:
                results["cohere"] = "❌ 401 — مفتاح Cohere غير صحيح"
            elif r.status_code == 402:
                results["cohere"] = "❌ 402 — رصيد Cohere منتهٍ"
            else:
                try: msg = r.json().get("message","")
                except: msg = r.text[:100]
                results["cohere"] = f"❌ {r.status_code} — {msg[:80]}"
        except requests.exceptions.ConnectionError:
            results["cohere"] = "❌ لا اتصال بـ api.cohere.com"
        except Exception as e:
            results["cohere"] = f"❌ {str(e)[:80]}"
    else:
        results["cohere"] = "⚠️ مفتاح غير موجود"

    return results

# ══ خبير وصف منتجات مهووس الكامل ══════════════════════════════════════════
MAHWOUS_EXPERT_SYSTEM = """أنت خبير عالمي في كتابة أوصاف منتجات العطور محسّنة لمحركات البحث التقليدية (Google SEO) ومحركات بحث الذكاء الصناعي (GEO/AIO). تعمل حصرياً لمتجر "مهووس" (Mahwous) - الوجهة الأولى للعطور الفاخرة في السعودية.---## هويتك ومهمتك**من أنت:**- خبير عطور محترف مع 15+ سنة خبرة في صناعة العطور الفاخرة- متخصص في SEO و Generative Engine Optimization (GEO)- كاتب محتوى عربي بارع بأسلوب راقٍ، ودود، عاطفي، وتسويقي مقنع- تمثل صوت متجر "مهووس" بكل احترافية وثقة**مهمتك:**كتابة أوصاف منتجات عطور شاملة، احترافية، ومحسّنة بشكل علمي صارم لتحقيق:1. تصدر نتائج البحث في Google (الصفحة الأولى)2. الظهور في إجابات محركات بحث الذكاء الصناعي (ChatGPT, Gemini, Perplexity)3. زيادة معدل التحويل (Conversion Rate) بنسبة 40-60%4. تعزيز ثقة العملاء (E-E-A-T: Experience, Expertise, Authoritativeness, Trustworthiness)---## القواعد العلمية الصارمة للكلمات المفتاحية### 1. هرمية الكلمات المفتاحية (إلزامية)**المستوى 1: الكلمة الرئيسية (Primary Keyword)**- الصيغة: "عطر [الماركة] [اسم العطر] [التركيز] [الحجم] [للجنس]"- مثال: "عطر أكوا دي بارما كولونيا إنتنسا أو دو كولون 180 مل للرجال"- التكرار: 5-7 مرات في وصف 1200 كلمة- الكثافة: 1.5-2%- المواقع الإلزامية:  * H1 (العنوان الرئيسي)  * أول 50 كلمة  * آخر 100 كلمة  * 2-3 عناوين فرعية  * قسم "لمسة خبير"**المستوى 2: الكلمات الثانوية (3 كلمات)**- أمثلة: "عطر رجالي خشبي"، "عطر فاخر ثابت"، "عطر رجالي للمكتب"- التكرار: 3-5 مرات لكل كلمة- الكثافة: 0.5-1% لكل كلمة- المواقع: العناوين الفرعية، النقاط النقطية، الفقرات الوصفية**المستوى 3: الكلمات الدلالية (LSI) (10-15 كلمة)**- الفئات:  * صفات: فاخر، راقٍ، أنيق، كلاسيكي، ثابت، فواح  * مكونات: برغموت، جلد، خشب الأرز، مسك، باتشولي  * أحاسيس: دافئ، منعش، حار، حمضي، ذكوري  * مناسبات: مكتب، رسمي، يومي، مساء، صيف، شتاء- التكرار: 2-3 مرات لكل كلمة- الكثافة: 0.3-0.5% لكل كلمة**المستوى 4: الكلمات الحوارية (5-8 عبارات)**- الأنماط:  * "أبحث عن عطر رجالي خشبي ثابت للعمل"  * "ما هو أفضل عطر رجالي حمضي للصيف"  * "هل يناسب [اسم العطر] الاستخدام اليومي"  * "الفرق بين EDC و EDP"- المواقع: FAQ، قسم "لمسة خبير"### 2. خريطة المواقع الاستراتيجية (إلزامية)**الأولوية القصوى (Critical Zones):****H1 (العنوان الرئيسي):**- يجب أن يطابق الكلمة الرئيسية 100%- صيغة: "عطر [الماركة] [اسم العطر] [التركيز] [الحجم] [للجنس]"**أول 100 كلمة (The Golden Paragraph):**- الكلمة الرئيسية في أول 50 كلمة- كلمة ثانوية واحدة على الأقل- 2-3 كلمات دلالية- أسلوب عاطفي جذاب- دعوة مبكرة للشراء- مثال: "قوة الحمضيات وعمق الجلد، توقيع خشبي فاخر للرجل الأنيق. عطر [الاسم الكامل] هو تحفة عطرية [جنسية الماركة] تجمع بين [مكون 1] و[مكون 2]. صدر عام [السنة] بتوقيع [المصمم]، ليمنحك حضوراً راقياً وثباتاً استثنائياً. هذا العطر [الجنس] الفاخر متوفر الآن حصرياً لدى مهووس بأفضل سعر. اشترِه الآن!"**العناوين الفرعية (H2/H3):**- 60% من العناوين يجب أن تحتوي على كلمات مفتاحية- أمثلة:  * "لماذا تختار عطر [الاسم] [الجنس]؟"  * "رحلة العطر: اكتشف الهرم العطري [العائلة العطرية] الفاخر"  * "متى وأين ترتدي هذا العطر [الجنس] الأنيق؟"  * "لمسة خبير من مهووس: تقييم احترافي لعطر [الاسم]"**النقاط النقطية:**- كل نقطة تبدأ بكلمة مفتاحية بولد- مثال: "**عطر رجالي خشبي فاخر:** يجمع بين..."**قسم FAQ:**- 6-8 أسئلة- كل سؤال = كلمة مفتاحية حوارية- الإجابة تكرر الكلمة المفتاحية مرة واحدة- الإجابة مفصلة (50-80 كلمة)**الفقرة الختامية (آخر 100 كلمة):**- الكلمة الرئيسية مرتين- كلمة ثانوية واحدة- دعوة قوية للشراء- تعزيز الثقة: "أصلي 100%"، "ضمان"، "آلاف العملاء"- الشعار: "عالمك العطري يبدأ من مهووس"---## بنية الوصف الإلزامية**الطول الإجمالي: 1200-1500 كلمة**### 1. الفقرة الافتتاحية (100-150 كلمة)- جملة افتتاحية عاطفية قوية- الكلمة الرئيسية كاملة في أول 50 كلمة- معلومات أساسية: الماركة، المصمم، سنة الإصدار، العائلة العطرية- دعوة مبكرة للشراء### 2. تفاصيل المنتج (نقاط نقطية)**العنوان:** "تفاصيل المنتج"- الماركة (مع رابط داخلي)- اسم العطر- المصمم/الموقّع- الجنس- العائلة العطرية- الحجم- التركيز- سنة الإصدار### 3. رحلة العطر: الهرم العطري (200-250 كلمة)**العنوان:** "رحلة العطر: اكتشف الهرم العطري [العائلة] الفاخر"- **النفحات العليا (Top Notes):** وصف حسي + المكونات- **النفحات الوسطى (Heart Notes):** وصف حسي + المكونات- **النفحات الأساسية (Base Notes):** وصف حسي + المكونات + معلومات الثبات**القاعدة:** استخدم لغة حسية عاطفية، ليس مجرد قائمة مكونات.### 4. لماذا تختار هذا العطر؟ (200-250 كلمة)**العنوان:** "لماذا تختار عطر [الاسم] [الجنس]؟"- 4-6 نقاط نقطية- كل نقطة تبدأ بكلمة مفتاحية بولد- تركز على الفوائد (Benefits) وليس الميزات (Features)- أمثلة:  * **توقيع عطري فريد:** ...  * **ثبات استثنائي طوال اليوم:** ...  * **حجم اقتصادي:** ...  * **مثالي للمكتب والمناسبات:** ...  * **عطر أصلي بسعر مميز:** ...### 5. متى وأين ترتدي هذا العطر؟ (150-200 كلمة) [جديد]**العنوان:** "متى وأين ترتدي عطر [الاسم] [الجنس]؟"- **الفصول المناسبة:** (مع تفسير)- **الأوقات المثالية:** (صباح، مساء، ليل)- **المناسبات:** (عمل، رسمي، كاجوال، رومانسي)- **الفئة العمرية:** (إن كان ذلك مناسباً)### 6. لمسة خبير من مهووس (200-250 كلمة) [إلزامي]**العنوان:** "لمسة خبير من مهووس: تقييمنا الاحترافي"- **الافتتاحية:** "بعد تجربتنا المعمقة لعطر [الاسم]، يمكننا القول بثقة..."- **التحليل الحسي:** وصف الافتتاحية، القلب، القاعدة من منظور الخبير- **الأداء:** الثبات (بالساعات)، الفوحان (ضعيف/متوسط/قوي)، الإسقاط- **المقارنات:** "إذا كنت من محبي [عطر مشابه 1] أو [عطر مشابه 2]، فإن [الاسم] سيكون..."- **التوصية:** "لمن نوصي به؟"- **نصيحة الخبير:** نصيحة عملية لأفضل استخدام**القاعدة:** استخدم ضمير "نحن"، اذكر تجربة فعلية، قدم نصيحة احترافية.### 7. الأسئلة الشائعة (FAQ) (250-300 كلمة)**العنوان:** "الأسئلة الشائعة حول عطر [الاسم]"- **6-8 أسئلة** (كل سؤال = كلمة مفتاحية حوارية)- أسئلة إلزامية:  1. "هل عطر [الاسم] مناسب للاستخدام اليومي في [المكان]؟"  2. "ما الفرق بين [التركيز الحالي] و[تركيز آخر]؟"  3. "ما هي مدة ثبات عطر [الاسم] على البشرة؟"  4. "هل يتوفر عطر [الاسم] كـ تستر؟"  5. "ما هو الفصل الأنسب لاستخدام عطر [الاسم]؟"  6. "هل عطر [الاسم] مناسب للمناسبات الرسمية؟"- أسئلة اختيارية:  7. "ما هي أفضل طريقة لرش عطر [الاسم] لأطول ثبات؟"  8. "هل يمكن دمج عطر [الاسم] مع عطور أخرى (Layering)؟"**القاعدة:** الإجابة 50-80 كلمة، تبدأ بـ "نعم/لا" عندما يكون مناسباً، تكرر الكلمة المفتاحية مرة واحدة.### 8. اكتشف أكثر من مهووس (100-120 كلمة)**العنوان:** "اكتشف المزيد من عطور [الجنس/الفئة]"- 3-5 روابط داخلية- كل رابط = Anchor Text محسّن (كلمة مفتاحية)- أمثلة:  * "تسوق المزيد من [عطور رجالية خشبية فاخرة](رابط)"  * "اكتشف [أفضل عطور [الماركة] للرجال](رابط)"  * "تصفح [عطور التستر الأصلية بأسعار مميزة](رابط)"  * "استكشف [عطور النيش الحصرية](رابط)"- **رابط خارجي واحد** (إلزامي):  * "اقرأ المزيد عن عطر [الاسم] على [Fragrantica Arabia](https://www.fragranticarabia.com/...)"### 9. الفقرة الختامية (80-100 كلمة)**العنوان:** "عالمك العطري يبدأ من مهووس"- الكلمة الرئيسية مرتين- كلمة ثانوية واحدة- تعزيز الثقة: "أصلي 100%"، "ضمان الأصالة"، "توصيل سريع"، "آلاف العملاء الراضين"- دعوة قوية للشراء: "اطلب الآن"، "اشترِ الآن"- الشعار: "عالمك العطري يبدأ من مهووس"---## الأسلوب الكتابي (إلزامي)### المزيج المطلوب:1. **راقٍ ومحترف** (40%): لغة فصحى سليمة، مصطلحات عطرية دقيقة2. **ودود وقريب** (25%): خطاب مباشر بضمير "أنت"، أسلوب محادثة3. **عاطفي ورومانسي** (20%): أوصاف حسية، استحضار مشاعر ومشاهد4. **تسويقي ومقنع** (15%): دعوات للشراء، تعزيز الثقة، خلق حاجة### القواعد الأسلوبية:- **لا تستخدم الإيموجي** (غير احترافي)- **استخدم Bold** للكلمات المفتاحية المهمة (لا تبالغ)- **تجنب التكرار الممل:** استخدم مرادفات- **اكتب بطبيعية:** لا حشو للكلمات المفتاحية- **استخدم أرقام وإحصائيات:** "ثبات 7-9 ساعات"، "فوحان متوسط إلى قوي"---## التعامل مع المدخلات### إذا أعطاك المستخدم:**1. معلومات كاملة (الاسم، الماركة، الحجم، السعر، الروابط):**- اكتب الوصف مباشرة بدون أسئلة- استخدم المعلومات المقدمة- ابحث في Fragrantica Arabia عن باقي التفاصيل**2. معلومات ناقصة (فقط الاسم والماركة):**- ابحث في Fragrantica Arabia عن:  * المصمم  * سنة الإصدار  * العائلة العطرية  * الهرم العطري  * الحجم الأكثر مبيعاً (إذا لم يحدد المستخدم)- ابحث في Google عن السعر التقريبي في السوق السعودي- اكتب الوصف بناءً على ما وجدته**3. فقط اسم العطر (بدون ماركة):**- ابحث في Google و Fragrantica لتحديد الماركة- ثم اتبع الخطوة 2### مصادر البحث (بالترتيب):1. **Fragrantica Arabia** (https://www.fragranticarabia.com/) - المصدر الأساسي2. **Google Search** - للأسعار والمعلومات الإضافية3. **موقع الماركة الرسمي** - للمعلومات الدقيقة---## التنسيق النهائي (إلزامي)### المخرجات يجب أن تكون:1. **جاهزة للنسخ واللصق مباشرة** (بدون شرح أو تعليمات)2. **بصيغة Markdown** مع العناوين والتنسيق3. **منظمة بالترتيب المذكور أعلاه**4. **الروابط جاهزة** (إذا قدمها المستخدم)### لا ترسل:- ❌ "هذا هو الوصف..."- ❌ "يمكنك نسخ..."- ❌ "ملاحظة: ..."- ❌ أي تعليمات أو شرح### فقط أرسل:- ✅ الوصف الكامل جاهز للاستخدام---## جدول التحقق النهائي (تحقق قبل الإرسال)قبل إرسال أي وصف، تأكد من:**الكلمات المفتاحية:**- [ ] الكلمة الرئيسية في H1- [ ] الكلمة الرئيسية في أول 50 كلمة- [ ] الكلمة الرئيسية في آخر 100 كلمة- [ ] الكلمة الرئيسية تكررت 5-7 مرات- [ ] 3 كلمات ثانوية (كل واحدة 3-5 مرات)- [ ] 10-15 كلمة دلالية (كل واحدة 2-3 مرات)- [ ] 5-8 عبارات حوارية في FAQ**البنية:**- [ ] الطول: 1200-1500 كلمة- [ ] 9 أقسام رئيسية (حسب البنية أعلاه)- [ ] قسم "لمسة خبير من مهووس" موجود- [ ] قسم "متى وأين ترتدي" موجود- [ ] FAQ يحتوي على 6-8 أسئلة- [ ] 3-5 روابط داخلية- [ ] 1 رابط خارجي (Fragrantica)**الأسلوب:**- [ ] مزيج: راقٍ + ودود + عاطفي + تسويقي- [ ] لا إيموجي- [ ] Bold للكلمات المهمة (بدون مبالغة)- [ ] 

## قواعد صارمة:
- اكتب باللغة العربية فقط
- الطول: 1200-1500 كلمة
- لا تختلق مكونات أو بيانات — ابنِ على الاسم فقط
- شخصيتك: الرجل الأنيق بالبدلة والغترة، خبير عطور متحمس
- لا تكتب JSON أو أكواد — نص مقروء فقط
"""

# ══ System Prompts للأقسام ══════════════════════════════════════════════════
PAGE_PROMPTS = {
"price_raise": """انت خبير تسعير عطور فاخرة (السوق السعودي) قسم سعر اعلى.
سعرنا اعلى من المنافس. قواعد: فرق<10 ابقاء | 10-30 مراجعة | >30 خفض فوري.
لكل منتج: 1.هل المطابقة صحيحة؟ 2.هل الفرق مبرر؟ 3.السعر المقترح.
اجب بالعربية بايجاز واحترافية.""",
"price_lower": """انت خبير تسعير عطور فاخرة (السوق السعودي) قسم سعر اقل.
سعرنا اقل من المنافس = فرصة ربح ضائعة. فرق<10 ابقاء | 10-50 رفع تدريجي | >50 رفع فوري.
لكل منتج: 1.هل يمكن رفع السعر؟ 2.السعر الامثل. اجب بالعربية بايجاز.""",
"approved": "انت خبير تسعير عطور. راجع المنتجات الموافق عليها وتاكد من استمرار صلاحيتها. اجب بالعربية.",
"missing": """انت خبير عطور فاخرة متخصص في المنتجات المفقودة بمتجر مهووس.
لكل منتج: 1.هل هو حقيقي وموثوق؟ 2.هل يستحق الاضافة؟ 3.السعر المقترح. 4.اولوية الاضافة (عالية/متوسطة/منخفضة). اجب بالعربية.""",
"review": """انت خبير تسعير عطور. هذه منتجات بمطابقة غير مؤكدة.
لكل منتج: هل هما نفس العطر فعلا؟ نعم / لا / غير متاكد. اشرح السبب. اجب بالعربية.""",
"extract": """أنت خبير بيانات عطور صارم. اتبع تعليمات المستخدم فقط.
أجب بـ JSON صالح فقط دون أي نص قبله أو بعده أو markdown أو ```.""",
"general": """انت مساعد ذكاء اصطناعي متخصص في تسعير العطور الفاخرة والسوق السعودي.
خبرتك: تحليل الاسعار، المنافسة، استراتيجيات التسعير، مكونات العطور.
اجب بالعربية باحترافية وايجاز يمكنك استخدام markdown.""",
"verify": """انت خبير تحقق من منتجات العطور دقيق جدا.
تحقق من: الماركة + اسم المنتج + الحجم (ml) + النوع (EDP/EDT) + الجنس.
اجب JSON فقط بدون اي نص اضافي:
{"match":true/false,"confidence":0-100,"reason":"سبب واضح","correct_section":"احد الاقسام","suggested_price":0}""",
"market_search": """انت محلل اسعار عطور (السوق السعودي) تبحث في الانترنت.
اجب JSON فقط:
{"market_price":0,"price_range":{"min":0,"max":0},"competitors":[{"name":"","price":0}],"recommendation":"","confidence":0}""",
"reclassify": """انت نظام تصنيف دقيق لمنتجات العطور.
القسم الصحيح:
- سعر اعلى: نفس المنتج وسعرنا اعلى باكثر من 10 ريال
- سعر اقل: نفس المنتج وسعرنا اقل باكثر من 10 ريال
- موافق: الفرق 10 ريال او اقل + المطابقة صحيحة
- مفقود: المنتج غير موجود لدينا فعلا
اجب JSON فقط:
{"results":[{"idx":1,"section":"القسم","confidence":85,"match":true,"reason":""},...]}"""
}

# ══ استدعاءات AI ═══════════════════════════════════════════════════════════
def _call_gemini(prompt, system="", grounding=False, temperature=0.3, max_tokens=8192):
    full = f"{system}\n\n{prompt}" if system else prompt
    payload = {
        "contents": [{"parts": [{"text": full}]}],
        "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens, "topP": 0.85}
    }
    if grounding:
        payload["tools"] = [{"google_search": {}}]

    if not GEMINI_API_KEYS:
        _log_err("Gemini", "لا توجد مفاتيح API")
        return None

    for i, key in enumerate(GEMINI_API_KEYS):
        if not key:
            continue
        try:
            r = requests.post(f"{_GU}?key={key}", json=payload, timeout=45)
            if r.status_code == 200:
                data = r.json()
                if data.get("candidates"):
                    parts = data["candidates"][0]["content"]["parts"]
                    return "".join(p.get("text","") for p in parts)
                else:
                    # blocked / safety filter
                    reason = data.get("promptFeedback",{}).get("blockReason","")
                    _log_err("Gemini", f"مفتاح {i+1}: لا نتائج — {reason}")
            elif r.status_code == 429:
                _log_err("Gemini", f"مفتاح {i+1}: Rate Limit (429) — انتظار 2 ثانية")
                time.sleep(2)  # ← 2 ثانية للـ 429
                continue
            elif r.status_code == 403:
                _log_err("Gemini", f"مفتاح {i+1}: IP محظور أو مفتاح غير مصرح (403)")
            elif r.status_code == 404:
                _log_err("Gemini", f"مفتاح {i+1}: نموذج غير متاح {_GM} (404)")
            else:
                try:
                    msg = r.json().get("error",{}).get("message","")
                except Exception:
                    msg = r.text[:100]
                _log_err("Gemini", f"مفتاح {i+1}: {r.status_code} — {msg[:80]}")
        except requests.exceptions.ConnectionError as e:
            _log_err("Gemini", f"مفتاح {i+1}: لا اتصال — {str(e)[:80]}")
        except requests.exceptions.Timeout:
            _log_err("Gemini", f"مفتاح {i+1}: Timeout (45s)")
        except Exception as e:
            _log_err("Gemini", f"مفتاح {i+1}: {str(e)[:80]}")
    return None

def _call_openrouter(prompt, system="", temperature=0.3):
    if not OPENROUTER_API_KEY:
        return None

    # نماذج مجانية صحيحة (محدَّثة مارس 2026)
    # نماذج مستقرة فقط — بدون النماذج التجريبية (exp)
    FREE_MODELS = [
        "meta-llama/llama-3.3-70b-instruct:free",
        "deepseek/deepseek-chat-v3-0324:free",
        "mistralai/mistral-7b-instruct:free",
        "qwen/qwen-2.5-72b-instruct:free",
        "google/gemma-3-27b-it:free",
    ]

    msgs = []
    if system:
        msgs.append({"role": "system", "content": system})
    msgs.append({"role": "user", "content": prompt})

    for model in FREE_MODELS:
        try:
            r = requests.post(_OR, json={
                "model": model,
                "messages": msgs,
                "temperature": temperature,
                "max_tokens": 8192
            }, headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "HTTP-Referer": "https://mahwous.com",
                "X-Title": "Mahwous"
            }, timeout=45)

            if r.status_code == 200:
                content = r.json()["choices"][0]["message"]["content"]
                if content and content.strip():
                    return content
            elif r.status_code == 429:
                _log_err("OpenRouter", f"{model}: Rate Limit (429) — انتظار 2 ثانية")
                time.sleep(2)  # ← 2 ثانية للـ 429
                continue
            elif r.status_code == 402:
                _log_err("OpenRouter", f"{model}: رصيد منتهٍ (402) — جرب النموذج التالي")
                continue
            elif r.status_code == 401:
                _log_err("OpenRouter", "مفتاح غير صحيح (401)")
                return None  # لا فائدة من تجربة نماذج أخرى
            else:
                try:
                    msg = r.json().get("error", {}).get("message", "")
                except Exception:
                    msg = r.text[:100]
                _log_err("OpenRouter", f"{model}: {r.status_code} — {msg[:80]}")
                continue

        except requests.exceptions.ConnectionError as e:
            _log_err("OpenRouter", f"لا اتصال — {str(e)[:80]}")
            return None  # إذا لا اتصال، لا فائدة من تجربة نماذج أخرى
        except requests.exceptions.Timeout:
            _log_err("OpenRouter", f"{model}: Timeout (45s)")
            continue
        except Exception as e:
            _log_err("OpenRouter", f"{model}: {str(e)[:80]}")
            continue

    return None

def _call_cohere(prompt, system="", temperature=0.3):
    """
    Cohere — Fallback صامت فقط.
    أي خطأ (401/402/429/...) يُسجَّل ويُعاد None بدون إيقاف سير العمل.
    """
    if not COHERE_API_KEY:
        return None
    try:
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        r = requests.post(
            "https://api.cohere.com/v2/chat",
            json={"model": "command-r-plus", "messages": messages, "temperature": temperature},
            headers={"Authorization": f"Bearer {COHERE_API_KEY}",
                     "Content-Type": "application/json"},
            timeout=30
        )
        if r.status_code == 200:
            data = r.json()
            return data.get("message", {}).get("content", [{}])[0].get("text", "")
        elif r.status_code == 401:
            _log_err("Cohere", "مفتاح غير صحيح (401) — تجاوز Cohere")
            return None  # ← لا يوقف العمل، يمرر للـ fallback التالي
        elif r.status_code in (402, 403):
            _log_err("Cohere", f"غير مصرح ({r.status_code}) — تجاوز")
            return None
        elif r.status_code == 429:
            _log_err("Cohere", "Rate Limit (429) — انتظار 2 ثانية")
            time.sleep(2)
            return None
        else:
            try:   msg = r.json().get("message", "")
            except Exception: msg = r.text[:100]
            _log_err("Cohere", f"{r.status_code} — {msg[:80]}")
    except Exception as e:
        _log_err("Cohere", f"Fallback صامت — {str(e)[:60]}")
    return None

def _parse_json(txt):
    if not txt: return None
    try:
        clean = re.sub(r'```json|```','',txt).strip()
        s = clean.find('{'); e = clean.rfind('}')+1
        if s >= 0 and e > s:
            return json.loads(clean[s:e])
    except: pass
    return None

def _search_ddg(query, num_results=5):
    """بحث DuckDuckGo مجاني"""
    try:
        r = requests.get("https://api.duckduckgo.com/", params={
            "q": query, "format": "json", "no_html": "1", "skip_disambig": "1"
        }, timeout=8)
        if r.status_code == 200:
            data = r.json()
            results = []
            if data.get("AbstractText"):
                results.append({"snippet": data["AbstractText"], "url": data.get("AbstractURL","")})
            for rel in data.get("RelatedTopics", [])[:num_results]:
                if isinstance(rel, dict) and rel.get("Text"):
                    results.append({"snippet": rel.get("Text",""), "url": rel.get("FirstURL","")})
            return results
    except: pass
    return []

def call_ai(prompt, page="general", temperature=0.3):
    sys = PAGE_PROMPTS.get(page, PAGE_PROMPTS["general"])
    for fn, src in [
        (lambda: _call_gemini(prompt, sys, temperature=temperature), "Gemini"),
        (lambda: _call_openrouter(prompt, sys, temperature=temperature), "OpenRouter"),
        (lambda: _call_cohere(prompt, sys, temperature=temperature), "Cohere")
    ]:
        r = fn()
        if r: return {"success":True,"response":r,"source":src}
    return {"success":False,"response":"فشل الاتصال بجميع مزودي AI","source":"none"}

# ══ Gemini Chat ══════════════════════════════════════════════════════════════
def gemini_chat(message, history=None, system_extra=""):
    sys = PAGE_PROMPTS["general"]
    if system_extra:
        sys = f"{sys}\n\nسياق: {system_extra}"
    needs_web = any(k in message.lower() for k in ["سعر","price","كم","متوفر","يباع","market","سوق","الان","اليوم","حالي","اخر","جديد"])
    contents = []
    for h in (history or [])[-12:]:
        contents.append({"role":"user","parts":[{"text":h["user"]}]})
        contents.append({"role":"model","parts":[{"text":h["ai"]}]})
    contents.append({"role":"user","parts":[{"text":f"{sys}\n\n{message}"}]})
    payload = {"contents":contents,
               "generationConfig":{"temperature":0.4,"maxOutputTokens":4096,"topP":0.9}}
    if needs_web:
        payload["tools"] = [{"google_search":{}}]
    for key in GEMINI_API_KEYS:
        if not key: continue
        try:
            r = requests.post(f"{_GU}?key={key}", json=payload, timeout=40)
            if r.status_code == 200:
                data = r.json()
                if data.get("candidates"):
                    parts = data["candidates"][0]["content"]["parts"]
                    text = "".join(p.get("text","") for p in parts)
                    return {"success":True,"response":text,
                            "source":"Gemini Flash" + (" + بحث ويب" if needs_web else "")}
            elif r.status_code == 429:
                time.sleep(1); continue
        except: continue
    r = _call_openrouter(message, sys)
    if r: return {"success":True,"response":r,"source":"OpenRouter"}
    return {"success":False,"response":"فشل الاتصال","source":"none"}

# ══ جلب صور المنتج من مصادر متعددة ══════════════════════════════════════════
def fetch_product_images(product_name, brand=""):
    """
    يجلب روابط صور المنتج من:
    1. Fragrantica Arabia (المصدر الأساسي)
    2. Google Images عبر Gemini Grounding
    3. DuckDuckGo كبديل
    يُرجع: {"images": [{"url":"...","source":"...","alt":"..."}], "fragrantica_url": "..."}
    """
    images = []
    fragrantica_url = ""

    # ── 1. Fragrantica Arabia (أفضل مصدر) ────────────────────────────────
    prompt_frag = f"""ابحث عن العطر "{product_name}" في موقع fragranticarabia.com وابحث أيضاً في fragrantica.com

أريد فقط:
1. رابط URL مباشر للصورة الرئيسية للعطر (يجب أن يكون رابط صورة حقيقي ينتهي بـ .jpg أو .png أو .webp)
2. روابط صور إضافية إذا وجدت (2-3 صور)
3. رابط صفحة المنتج على Fragrantica Arabia

أجب JSON فقط:
{{
  "main_image": "رابط URL الصورة الرئيسية المباشر",
  "extra_images": ["رابط2", "رابط3"],
  "fragrantica_url": "رابط الصفحة",
  "found": true/false
}}"""

    txt_frag = _call_gemini(prompt_frag, grounding=True)
    if txt_frag:
        data = _parse_json(txt_frag)
        if data and data.get("found") and data.get("main_image"):
            main = data["main_image"]
            if main and main.startswith("http") and any(ext in main.lower() for ext in [".jpg",".png",".webp",".jpeg"]):
                images.append({"url": main, "source": "Fragrantica Arabia", "alt": product_name})
            for extra in data.get("extra_images", []):
                if extra and extra.startswith("http") and len(images) < 4:
                    images.append({"url": extra, "source": "Fragrantica", "alt": product_name})
            fragrantica_url = data.get("fragrantica_url", "")

    # ── 2. Google Images عبر Gemini ───────────────────────────────────────
    if len(images) < 2:
        search_q = f"{product_name} {brand} perfume bottle official image site:sephora.com OR site:nocibé.fr OR site:parfumdreams.com"
        prompt_google = f"""ابحث عن صور المنتج: "{product_name}"
أريد روابط URL مباشرة لصور زجاجة العطر من المتاجر الرسمية مثل Sephora أو الموقع الرسمي للماركة.
الروابط يجب أن تنتهي بـ .jpg أو .png أو .webp وتكون صور حقيقية للمنتج.
أجب JSON: {{"images": ["رابط1","رابط2","رابط3"], "sources": ["مصدر1","مصدر2","مصدر3"]}}"""

        txt_google = _call_gemini(prompt_google, grounding=True)
        if txt_google:
            data2 = _parse_json(txt_google)
            if data2 and data2.get("images"):
                sources = data2.get("sources", [])
                for i, img_url in enumerate(data2["images"][:3]):
                    if img_url and img_url.startswith("http") and len(images) < 4:
                        src = sources[i] if i < len(sources) else "Google"
                        images.append({"url": img_url, "source": src, "alt": product_name})

    # ── 3. DuckDuckGo كبديل ───────────────────────────────────────────────
    if not images:
        ddg = _search_ddg(f"{product_name} perfume official image fragrantica")
        for r in ddg[:3]:
            url = r.get("url","")
            if url and any(ext in url.lower() for ext in [".jpg",".png",".webp"]):
                images.append({"url": url, "source": "DuckDuckGo", "alt": product_name})
                if len(images) >= 2: break

    # ── إذا لم نجد صور مباشرة، نُعيد رابط بحث ──────────────────────────
    if not images:
        search_url = f"https://www.fragranticarabia.com/?s={requests.utils.quote(product_name)}"
        images.append({
            "url": search_url,
            "source": "بحث Fragrantica",
            "alt": product_name,
            "is_search": True
        })

    return {
        "images": images,
        "fragrantica_url": fragrantica_url,
        "success": len(images) > 0
    }

# ══ جلب معلومات Fragrantica Arabia الكاملة ══════════════════════════════════
def fetch_fragrantica_info(product_name):
    """جلب صورة + مكونات + وصف من Fragrantica Arabia"""
    prompt = f"""ابحث عن العطر "{product_name}" في موقع fragranticarabia.com

احتاج:
1. رابط صورة المنتج المباشر (.jpg/.png/.webp)
2. مكونات العطر (top notes, middle notes, base notes)
3. وصف قصير بالعربية
4. الماركة والنوع (EDP/EDT) والحجم
5. رابط الصفحة

اجب JSON فقط:
{{
  "image_url": "رابط الصورة المباشر",
  "top_notes": ["مكون1","مكون2"],
  "middle_notes": ["مكون1","مكون2"],
  "base_notes": ["مكون1","مكون2"],
  "description_ar": "وصف قصير بالعربية",
  "brand": "",
  "type": "",
  "size": "",
  "year": "",
  "designer": "",
  "fragrance_family": "",
  "fragrantica_url": "رابط الصفحة"
}}"""

    txt = _call_gemini(prompt, grounding=True)
    if not txt: txt = _call_gemini(prompt)
    if not txt: return {"success":False}

    data = _parse_json(txt)
    if data: return {"success":True, **data}
    return {"success":False,"description_ar":txt[:200] if txt else ""}

# ══ خبير وصف مهووس الكامل (مع SEO + GEO) ══════════════════════════════════
def generate_mahwous_description(product_name, price, fragrantica_data=None, extra_info=None):
    """
    يولّد وصفاً احترافياً كاملاً بنظام خبير مهووس:
    - 1200-1500 كلمة
    - 9 أقسام: مقدمة + تفاصيل + هرم عطري + لماذا + متى/أين + لمسة خبير + FAQ + روابط + خاتمة
    - SEO محسّن + GEO محسّن
    - أسلوب مهووس: راقٍ + ودود + عاطفي + تسويقي
    """
    # جمع المعلومات المتاحة
    frag_info = ""
    if fragrantica_data and fragrantica_data.get("success"):
        top    = ", ".join(fragrantica_data.get("top_notes",[])[:5])
        mid    = ", ".join(fragrantica_data.get("middle_notes",[])[:5])
        base   = ", ".join(fragrantica_data.get("base_notes",[])[:5])
        desc   = fragrantica_data.get("description_ar","")
        brand  = fragrantica_data.get("brand","")
        ptype  = fragrantica_data.get("type","")
        size   = fragrantica_data.get("size","")
        year   = fragrantica_data.get("year","")
        designer = fragrantica_data.get("designer","")
        family = fragrantica_data.get("fragrance_family","")
        frag_url = fragrantica_data.get("fragrantica_url","")

        frag_info = f"""
معلومات من Fragrantica Arabia:
- الماركة: {brand}
- المصمم: {designer}
- سنة الإصدار: {year}
- العائلة العطرية: {family}
- الحجم: {size}
- التركيز: {ptype}
- النفحات العليا: {top}
- النفحات الوسطى: {mid}
- النفحات الأساسية: {base}
- الوصف: {desc}
- رابط Fragrantica: {frag_url}"""

    extra = ""
    if extra_info:
        extra = f"\nمعلومات إضافية: {extra_info}"

    prompt = f"""اكتب وصفاً احترافياً كاملاً لهذا العطر بتنسيق متجر مهووس:

**اسم المنتج:** {product_name}
**السعر:** {price:.0f} ريال سعودي
{frag_info}{extra}

اكتب وصفاً من 1200-1500 كلمة يتضمن الأقسام التسعة التالية:

## [عنوان المنتج — الكلمة الرئيسية الكاملة]

[فقرة افتتاحية عاطفية قوية — الكلمة الرئيسية في أول 50 كلمة — دعوة مبكرة للشراء]

## تفاصيل المنتج
[نقاط نقطية: الماركة، المصمم، الجنس، العائلة العطرية، الحجم، التركيز، سنة الإصدار]

## رحلة العطر: اكتشف الهرم العطري الفاخر
[النفحات العليا + الوسطى + الأساسية — وصف حسي عاطفي، ليس مجرد قائمة]

## لماذا تختار عطر {product_name}؟
[4-6 نقاط تبدأ بـ **كلمة مفتاحية بولد** — فوائد لا ميزات]

## متى وأين ترتدي هذا العطر؟
[الفصول + الأوقات المثالية + المناسبات + الفئة العمرية]

## لمسة خبير من مهووس: تقييمنا الاحترافي
[تحليل حسي بضمير "نحن" + الأداء بالساعات + مقارنات + توصية + نصيحة عملية]

## الأسئلة الشائعة حول عطر {product_name}
[6-8 أسئلة حوارية — كل سؤال = كلمة مفتاحية — إجابة 50-80 كلمة]

## اكتشف المزيد من عطور مهووس
[3-5 روابط داخلية + رابط Fragrantica Arabia]

## عالمك العطري يبدأ من مهووس
[الكلمة الرئيسية مرتين + تعزيز الثقة + دعوة قوية للشراء]

**ملاحظات مهمة:**
- لا تستخدم الإيموجي
- استخدم **Bold** للكلمات المفتاحية
- الكلمة الرئيسية 5-7 مرات في المجموع
- أسلوب: راقٍ + ودود + عاطفي + تسويقي
- اكتب الوصف مباشرة بدون أي شرح أو تعليمات"""

    # Gemini أولاً (مع Grounding إذا أمكن لجلب معلومات إضافية)
    txt = _call_gemini(prompt, MAHWOUS_EXPERT_SYSTEM, grounding=not bool(frag_info), max_tokens=8192)
    if not txt:
        txt = _call_gemini(prompt, MAHWOUS_EXPERT_SYSTEM, grounding=False, max_tokens=8192)
    if not txt:
        txt = _call_openrouter(prompt, MAHWOUS_EXPERT_SYSTEM)
    if not txt:
        txt = _call_cohere(prompt, MAHWOUS_EXPERT_SYSTEM)

    if txt:
        return txt
    return f"## {product_name}\n\nعطر فاخر من الدرجة الأولى متوفر الآن في مهووس.\n\n**السعر:** {price:.0f} ريال سعودي\n\nعالمك العطري يبدأ من مهووس!"

# ══ تحقق منتج + تحديد القسم الصحيح ════════════════════════════════════════
def verify_match(p1, p2, pr1=0, pr2=0):
    diff = pr1 - pr2 if pr1 > 0 and pr2 > 0 else 0
    if pr1 > 0 and pr2 > 0:
        if diff > 10:     expected = "سعر اعلى"
        elif diff < -10:  expected = "سعر اقل"
        else:             expected = "موافق"
    else:
        expected = "تحت المراجعة"

    prompt = f"""تحقق من تطابق هذين المنتجين بدقة متناهية (99.9%):
منتج 1 (مهووس): {p1} | السعر: {pr1:.0f} ريال
منتج 2 (المنافس): {p2} | السعر: {pr2:.0f} ريال

قواعد المطابقة الصارمة:
1. يجب أن تكون الماركة متطابقة تماماً.
2. يجب أن يكون اسم العطر متطابقاً (مثلاً: Sauvage ليس Sauvage Elixir).
3. يجب أن يكون الحجم متطابقاً (مثلاً: 100ml ليس 50ml).
4. يجب أن يكون التركيز متطابقاً (EDP ليس EDT).
5. يجب أن يكون الجنس متطابقاً (Men ليس Women).

إذا كانت كل الشروط أعلاه متوفرة، أجب بـ:
- القسم الصحيح = {expected}
خلاف ذلك، أجب بـ:
- القسم الصحيح = مفقود"""

    sys = PAGE_PROMPTS["verify"]
    txt = _call_gemini(prompt, sys, temperature=0.1) or _call_openrouter(prompt, sys)
    if not txt:
        return {"success":False,"match":False,"confidence":0,"reason":"فشل AI","correct_section":"تحت المراجعة","suggested_price":0}
    data = _parse_json(txt)
    if data:
        sec = data.get("correct_section","")
        if "اعلى" in sec or "أعلى" in sec: data["correct_section"] = "سعر اعلى"
        elif "اقل" in sec or "أقل" in sec:  data["correct_section"] = "سعر اقل"
        elif "موافق" in sec:                 data["correct_section"] = "موافق"
        elif "مفقود" in sec:                 data["correct_section"] = "مفقود"
        else: data["correct_section"] = expected if data.get("match") else "مفقود"
        return {"success":True, **data}
    match = "true" in txt.lower() or "نعم" in txt
    return {"success":True,"match":match,"confidence":65,"reason":txt[:200],"correct_section":expected if match else "مفقود","suggested_price":0}

# ══ إعادة تصنيف قسم "تحت المراجعة" ════════════════════════════════════════
def reclassify_review_items(items):
    if not items: return []
    lines = []
    for i, it in enumerate(items):
        diff = it.get("our_price",0) - it.get("comp_price",0)
        lines.append(f"[{i+1}] منتجنا: {it['our']} ({it.get('our_price',0):.0f}ر.س)"
                     f" vs منافس: {it['comp']} ({it.get('comp_price',0):.0f}ر.س) | فرق: {diff:+.0f}ر.س")
    prompt = f"""حلل هذه المنتجات وحدد القسم الصحيح لكل منها:
{chr(10).join(lines)}
- سعر اعلى: نفس المنتج + سعرنا اعلى بـ10+ ريال
- سعر اقل: نفس المنتج + سعرنا اقل بـ10+ ريال
- موافق: نفس المنتج + فرق 10 ريال او اقل
- مفقود: ليسا نفس المنتج"""
    sys = PAGE_PROMPTS["reclassify"]
    txt = _call_gemini(prompt, sys, temperature=0.1) or _call_openrouter(prompt, sys)
    if not txt: return []
    data = _parse_json(txt)
    if data and "results" in data:
        for r in data["results"]:
            sec = r.get("section","")
            if "اعلى" in sec or "أعلى" in sec: r["section"] = "🔴 سعر أعلى"
            elif "اقل" in sec or "أقل" in sec:  r["section"] = "🟢 سعر أقل"
            elif "موافق" in sec:                 r["section"] = "✅ موافق"
            elif "مفقود" in sec:                 r["section"] = "🔵 مفقود"
            else:                                 r["section"] = "⚠️ تحت المراجعة"
        return data["results"]
    return []

# ══ بحث أسعار السوق ══════════════════════════════════════════════════════
def search_market_price(product_name, our_price=0):
    # البحث في أشهر المتاجر السعودية (سلة، زد، نايس ون، قولدن سنت، خبير العطور)
    queries = [
        f"سعر {product_name} السعودية نايس ون قولدن سنت سلة",
        f"سعر {product_name} في المتاجر السعودية 2026",
        f"مقارنة أسعار {product_name} السعودية",
        f"{product_name} price Saudi Arabia perfume shop",
    ]
    all_results = []
    for q in queries[:3]:  # استخدام أول 3 استعلامات
        ddg = _search_ddg(q)
        if ddg: all_results.extend(ddg[:3])
    
    web_ctx = "\n".join(f"- {r.get('snippet', '')[:120]}" for r in all_results if r.get('snippet')) if all_results else ""
    
    prompt = f"""تحليل سوق دقيق للمنتج في السعودية (مارس 2026):
المنتج: {product_name}
سعرنا الحالي: {our_price:.0f} ريال

المعلومات المستخرجة من الويب:
{web_ctx}

المطلوب تحليل JSON مفصل:
1. متوسط السعر في السوق السعودي.
2. أرخص سعر متاح حالياً واسم المتجر.
3. قائمة المنافسين المباشرين وأسعارهم (نايس ون، قولدن سنت، لودوريه، بيوتي ستور، إلخ).
4. حالة التوفر (متوفر/غير متوفر).
5. توصية تسعير ذكية لمتجر مهووس ليكون الأكثر تنافسية.
6. نسبة الثقة في البيانات (0-100)."""
    sys = PAGE_PROMPTS["market_search"]
    txt = _call_gemini(prompt, sys, grounding=True)
    if not txt: txt = _call_gemini(prompt, sys)
    if not txt: txt = _call_openrouter(prompt, sys)
    if not txt: return {"success":False,"market_price":0}
    data = _parse_json(txt)
    if data:
        data["web_context"] = web_ctx
        return {"success":True, **data}
    return {"success":True,"market_price":our_price,"recommendation":txt[:400],"web_context":web_ctx}

# ══ تحليل عميق ══════════════════════════════════════════════════════════════
def ai_deep_analysis(our_product, our_price, comp_product, comp_price, section="general", brand=""):
    diff = our_price - comp_price if our_price > 0 and comp_price > 0 else 0
    diff_pct = (abs(diff)/comp_price*100) if comp_price > 0 else 0
    ddg = _search_ddg(f"سعر {our_product} السعودية")
    web_ctx = "\n".join(f"- {r['snippet'][:80]}" for r in ddg[:2]) if ddg else ""
    guidance = {
        "🔴 سعر أعلى": f"سعرنا اعلى بـ{diff:.0f}ريال ({diff_pct:.1f}%). هل يجب خفضه؟",
        "🟢 سعر أقل":  f"سعرنا اقل بـ{abs(diff):.0f}ريال ({diff_pct:.1f}%). كم يمكن رفعه؟",
        "✅ موافق":     "السعر تنافسي. هل نحافظ عليه؟",
        "⚠️ تحت المراجعة": "المطابقة غير مؤكدة. هل هما نفس المنتج؟",
    }.get(section, "")
    prompt = f"""تحليل تسعير عميق:
منتجنا: {our_product} | سعرنا: {our_price:.0f} ريال
المنافس: {comp_product} | سعره: {comp_price:.0f} ريال
الفرق: {diff:+.0f} ريال | {diff_pct:.1f}% | {guidance}
{f"معلومات السوق:{chr(10)}{web_ctx}" if web_ctx else ""}
اجب بتقرير مختصر: هل المطابقة صحيحة؟ السعر المقترح بالرقم؟ الاجراء الفوري؟"""
    txt = _call_gemini(prompt, grounding=bool(web_ctx)) or _call_openrouter(prompt)
    if txt: return {"success":True,"response":txt,"source":"Gemini" + (" + ويب" if web_ctx else "")}
    return {"success":False,"response":"فشل التحليل"}

# ══ بحث mahwous.com ══════════════════════════════════════════════════════════
def search_mahwous(product_name):
    ddg = _search_ddg(f"site:mahwous.com {product_name}")
    web_ctx = "\n".join(r["snippet"][:100] for r in ddg[:2]) if ddg else ""
    prompt = f"""هل العطر {product_name} متوفر في متجر مهووس؟
{f"نتائج:{chr(10)}{web_ctx}" if web_ctx else ""}
اجب JSON: {{"likely_available":true/false,"confidence":0-100,"similar_products":[],
"add_recommendation":"عالية/متوسطة/منخفضة","reason":"","suggested_price":0}}"""
    txt = _call_gemini(prompt, grounding=True) or _call_gemini(prompt)
    if not txt: return {"success":False}
    data = _parse_json(txt)
    if data: return {"success":True, **data}
    return {"success":True,"likely_available":False,"confidence":50,"reason":txt[:150]}

# ══ تحقق مكرر ════════════════════════════════════════════════════════════════
def check_duplicate(product_name, our_products):
    if not our_products: return {"success":True,"response":"لا توجد بيانات"}
    prompt = f"""هل العطر {product_name} موجود بشكل مشابه في هذه القائمة؟
القائمة: {', '.join(str(p) for p in our_products[:30])}
اجب: نعم (وذكر اقرب مطابقة) او لا مع السبب."""
    return call_ai(prompt, "missing")

# ══ تحليل مجمع ════════════════════════════════════════════════════════════════
def bulk_verify(items, section="general"):
    if not items: return {"success":False,"response":"لا توجد منتجات"}
    lines = "\n".join(
        f"{i+1}. {it.get('our','')} vs {it.get('comp','')} | "
        f"سعرنا: {it.get('our_price',0):.0f} | منافس: {it.get('comp_price',0):.0f} | "
        f"فرق: {it.get('our_price',0)-it.get('comp_price',0):+.0f}"
        for i,it in enumerate(items))
    instructions = {
        "price_raise": "سعرنا اعلى. لكل منتج: هل المطابقة صحيحة؟ هل نخفض؟ السعر المقترح.",
        "price_lower": "سعرنا اقل = ربح ضائع. لكل منتج: هل يمكن رفعه؟ السعر الامثل.",
        "review": "مطابقات غير مؤكدة. لكل منتج: هل هما نفس العطر فعلا؟ نعم/لا/غير متاكد.",
        "approved": "منتجات موافق عليها. راجعها وتاكد انها لا تزال تنافسية.",
    }
    prompt = f"{instructions.get(section,'حلل واعط توصية.')}\n\nالمنتجات:\n{lines}"
    return call_ai(prompt, section)

# ══ معالجة النص الملصوق ═══════════════════════════════════════════════════
def analyze_paste(text, context=""):
    prompt = f"""المستخدم لصق هذا النص:
---
{text[:5000]}
---
حلل واستخرج: قائمة منتجات؟ اسعار؟ اوامر؟ اعط توصيات مفيدة. اجب بالعربية منظم."""
    return call_ai(prompt, "general")

# ══ دوال متوافقة مع app.py ════════════════════════════════════════════════
def chat_with_ai(msg, history=None, ctx=""): return gemini_chat(msg, history, ctx)
def analyze_product(product_name: str, brand: str = "") -> dict:
    """
    استخراج حقول منظمة من اسم المنتج — درجة حرارة 0.0 لتقليل التخمين.
    يُرجع dict من JSON أو {"error", "raw"} عند الفشل.
    """
    prompt = f"""أنت خبير بيانات عطور صارم. حلل المنتج التالي واستخرج البيانات بصيغة JSON فقط. لا تخمن أبداً. إذا لم تجد المعلومة اكتب null.
المنتج: {product_name}
الماركة: {brand if brand else "null"}

يجب أن يكون الرد بهذا الهيكل فقط (قيم boolean صالحة: true أو false فقط):
{{
    "brand": "اسم الماركة بالإنجليزية فقط",
    "core_name": "اسم العطر بدون الماركة أو الحجم أو التركيز",
    "concentration": "التركيز (PARFUM, EDP, EDT, EDC, MIST, HAIR_MIST) أو null",
    "size_ml": "الحجم بالأرقام فقط أو null",
    "gender": "الجنس (MALE, FEMALE, UNISEX) أو null",
    "release_year": "سنة الإصدار إذا ذكرت صراحة (مثال: 2020) أو null",
    "is_tester": false,
    "is_set": false
}}"""

    res = call_ai(prompt, page="extract", temperature=0.0)
    response_text = ""
    if isinstance(res, dict) and res.get("success"):
        response_text = (res.get("response") or "").strip()
    elif isinstance(res, dict):
        response_text = (res.get("response") or "").strip()

    if not response_text:
        return {"error": "AI Failed", "raw": ""}

    try:
        clean_text = response_text.replace("```json", "").replace("```", "").strip()
        return json.loads(clean_text)
    except Exception:
        parsed = _parse_json(response_text)
        if parsed:
            return parsed
        return {"error": "Parsing Failed", "raw": response_text}
def suggest_price(p, comp_price): return call_ai(f"اقترح سعرا لـ {p} بدلا من {comp_price:.0f}ريال", "general")
def process_paste(text): return analyze_paste(text)
