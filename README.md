# مهووس — نظام التسعير الذكي (KIX / Mahwous)

منصة **Streamlit** لتحليل أسعار المنافسين، المطابقة الذكية للمنتجات، ومقترحات تسعير مدعومة بـ **AI (Gemini)**، مع **كشط ويب غير متزامن** لاستخراج بيانات المتاجر (سلة، زد، وغيرها عبر JSON-LD)، ولوحة **Dashboard** للرؤى والمزامنة الجماعية مع **Make.com** ومتجر **سلة**.

> **الإصدار:** راجع `config.py` (`APP_VERSION`).  
> **النشر الموصى به:** Docker على [Railway](https://railway.app) (يُستخدم `Dockerfile` و `railway.json`).

---

## المميزات

| المجال | الوصف |
|--------|--------|
| **تحليل الملفات** | رفع CSV/Excel لمنتجاتكم ومنافسيكم، وتصنيف تلقائي (سعر أعلى / أقل / موافق / مراجعة / مفقود). |
| **محرك مطابقة** | مطابقة ذكية (Fuzzy + قواعد) بين كتالوجكم وبيانات المنافس. |
| **تسعير VSP** | خط أنابيب في `utils/pricing_pipeline.py` يدمج المطابقة مع محرك تسعير معزز (`engines/ai_engine_enhanced.py`). |
| **كشط ويب** | `utils/async_scraper.py` — جلب من sitemap/صفحات المنتج مع استخراج منظم (مثل JSON-LD). |
| **لوحة تسعير** | مؤشرات، جدول قرار ملون، فلاتر توصيات، ومزامنة أسعار مقترحة إلى المتجر عبر **Make** (دفعات). |
| **أتمتة** | قواعد أتمتة، سجل قرارات، وربط اختياري بـ Make.com. |

---

## المتطلبات

- Python **3.11+**
- مفاتيح API حسب الاستخدام (مثلاً **Gemini**) — عبر البيئة أو Streamlit Secrets

---

## التشغيل محلياً

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate   # Linux / macOS

pip install -r requirements.txt
streamlit run app.py
```

### الأسرار (محلياً)

أنشئ `.streamlit/secrets.toml` (هذا الملف **مُستثنى في `.gitignore`**) مثلاً:

```toml
GEMINI_API_KEY = "your-key"
WEBHOOK_UPDATE_PRICES = "https://hook.eu2.make.com/..."
WEBHOOK_NEW_PRODUCTS = "https://hook.eu2.make.com/..."
```

أو عيّن نفس المفاتيح كمتغيرات بيئة (`GEMINI_API_KEY`, `WEBHOOK_UPDATE_PRICES`, …).

---

## النشر على Railway

1. اربط المستودع بـ Railway واختر النشر عبر **Dockerfile**.
2. في **Variables** أضف أسرار الإنتاج (Gemini، Webhooks، إلخ) — **لا** تضعها في الكود.
3. المنفذ: التطبيق يستمع على **8501** (كما في `Dockerfile`).

### تشغيل 24/7 (موصى به)

لتحقيق التشغيل التلقائي الكامل: أنشئ **خدمتين** في Railway من نفس المستودع:

1. **Web Service** (واجهة Streamlit)
   - Start Command: الافتراضي (Dockerfile / Streamlit)
2. **Worker Service** (الخلفية المستمرة)
   - Start Command:
     ```bash
     python run_background_worker.py
     ```
   - Variables:
     - `SCRAPER_CONTINUOUS=1`
     - `SCRAPER_PENDING_BATCH_SIZE=200` (اختياري)
     - `SCRAPER_IDLE_POLL_SECONDS=20` (اختياري)
     - `AUTO_PIPELINE_MIN_INTERVAL_SEC=120` (اختياري)

بهذا الشكل:
- Worker يزامن الـ sitemap كل ساعتين ويكمل من pending بعد أي restart.
- Web يعرض النتائج الجاهزة فقط (`data/final_priced_latest.csv`) بدون انتظار تشغيل يدوي.

---

## البيانات والكشط

- **`data/competitors_list.json`**: قائمة روابط (مثل sitemap منتجات). في المستودع يُفضَّل الإبقاء على قائمة **فارغة** `[]` أو روابط **تجريبية** فقط؛ أضف روابط الإنتاج محلياً أو عبر نسخة خاصة من الملف.
- **`data/competitors_latest.csv`**: مخرجات الكاشط — **مستثناة من Git** (انظر `.gitignore`) لأنها بيانات تشغيل وليست جزءاً من الكود.
- **`data/scraper_state.db`**: حالة الكشط (pending/completed/failed) للاستئناف بعد الانقطاع.
- **`data/final_priced_latest.csv`**: ناتج التسعير التلقائي الخلفي الجاهز للعرض في لوحة التسعير.

الكشط يعمل **في بيئتكم** (جهاز أو سيرفر)؛ لا يعتمد على خوادم خارجية لاستضافة عملية الزحف نيابة عنكم.

### تشغيل تلقائي على Windows (Task Scheduler)

أمر سريع لإنشاء مهمة تبدأ مع الإقلاع:

```powershell
schtasks /Create /SC ONSTART /TN "KIX Background Worker" /TR "cmd /c cd /d C:\Users\Hp\Downloads\kix333 && python run_background_worker.py" /RL HIGHEST /F
```

للتحقق:

```powershell
schtasks /Query /TN "KIX Background Worker" /V /FO LIST
```

للحذف:

```powershell
schtasks /Delete /TN "KIX Background Worker" /F
```

---

## هيكل المشروع ( مختصر )

```
app.py                 # واجهة Streamlit الرئيسية
config.py              # إعدادات مركزية
engines/               # محرك التحليل، AI، أتمتة
utils/                 # قاعدة البيانات، الكاشط، Make، واجهات UI
data/                  # بيانات إعداد (قائمة منافسين؛ CSV يُستبعد من Git)
Dockerfile
railway.json
```

---

## الأمان

- لا ترفع `.env` أو `secrets.toml` أو قواعد بيانات الإنتاج.
- لا ترفع مفاتيح API أو Webhooks داخل المستودع.
- راجع `.gitignore` قبل كل `git push`.

---

## الترخيص والمساهمة

حدّد الترخيص المناسب لمشروعك (مثلاً MIT) وأضف ملف `LICENSE` إن رغبت.  
للمساهمات: فروع (branches) واضحة، ورسائل commit وصفية.

---

**مهووس / KIX-333** — من البيانات الخام إلى قرارات تسعير قابلة للتنفيذ.
