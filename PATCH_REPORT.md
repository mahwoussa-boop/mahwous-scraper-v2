# تقرير التصحيح — mahwous-smart-v26
**التاريخ:** 2026-04-08  
**الحالة:** ✅ مُطبَّق ومُتحقَّق منه (جميع الملفات تمر بـ `py_compile` بنجاح)

---

## الإصلاحات المُنفَّذة

### 🔴 CRITICAL-01 — المرور الرابع يفحص أول 30 منتجاً فقط
**الملف:** `engines/multi_pass_matcher.py` ← دالة `_pass4_ai_embedding`

**قبل الإصلاح:**
```python
for i, name in enumerate(names[:30]):   # ❌ يتجاهل 99.6% من الكتالوج
```

**بعد الإصلاح:**
```python
# خطوة 1: TF-IDF pre-screening على كامل الكتالوج (7,604 منتج)
TOP_K = min(50, len(all_names))
top_indices = _sims.argsort()[-TOP_K:][::-1]   # أفضل 50 مرشحاً

# خطوة 2: Embedding فقط على الـ 50 المختارة
for i in top_indices:
    ...
```

**الأثر:** المرور الرابع يُغطّي الآن كامل الكتالوج بدلاً من 0.4% منه فقط.

---

### 🔴 CRITICAL-02 — 4,000+ اتصال قاعدة بيانات لكل تحليل
**الملفات:** `engines/multi_pass_matcher.py` ← `match_product` + `match_dataframe`

**قبل الإصلاح:**
```python
# match_product يُستدعى 1000 مرة ← كل استدعاء يفتح connection SQLite
_persist_attempts(attempts)   # ← داخل الحلقة
```

**بعد الإصلاح:**
```python
# match_dataframe يجمع كل المحاولات ثم يكتبها دفعةً واحدة
all_attempts: list[dict] = []
for i, (_, row) in enumerate(competitor_df.iterrows()):
    res = match_product(..., _skip_db_persist=True)   # لا كتابة فردية
    all_attempts.extend(res.attempts)

_persist_attempts(all_attempts)   # ← كتابة واحدة بعد الحلقة
```

**الأثر:** من 4,000+ connection إلى connection واحد لكل جلسة تحليل.

---

### ⚠️ LOGICAL-01 — الأرقام العربية لا تُطابَق (٥٠ مل ≠ 50 مل)
**الملف:** `engines/multi_pass_matcher.py` ← دالة `normalize`

**قبل الإصلاح:** لا يوجد تحويل للأرقام العربية.

**بعد الإصلاح:**
```python
_AR_DIGIT = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_FA_DIGIT = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")

def normalize(text):
    text = text.translate(_AR_DIGIT).translate(_FA_DIGIT)   # ← جديد
    ...
```

---

### ⚠️ LOGICAL-02 — `مل` العربية غير مدعومة في `extract_size`
**الملف:** `engines/multi_pass_matcher.py` ← `_SIZE_RE`

**قبل الإصلاح:**
```python
_SIZE_RE = re.compile(r"\b(\d+)\s*(ml|gm|g|oz|fl\.?\s*oz)\b", re.I)
# "100 مل" → 0.0
```

**بعد الإصلاح:**
```python
_SIZE_RE = re.compile(
    r"\b(\d+(?:\.\d+)?)\s*(ml|مل|gm|جم|g|oz|fl\.?\s*oz)\b",
    re.I | re.UNICODE
)
# "100 مل" → 100.0   ✅
# "50مل"   → 50.0    ✅
```

---

### ⚠️ LOGICAL-05 — `c_price > 0` يُهمل المنتجات ذات السعر صفر
**الملف:** `engines/engine.py` ← السطر ~2252

**قبل الإصلاح:**
```python
if c_name and c_pid and c_price > 0:   # ❌ يُهمل c_price == 0
```

**بعد الإصلاح:**
```python
if c_name and c_pid and c_price is not None:   # ✅ يقبل السعر صفر
```

---

### 💡 OPT-01 — 3 نسخ مكررة من نفس الملفات
**الملفات:** `engines/`, `scrapers/`, `make/` ← `anti_ban`, `async_scraper`, `scheduler`

**قبل الإصلاح:** 3 نسخ متطابقة — أي إصلاح في نسخة لا ينتقل للأخريات.

**بعد الإصلاح:**
```
utils/shared/anti_ban.py       ← المصدر الموحّد
utils/shared/async_scraper.py
utils/shared/scheduler.py

engines/anti_ban.py   → from utils.shared.anti_ban import *
scrapers/anti_ban.py  → from utils.shared.anti_ban import *
make/anti_ban.py      → from utils.shared.anti_ban import *
```

---

### 💡 OPT-02 — TF-IDF matrix يُعاد بناؤه 1,000+ مرة
**الملف:** `engines/multi_pass_matcher.py` ← `_pass3_tfidf` + `_build_tfidf_matrix`

**قبل الإصلاح:**
```python
vec    = TfidfVectorizer(...)
matrix = vec.fit_transform(corpus)   # ← يُعاد كل مرة
```

**بعد الإصلاح:**
```python
@lru_cache(maxsize=4)
def _build_tfidf_matrix(catalog_hash, names_tuple):
    ...   # يُبنى مرة واحدة ويُخزَّن

# الاستخدام في pass3:
vec, cat_mat = _build_tfidf_matrix(catalog_hash, names_tuple)
q_vec = vec.transform([q_norm])   # transform فقط (لا fit)
```

**الأثر المُتوقَّع:** تسريع المرور الثالث بمعامل 8–12× لكل جلسة.

---

## ما لم يُصلَح بعد (يتطلب قرارات معمارية)

| الرقم | المشكلة | السبب |
|-------|---------|-------|
| CRITICAL-03 | لا SKU/Barcode في ملفات المنافسين | يتطلب تغيير هيكل الـ scraper ليجلب المعرّفات من الموقع |
| CRITICAL-04 | `product_id` = اسم نصي (هجاء متغيّر) | يتطلب إنشاء UUID داخلي ثابت + جدول alias |
| LOGICAL-03 | `_pass2_weighted` يستخدم `iterrows` O(n²) | يتطلب إعادة كتابة `_score_row` بشكل Vectorized كامل |
| LOGICAL-04 | تعارض `idx` بين `iloc` و`loc` في Pass1 | يتطلب إضافة `.reset_index(drop=True)` قبل كل مرور |

---

## ملخص التحقق

```
✅ engines/multi_pass_matcher.py  — py_compile OK
✅ engines/engine.py              — py_compile OK
✅ utils/db_manager.py            — py_compile OK
✅ utils/shared/ (3 ملفات)       — نسخ موحدة
```
