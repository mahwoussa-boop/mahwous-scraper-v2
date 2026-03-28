# 🐛 Mahwous KIX-333 — Bug Report & Fixes

**Date:** March 28, 2026  
**Codebase Version:** v26.1  
**Bugs Found:** 9 (4 Critical, 4 Important, 1 Moderate)

---

## 🔴 BUG #1 — CRITICAL: Pricing Dashboard Crashes When No Data

**File:** `app.py` — `elif page == "📊 لوحة التسعير":` section  
**Impact:** App crashes with `NameError: name 'work' is not defined` when `final_priced_df` is None

### Root Cause
The `if df is not None:` block only wraps 4 lines, but the entire pricing logic (~150 lines after it) references `work = df.copy()` which was created inside that block. When `df` is `None`, Python skips the `if` block but continues executing code that depends on `work`.

### Fix
Indent all code from `for c in ("price", ...)` through the end of the try block to be inside `if df is not None:`. See `app.py` patch below.

---

## 🔴 BUG #2 — CRITICAL: Undefined Variable `_FR` in AI Page

**File:** `app.py` — AI page → Tab 4 → Fragrantica expander  
**Impact:** `NameError: name '_FR' is not defined` when clicking Fragrantica search

### Root Cause
The variable `_FR` is used in an f-string but never defined:
```python
st.markdown(f"[🔗 Fragrantica Arabia]({_FR}/search/?query=...)")
```

### Fix
Add `_FR = "https://www.fragranticarabia.com"` at module level or before the usage.

---

## 🟡 BUG #3 — IMPORTANT: AI Matching Has No API Keys on Streamlit Cloud

**File:** `engines/engine.py` (lines ~85–100) and `utils/engine.py` (same section)  
**Impact:** AI-powered matching silently fails on Streamlit Cloud (falls back to fuzzy-only)

### Root Cause
Both files import `GEMINI_API_KEYS` from `config.py` (which reads from `st.secrets` + env vars), then **immediately override** it:
```python
from config import (..., GEMINI_API_KEYS, ...)  # ✅ Has keys from st.secrets
...
GEMINI_API_KEYS = _load_gemini_keys()  # ❌ Only reads os.environ → EMPTY on Streamlit Cloud
```

### Fix
Remove the local `_load_gemini_keys()` function and the override line. Use the imported keys from `config.py`.

---

## 🟡 BUG #4 — IMPORTANT: utils/engine.py Missing Gender/Year Checks

**File:** `utils/engine.py` — `_V12Product` dataclass  
**Impact:** ClusterMatchEngine may match men's perfume with women's (false positives in missing products detection)

### Root Cause
The `_V12Product` in `utils/engine.py` is missing the `year` and `gender` fields that were added in `engines/engine.py`. The `_check_pair` method doesn't validate gender or year.

### Fix
Sync the `_V12Product` dataclass and `_check_pair` with `engines/engine.py`.

---

## 🔴 BUG #5 — SECURITY: API Key Exposed in Repository

**File:** `.streamlit/secrets.toml`  
**Impact:** Gemini API key is visible in the uploaded code

### Fix
1. Rotate the API key immediately in Google AI Studio
2. Ensure `.streamlit/secrets.toml` is in `.gitignore` (it already is)
3. Never share this file in uploads/issues

---

## 🟡 BUG #6 — IMPORTANT: Silent Error Swallowing in Pricing Dashboard

**File:** `app.py` — pricing dashboard `except Exception`  
**Impact:** Bugs #1/#7 are hidden by a broad try/except that shows a useless generic error

### Fix
Log the actual exception traceback before showing the user error.

---

## Summary of Files to Fix

| File | Bugs | Priority |
|------|------|----------|
| `app.py` | #1, #2, #6 | **Fix immediately** |
| `engines/engine.py` | #3 | **Fix immediately** |
| `utils/engine.py` | #3, #4 | Fix soon |
| `.streamlit/secrets.toml` | #5 | Rotate key |
