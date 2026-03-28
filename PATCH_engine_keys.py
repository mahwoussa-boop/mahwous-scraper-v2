"""
PATCH: engines/engine.py — Fix GEMINI_API_KEYS Override (Bug #3)

FIND this section (around line 85-100):
─────────────────────────────────────
# ─── قراءة مفاتيح Gemini من Railway Environment Variables ───
import os as _os
def _load_gemini_keys():
    keys = []
    v = _os.environ.get("GEMINI_API_KEYS", "")
    if v:
        keys += [k.strip() for k in v.split(",") if k.strip()]
    for i in range(1, 10):
        k = _os.environ.get(f"GEMINI_KEY_{i}", "")
        if k.strip():
            keys.append(k.strip())
    for env_name in ["GEMINI_API_KEY", "GEMINI_KEY"]:
        k = _os.environ.get(env_name, "")
        if k.strip():
            keys.append(k.strip())
    return list(dict.fromkeys(keys))

GEMINI_API_KEYS = _load_gemini_keys()   # <── THIS LINE IS THE BUG
─────────────────────────────────────

REPLACE WITH:
─────────────────────────────────────
# ─── مفاتيح Gemini — تُقرأ من config.py ───
# GEMINI_API_KEYS already imported from config.py (supports st.secrets + env)
# Do NOT override here — config.py handles all key sources
import os as _os
─────────────────────────────────────

WHY:
config.py reads keys from:
  1. os.environ
  2. st.secrets (Streamlit Cloud)
  3. Individual key names (GEMINI_KEY_1..10)
  4. Aliases (GOOGLE_API_KEY, etc.)

The local _load_gemini_keys() only reads os.environ, which LOSES
the st.secrets keys → AI matching completely fails on Streamlit Cloud.

APPLY TO BOTH FILES:
  - engines/engine.py
  - utils/engine.py
"""
