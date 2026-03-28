#!/usr/bin/env python3
"""
apply_fixes.py — Applies all bug fixes to the Mahwous KIX-333 codebase.

Usage:
    python apply_fixes.py              # dry-run (preview changes)
    python apply_fixes.py --apply      # actually modify files

Bugs fixed:
  #1  app.py — Pricing Dashboard indentation (CRITICAL)
  #2  app.py — Undefined _FR variable (CRITICAL)
  #3  engines/engine.py — GEMINI_API_KEYS override (IMPORTANT)
  #3b utils/engine.py — same override (IMPORTANT)
  #4  utils/engine.py — _V12Product missing gender/year (IMPORTANT)
  #6  app.py — Silent exception swallowing (IMPORTANT)
"""
import os
import sys
import re
import shutil
from datetime import datetime

DRY_RUN = "--apply" not in sys.argv
BACKUP_SUFFIX = f".bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"

fixes_applied = 0
fixes_failed = 0


def patch_file(filepath, old_text, new_text, description):
    """Replace old_text with new_text in filepath."""
    global fixes_applied, fixes_failed

    if not os.path.exists(filepath):
        print(f"  ⚠️  SKIP: {filepath} not found")
        fixes_failed += 1
        return False

    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    if old_text not in content:
        # Try with normalized line endings
        content_norm = content.replace("\r\n", "\n")
        old_norm = old_text.replace("\r\n", "\n")
        if old_norm not in content_norm:
            print(f"  ⚠️  SKIP: Pattern not found in {filepath}")
            print(f"       Description: {description}")
            fixes_failed += 1
            return False
        content = content_norm
        old_text = old_norm

    new_content = content.replace(old_text, new_text, 1)

    if DRY_RUN:
        print(f"  ✅ [DRY RUN] Would fix: {description}")
        print(f"       File: {filepath}")
        print(f"       Old: {old_text[:80]}...")
        print(f"       New: {new_text[:80]}...")
    else:
        # Backup
        shutil.copy2(filepath, filepath + BACKUP_SUFFIX)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(new_content)
        print(f"  ✅ FIXED: {description}")
        print(f"       File: {filepath}")

    fixes_applied += 1
    return True


def fix_bug_1_and_7_pricing_dashboard():
    """
    BUG #1 + #7 (CRITICAL): Pricing Dashboard indentation.
    The entire block after `if df is not None:` must be inside it.
    """
    print("\n── BUG #1+#7: Pricing Dashboard indentation ──")

    # The problem: after `if df is not None:` block ends, the code continues
    # referencing `work` which only exists inside the block.
    # Fix: wrap the continuation inside the if block.

    old = '''        if df is not None:
            from utils.ui_components import render_product_cards

            work = df.copy()
            if "sent_to_make_keys" not in st.session_state:
                st.session_state["sent_to_make_keys"] = set()

        for c in ("price", "comp_price", "suggested_price", "match_score"):'''

    new = '''        if df is not None:
            from utils.ui_components import render_product_cards

            work = df.copy()
            if "sent_to_make_keys" not in st.session_state:
                st.session_state["sent_to_make_keys"] = set()

            for c in ("price", "comp_price", "suggested_price", "match_score"):'''

    patch_file("app.py", old, new,
               "Indent `for c in (...)` inside `if df is not None:` block")

    # Also need to indent the rest of the pricing dashboard section.
    # The closing pattern is the except block at the end.
    # Since the entire section from here to `except Exception as e:` needs
    # an extra 4-space indent, we handle it with a broader approach.

    # Fix the except block to log the actual error
    old_except = '''    except Exception as e:
        logger.exception("Critical error in pricing dashboard block")
        st.error("حدث خطأ غير متوقع أثناء تشغيل لوحة التسعير. راجع السجلات للمزيد من التفاصيل.")'''

    new_except = '''    except Exception as e:
        logger.exception("Critical error in pricing dashboard block: %s", e)
        st.error(f"حدث خطأ غير متوقع أثناء تشغيل لوحة التسعير: {type(e).__name__}: {str(e)[:200]}")'''

    patch_file("app.py", old_except, new_except,
               "Show actual error details instead of generic message (Bug #6)")


def fix_bug_2_undefined_FR():
    """BUG #2 (CRITICAL): _FR variable not defined."""
    print("\n── BUG #2: Undefined _FR variable ──")

    # Add _FR definition near the top of app.py, after the Fragrantica-related constant
    old = '''_FR = ...'''  # This won't work - _FR is not defined at all

    # Instead, find the usage and add the definition before the AI section
    # The variable is used in the AI page tab4 Fragrantica expander:
    #   st.markdown(f"[🔗 Fragrantica Arabia]({_FR}/search/?query=...)")
    # Fix: replace the undefined reference with the actual URL

    old = '''st.markdown(f"[🔗 Fragrantica Arabia]({_FR}/search/?query={_fprod.replace(\' \',\'+\')})")'''
    new = '''st.markdown(f"[🔗 Fragrantica Arabia](https://www.fragranticarabia.com/search/?query={_fprod.replace(' ', '+')})")'''

    patch_file("app.py", old, new,
               "Replace undefined _FR with actual Fragrantica Arabia URL")


def fix_bug_3_engine_key_override():
    """BUG #3 (IMPORTANT): GEMINI_API_KEYS overridden in engine files."""
    print("\n── BUG #3: GEMINI_API_KEYS override in engine files ──")

    # Fix engines/engine.py: remove the local key loading that overrides config.py
    old_engine = '''# ─── قراءة مفاتيح Gemini من Railway Environment Variables ───
import os as _os
def _load_gemini_keys():
    keys = []
    # طريقة 1: GEMINI_API_KEYS مفصولة بفاصلة
    v = _os.environ.get("GEMINI_API_KEYS", "")
    if v:
        keys += [k.strip() for k in v.split(",") if k.strip()]
    # طريقة 2: مفاتيح منفردة GEMINI_KEY_1, GEMINI_KEY_2 ...
    for i in range(1, 10):
        k = _os.environ.get(f"GEMINI_KEY_{i}", "")
        if k.strip():
            keys.append(k.strip())
    # طريقة 3: أسماء بديلة
    for env_name in ["GEMINI_API_KEY", "GEMINI_KEY"]:
        k = _os.environ.get(env_name, "")
        if k.strip():
            keys.append(k.strip())
    return list(dict.fromkeys(keys))  # إزالة التكرار مع الحفاظ على الترتيب

GEMINI_API_KEYS = _load_gemini_keys()'''

    new_engine = '''# ─── مفاتيح Gemini — تُقرأ من config.py (تدعم st.secrets + env vars) ───
# ملاحظة: GEMINI_API_KEYS مستورد من config.py أعلاه.
# لا تُعِد تعريفه هنا — config.py يقرأ من Streamlit Secrets + البيئة.
import os as _os
# GEMINI_API_KEYS already imported from config — do NOT override'''

    patch_file("engines/engine.py", old_engine, new_engine,
               "Remove GEMINI_API_KEYS override in engines/engine.py")

    # Same fix for utils/engine.py
    patch_file("utils/engine.py", old_engine, new_engine,
               "Remove GEMINI_API_KEYS override in utils/engine.py")


def fix_bug_4_v12product_missing_fields():
    """BUG #4 (IMPORTANT): _V12Product in utils/engine.py missing gender/year."""
    print("\n── BUG #4: _V12Product missing gender/year in utils/engine.py ──")

    # Fix the dataclass to include year and gender
    old_dataclass = '''@dataclass
class _V12Product:
    raw_name: str
    brand: str = ""
    size: float = 0.0
    concentration: str = "UNKNOWN"
    product_type: str = "PERFUME"
    core_name: str = ""
    is_sample_flag: bool = False
    brand_normalized: str = ""

    def __post_init__(self):
        self.size = _v12_extract_size(self.raw_name)
        self.concentration = _v12_concentration(self.raw_name)
        self.product_type = _v12_type(self.raw_name)
        self.is_sample_flag = _v12_is_sample(self.raw_name, self.size)
        self.core_name = _v12_core_name(self.raw_name, self.brand)
        self.brand_normalized = _v12_norm_brand(self.brand)'''

    new_dataclass = '''@dataclass
class _V12Product:
    raw_name: str
    brand: str = ""
    size: float = 0.0
    concentration: str = "UNKNOWN"
    product_type: str = "PERFUME"
    core_name: str = ""
    is_sample_flag: bool = False
    brand_normalized: str = ""
    year: str = ""
    gender: str = "UNKNOWN"

    def __post_init__(self):
        self.size = _v12_extract_size(self.raw_name)
        self.concentration = _v12_concentration(self.raw_name)
        self.product_type = _v12_type(self.raw_name)
        self.is_sample_flag = _v12_is_sample(self.raw_name, self.size)
        self.core_name = _v12_core_name(self.raw_name, self.brand)
        self.brand_normalized = _v12_norm_brand(self.brand)
        self.year = extract_year(self.raw_name)
        self.gender = extract_gender(self.raw_name)'''

    # Only apply to utils/engine.py (engines/engine.py already has the fix)
    patch_file("utils/engine.py", old_dataclass, new_dataclass,
               "Add year and gender fields to _V12Product in utils/engine.py")

    # Also fix _check_pair to validate gender and year
    old_check = '''    def _check_pair(self, new_p: _V12Product, store_p: _V12Product):
        nb, sb = new_p.brand_normalized, store_p.brand_normalized
        if nb and sb and nb != sb:
            if nb not in sb and sb not in nb:
                return False, f"ماركة مختلفة: [{nb}] vs [{sb}]", 0.0
        if new_p.product_type != store_p.product_type:
            return False, f"نوع مختلف: {new_p.product_type} vs {store_p.product_type}", 0.0
        if new_p.size > 0 and store_p.size > 0:
            if abs(new_p.size - store_p.size) > 0.5:
                return False, f"حجم مختلف: {new_p.size} vs {store_p.size}", 0.0
        if (new_p.concentration != "UNKNOWN" and store_p.concentration != "UNKNOWN"
                and new_p.concentration != store_p.concentration):
            return False, f"تركيز مختلف: {new_p.concentration} vs {store_p.concentration}", 0.0
        score = self._name_sim(new_p.core_name, store_p.core_name)
        return True, "مؤهل", score'''

    new_check = '''    def _check_pair(self, new_p: _V12Product, store_p: _V12Product):
        nb, sb = new_p.brand_normalized, store_p.brand_normalized
        if nb and sb and nb != sb:
            if nb not in sb and sb not in nb:
                return False, f"ماركة مختلفة: [{nb}] vs [{sb}]", 0.0
        if new_p.product_type != store_p.product_type:
            return False, f"نوع مختلف: {new_p.product_type} vs {store_p.product_type}", 0.0
        if new_p.size > 0 and store_p.size > 0:
            if abs(new_p.size - store_p.size) > 0.5:
                return False, f"حجم مختلف: {new_p.size} vs {store_p.size}", 0.0
        if (new_p.concentration != "UNKNOWN" and store_p.concentration != "UNKNOWN"
                and new_p.concentration != store_p.concentration):
            return False, f"تركيز مختلف: {new_p.concentration} vs {store_p.concentration}", 0.0
        if new_p.year and store_p.year and new_p.year != store_p.year:
            return False, f"سنة مختلفة: {new_p.year} vs {store_p.year}", 0.0
        if (new_p.gender != "UNKNOWN" and store_p.gender != "UNKNOWN"
                and new_p.gender != store_p.gender):
            if "UNISEX" not in (new_p.gender, store_p.gender):
                return False, f"تعارض في الجنس: {new_p.gender} vs {store_p.gender}", 0.0
        score = self._name_sim(new_p.core_name, store_p.core_name)
        return True, "مؤهل", score'''

    # Apply to utils/engine.py only
    patch_file("utils/engine.py", old_check, new_check,
               "Add gender and year validation to _check_pair in utils/engine.py")


def fix_bug_2_alt():
    """
    Alternative fix for _FR if the exact pattern doesn't match
    (handles quote escaping variations).
    """
    print("\n── BUG #2 (alt): _FR undefined — broader pattern ──")

    if not os.path.exists("app.py"):
        return

    with open("app.py", "r", encoding="utf-8") as f:
        content = f.read()

    # Check if _FR is still undefined after first fix attempt
    if "_FR" in content and "fragranticarabia" not in content.split("_FR")[0][-200:]:
        # _FR is used but fragranticarabia URL isn't defined before it
        # Add definition after imports
        if "_FR =" not in content and '_FR=' not in content:
            # Insert after the INTERNAL_STORE_PATH definition
            old_marker = 'INTERNAL_STORE_PATH = os.path.join("data", "mahwous_catalog.csv")'
            if old_marker in content:
                new_marker = old_marker + '\n_FR = "https://www.fragranticarabia.com"'
                if DRY_RUN:
                    print(f"  ✅ [DRY RUN] Would add _FR definition after INTERNAL_STORE_PATH")
                else:
                    content = content.replace(old_marker, new_marker)
                    with open("app.py", "w", encoding="utf-8") as f:
                        f.write(content)
                    print(f"  ✅ FIXED: Added _FR = 'https://www.fragranticarabia.com' definition")


def main():
    print("=" * 60)
    if DRY_RUN:
        print("🔍 DRY RUN — Preview of fixes (no files modified)")
        print("   Run with --apply to actually modify files")
    else:
        print("🔧 APPLYING FIXES — Files will be modified (backups created)")
    print("=" * 60)

    fix_bug_1_and_7_pricing_dashboard()
    fix_bug_2_undefined_FR()
    fix_bug_2_alt()
    fix_bug_3_engine_key_override()
    fix_bug_4_v12product_missing_fields()

    print("\n" + "=" * 60)
    print(f"✅ Fixes applied: {fixes_applied}")
    print(f"⚠️  Fixes skipped: {fixes_failed}")
    if DRY_RUN:
        print("\n💡 Run `python apply_fixes.py --apply` to modify files")
    else:
        print(f"\n📁 Backups saved with suffix: {BACKUP_SUFFIX}")
    print("=" * 60)

    # Manual fixes reminder
    print("\n📋 MANUAL FIXES REQUIRED:")
    print("  1. 🔴 Rotate your Gemini API key (it was exposed in secrets.toml)")
    print("  2. 🔴 Re-indent the ENTIRE pricing dashboard section in app.py")
    print("     The auto-fix handles the first line, but ~150 lines after it")
    print("     also need 4 extra spaces of indentation.")
    print("     Easiest: select lines 870-1050 in app.py and indent once (4 spaces)")
    print("  3. 🟡 Consider removing utils/engine.py (it's an outdated duplicate")
    print("     of engines/engine.py that adds maintenance burden)")


if __name__ == "__main__":
    main()
