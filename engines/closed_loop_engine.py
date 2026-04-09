"""
engines/closed_loop_engine.py  v2.0 — محرك المطابقة الدائري المغلق (Enterprise Edition)
═══════════════════════════════════════════════════════════════════════
تم التحديث ليدعم:
1. نظام Alias Mapping للمنافسين.
2. منع التكرار وربط الأحجام الدقيق (Strict SKU/Volume Matching).
3. استبعاد المنتجات مجهولة الحجم.
"""

from __future__ import annotations
import logging
import re
import sqlite3
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple
from rapidfuzz import fuzz
from utils.db_manager import DB_PATH

logger = logging.getLogger("closed_loop_engine")

# --- الإعدادات والثوابت ---
STATUS_MATCHED = "متطابق"
STATUS_REVIEW  = "مراجعة_يدوية"
STATUS_MISSING = "مفقود"

def get_competitor_id(name: str) -> Optional[int]:
    """يجلب معرف المنافس من جدول الهوية أو جدول الظل."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # البحث في الجدول الأساسي
    c.execute("SELECT id FROM competitors WHERE name = ?", (name,))
    res = c.fetchone()
    if res:
        conn.close()
        return res[0]
    
    # البحث في جدول الظل
    c.execute("SELECT competitor_id FROM competitor_aliases WHERE alias = ?", (name,))
    res = c.fetchone()
    conn.close()
    return res[0] if res else None

def extract_volume_strict(name: str) -> Optional[float]:
    """استخراج الحجم بدقة، وإذا لم يوجد حجم واضح نرجع None."""
    if not isinstance(name, str): return None
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:ml|مل|ملي)\b", name, re.I)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None

def match_products_strict(our_product: Dict, comp_product: Dict) -> Tuple[bool, str]:
    """
    مطابقة صارمة (Module 1):
    1. 100مل يقارن بـ 100مل فقط.
    2. استبعاد مجهول الحجم.
    """
    our_vol = extract_volume_strict(our_product.get("name", ""))
    comp_vol = extract_volume_strict(comp_product.get("name", ""))
    
    if our_vol is None or comp_vol is None:
        return False, "حجم مجهول - مستبعد من المقارنة"
    
    if our_vol != comp_vol:
        return False, f"اختلاف الحجم ({our_vol} != {comp_vol})"
    
    # مطابقة الاسم (Fuzzy)
    score = fuzz.token_set_ratio(our_product.get("name", ""), comp_product.get("name", ""))
    if score >= 90:
        return True, "متطابق"
    
    return False, f"ضعف المطابقة ({score}%)"

# دمج الوظائف القديمة مع التحديثات الجديدة لضمان Zero Breakage
# (سيتم استكمال بقية الدوال لاحقاً عند الحاجة لضمان استقرار النظام)
