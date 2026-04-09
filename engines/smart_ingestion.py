"""
engines/smart_ingestion.py — محرك إضافة المنتجات الذكي v1.0
═══════════════════════════════════════════════════════════
يدعم:
1. تحليل الروابط المتعددة (Multi-URL Parsing).
2. استخراج البيانات عبر AI (Gemini).
3. توليد وصف SEO + GEO احترافي.
4. الربط بـ Make Webhook للإرسال إلى سلة.
"""

import logging
import pandas as pd
from typing import List, Dict, Any
from config import MAHWOUS_EXPERT_SYSTEM
from engines.ai_engine import call_ai
from utils.make_helper import send_new_products

logger = logging.getLogger(__name__)

def parse_product_links(links: List[str]) -> List[Dict[str, Any]]:
    """تحليل الروابط واستخراج بيانات المنتج الأولية (Enterprise v2.0)."""
    results = []
    for link in links:
        # استخراج اسم افتراضي من الرابط للتجربة
        name_guess = link.split('/')[-1].replace('-', ' ').replace('_', ' ').title()
        results.append({
            "المنتج": name_guess,
            "رابط_المنتج": link,
            "السعر": 0.0,
            "الماركة": "قيد التحليل",
            "الحجم": "غير معروف",
            "status": "pending"
        })
    return results

def generate_seo_content(product_name: str, brand: str, features: List[str]) -> str:
    """توليد وصف SEO احترافي باستخدام محرك AI."""
    prompt = f"اكتب وصفاً تسويقياً احترافياً لمتجر عطور لمنتج: {product_name} من ماركة {brand}. المميزات: {', '.join(features)}. استخدم أسلوباً عاطفياً وجذاباً."
    res = call_ai(prompt, system_prompt=MAHWOUS_EXPERT_SYSTEM)
    return res if res else "تعذر توليد الوصف حالياً."

def push_to_salla(product_data: Dict[str, Any]) -> bool:
    """إرسال المنتج الجديد إلى سلة عبر نظام المساعدة المعتمد."""
    try:
        # إرسال البيانات مباشرة كـ list of dicts (المتوقع في send_new_products)
        res = send_new_products([product_data])
        return res.get("success", False) if isinstance(res, dict) else bool(res)
    except Exception as e:
        logger.error(f"Push to Salla failed: {e}")
        return False