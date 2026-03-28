"""
utils/make_helper.py v24.0 — إرسال صحيح لـ Make.com
══════════════════════════════════════════════════════
سيناريو تحديث الأسعار (Integration Webhooks, Salla):
  Webhook → BasicFeeder يقرأ {{2.products}} → UpdateProduct
  Payload المطلوب: {"products": [{"product_id":"...","name":"...","price":...}]}

سيناريو المنتجات الجديدة (Mahwous - إضافة منتجات جديدة لسلة):
  Webhook → BasicFeeder يقرأ {{1.data}} → CreateProduct
  Payload المطلوب: {"data": [{"أسم المنتج":"...","سعر المنتج":...,"الوصف":"..."}]}

⚠️ الإصلاح الحرج v24:
   تحديث الأسعار → {"products": [{product_id, name, price, ...}]}
   المنتجات الجديدة/المفقودة → {"data": [{أسم المنتج, سعر المنتج, ...}]}
"""

import requests
import json
import os
import time
import logging
from typing import List, Dict, Any, Optional
import pandas as pd

logger = logging.getLogger(__name__)


# ── Webhook URLs (Environment أو Streamlit Secrets فقط — لا تضع روابط إنتاج في الكود) ──
def _webhook_from_env_or_secrets(key: str) -> str:
    v = (os.environ.get(key) or "").strip()
    if v:
        return v
    try:
        import streamlit as st

        if hasattr(st, "secrets") and key in st.secrets:
            return str(st.secrets[key]).strip()
    except Exception:
        pass
    return ""


def get_webhook_update_prices() -> str:
    """قراءة حيّة من البيئة أو secrets (لا تعتمد على وقت استيراد الوحدة)."""
    return _webhook_from_env_or_secrets("WEBHOOK_UPDATE_PRICES") or _webhook_from_env_or_secrets("MAKE_WEBHOOK_URL")


def get_webhook_new_products() -> str:
    return _webhook_from_env_or_secrets("WEBHOOK_NEW_PRODUCTS")

TIMEOUT = 15  # ثانية


# ── الإرسال الأساسي ────────────────────────────────────────────────────────
def _post_to_webhook(url: str, payload: Any) -> Dict:
    """
    إرسال بيانات JSON إلى Webhook URL.
    يُعيد dict: {"success": bool, "message": str, "status_code": int}
    """
    if not url:
        return {"success": False, "message": "❌ Webhook URL غير محدد", "status_code": 0}
    try:
        headers = {"Content-Type": "application/json"}
        logger.info("POST webhook payload keys=%s", list(payload.keys()) if isinstance(payload, dict) else type(payload))
        resp = requests.post(
            url,
            json=payload,
            headers=headers,
            timeout=TIMEOUT
        )
        resp.raise_for_status()
        return {
            "success": True,
            "message": f"✅ تم الإرسال بنجاح ({resp.status_code})",
            "status_code": resp.status_code,
        }
    except requests.exceptions.HTTPError as e:
        code = getattr(e.response, "status_code", 0)
        body = getattr(e.response, "text", "")[:200] if getattr(e, "response", None) else ""
        logger.exception("Make webhook HTTP error: status=%s body=%s", code, body)
        return {"success": False, "message": f"❌ HTTP {code}: {body}", "status_code": code}
    except requests.exceptions.Timeout:
        logger.exception("Make webhook timeout")
        return {"success": False, "message": "❌ انتهت مهلة الاتصال (Timeout)", "status_code": 0}
    except requests.exceptions.ConnectionError:
        logger.exception("Make webhook connection error")
        return {"success": False, "message": "❌ فشل الاتصال بـ Make — تحقق من الإنترنت", "status_code": 0}
    except Exception as e:
        logger.exception("Make webhook unexpected error")
        return {"success": False, "message": f"❌ خطأ غير متوقع: {str(e)}", "status_code": 0}


def send_approved_prices_to_make(df: pd.DataFrame) -> bool:
    """
    إرسال أسعار المنتجات المعتمدة إلى Make.com بصيغة إنتاجية.
    Payload:
      {"products": [{"sku":"...", "new_price": 123.0, "action":"Increase Price 📈"}, ...]}
    """
    if df is None or df.empty:
        logger.info("send_approved_prices_to_make: empty dataframe")
        return False

    webhook = get_webhook_update_prices()
    if not webhook:
        logger.error("send_approved_prices_to_make: webhook URL missing")
        return False

    payload_rows: List[Dict[str, Any]] = []
    work = df.copy()
    if "sku" not in work.columns:
        logger.error("send_approved_prices_to_make: sku column missing")
        return False

    for _, row in work.iterrows():
        sku = _clean_pid(row.get("sku", "")) or str(row.get("sku", "")).strip()
        if not sku:
            continue
        new_price = _safe_float(
            row.get("suggested_price", 0) or row.get("new_price", 0) or row.get("price", 0)
        )
        if new_price <= 0:
            continue
        action = str(row.get("action_required", "") or row.get("action", "approved")).strip() or "approved"
        payload_rows.append(
            {
                "sku": sku,
                "new_price": float(new_price),
                "action": action,
            }
        )

    if not payload_rows:
        logger.warning("send_approved_prices_to_make: no valid rows to send")
        return False

    payload = {"products": payload_rows}
    try:
        logger.info("Sending approved prices to Make rows=%s", len(payload_rows))
        resp = requests.post(
            webhook,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        logger.info("Make approved prices success status=%s", resp.status_code)
        return True
    except requests.exceptions.Timeout:
        logger.exception("send_approved_prices_to_make timeout")
        return False
    except requests.exceptions.HTTPError:
        logger.exception(
            "send_approved_prices_to_make http_error status=%s body=%s",
            getattr(resp, "status_code", 0),
            getattr(resp, "text", "")[:300],
        )
        return False
    except Exception:
        logger.exception("send_approved_prices_to_make unexpected error")
        return False


# ── تحويل float آمن ───────────────────────────────────────────────────────
def _safe_float(val, default: float = 0.0) -> float:
    """تحويل آمن إلى float"""
    try:
        if val is None or str(val).strip() in ("", "nan", "None", "NaN"):
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


# ── تنظيف product_id ──────────────────────────────────────────────────────
def _clean_pid(raw) -> str:
    """
    product_id دائماً كـ str(int(float(value)))
    مثال: 100.0 → "100" | "1081786650.0" → "1081786650"
    """
    if raw is None: return ""
    s = str(raw).strip()
    if s in ("", "nan", "None", "NaN", "0", "0.0"): return ""
    try:
        return str(int(float(s)))
    except (ValueError, TypeError):
        return s


# ══════════════════════════════════════════════════════════════════════════
#  تحويل DataFrame → قائمة منتجات مع حساب السعر الصحيح لكل قسم
# ══════════════════════════════════════════════════════════════════════════
def export_to_make_format(df, section_type: str = "update") -> List[Dict]:
    """
    تحويل DataFrame إلى قائمة منتجات جاهزة لـ Make.
    section_type: raise | lower | approved | update | missing | new
    كل منتج يحتوي على: product_id, name, price, section, + حقول سياقية
    """
    if df is None or (hasattr(df, "empty") and df.empty):
        return []

    products = []
    for _, row in df.iterrows():

        # ── رقم المنتج ────────────────────────────────────────────────────
        product_id = _clean_pid(
            row.get("معرف_المنتج")  or row.get("product_id")     or
            row.get("رقم المنتج")   or row.get("رقم_المنتج")    or
            row.get("معرف المنتج")  or row.get("sku")            or
            row.get("SKU")          or ""
        )

        # ── اسم المنتج ────────────────────────────────────────────────────
        name = (
            str(row.get("المنتج",         "")) or
            str(row.get("منتج_المنافس",   "")) or
            str(row.get("أسم المنتج",     "")) or
            str(row.get("اسم المنتج",     "")) or
            str(row.get("name",           "")) or ""
        ).strip()
        if name in ("", "nan", "None"): name = ""

        # ── السعر حسب القسم ───────────────────────────────────────────────
        comp_price = _safe_float(row.get("سعر_المنافس", 0))
        our_price  = _safe_float(
            row.get("السعر", 0) or row.get("سعر المنتج", 0) or
            row.get("price",  0) or 0
        )

        if section_type == "raise":
            # سعرنا أعلى → نُخفّض لسعر المنافس مطروحاً ريال
            price = round(comp_price - 1, 2) if comp_price > 0 else our_price
        elif section_type == "lower":
            # سعرنا أقل → نرفع لسعر المنافس مطروحاً ريال (نبقى أقل بريال)
            price = round(comp_price - 1, 2) if comp_price > 0 else our_price
        elif section_type in ("approved", "update"):
            price = our_price
        else:
            # missing / new: سعر المنافس
            price = comp_price if comp_price > 0 else our_price

        if not name: continue

        # ── حقول سياقية إضافية ───────────────────────────────────────────
        comp_name  = str(row.get("منتج_المنافس", ""))
        comp_src   = str(row.get("المنافس", ""))
        diff       = _safe_float(row.get("الفرق", 0))
        match_pct  = _safe_float(row.get("match_score", 0))
        decision   = str(row.get("القرار", ""))
        brand      = str(row.get("الماركة", ""))

        product = {
            "product_id": product_id,
            "name":       name,
            "price":      float(price),
            "section":    section_type,
        }

        if comp_name and comp_name not in ("nan", "None", "—"):
            product["comp_name"] = comp_name
        if comp_src and comp_src not in ("nan", "None"):
            product["competitor"] = comp_src
        if diff:
            product["price_diff"] = diff
        if match_pct:
            product["match_score"] = match_pct
        if decision and decision not in ("nan", "None"):
            product["decision"] = decision
        if brand and brand not in ("nan", "None"):
            product["brand"] = brand

        products.append(product)

    return products


# ══════════════════════════════════════════════════════════════════════════
#  إرسال منتج واحد — تحديث السعر
#  Payload: {"products": [{"product_id":"...","name":"...","price":...}]}
# ══════════════════════════════════════════════════════════════════════════
def send_single_product(product: Dict) -> Dict:
    """
    إرسال منتج واحد لتحديث سعره في سلة عبر Make.
    Make يقرأ: {{2.products}} → product_id | name | price
    Payload: {"products": [{...}]}
    """
    if not product:
        return {"success": False, "message": "❌ لا توجد بيانات للإرسال"}

    name       = str(product.get("name", "")).strip()
    price      = _safe_float(product.get("price", 0))
    product_id = _clean_pid(product.get("product_id", ""))

    if not name:
        return {"success": False, "message": "❌ اسم المنتج مطلوب"}
    if price <= 0:
        return {"success": False, "message": f"❌ السعر غير صحيح: {price}"}

    # ── Payload مطابق لما يقرأه Make: {{2.products}} ─────────────────────
    payload = {
        "products": [{
            "product_id":  product_id,
            "name":        name,
            "price":       float(price),
            "section":     product.get("section", "update"),
            "comp_name":   product.get("comp_name", ""),
            "competitor":  product.get("competitor", ""),
            "price_diff":  product.get("price_diff", product.get("diff", 0)),
            "match_score": product.get("match_score", 0),
            "decision":    product.get("decision", ""),
            "brand":       product.get("brand", ""),
        }]
    }

    result = _post_to_webhook(get_webhook_update_prices(), payload)
    if result["success"]:
        pid_info = f" [ID: {product_id}]" if product_id else ""
        result["message"] = f"✅ تم تحديث «{name}»{pid_info} ← {price:,.0f} ر.س"
    return result


# ══════════════════════════════════════════════════════════════════════════
#  إرسال عدة منتجات — تحديث الأسعار
#  Payload: {"products": [{product_id, name, price, ...}]}
#  Make يقرأ: {{2.products}} → BasicFeeder → UpdateProduct
# ══════════════════════════════════════════════════════════════════════════
def send_price_updates(products: List[Dict]) -> Dict:
    """
    إرسال قائمة منتجات لتحديث أسعارها في سلة عبر Make.
    Payload: {"products": [{product_id, name, price, ...}]}
    Make يقرأ {{2.products}} ويمرر كل عنصر لـ UpdateProduct.
    """
    if not products:
        return {"success": False, "message": "❌ لا توجد منتجات للإرسال"}

    valid_products = []
    skipped = 0

    for p in products:
        name       = str(p.get("name", "")).strip()
        price      = _safe_float(p.get("price", 0))
        product_id = _clean_pid(p.get("product_id", ""))

        if not name or price <= 0:
            skipped += 1
            continue

        valid_products.append({
            "product_id":  product_id,
            "name":        name,
            "price":       float(price),
            "section":     p.get("section", "update"),
            "comp_name":   p.get("comp_name", ""),
            "competitor":  p.get("competitor", ""),
            "price_diff":  p.get("price_diff", p.get("diff", 0)),
            "match_score": p.get("match_score", 0),
            "decision":    p.get("decision", ""),
            "brand":       p.get("brand", ""),
        })

    if not valid_products:
        return {
            "success": False,
            "message": f"❌ لا توجد منتجات صالحة (تم تخطي {skipped} منتج)"
        }

    # ── Payload مطابق لما يقرأه Make: {{2.products}} ─────────────────────
    payload = {"products": valid_products}
    result = _post_to_webhook(get_webhook_update_prices(), payload)

    if result["success"]:
        skip_msg = f" (تم تخطي {skipped})" if skipped else ""
        result["message"] = f"✅ تم إرسال {len(valid_products)} منتج لتحديث الأسعار{skip_msg}"
    return result


# ══════════════════════════════════════════════════════════════════════════
#  إرسال منتجات جديدة — Webhook منفصل
#  Payload: {"data": [{"أسم المنتج":"...","سعر المنتج":...,"الوصف":"..."}]}
#  Make يقرأ: {{1.data}} → BasicFeeder → CreateProduct
# ══════════════════════════════════════════════════════════════════════════
def send_new_products(products: List[Dict]) -> Dict:
    """
    إرسال منتجات جديدة لإضافتها في سلة عبر Make.
    Payload: {"data": [{أسم المنتج, سعر المنتج, رمز المنتج sku, الوزن, ...}]}
    Make يقرأ {{1.data}} ويمرر كل عنصر لـ CreateProduct.
    يُرسل كل منتج في طلب مستقل.
    """
    if not products:
        return {"success": False, "message": "❌ لا توجد منتجات للإرسال"}

    sent, skipped, errors = 0, 0, []

    for p in products:
        name  = str(p.get("name", p.get("أسم المنتج", ""))).strip()
        price = _safe_float(
            p.get("price", 0) or p.get("سعر المنتج", 0) or p.get("السعر", 0)
        )
        pid   = _clean_pid(p.get("product_id", p.get("معرف_المنتج", "")))

        if not name:
            skipped += 1
            continue

        # ── بنية البيانات المطابقة لـ Interface سيناريو Make ─────────────
        item = {
            "product_id":      pid,
            "أسم المنتج":      name,
            "سعر المنتج":      float(price),
            "رمز المنتج sku":  str(p.get("sku", p.get("رمز المنتج sku", ""))).strip(),
            "الوزن":           int(_safe_float(p.get("weight", p.get("الوزن", 1))) or 1),
            "سعر التكلفة":     float(_safe_float(p.get("cost_price", p.get("سعر التكلفة", 0)))),
            "السعر المخفض":    float(_safe_float(p.get("sale_price",  p.get("السعر المخفض", 0)))),
            "الوصف":           str(p.get("الوصف", p.get("description", ""))).strip(),
        }
        # حقل صورة اختياري
        if p.get("image_url"):
            item["صورة المنتج"] = str(p["image_url"])

        result = _post_to_webhook(get_webhook_new_products(), {"data": [item]})
        if result["success"]:
            sent += 1
        else:
            errors.append(name)

        if len(products) > 1:
            time.sleep(0.3)

    if sent == 0:
        return {"success": False, "message": f"❌ فشل إرسال جميع المنتجات. تم تخطي {skipped}"}

    skip_msg = f" (تم تخطي {skipped})" if skipped else ""
    err_msg  = f" (فشل {len(errors)})" if errors else ""
    return {"success": True, "message": f"✅ تم إرسال {sent} منتج جديد إلى Make{skip_msg}{err_msg}"}


# ══════════════════════════════════════════════════════════════════════════
#  إرسال المنتجات المفقودة — نفس سيناريو المنتجات الجديدة
#  Payload: {"data": [{"أسم المنتج":"...","سعر المنتج":...,"الوصف":"..."}]}
# ══════════════════════════════════════════════════════════════════════════
def send_missing_products(products: List[Dict]) -> Dict:
    """
    إرسال المنتجات المفقودة لإضافتها في سلة عبر Make.
    يُستخدم نفس Webhook المنتجات الجديدة.
    Payload: {"data": [{أسم المنتج, سعر المنتج, ...}]}
    """
    if not products:
        return {"success": False, "message": "❌ لا توجد منتجات مفقودة للإرسال"}

    sent, skipped, errors = 0, 0, []

    for p in products:
        name  = str(p.get("name", p.get("المنتج", p.get("منتج_المنافس", "")))).strip()
        price = _safe_float(
            p.get("price", 0) or p.get("السعر", 0) or p.get("سعر_المنافس", 0)
        )
        pid   = _clean_pid(p.get("product_id", p.get("معرف_المنتج", "")))

        if not name:
            skipped += 1
            continue

        # ── بنية البيانات المطابقة لـ Interface سيناريو Make ─────────────
        item = {
            "product_id":      pid,
            "أسم المنتج":      name,
            "سعر المنتج":      float(price),
            "رمز المنتج sku":  str(p.get("sku", p.get("رمز المنتج sku", ""))).strip(),
            "الوزن":           int(_safe_float(p.get("weight", p.get("الوزن", 1))) or 1),
            "سعر التكلفة":     float(_safe_float(p.get("cost_price", p.get("سعر التكلفة", 0)))),
            "السعر المخفض":    float(_safe_float(p.get("sale_price",  p.get("السعر المخفض", 0)))),
            "الوصف":           str(p.get("الوصف", p.get("description", ""))).strip(),
        }
        if p.get("image_url"):
            item["صورة المنتج"] = str(p["image_url"])

        result = _post_to_webhook(get_webhook_new_products(), {"data": [item]})
        if result["success"]:
            sent += 1
        else:
            errors.append(name)

        if len(products) > 1:
            time.sleep(0.3)

    if sent == 0:
        return {"success": False, "message": f"❌ فشل إرسال جميع المنتجات المفقودة. تم تخطي {skipped}"}

    skip_msg = f" (تم تخطي {skipped})" if skipped else ""
    err_msg  = f" (فشل {len(errors)})" if errors else ""
    return {"success": True, "message": f"✅ تم إرسال {sent} منتج مفقود إلى Make{skip_msg}{err_msg}"}


# ══# ══════════════════════════════════════════════════════════════════════
#  إرسال بدفعات ذكية مع retry و progress callback
# ══════════════════════════════════════════════════════════════════════
def send_batch_smart(products: list, batch_type: str = "update",
                     batch_size: int = 20, max_retries: int = 3,
                     progress_cb=None, confidence_filter: str = "") -> Dict:
    """
    إرسال بدفعات ذكية مع retry تلقائي و progress callback.
    batch_type: "update" (تحديث أسعار) | "new" (منتجات جديدة/مفقودة)
    confidence_filter: "green" | "yellow" | "" (كل المستويات)
    progress_cb: callable(sent, failed, total, current_name)
    """
    if not products:
        return {"success": False, "message": "❌ لا توجد منتجات للإرسال",
                "sent": 0, "failed": 0, "total": 0, "errors": []}

    # فلترة حسب الثقة (للمفقودات)
    if confidence_filter:
        products = [p for p in products
                    if p.get("مستوى_الثقة", "green") == confidence_filter
                    or p.get("confidence_level", "green") == confidence_filter]

    total = len(products)
    if total == 0:
        return {"success": False, "message": "❌ لا توجد منتجات بهذا المستوى من الثقة",
                "sent": 0, "failed": 0, "total": 0, "errors": []}

    sent_count = 0
    fail_count = 0
    error_names = []

    # تقسيم لدفعات
    for i in range(0, total, batch_size):
        batch = products[i:i + batch_size]

        for attempt in range(1, max_retries + 1):
            try:
                if batch_type == "update":
                    result = send_price_updates(batch)
                else:
                    result = send_new_products(batch)

                if result["success"]:
                    sent_count += len(batch)
                    break
                elif attempt < max_retries:
                    time.sleep(2 * attempt)  # backoff
                    continue
                else:
                    fail_count += len(batch)
                    error_names.extend([p.get("name", p.get("منتج_المنافس", "?"))[:30] for p in batch])
            except Exception:
                if attempt >= max_retries:
                    fail_count += len(batch)
                    error_names.extend([p.get("name", "?")[:30] for p in batch])
                else:
                    time.sleep(2 * attempt)

        # progress callback
        if progress_cb:
            try:
                progress_cb(sent_count, fail_count, total,
                           batch[-1].get("name", "")[:30] if batch else "")
            except Exception:
                pass

        # تأخير بين الدفعات
        if i + batch_size < total:
            time.sleep(0.5)

    success = sent_count > 0
    msg_parts = []
    if sent_count > 0:
        msg_parts.append(f"✅ نجح {sent_count}")
    if fail_count > 0:
        msg_parts.append(f"❌ فشل {fail_count}")
    msg = f"إرسال {total} منتج: {' | '.join(msg_parts)}"

    return {
        "success":  success,
        "message":  msg,
        "sent":     sent_count,
        "failed":   fail_count,
        "total":    total,
        "errors":   error_names[:20],  # أول 20 خطأ فقط
    }



# ══════════════════════════════════════════════════════════════════════════
#  فحص Safety Net قبل إرسال تحديثات الأسعار
# ══════════════════════════════════════════════════════════════════════════

def _safety_validate_price_update(product: dict,
                                   max_drop_pct: float = 30.0,
                                   max_raise_pct: float = 50.0,
                                   abs_min_price: float = 10.0) -> tuple:
    """
    يتحقق من أن تحديث السعر منطقي قبل الإرسال.
    يُعيد: (is_safe: bool, reason: str)
    """
    new_price  = _safe_float(product.get("price", 0))
    old_price  = _safe_float(product.get("old_price", 0))

    # حد أدنى مطلق
    if new_price > 0 and new_price < abs_min_price:
        return False, f"السعر {new_price:.0f} ر.س أقل من الحد الأدنى {abs_min_price:.0f} ر.س"

    if old_price > 0 and new_price > 0:
        change_pct = abs(new_price - old_price) / old_price * 100
        if new_price < old_price and change_pct > max_drop_pct:
            return False, f"انخفاض {change_pct:.1f}% > {max_drop_pct:.0f}% ({old_price:.0f} → {new_price:.0f})"
        if new_price > old_price and change_pct > max_raise_pct:
            return False, f"ارتفاع {change_pct:.1f}% > {max_raise_pct:.0f}% ({old_price:.0f} → {new_price:.0f})"

    return True, ""


def send_price_updates_safe(products: list,
                             max_drop_pct: float = 30.0,
                             max_raise_pct: float = 50.0,
                             abs_min_price: float = 10.0) -> dict:
    """
    نسخة آمنة من send_price_updates — تفحص كل منتج بـ Safety Net أولاً.
    المنتجات المحجوبة تُعاد في blocked_products للمراجعة اليدوية.
    """
    if not products:
        return {"success": False, "message": "❌ لا توجد منتجات",
                "sent": 0, "blocked": 0, "blocked_products": []}

    safe_products    = []
    blocked_products = []

    for p in products:
        is_safe, reason = _safety_validate_price_update(
            p, max_drop_pct, max_raise_pct, abs_min_price
        )
        if is_safe:
            safe_products.append(p)
        else:
            blocked_products.append({**p, "_block_reason": reason})

    result = {
        "sent": 0,
        "blocked": len(blocked_products),
        "blocked_products": blocked_products,
    }

    if safe_products:
        send_result = send_price_updates(safe_products)
        result["success"] = send_result.get("success", False)
        result["sent"]    = len(safe_products) if send_result.get("success") else 0
        result["message"] = send_result.get("message", "")
    else:
        result["success"] = False
        result["message"] = "⛔ جميع المنتجات محجوبة بواسطة Safety Net"

    if blocked_products:
        result["message"] += f" | ⛔ {len(blocked_products)} محجوب للمراجعة"

    return result


def is_pricing_webhook_configured() -> bool:
    """True إذا كان WEBHOOK_UPDATE_PRICES مُعرّفاً في البيئة أو secrets."""
    u = get_webhook_update_prices()
    return bool(u and u.strip())


def build_pricing_sync_payload(
    df,
    *,
    price_col: str = "suggested_price",
    skip_unchanged: bool = True,
    min_new_price: float = 1.0,
) -> List[Dict]:
    """
    يحوّل مخرجات لوحة التسعير (final_priced_df) إلى قائمة جاهزة لـ Make:
    السعر المرسل = السعر المقترح، مع old_price للسلامة (Safety Net).
    """
    if df is None or (hasattr(df, "empty") and df.empty):
        return []

    products: List[Dict] = []
    for _, row in df.iterrows():
        name = str(row.get("name", "") or "").strip()
        if not name or name in ("nan", "None"):
            continue
        old_p = _safe_float(row.get("price", 0))
        new_p = _safe_float(row.get(price_col, 0))
        if new_p < min_new_price:
            continue
        if skip_unchanged and abs(new_p - old_p) < 0.01:
            continue
        pid = _clean_pid(
            row.get("معرف_المنتج")
            or row.get("product_id")
            or row.get("sku")
            or row.get("SKU")
            or ""
        )
        comp_name = str(row.get("comp_name", "") or "")
        mscore = _safe_float(row.get("match_score", 0))
        products.append(
            {
                "product_id": pid,
                "name": name,
                "price": float(new_p),
                "old_price": float(old_p),
                "section": "vsp_dashboard",
                "comp_name": comp_name if comp_name not in ("nan", "None", "") else "",
                "match_score": mscore,
            }
        )
    return products


def bulk_sync_pricing_recommendations(
    products: List[Dict],
    *,
    batch_size: int = 40,
    use_safe: bool = True,
) -> Dict[str, Any]:
    """
    مزامنة جماعية: إرسال السعر المقترح إلى Make في **دفعات** (مصفوفة لكل طلب)
    لتقليل الضغط على الحدود (Rate limits) مع بقاء كل طلب = payload واحد {products: [...]}.
    """
    if not products:
        return {
            "success": False,
            "message": "❌ لا توجد منتجات للمزامنة",
            "sent": 0,
            "failed_batches": 0,
            "blocked": 0,
        }
    if not is_pricing_webhook_configured():
        return {
            "success": False,
            "message": "❌ عرّف WEBHOOK_UPDATE_PRICES في متغيرات البيئة أو `.streamlit/secrets.toml`",
            "sent": 0,
            "failed_batches": 0,
            "blocked": 0,
        }

    total_sent = 0
    total_blocked = 0
    failed_batches = 0
    details: List[str] = []

    for i in range(0, len(products), max(1, int(batch_size))):
        batch = products[i : i + batch_size]
        if use_safe:
            r = send_price_updates_safe(batch)
            total_sent += int(r.get("sent", 0) or 0)
            total_blocked += int(r.get("blocked", 0) or 0)
            if r.get("sent", 0) == 0 and r.get("blocked", 0) == 0:
                failed_batches += 1
            details.append(str(r.get("message", "")))
        else:
            r = send_price_updates(batch)
            if r.get("success"):
                total_sent += len(
                    [
                        p
                        for p in batch
                        if str(p.get("name", "")).strip()
                        and _safe_float(p.get("price", 0)) > 0
                    ]
                )
            else:
                failed_batches += 1
            details.append(str(r.get("message", "")))
        if i + batch_size < len(products):
            time.sleep(0.5)

    ok = total_sent > 0
    msg = f"✅ أُرسل {total_sent} تحديثاً"
    if total_blocked:
        msg += f" | ⛔ محجوب للمراجعة: {total_blocked}"
    if failed_batches:
        msg += f" | ⚠️ دفعات بلا إرسال: {failed_batches}"
    return {
        "success": ok,
        "message": msg,
        "sent": total_sent,
        "blocked": total_blocked,
        "failed_batches": failed_batches,
        "details": details[:10],
    }


# ══════════════════════════════════════════════════════════════════════
#  فحص حالة الاتصال بـ Webhooks
# ══════════════════════════════════════════════════════════════════════════
def verify_webhook_connection() -> Dict:
    """
    فحص حالة الاتصال بجميع Webhooks.
    يُعيد dict: {"update_prices": {...}, "new_products": {...}, "all_connected": bool}
    """
    # فحص Webhook تحديث الأسعار — Payload المطابق للـ Parameters
    test_price_payload = {
        "products": [{
            "product_id": "test-001",
            "name":       "اختبار الاتصال",
            "price":      1.0,
            "section":    "test",
        }]
    }
    _wu = get_webhook_update_prices()
    _wn = get_webhook_new_products()
    r1 = _post_to_webhook(_wu, test_price_payload)

    # فحص Webhook المنتجات الجديدة
    test_new_payload = {
        "data": [{
            "product_id":     "",
            "أسم المنتج":     "اختبار الاتصال",
            "سعر المنتج":     1.0,
            "رمز المنتج sku": "",
            "الوزن":          1,
            "سعر التكلفة":    0,
            "السعر المخفض":   0,
            "الوصف":          "test",
        }]
    }
    r2 = _post_to_webhook(_wn, test_new_payload)

    return {
        "update_prices": {
            "success": r1["success"],
            "message": r1["message"],
            "url": _wu[:55] + "..." if len(_wu) > 55 else _wu,
        },
        "new_products": {
            "success": r2["success"],
            "message": r2["message"],
            "url": _wn[:55] + "..." if len(_wn) > 55 else _wn,
        },
        "all_connected": r1["success"] and r2["success"],
    }
