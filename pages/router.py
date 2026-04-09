"""
pages/router.py
================
[FIX OPT-03] مُوجِّه الصفحات — يفصل منطق التوجيه عن محتوى الصفحات.

يُقلِّص app.py من 4,100+ سطر إلى router خفيف < 50 سطر.

كيفية الاستخدام:
    # في app.py — أضف في القمة:
    from pages.router import dispatch_page

    # ثم استبدل كتلة if/elif الضخمة بسطر واحد:
    dispatch_page(page)

التقسيم التدريجي (migration path):
    المرحلة 1 — router.py موجود (الآن): لا تغيير وظيفي، فقط توثيق.
    المرحلة 2 — انقل كل قسم إلى ملفه (pages/dashboard.py إلخ).
    المرحلة 3 — استبدل كتلة if/elif بـ dispatch_page(page).
"""
from __future__ import annotations
import importlib
from typing import Callable

# خريطة: اسم الصفحة → (اسم الوحدة, اسم الدالة)
# تُملَأ تدريجياً مع نقل كل قسم إلى ملفه الخاص
_PAGE_REGISTRY: dict[str, tuple[str, str]] = {
    # مثال عند نقل قسم الكشط:
    # "🕷️ كشط المنافسين": ("pages.scraper", "render"),
    # "⚙️ الإعدادات":      ("pages.settings", "render"),
}


def dispatch_page(page: str) -> bool:
    """
    يُوجِّه الصفحة المطلوبة إلى وحدتها.
    يُعيد True إذا وجد handler مسجَّل، False إذا يجب أن يعالجها app.py.

    يسمح بالنقل التدريجي: ابدأ بنقل الأقسام الأكبر
    (كشط المنافسين 571 سطر، لوحة التحكم 458 سطر) واحداً تلو الآخر.
    """
    if page not in _PAGE_REGISTRY:
        return False   # app.py يتولى المعالجة كالمعتاد

    module_name, func_name = _PAGE_REGISTRY[page]
    try:
        mod = importlib.import_module(module_name)
        fn: Callable = getattr(mod, func_name)
        fn()
        return True
    except Exception as exc:
        import streamlit as st
        st.error(f"خطأ في تحميل صفحة '{page}': {exc}")
        return False


def register_page(page_name: str, module: str, func: str = "render") -> None:
    """يُسجِّل صفحة جديدة في الـ registry."""
    _PAGE_REGISTRY[page_name] = (module, func)
