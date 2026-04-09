"""
engines/file_reader.py  v1.0 — وحدة قراءة الملفات المستقلة
════════════════════════════════════════════════════════════════

مسؤولية واحدة فقط: قراءة ملفات CSV/Excel وإرجاعها كـ DataFrame.

مبدأ التصميم (Separation of Concerns):
    - هذه الوحدة مسؤولة عن الـ I/O فقط.
    - لا تحتوي على أي منطق مطابقة أو تسعير.
    - محرك المطابقة (closed_loop_engine.py) يستقبل DataFrame جاهزاً فقط.
    - إذا أردت استبدال طريقة القراءة لاحقاً، لا تلمس سوى هذا الملف.

سلسلة اكتشاف الترميز (Encoding Detection Chain):
    1. utf-8-sig  — UTF-8 مع BOM (تصدير Excel/Windows)
    2. cp1256     — Windows-1256 (عربي Windows قديم) ← الأكثر شيوعاً بعد UTF-8
    3. utf-8      — UTF-8 بدون BOM
    4. latin-1    — لا يُعطي UnicodeDecodeError أبداً (fallback صامت مع تحذير)

⚠️  للتحليل والعرض فقط — لا يُعدِّل أي أسعار ولا يتصل بأي API ⚠️
"""
from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import IO, Optional, Tuple, Union

import pandas as pd

logger = logging.getLogger(__name__)

# ترتيب محاولات الترميز
_ENCODING_CHAIN: list[str] = ["utf-8-sig", "cp1256", "utf-8", "latin-1"]


# ══════════════════════════════════════════════════════════════════════════════
#  1.  الدالة الأساسية — read_csv_safe
# ══════════════════════════════════════════════════════════════════════════════

def read_csv_safe(
    source: Union[str, Path, IO[bytes]],
    *,
    fallback_encoding: str = "latin-1",
    low_memory: bool = False,
) -> Tuple[pd.DataFrame, str]:
    """
    يقرأ ملف CSV مع اكتشاف الترميز تلقائياً.

    يجرب السلسلة: utf-8-sig → cp1256 → utf-8 → latin-1 (لا يفشل أبداً).

    Parameters
    ----------
    source : str | Path | file-like
        مسار الملف أو كائن ملف (مثل Streamlit UploadedFile).
    fallback_encoding : str
        الترميز الأخير الاحتياطي. latin-1 لا يُعطي خطأ مع أي بيانات.
    low_memory : bool
        تمرير إلى pandas. False أآمن للأعمدة المختلطة.

    Returns
    -------
    tuple[pd.DataFrame, str]
        (DataFrame المحمّل, اسم الترميز الذي نجح).

    Raises
    ------
    ValueError
        إذا كان الملف فارغاً تماماً بعد القراءة الناجحة.
    """
    # قراءة bytes مرة واحدة حتى نتجنب seek errors في كائنات Streamlit
    if hasattr(source, "read"):
        raw_bytes: bytes = source.read()
        if hasattr(source, "seek"):
            source.seek(0)
    else:
        raw_bytes = Path(source).read_bytes()

    encodings = list(_ENCODING_CHAIN)
    if fallback_encoding not in encodings:
        encodings.append(fallback_encoding)

    last_error: Exception = RuntimeError("لم يتم تحديد خطأ")

    for enc in encodings:
        try:
            df = pd.read_csv(
                io.BytesIO(raw_bytes),
                dtype=str,
                encoding=enc,
                low_memory=low_memory,
            )
            if df.empty:
                raise ValueError(f"الملف فارغ بعد القراءة بترميز {enc!r}.")
            logger.info("✅ قُرئ الملف بترميز: %s (%d صف)", enc, len(df))
            return df, enc

        except (UnicodeDecodeError, UnicodeError) as exc:
            logger.debug("⏩ فشل الترميز %r: %s — جرب التالي…", enc, exc)
            last_error = exc
            continue

        except pd.errors.EmptyDataError as exc:
            raise ValueError("الملف فارغ أو لا يحتوي على أعمدة صالحة.") from exc

    # آخر محاولة: latin-1 مع errors='replace' (لا ينهار أبداً)
    logger.warning(
        "⚠️ فشلت جميع الترميزات القياسية — القراءة بـ latin-1/replace "
        "(احتمال ظهور رموز مشوهة في الأسماء العربية)."
    )
    df = pd.read_csv(
        io.BytesIO(raw_bytes),
        dtype=str,
        encoding="latin-1",
        encoding_errors="replace",
        low_memory=low_memory,
    )
    return df, "latin-1 (replace)"


# ══════════════════════════════════════════════════════════════════════════════
#  2.  واجهة مبسطة للاستخدام السريع في app.py
# ══════════════════════════════════════════════════════════════════════════════

def load_csv(
    source: Union[str, Path, IO[bytes]],
) -> pd.DataFrame:
    """
    يُحمِّل CSV ويُرجع DataFrame فقط (بدون بيانات الترميز).

    للاستخدام السريع في Streamlit:
        df = load_csv(uploaded_file)
    """
    df, enc = read_csv_safe(source)
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  3.  تصدير آمن مع طابع زمني لمنع الكتابة فوق الملفات القديمة
# ══════════════════════════════════════════════════════════════════════════════

def make_export_filename(base_name: str, extension: str = "xlsx") -> str:
    """
    يُنشئ اسم ملف يحمل طابعاً زمنياً بدقة الثانية لمنع الكتابة فوق تقارير سابقة.

    مثال:
        make_export_filename("تقرير_المطابقة")
        → "تقرير_المطابقة_2024-11-15_143022.xlsx"

    Parameters
    ----------
    base_name : str
        الاسم الأساسي بدون امتداد.
    extension : str
        امتداد الملف بدون نقطة (افتراضي: xlsx).

    Returns
    -------
    str
        اسم الملف الكامل مع الطابع الزمني.
    """
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    ext = extension.lstrip(".")
    return f"{base_name}_{timestamp}.{ext}"
