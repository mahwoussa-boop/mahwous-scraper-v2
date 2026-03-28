"""
utils/helpers.py — دوال مساعدة محسّنة لمعالجة البيانات والفلاتر والتصدير
════════════════════════════════════════════════════════════════════════════
✅ معالجة آمنة للبيانات الفارغة والمفقودة
✅ فلاتر ذكية تتعامل مع جميع الأعمدة
✅ تصدير Excel متقدم مع دعم البادئات
✅ توافق كامل مع جميع استدعاءات app.py
"""
import pandas as pd
import io
import re
from typing import Dict, List, Any, Optional, Tuple


# ════════════════════════════════════════════════════════════════════════════
# دوال تحويل البيانات (Data Conversion)
# ════════════════════════════════════════════════════════════════════════════

def safe_float(val):
    """تحويل القيمة إلى رقم عشري بأمان"""
    try:
        if pd.isna(val) or val is None:
            return 0.0
        if isinstance(val, (int, float)):
            return float(val)
        # إزالة العملة والرموز
        s = str(val).replace('ر.س', '').replace('SR', '').replace(',', '').strip()
        return float(re.sub(r'[^\d.]', '', s))
    except:
        return 0.0


def format_price(val):
    """تنسيق السعر للعرض"""
    try:
        return f"{safe_float(val):,.2f} ر.س"
    except:
        return "0.00 ر.س"


def format_diff(val):
    """تنسيق فرق السعر"""
    try:
        val = safe_float(val)
        if val > 0:
            return f"+{val:,.2f} ر.س"
        return f"{val:,.2f} ر.س"
    except:
        return "0.00 ر.س"


# ════════════════════════════════════════════════════════════════════════════
# دوال الفلترة (Filtering)
# ════════════════════════════════════════════════════════════════════════════

def apply_filters(df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
    """
    تطبيق الفلاتر على DataFrame
    
    Args:
        df: البيانات الأصلية
        filters: قاموس يحتوي على الفلاتر (search, brand, competitor, type, match_min, price_min, price_max)
    
    Returns:
        DataFrame مع تطبيق الفلاتر
    """
    if df is None or df.empty:
        return df
    
    try:
        filtered_df = df.copy()
        
        # فلتر البحث (Search)
        if filters.get("search"):
            search_term = str(filters["search"]).lower()
            # البحث في جميع الأعمدة النصية
            mask = filtered_df.astype(str).apply(
                lambda row: row.str.contains(search_term, case=False, na=False).any(), 
                axis=1
            )
            filtered_df = filtered_df[mask]
        
        # فلتر الماركة (Brand)
        if filters.get("brand") and filters["brand"] != "الكل":
            brand_cols = [col for col in filtered_df.columns if 'ماركة' in col or 'brand' in col.lower()]
            if brand_cols:
                filtered_df = filtered_df[filtered_df[brand_cols[0]].astype(str) == str(filters["brand"])]
        
        # فلتر المنافس (Competitor)
        if filters.get("competitor") and filters["competitor"] != "الكل":
            comp_cols = [col for col in filtered_df.columns if 'منافس' in col or 'competitor' in col.lower()]
            if comp_cols:
                filtered_df = filtered_df[filtered_df[comp_cols[0]].astype(str) == str(filters["competitor"])]
        
        # فلتر النوع (Type)
        if filters.get("type") and filters["type"] != "الكل":
            type_cols = [col for col in filtered_df.columns if 'نوع' in col or 'type' in col.lower() or 'فئة' in col or 'category' in col.lower()]
            if type_cols:
                filtered_df = filtered_df[filtered_df[type_cols[0]].astype(str) == str(filters["type"])]
        
        # فلتر الحد الأدنى للتطابق (Match Min)
        if filters.get("match_min") and filters["match_min"] > 0:
            match_cols = [col for col in filtered_df.columns if 'تطابق' in col or 'match' in col.lower()]
            if match_cols:
                filtered_df = filtered_df[filtered_df[match_cols[0]].apply(safe_float) >= filters["match_min"]]
        
        # فلتر السعر الأدنى (Price Min)
        if filters.get("price_min") and filters["price_min"] > 0:
            price_cols = [col for col in filtered_df.columns if 'السعر' in col or 'price' in col.lower()]
            if price_cols:
                filtered_df = filtered_df[filtered_df[price_cols[0]].apply(safe_float) >= filters["price_min"]]
        
        # فلتر السعر الأقصى (Price Max)
        if filters.get("price_max") and filters["price_max"] > 0:
            price_cols = [col for col in filtered_df.columns if 'السعر' in col or 'price' in col.lower()]
            if price_cols:
                filtered_df = filtered_df[filtered_df[price_cols[0]].apply(safe_float) <= filters["price_max"]]
        
        return filtered_df
    
    except Exception as e:
        return df


def get_filter_options(df: pd.DataFrame) -> Dict[str, List]:
    """
    الحصول على خيارات الفلترة من جميع الأعمدة المتاحة
    
    Args:
        df: DataFrame للحصول على الخيارات منه
    
    Returns:
        قاموس يحتوي على:
        - brands: قائمة الماركات الفريدة
        - competitors: قائمة المنافسين الفريدة
        - types: قائمة الأنواع الفريدة
    """
    options = {
        "brands": ["الكل"],
        "competitors": ["الكل"],
        "types": ["الكل"]
    }
    
    if df is None or df.empty:
        return options
    
    try:
        # البحث عن أعمدة الماركات (بأسماء مختلفة محتملة)
        brand_cols = [col for col in df.columns if 'ماركة' in col or 'brand' in col.lower() or 'العلامة' in col]
        if brand_cols:
            col = brand_cols[0]
            brands = sorted([str(x) for x in df[col].dropna().unique() if x and str(x) != 'nan'])
            options["brands"] = ["الكل"] + brands
        
        # البحث عن أعمدة المنافسين
        comp_cols = [col for col in df.columns if 'منافس' in col or 'competitor' in col.lower() or 'المنافس' in col]
        if comp_cols:
            col = comp_cols[0]
            competitors = sorted([str(x) for x in df[col].dropna().unique() if x and str(x) != 'nan'])
            options["competitors"] = ["الكل"] + competitors
        
        # البحث عن أعمدة النوع
        type_cols = [col for col in df.columns if 'نوع' in col or 'type' in col.lower() or 'فئة' in col or 'category' in col.lower()]
        if type_cols:
            col = type_cols[0]
            types = sorted([str(x) for x in df[col].dropna().unique() if x and str(x) != 'nan'])
            options["types"] = ["الكل"] + types
    
    except Exception as e:
        pass
    
    return options


# ════════════════════════════════════════════════════════════════════════════
# دوال التصدير (Export)
# ════════════════════════════════════════════════════════════════════════════

def export_to_excel(df: pd.DataFrame, prefix: str = "export") -> bytes:
    """
    تصدير DataFrame إلى ملف Excel في الذاكرة
    
    Args:
        df: البيانات المراد تصديرها
        prefix: بادئة اسم الملف (اختياري)
    
    Returns:
        بيانات الملف بصيغة bytes
    """
    try:
        if df is None or df.empty:
            return b""
        
        output = io.BytesIO()
        
        # إنشاء نسخة من DataFrame لتجنب التعديل على الأصلي
        export_df = df.copy()
        
        # تنظيف الأعمدة غير المرغوبة
        cols_to_drop = [col for col in export_df.columns if 'جميع المنافسين' in col or 'جميع_المنافسين' in col]
        if cols_to_drop:
            export_df = export_df.drop(columns=cols_to_drop)
        
        # كتابة إلى Excel
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            export_df.to_excel(writer, index=False, sheet_name=prefix[:31])  # حد أقصى 31 حرف
        
        output.seek(0)
        return output.getvalue()
    
    except Exception as e:
        return b""


def export_multiple_sheets(sheets_dict: Dict[str, pd.DataFrame]) -> bytes:
    """
    تصدير عدة جداول إلى ملف Excel واحد بصفحات متعددة
    
    Args:
        sheets_dict: قاموس يحتوي على {اسم_الورقة: DataFrame}
    
    Returns:
        بيانات الملف بصيغة bytes
    """
    try:
        if not sheets_dict:
            return b""
        
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            for sheet_name, df in sheets_dict.items():
                if df is not None and not df.empty:
                    # تحديد طول اسم الورقة (Excel حد أقصى 31 حرف)
                    safe_name = str(sheet_name)[:31]
                    df.to_excel(writer, index=False, sheet_name=safe_name)
        
        output.seek(0)
        return output.getvalue()
    
    except Exception as e:
        return b""


# ════════════════════════════════════════════════════════════════════════════
# دوال معالجة النصوص (Text Processing)
# ════════════════════════════════════════════════════════════════════════════

def parse_pasted_text(text: str) -> List[Dict[str, Any]]:
    """
    تحليل النص الملصق لاستخراج المنتجات والأسعار
    
    Args:
        text: النص المراد تحليله
    
    Returns:
        قائمة من قواميس تحتوي على {name, price}
    """
    products = []
    
    if not text:
        return products
    
    try:
        lines = text.strip().split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # محاولة استخراج اسم وسعر
            match = re.search(r'(\d+(?:\.\d+)?)', line)
            if match:
                try:
                    price = float(match.group(1))
                    name = line.replace(match.group(0), '').strip()
                    if name:
                        products.append({'name': name, 'price': price})
                except:
                    continue
    except Exception as e:
        pass
    
    return products


# ════════════════════════════════════════════════════════════════════════════
# دوال معالجة البيانات (Data Processing)
# ════════════════════════════════════════════════════════════════════════════

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    تنظيف DataFrame من الأخطاء والقيم الفارغة
    
    Args:
        df: البيانات المراد تنظيفها
    
    Returns:
        DataFrame منظف
    """
    if df is None or df.empty:
        return df
    
    try:
        df = df.copy()
        
        # ملء القيم الفارغة بـ 0 للأعمدة الرقمية
        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            df[col] = df[col].fillna(0)
        
        # ملء القيم الفارغة بـ "غير محدد" للأعمدة النصية
        string_cols = df.select_dtypes(include=['object']).columns
        for col in string_cols:
            df[col] = df[col].fillna("غير محدد")
        
        return df
    except:
        return df


def get_column_by_name_variant(df: pd.DataFrame, variants: List[str]) -> Optional[str]:
    """
    البحث عن عمود في DataFrame بناءً على قائمة من الأسماء المحتملة
    
    Args:
        df: DataFrame للبحث فيه
        variants: قائمة الأسماء المحتملة
    
    Returns:
        اسم العمود إن وجد، وإلا None
    """
    if df is None:
        return None
    
    for variant in variants:
        if variant in df.columns:
            return variant
    
    return None


def safe_get_column(df: pd.DataFrame, col_name: str, default=None):
    """
    الحصول على عمود من DataFrame بأمان
    
    Args:
        df: DataFrame
        col_name: اسم العمود
        default: القيمة الافتراضية إذا لم يكن العمود موجوداً
    
    Returns:
        العمود أو القيمة الافتراضية
    """
    if df is None or col_name not in df.columns:
        return default
    return df[col_name]


def rename_columns_safe(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    """
    إعادة تسمية أعمدة DataFrame بأمان
    
    Args:
        df: البيانات
        mapping: قاموس يحتوي على {الاسم_القديم: الاسم_الجديد}
    
    Returns:
        DataFrame مع الأعمدة المعاد تسميتها
    """
    if df is None:
        return df
    
    try:
        # تصفية المفاتيح التي توجد فعلاً في الأعمدة
        valid_mapping = {k: v for k, v in mapping.items() if k in df.columns}
        return df.rename(columns=valid_mapping)
    except:
        return df


# ════════════════════════════════════════════════════════════════════════════
# دوال مساعدة إضافية (Utility Functions)
# ════════════════════════════════════════════════════════════════════════════

def get_dataframe_info(df: pd.DataFrame) -> Dict[str, Any]:
    """
    الحصول على معلومات عن DataFrame
    
    Args:
        df: البيانات
    
    Returns:
        قاموس يحتوي على معلومات عن البيانات
    """
    if df is None or df.empty:
        return {
            "rows": 0,
            "columns": 0,
            "column_names": [],
            "dtypes": {}
        }
    
    return {
        "rows": len(df),
        "columns": len(df.columns),
        "column_names": df.columns.tolist(),
        "dtypes": df.dtypes.to_dict(),
        "memory_usage": df.memory_usage(deep=True).sum()
    }


def filter_by_column_value(df: pd.DataFrame, column: str, value: Any) -> pd.DataFrame:
    """
    فلترة DataFrame بناءً على قيمة عمود معين
    
    Args:
        df: البيانات
        column: اسم العمود
        value: القيمة المراد البحث عنها
    
    Returns:
        DataFrame مع الصفوف المطابقة
    """
    if df is None or column not in df.columns:
        return df
    
    try:
        return df[df[column].astype(str) == str(value)]
    except:
        return df

def make_columns_unique(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames duplicate columns in a DataFrame to make them unique.
    Appends a suffix like '_1', '_2' to duplicate column names.
    """
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        # Get all indices of the duplicate column
        indices = cols[cols == dup].index.tolist()
        for i, idx in enumerate(indices):
            if i > 0: # Only rename subsequent duplicates
                cols[idx] = f"{dup}_{i}"
    df.columns = cols
    return df
