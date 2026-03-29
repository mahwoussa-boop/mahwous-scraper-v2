"""
خط أنابيب التسعير الكامل — دمج مطابقة ذكية + بيانات المنافس من الكاشط + محرك التسعير.
"""
from __future__ import annotations

import logging
import os
import threading
import time

import pandas as pd

from engines.ai_engine_enhanced import EnhancedAIPricingEngine
from utils.gemini_verifier import GeminiMatchVerifier
from utils.matcher import SmartMatcher

logger = logging.getLogger(__name__)
_AUTO_PIPELINE_LOCK = threading.Lock()
_LAST_AUTO_PIPELINE_AT = 0.0


def _merge_priced_with_previous(
    priced_df: pd.DataFrame,
    out_csv: str,
    df_mine: pd.DataFrame,
) -> pd.DataFrame:
    """
    يدمج نتيجة المطابقة الحالية مع صفوف سابقة في final_priced_latest.csv
    لنفس skus الكتالوج التي لم تُعدَّ في الجولة الحالية (لا يُفقد صف بسبب تذبذب المطابقة).
    """
    if priced_df is None or priced_df.empty or "sku" not in priced_df.columns:
        return priced_df
    if not os.path.isfile(out_csv):
        return priced_df
    try:
        old = pd.read_csv(out_csv, encoding="utf-8-sig")
    except Exception:
        return priced_df
    if old is None or old.empty or "sku" not in old.columns:
        return priced_df
    mine = set(df_mine["sku"].fillna("").astype(str).str.strip())
    new_skus = set(priced_df["sku"].fillna("").astype(str).str.strip())
    osku = old["sku"].fillna("").astype(str).str.strip()
    carry = old.loc[osku.isin(mine) & ~osku.isin(new_skus)].copy()
    if carry.empty:
        return priced_df
    for c in priced_df.columns:
        if c not in carry.columns:
            carry[c] = pd.NA
    carry = carry.reindex(columns=list(priced_df.columns), fill_value=pd.NA)
    out = pd.concat([priced_df, carry], ignore_index=True, sort=False)
    return out


def _normalize_competitor_csv(df_comp: pd.DataFrame) -> pd.DataFrame:
    """يُوحّد أسماء الأعمدة بعد قراءة competitors_latest.csv من المكشطة."""
    import hashlib

    d = df_comp.copy()
    # رؤوس عربية من الكاشط الحديث
    _ar_map = {
        "الاسم": "name",
        "السعر": "price",
        "الماركة": "brand",
        "رابط_الصورة": "image_url",
        "رابط_المنتج": "comp_url",
    }
    for ar, en in _ar_map.items():
        if ar in d.columns and en not in d.columns:
            d[en] = d[ar]
    if "comp_sku" not in d.columns and "sku" in d.columns:
        d["comp_sku"] = d["sku"].fillna("").astype(str)
    elif "comp_sku" in d.columns:
        d["comp_sku"] = d["comp_sku"].fillna("").astype(str)
    elif "comp_url" in d.columns:
        d["comp_sku"] = d["comp_url"].apply(
            lambda u: hashlib.sha256(str(u).encode("utf-8")).hexdigest()[:16]
            if pd.notna(u) and str(u).strip()
            else ""
        )
    if "comp_price" not in d.columns and "price" in d.columns:
        d["comp_price"] = d["price"]
    if "comp_name" not in d.columns and "name" in d.columns:
        d["comp_name"] = d["name"]
    if "sku" not in d.columns and "comp_sku" in d.columns:
        d["sku"] = d["comp_sku"].astype(str)
    return d


def load_competitors_latest_for_engine(path: str | None = None):
    """
    يقرأ competitors_latest.csv ويُحضّر أعمدة متوافقة مع run_full_analysis / CompIndex
    (اسم المنتج، السعر، sku إن وُجد).
    """
    p = path or os.path.join("data", "competitors_latest.csv")
    if not os.path.isfile(p):
        return None, f"الملف غير موجود: {p}"
    try:
        raw = pd.read_csv(p, encoding="utf-8-sig")
    except PermissionError:
        return (
            None,
            "تعذر قراءة الملف (قد يكون مفتوحاً للكتابة أثناء الكشط). انتظر ثوانٍ ثم أعد المحاولة.",
        )
    except Exception as e:
        return None, str(e)
    if raw is None or raw.empty:
        return None, "ملف الكشط فارغ — انتظر حتى يُسجّل الكاشط صفوفاً."
    d = _normalize_competitor_csv(raw)
    if "اسم المنتج" not in d.columns:
        if "name" in d.columns:
            d = d.rename(columns={"name": "اسم المنتج"})
        elif "الاسم" in d.columns:
            d = d.rename(columns={"الاسم": "اسم المنتج"})
    if "اسم المنتج" not in d.columns:
        return None, "ملف الكشط لا يحتوي عمود اسم منتج (الاسم / name)."
    if "السعر" not in d.columns:
        if "price" in d.columns:
            d["السعر"] = pd.to_numeric(d["price"], errors="coerce").fillna(0.0)
        elif "comp_price" in d.columns:
            d["السعر"] = pd.to_numeric(d["comp_price"], errors="coerce").fillna(0.0)
        else:
            return None, "ملف الكشط لا يحتوي عمود سعر."
    return d, None


def run_full_pricing_pipeline(df_mine: pd.DataFrame) -> pd.DataFrame:
    """
    هذا هو القلب النابض للنظام (Data Fusion).
    يقوم بربط بياناتك مع بيانات المنافسين المحدثة، يطابقها بذكاء، ثم يسعرها عبر الـ AI.
    """
    comp_file = "data/competitors_latest.csv"
    if not os.path.exists(comp_file):
        raise FileNotFoundError(
            "لم يتم العثور على بيانات المنافسين. الرجاء تشغيل الكاشط أولاً."
        )

    df_comp = pd.read_csv(comp_file)
    df_comp = _normalize_competitor_csv(df_comp)

    required_mine = ["sku", "name", "price", "cost"]
    for col in required_mine:
        if col not in df_mine.columns:
            df_mine[col] = 0 if col in ["price", "cost"] else ""

    if "image_url" not in df_mine.columns:
        df_mine["image_url"] = ""

    df_mine = df_mine.copy()
    df_mine["sku"] = df_mine["sku"].fillna("").astype(str)

    logger.info("جاري بدء عملية المطابقة الذكية...")
    matcher = SmartMatcher(fuzzy_threshold=88)
    matched_pairs = matcher.match_products(df_mine, df_comp)

    if matched_pairs.empty:
        raise ValueError(
            "لم يتم العثور على أي تطابق بين منتجاتك ومنتجات المنافسين."
        )

    logger.info("جاري دمج البيانات (Data Fusion)...")
    mp = matched_pairs.copy()
    mp["sku_mine"] = mp["sku_mine"].fillna("").astype(str)

    final_df = pd.merge(
        mp,
        df_mine,
        left_on="sku_mine",
        right_on="sku",
        how="left",
    )

    need_cols = ["comp_sku", "comp_price", "comp_url"]
    for c in need_cols:
        if c not in df_comp.columns:
            raise ValueError(
                f"ملف المنافسين يفتقد العمود المطلوب '{c}' بعد التطبيع. "
                "تأكد من تشغيل المكشطة الأحدث (عمود sku و price و comp_url)."
            )

    subset_cols = ["comp_sku", "comp_price", "comp_url"] + (
        ["comp_name"] if "comp_name" in df_comp.columns else []
    )
    if "brand" in df_comp.columns:
        subset_cols.append("brand")
    if "image_url" in df_comp.columns:
        subset_cols.append("image_url")

    subset = df_comp[subset_cols].drop_duplicates(subset=["comp_sku"])
    _ren = {}
    if "brand" in subset.columns:
        _ren["brand"] = "comp_brand"
    if "image_url" in subset.columns:
        _ren["image_url"] = "comp_image_url"
    if _ren:
        subset = subset.rename(columns=_ren)

    final_df["sku_comp"] = final_df["sku_comp"].fillna("").astype(str)
    subset["comp_sku"] = subset["comp_sku"].astype(str)

    final_df = pd.merge(
        final_df,
        subset,
        left_on="sku_comp",
        right_on="comp_sku",
        how="left",
    )

    if "comp_name" not in final_df.columns and "name_comp" in final_df.columns:
        final_df["comp_name"] = final_df["name_comp"].astype(str)

    if "comp_name" not in final_df.columns:
        final_df["comp_name"] = (
            final_df["name_comp"].astype(str)
            if "name_comp" in final_df.columns
            else ""
        )

    final_df["price"] = pd.to_numeric(final_df["price"], errors="coerce").fillna(0)
    final_df["cost"] = pd.to_numeric(final_df["cost"], errors="coerce").fillna(0)
    final_df["comp_price"] = pd.to_numeric(final_df["comp_price"], errors="coerce").fillna(
        0
    )

    # Gemini AI verifier for doubtful matches (50%..79%)
    final_df["ai_verification_state"] = "not_checked"
    final_df["ai_verification_confidence"] = 0
    final_df["ai_verification_reason"] = ""
    verifier = GeminiMatchVerifier()
    doubtful_mask = final_df["match_score"].between(50, 79, inclusive="both")
    missing_rows = pd.DataFrame()
    if doubtful_mask.any():
        idxs = final_df.index[doubtful_mask].tolist()
        _gemini_log_n = 0
        for idx in idxs:
            row = final_df.loc[idx]
            mah_name = str(row.get("name", "") or "")
            comp_name = str(row.get("comp_name", row.get("name_comp", "")) or "")
            if _gemini_log_n < 12:
                try:
                    from utils.live_price_store import append_activity_log

                    append_activity_log(
                        f"🤖 Gemini: فحص مطابقة «{mah_name[:70]}» ↔ «{comp_name[:50]}»"
                    )
                    _gemini_log_n += 1
                except Exception:
                    pass
            vr = verifier.verify_perfume_match(mah_name, comp_name)
            is_match = bool(vr.get("is_match", False))
            conf = int(vr.get("confidence", 0) or 0)
            reason = str(vr.get("reason", "") or "")
            final_df.at[idx, "ai_verification_confidence"] = conf
            final_df.at[idx, "ai_verification_reason"] = reason
            if is_match and conf >= 85:
                final_df.at[idx, "ai_verification_state"] = "verified_by_ai"
                if final_df.at[idx, "match_score"] < conf:
                    final_df.at[idx, "match_score"] = float(conf)
            elif not is_match:
                final_df.at[idx, "ai_verification_state"] = "missing_candidate"
                final_df.at[idx, "status"] = "missing_after_verification"
                final_df.at[idx, "action_required"] = "🔍 منتجات مفقودة"
            else:
                final_df.at[idx, "ai_verification_state"] = "under_review"

        missing_rows = final_df[
            final_df["ai_verification_state"] == "missing_candidate"
        ].copy()
        final_df = final_df[
            final_df["ai_verification_state"] != "missing_candidate"
        ].copy()

    logger.info("جاري تشغيل محرك التسعير (VSP)...")
    ai_engine = EnhancedAIPricingEngine()
    priced_df = ai_engine.process_pricing_strategy(final_df, target_margin=0.35)

    # Keep missing rows visible for procurement workflow, but never priced
    if not missing_rows.empty:
        missing_rows["action_required"] = "🔍 منتجات مفقودة"
        missing_rows["status"] = "missing_after_verification"
        missing_rows["suggested_price"] = pd.to_numeric(
            missing_rows.get("price", 0), errors="coerce"
        ).fillna(0)
        priced_df = pd.concat([priced_df, missing_rows], ignore_index=True, sort=False)

    columns_to_keep = [
        "sku",
        "name",
        "image_url",
        "cost",
        "price",
        "comp_price",
        "comp_name",
        "comp_brand",
        "comp_image_url",
        "comp_url",
        "match_type",
        "match_score",
        "suggested_price",
        "action_required",
        "status",
        "ai_verification_state",
        "ai_verification_confidence",
        "ai_verification_reason",
        "ai_luxury_factor",
        "ai_scarcity_factor",
    ]

    existing_columns_to_keep = [col for col in columns_to_keep if col in priced_df.columns]
    return priced_df[existing_columns_to_keep]


def _load_our_catalog_df(db_path: str | None = None) -> pd.DataFrame:
    """
    يحمّل كتالوج متجرنا من SQLite (our_catalog) ليُستخدم تلقائياً في الـ background pipeline.
    """
    if db_path is None:
        try:
            from utils.db_manager import DB_PATH as _DB_PATH

            db_path = _DB_PATH
        except Exception:
            db_path = "perfume_pricing.db"

    if not os.path.exists(db_path):
        raise FileNotFoundError(f"قاعدة متجرنا غير موجودة: {db_path}")
    conn = None
    try:
        import sqlite3

        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(
            """
            SELECT
                COALESCE(product_id, '') AS sku,
                COALESCE(product_name, '') AS name,
                COALESCE(price, 0) AS price,
                COALESCE(cost_price, 0) AS cost,
                '' AS image_url
            FROM our_catalog
            """,
            conn,
        )
    finally:
        if conn is not None:
            conn.close()
    if df.empty:
        raise ValueError("جدول our_catalog فارغ؛ ارفع ملف منتجات المتجر أولاً.")
    df["sku"] = df["sku"].fillna("").astype(str).str.strip()
    return df


def run_auto_pricing_pipeline_background(reason: str = "", changed_rows: int = 0) -> bool:
    """
    تشغيل تلقائي في الخلفية:
    - يقرأ كتالوج متجرنا من SQLite.
    - يقرأ competitors_latest.csv.
    - يشغّل matcher + Gemini pricing.
    - يحفظ النتائج الجاهزة للواجهة في data/final_priced_latest.csv.
    """
    global _LAST_AUTO_PIPELINE_AT

    # Debounce بسيط لتجنب تشغيل متكرر جداً عند الدُفعات السريعة.
    min_interval_sec = int(os.environ.get("AUTO_PIPELINE_MIN_INTERVAL_SEC", "120"))
    now = time.time()
    if (now - _LAST_AUTO_PIPELINE_AT) < max(5, min_interval_sec):
        return False

    acquired = _AUTO_PIPELINE_LOCK.acquire(blocking=False)
    if not acquired:
        return False
    try:
        now2 = time.time()
        if (now2 - _LAST_AUTO_PIPELINE_AT) < max(5, min_interval_sec):
            return False

        comp_file = "data/competitors_latest.csv"
        if not os.path.exists(comp_file):
            return False

        df_mine = _load_our_catalog_df()
        priced_df = run_full_pricing_pipeline(df_mine)
        if priced_df is None or priced_df.empty:
            return False

        os.makedirs("data", exist_ok=True)
        out_csv = "data/final_priced_latest.csv"
        out_meta = "data/final_priced_latest_meta.json"
        priced_df = _merge_priced_with_previous(priced_df, out_csv, df_mine)

        try:
            from utils.scrape_live_buffer import replace_pricing_preview

            replace_pricing_preview(priced_df)
        except Exception:
            pass

        priced_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
        meta = {
            "status": "ok",
            "generated_at_utc": pd.Timestamp.utcnow().isoformat(),
            "rows": int(len(priced_df)),
            "reason": reason or "auto_scraper_update",
            "changed_rows": int(changed_rows or 0),
            "source_competitors_csv": comp_file,
        }
        import json

        with open(out_meta, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

        _LAST_AUTO_PIPELINE_AT = time.time()
        logger.info(
            "Auto pricing pipeline completed: rows=%s reason=%s changed_rows=%s",
            len(priced_df),
            reason,
            changed_rows,
        )
        try:
            from utils.live_price_store import append_activity_log, sync_from_final_priced_csv

            append_activity_log(
                f"📊 اكتمل خط التسعير التلقائي — {len(priced_df)} صف (سبب: {reason or 'auto'})"
            )
            sync_from_final_priced_csv()
        except Exception as le:
            logger.debug("live_price_store sync skipped: %s", le)
        return True
    except Exception as e:
        logger.error("Auto pricing pipeline failed: %s", e)
        return False
    finally:
        _AUTO_PIPELINE_LOCK.release()
