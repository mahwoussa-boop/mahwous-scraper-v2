"""
محرك تسعير معزّز (VSP) — قواعد هامش + عوامل فخامة/ندرة بدون استدعاء شبكة إلزامي.
"""
from __future__ import annotations

import logging
from typing import Iterable

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_LUXURY_KEYWORDS: Iterable[str] = (
    "chanel",
    "dior",
    "tom ford",
    "creed",
    "amouage",
    "clive",
    "xerjoff",
    "bond no",
    "شانيل",
    "ديور",
    "كريد",
    "امواج",
)


def _luxury_factor_from_text(text: str) -> float:
    if not isinstance(text, str) or not text.strip():
        return 0.42
    low = text.lower()
    hits = sum(1 for k in _LUXURY_KEYWORDS if k in low)
    return float(min(0.95, 0.38 + hits * 0.12))


class EnhancedAIPricingEngine:
    """استراتيجية تسعير مبنية على التكلفة، سعر المنافس، والهامش المستهدف."""

    def process_pricing_strategy(
        self,
        df: pd.DataFrame,
        target_margin: float = 0.35,
    ) -> pd.DataFrame:
        if df is None or df.empty:
            return df

        out = df.copy()
        tm = float(target_margin)
        if tm < 0 or tm > 0.9:
            tm = 0.35

        name_col = "name" if "name" in out.columns else None
        if name_col is None and "name_mine" in out.columns:
            out["name"] = out["name_mine"].astype(str)
            name_col = "name"

        if name_col is not None:
            out["ai_luxury_factor"] = out[name_col].astype(str).map(_luxury_factor_from_text)
        else:
            out["ai_luxury_factor"] = 0.5

        if "match_score" in out.columns:
            ms = pd.to_numeric(out["match_score"], errors="coerce").fillna(70.0)
            out["ai_scarcity_factor"] = ((100.0 - ms) / 100.0).clip(0.0, 1.0)
        else:
            out["ai_scarcity_factor"] = 0.35

        price = pd.to_numeric(out.get("price", 0), errors="coerce").fillna(0.0)
        cost = pd.to_numeric(out.get("cost", 0), errors="coerce").fillna(0.0)
        comp_price = pd.to_numeric(out.get("comp_price", 0), errors="coerce").fillna(0.0)

        floor_price = np.maximum(cost * (1.0 + tm), 0.01)
        lux = out["ai_luxury_factor"].astype(float)
        scar = out["ai_scarcity_factor"].astype(float)

        # سعر مقترح: تحت سعر المنافس قليلاً مع احترام الأرضية؛ عند غياب المنافس نرفع بشكل محافظ
        undercut = np.clip(0.96 - 0.03 * lux - 0.02 * scar, 0.88, 0.99)
        target_from_comp = np.where(
            comp_price > 0,
            comp_price * undercut,
            np.maximum(floor_price, price * (1.0 + 0.015 * lux)),
        )
        suggested = np.maximum(floor_price, target_from_comp)
        suggested = np.where(price > 0, np.minimum(suggested, price * 1.15), suggested)
        out["suggested_price"] = np.round(suggested, 2)

        ratio = np.where(comp_price > 0, price / comp_price, np.nan)
        action = []
        for i in range(len(out)):
            c = float(comp_price.iloc[i]) if hasattr(comp_price, "iloc") else float(comp_price[i])
            p = float(price.iloc[i]) if hasattr(price, "iloc") else float(price[i])
            r = ratio[i] if not np.isnan(ratio[i]) else None
            if c <= 0:
                action.append("مراجعة — سعر المنافس غير متوفر")
            elif r is not None and r > 1.04:
                action.append("خفض السعر")
            elif r is not None and r < 0.96:
                action.append("رفع السعر (فرصة هامش)")
            else:
                action.append("محايد — لا إجراء عاجل")
        out["action_required"] = action

        logger.info("Enhanced pricing: processed %s rows (target_margin=%.2f)", len(out), tm)
        return out
