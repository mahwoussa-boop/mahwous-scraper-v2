"""
utils/merchant_brain.py — محرك عقل التاجر v1.0
=====================================================
خوارزمية التسعير السيكولوجي الذكي مع:
 • حماية هامش التكلفة (لا بيع بخسارة أبداً)
 • تسعير نفسي (نهايات .99 / .95 / .49)
 • استراتيجية المنافسة الذكية (undercut / raise / hold)
 • تنبيهات Make.com عند الفرص والتهديدات
 • تتبع قرارات المستخدم (Human-in-the-loop)
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Literal, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ── ثوابت الأمان ──────────────────────────────────────────
MIN_MARGIN_PCT: float = 5.0        # أدنى هامش ربح مسموح (5%)
ABSOLUTE_MIN_PRICE: float = 10.0  # أدنى سعر مطلق بالريال
UNDERCUT_AMOUNT: float = 1.0      # الفارق الافتراضي تحت المنافس (1 ريال)
MAKE_ALERT_DROP_PCT: float = 10.0 # نسبة انخفاض تستدعي تنبيه Make
MAKE_ALERT_RISE_PCT: float = 10.0 # نسبة ارتفاع تستدعي تنبيه Make

# نهايات الأسعار النفسية مرتبة تصاعدياً
_PSYCH_ENDINGS = [0.49, 0.95, 0.99]
_PSYCH_FRAC_TOL = 0.005  # تسامح عائم للمقارنة مع .49 / .95 / .99


def _price_already_psych_fractional(price: float) -> bool:
    """السعر ينتهي أصلاً بنهاية نفسية (مثل 100.99 أو 100.49) — لا نخفضه إلى (base-1)+.99."""
    if price <= 0:
        return False
    frac = price - math.floor(price)
    return any(abs(frac - e) <= _PSYCH_FRAC_TOL for e in _PSYCH_ENDINGS)


# ── هياكل البيانات ─────────────────────────────────────────
@dataclass
class PricingDecision:
    sku: str
    name: str
    our_price: float
    cost_price: float
    comp_price: float
    comp_name: str
    comp_url: str
    suggested_price: float
    strategy: Literal["undercut", "raise", "hold", "below_cost_warning"]
    margin_pct: float
    margin_safe: bool
    psych_applied: bool
    alert_type: Optional[Literal["threat", "opportunity", "none"]] = "none"
    alert_reason: str = ""
    raw_suggested: float = 0.0  # قبل التعديل النفسي
    notes: str = ""

    def to_dict(self) -> dict:
        return {
            "sku": self.sku,
            "name": self.name,
            "our_price": self.our_price,
            "cost_price": self.cost_price,
            "comp_price": self.comp_price,
            "comp_name": self.comp_name,
            "comp_url": self.comp_url,
            "suggested_price": self.suggested_price,
            "strategy": self.strategy,
            "margin_pct": round(self.margin_pct, 2),
            "margin_safe": self.margin_safe,
            "psych_applied": self.psych_applied,
            "alert_type": self.alert_type,
            "alert_reason": self.alert_reason,
            "notes": self.notes,
        }


@dataclass
class MerchantBrainConfig:
    min_margin_pct: float = MIN_MARGIN_PCT
    absolute_min_price: float = ABSOLUTE_MIN_PRICE
    undercut_amount: float = UNDERCUT_AMOUNT
    apply_psych_pricing: bool = True
    make_alert_drop_pct: float = MAKE_ALERT_DROP_PCT
    make_alert_rise_pct: float = MAKE_ALERT_RISE_PCT
    # إذا كان المنافس موثوقاً، نخفض أكثر
    trusted_competitors: list[str] = field(default_factory=list)


# ── دوال مساعدة ────────────────────────────────────────────
def _round_to_psych(price: float) -> float:
    """تحويل السعر لأقرب نهاية نفسية أقل منه أو مساوية."""
    if price <= 0:
        return price
    base = math.floor(price)
    best = price  # fallback
    for ending in _PSYCH_ENDINGS:
        candidate = base + ending
        if candidate <= price:
            best = candidate
    # إذا كانت (القيمة الصحيحة السابقة) + 0.99 أقرب نفسياً من price
    # (مثلاً: price=200.5 → 199.99 أفضل من 200.49)
    # لا نطبقها إذا كان السعر أصلاً ينتهي بـ .49/.95/.99 — لتفادي خفض 100.99→99.99 أو 100.49→99.99
    if not _price_already_psych_fractional(price):
        candidate_below = (base - 1) + 0.99 if base >= 1 else price
        if (
            candidate_below < best - _PSYCH_FRAC_TOL
            and abs(candidate_below - price) / price < 0.02
        ):
            best = candidate_below
    return best


def _calc_margin(suggested: float, cost: float) -> float:
    if cost <= 0 or suggested <= 0:
        return 0.0
    return ((suggested - cost) / suggested) * 100.0


def _is_margin_safe(suggested: float, cost: float, min_margin: float) -> bool:
    if cost <= 0:
        return True  # لا تكلفة معروفة → لا قيد
    if suggested < cost:
        return False
    return _calc_margin(suggested, cost) >= min_margin


def _detect_alert(
    our_price: float,
    comp_price: float,
    drop_thresh: float,
    rise_thresh: float,
) -> tuple[str, str]:
    """إرجاع (alert_type, reason)."""
    if our_price <= 0 or comp_price <= 0:
        return "none", ""
    diff_pct = ((our_price - comp_price) / our_price) * 100.0
    if diff_pct >= drop_thresh:
        # المنافس أرخص بكثير → تهديد (عتبة انخفاض سعرنا النسبي مقابل المنافس)
        return "threat", f"المنافس أرخص بـ {diff_pct:.1f}% مقارنة بسعرنا"
    rev_pct = ((comp_price - our_price) / our_price) * 100.0
    if rev_pct >= rise_thresh:
        # المنافس أغلى بكثير → فرصة لرفع سعرنا
        return "opportunity", f"المنافس أغلى بـ {rev_pct:.1f}% — فرصة لزيادة الربح"
    return "none", ""


# ── المحرك الرئيسي ─────────────────────────────────────────
class MerchantBrain:
    """
    عقل التاجر الذكي — يأخذ صفاً واحداً من DataFrame ويُقرر السعر الأمثل.
    """

    def __init__(self, config: MerchantBrainConfig | None = None):
        self.cfg = config or MerchantBrainConfig()

    def decide(
        self,
        sku: str,
        name: str,
        our_price: float,
        cost_price: float,
        comp_price: float,
        comp_name: str = "",
        comp_url: str = "",
        is_trusted_competitor: bool = False,
    ) -> PricingDecision:
        """
        القرار الكامل لمنتج واحد.
        """
        our_price = float(our_price or 0)
        cost_price = float(cost_price or 0)
        comp_price = float(comp_price or 0)

        # ── تحديد الاستراتيجية ──────────────────────────
        strategy: Literal["undercut", "raise", "hold", "below_cost_warning"] = "hold"
        raw_suggested = our_price

        if comp_price <= 0:
            # لا سعر منافس — احتفظ بسعرنا
            strategy = "hold"
            raw_suggested = our_price

        elif comp_price < our_price:
            # المنافس أرخص → undercut أو hold إذا لم يكن موثوقاً
            undercut = UNDERCUT_AMOUNT if not is_trusted_competitor else UNDERCUT_AMOUNT * 1.5
            raw_suggested = comp_price - undercut
            strategy = "undercut"

        elif comp_price > our_price * 1.05:
            # المنافس أغلى بأكثر من 5% → ارفع سعرنا قليلاً
            # لا تتجاوز سعر المنافس - 1 ريال
            raw_suggested = min(comp_price - UNDERCUT_AMOUNT, our_price * 1.10)
            strategy = "raise"

        else:
            # قريب منا ← احتفظ
            strategy = "hold"
            raw_suggested = our_price

        # ── حماية التكلفة ──────────────────────────────
        min_safe_price = max(
            self.cfg.absolute_min_price,
            cost_price * (1 + self.cfg.min_margin_pct / 100) if cost_price > 0 else 0,
        )

        if raw_suggested < min_safe_price and cost_price > 0:
            raw_suggested = min_safe_price
            strategy = "below_cost_warning"

        # ── التسعير النفسي ──────────────────────────────
        psych_applied = False
        suggested = raw_suggested
        if self.cfg.apply_psych_pricing and raw_suggested > 0:
            psych = _round_to_psych(raw_suggested)
            if psych != raw_suggested and psych >= min_safe_price:
                suggested = psych
                psych_applied = True

        # ── حساب الهامش النهائي ─────────────────────────
        margin = _calc_margin(suggested, cost_price)
        safe = _is_margin_safe(suggested, cost_price, self.cfg.min_margin_pct)

        # ── كشف التنبيهات ───────────────────────────────
        alert_type, alert_reason = _detect_alert(
            our_price, comp_price,
            self.cfg.make_alert_drop_pct,
            self.cfg.make_alert_rise_pct,
        )

        # ── ملاحظات ─────────────────────────────────────
        notes_parts = []
        if not safe:
            notes_parts.append(f"⚠️ هامش الربح {margin:.1f}% < الحد الأدنى {self.cfg.min_margin_pct}%")
        if strategy == "below_cost_warning":
            notes_parts.append("🔴 سعر المنافس أقل من تكلفتنا — تم ضبط السعر تلقائياً")
        if psych_applied:
            notes_parts.append(f"✨ تعديل نفسي: {raw_suggested:.2f} → {suggested:.2f}")

        return PricingDecision(
            sku=sku,
            name=name,
            our_price=our_price,
            cost_price=cost_price,
            comp_price=comp_price,
            comp_name=comp_name,
            comp_url=comp_url,
            suggested_price=round(suggested, 2),
            strategy=strategy,
            margin_pct=round(margin, 2),
            margin_safe=safe,
            psych_applied=psych_applied,
            alert_type=alert_type,
            alert_reason=alert_reason,
            raw_suggested=round(raw_suggested, 2),
            notes="\n".join(notes_parts),
        )

    def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        يطبق عقل التاجر على DataFrame كامل ويُضيف أعمدة القرارات.
        الـ DataFrame يجب أن يحتوي على:
          sku, name, price (سعرنا), cost (تكلفة), comp_price, comp_name, comp_url
        """
        decisions = []
        for _, row in df.iterrows():
            is_trusted = str(row.get("comp_url", "")) in self.cfg.trusted_competitors
            d = self.decide(
                sku=str(row.get("sku", "")),
                name=str(row.get("name", "")),
                our_price=float(row.get("price", 0) or 0),
                cost_price=float(row.get("cost", 0) or 0),
                comp_price=float(row.get("comp_price", 0) or 0),
                comp_name=str(row.get("comp_name", "") or ""),
                comp_url=str(row.get("comp_url", "") or ""),
                is_trusted_competitor=is_trusted,
            )
            decisions.append(d.to_dict())

        decisions_df = pd.DataFrame(decisions)
        # دمج مع الـ DataFrame الأصلي
        merge_cols = [c for c in decisions_df.columns if c not in ("sku", "name")]
        out = df.copy()
        for col in merge_cols:
            out[col] = decisions_df[col].values
        return out

    @staticmethod
    def classify_products(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        """
        تصنيف المنتجات لأقسام القائمة الجانبية:
          🔴 high_price   — undercut أو below_cost_warning (ضغط سعر / تكلفة)
          🟢 low_price    — سعرنا أقل ← يمكن رفعه (raise)
          🔍 missing      — منتجات مفقودة
          ✅ optimal      — سعر مثالي (hold)
        """
        sections: dict[str, pd.DataFrame] = {
            "high_price": pd.DataFrame(),
            "low_price": pd.DataFrame(),
            "missing": pd.DataFrame(),
            "optimal": pd.DataFrame(),
        }

        if df is None or df.empty:
            return sections

        if "action_required" in df.columns:
            missing_mask = df["action_required"].str.contains("مفقود", na=False)
            sections["missing"] = df[missing_mask].copy()
            df = df[~missing_mask]

        if "strategy" in df.columns:
            sections["high_price"] = df[
                df["strategy"].isin(["undercut", "below_cost_warning"])
            ].copy()
            sections["low_price"] = df[df["strategy"] == "raise"].copy()
            optimal_mask = df["strategy"].isin(["hold"])
            sections["optimal"] = df[optimal_mask].copy()
        elif "comp_price" in df.columns and "price" in df.columns:
            price_col = pd.to_numeric(df["price"], errors="coerce").fillna(0)
            comp_col = pd.to_numeric(df["comp_price"], errors="coerce").fillna(0)
            high_mask = (comp_col > 0) & (comp_col < price_col)
            low_mask = (comp_col > 0) & (comp_col > price_col * 1.05)
            sections["high_price"] = df[high_mask].copy()
            sections["low_price"] = df[low_mask].copy()
            sections["optimal"] = df[~high_mask & ~low_mask].copy()

        return sections
