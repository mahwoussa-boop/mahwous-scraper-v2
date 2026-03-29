"""
utils/user_preferences.py — نظام التعلم من المستخدم v1.0
===========================================================
Human-in-the-loop: يحفظ قرارات المستخدم (موافق/رفض) على
اقتراحات الذكاء الاصطناعي لتحسين الاقتراحات مستقبلاً.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_DB_FILE = Path("data") / "user_preferences.db"
_DB_FILE.parent.mkdir(parents=True, exist_ok=True)


# ── اتصال قاعدة البيانات ──────────────────────────────────
@contextmanager
def _conn():
    con = sqlite3.connect(str(_DB_FILE), timeout=10, check_same_thread=False)
    con.row_factory = sqlite3.Row
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def init_preferences_db() -> None:
    """إنشاء جداول قاعدة بيانات التفضيلات إن لم تكن موجودة."""
    with _conn() as con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS price_decisions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            sku          TEXT NOT NULL,
            product_name TEXT,
            our_price    REAL,
            comp_price   REAL,
            suggested    REAL,
            final_price  REAL,
            strategy     TEXT,
            decision     TEXT NOT NULL,   -- 'approved' | 'rejected' | 'modified'
            reason       TEXT,
            competitor   TEXT,
            match_score  REAL,
            created_at   TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_pd_sku ON price_decisions(sku);
        CREATE INDEX IF NOT EXISTS idx_pd_decision ON price_decisions(decision);
        CREATE INDEX IF NOT EXISTS idx_pd_created ON price_decisions(created_at);

        CREATE TABLE IF NOT EXISTS match_feedback (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            sku_mine     TEXT NOT NULL,
            name_mine    TEXT,
            sku_comp     TEXT,
            name_comp    TEXT,
            match_score  REAL,
            ai_said      TEXT,         -- "match" | "no_match"
            user_said    TEXT NOT NULL, -- "match" | "no_match"
            correct      INTEGER,       -- 1 = AI was right, 0 = AI was wrong
            created_at   TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS rule_feedback (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_name   TEXT NOT NULL,
            approved    INTEGER DEFAULT 0,
            rejected    INTEGER DEFAULT 0,
            modified    INTEGER DEFAULT 0,
            updated_at  TEXT DEFAULT (datetime('now'))
        );
        """)


# ── حفظ قرار التسعير ──────────────────────────────────────
def save_price_decision(
    sku: str,
    product_name: str,
    our_price: float,
    comp_price: float,
    suggested: float,
    decision: str,  # "approved" | "rejected" | "modified"
    final_price: Optional[float] = None,
    strategy: str = "",
    reason: str = "",
    competitor: str = "",
    match_score: float = 0.0,
) -> None:
    """حفظ قرار المستخدم على اقتراح سعر."""
    init_preferences_db()
    try:
        with _conn() as con:
            con.execute(
                """INSERT INTO price_decisions
                   (sku, product_name, our_price, comp_price, suggested, final_price,
                    strategy, decision, reason, competitor, match_score)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (sku, product_name, our_price, comp_price, suggested,
                 final_price or suggested, strategy, decision, reason,
                 competitor, match_score),
            )
        logger.debug("Saved price decision: %s → %s", sku, decision)
    except Exception as e:
        logger.error("save_price_decision error: %s", e)


def save_match_feedback(
    sku_mine: str,
    name_mine: str,
    sku_comp: str,
    name_comp: str,
    match_score: float,
    ai_said: str,
    user_said: str,
) -> None:
    """حفظ تغذية راجعة على قرار المطابقة."""
    init_preferences_db()
    correct = 1 if ai_said == user_said else 0
    try:
        with _conn() as con:
            con.execute(
                """INSERT INTO match_feedback
                   (sku_mine, name_mine, sku_comp, name_comp, match_score, ai_said, user_said, correct)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (sku_mine, name_mine, sku_comp, name_comp, match_score, ai_said, user_said, correct),
            )
    except Exception as e:
        logger.error("save_match_feedback error: %s", e)


# ── استرجاع الإحصائيات ────────────────────────────────────
def get_decision_stats(days: int = 30) -> Dict[str, Any]:
    """إحصائيات قرارات المستخدم خلال الأيام الأخيرة."""
    init_preferences_db()
    try:
        with _conn() as con:
            rows = con.execute(
                """SELECT decision, COUNT(*) as cnt
                   FROM price_decisions
                   WHERE created_at >= datetime('now', ?)
                   GROUP BY decision""",
                (f"-{days} days",),
            ).fetchall()
        stats = {"approved": 0, "rejected": 0, "modified": 0, "total": 0}
        for r in rows:
            stats[r["decision"]] = r["cnt"]
            stats["total"] += r["cnt"]
        # نسبة الموافقة
        if stats["total"] > 0:
            stats["approval_rate"] = round(stats["approved"] / stats["total"] * 100, 1)
        else:
            stats["approval_rate"] = 0.0
        return stats
    except Exception as e:
        logger.error("get_decision_stats error: %s", e)
        return {}


def get_sku_decision_history(sku: str, limit: int = 10) -> List[Dict]:
    """تاريخ القرارات لمنتج محدد."""
    init_preferences_db()
    try:
        with _conn() as con:
            rows = con.execute(
                """SELECT * FROM price_decisions WHERE sku=?
                   ORDER BY created_at DESC LIMIT ?""",
                (sku, limit),
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def get_match_accuracy() -> Dict[str, Any]:
    """دقة نظام المطابقة بناءً على تغذية المستخدم."""
    init_preferences_db()
    try:
        with _conn() as con:
            total = con.execute("SELECT COUNT(*) FROM match_feedback").fetchone()[0]
            correct = con.execute(
                "SELECT COUNT(*) FROM match_feedback WHERE correct=1"
            ).fetchone()[0]
        if total == 0:
            return {"total": 0, "correct": 0, "accuracy_pct": 0.0}
        return {
            "total": total,
            "correct": correct,
            "accuracy_pct": round(correct / total * 100, 1),
        }
    except Exception:
        return {"total": 0, "correct": 0, "accuracy_pct": 0.0}


def get_preferred_strategy_for_sku(sku: str) -> Optional[str]:
    """
    بناءً على تاريخ القرارات، ما هي الاستراتيجية المفضلة للمستخدم لهذا المنتج؟
    يُعيد None إذا لم يكن هناك بيانات كافية.
    """
    history = get_sku_decision_history(sku, limit=5)
    if not history:
        return None
    # أكثر strategy تمت الموافقة عليها
    approved = [h for h in history if h.get("decision") == "approved"]
    if not approved:
        return None
    strategies = [h.get("strategy", "") for h in approved if h.get("strategy")]
    if not strategies:
        return None
    return max(set(strategies), key=strategies.count)


# ── واجهة Streamlit ────────────────────────────────────────
def render_decision_buttons(
    sku: str,
    product_name: str,
    our_price: float,
    comp_price: float,
    suggested: float,
    strategy: str = "",
    competitor: str = "",
    match_score: float = 0.0,
    key_prefix: str = "",
) -> Optional[str]:
    """
    عرض أزرار موافق/رفض/تعديل ويحفظ القرار تلقائياً.

    Returns:
        "approved" | "rejected" | "modified" | None
    """
    import streamlit as st

    key = key_prefix or sku.replace(" ", "_")[:20]
    col1, col2, col3 = st.columns([1, 1, 2])

    decision = None
    with col1:
        if st.button("✅ موافق", key=f"approve_{key}", use_container_width=True):
            save_price_decision(
                sku=sku, product_name=product_name, our_price=our_price,
                comp_price=comp_price, suggested=suggested, decision="approved",
                strategy=strategy, competitor=competitor, match_score=match_score,
            )
            st.toast(f"✅ تمت الموافقة على سعر {suggested:.2f} ر.س")
            decision = "approved"

    with col2:
        if st.button("❌ رفض", key=f"reject_{key}", use_container_width=True):
            save_price_decision(
                sku=sku, product_name=product_name, our_price=our_price,
                comp_price=comp_price, suggested=suggested, decision="rejected",
                strategy=strategy, competitor=competitor, match_score=match_score,
            )
            st.toast("❌ تم رفض الاقتراح")
            decision = "rejected"

    with col3:
        new_price = st.number_input(
            "سعر مخصص", min_value=0.0, value=float(suggested),
            step=0.5, key=f"custom_{key}", label_visibility="collapsed",
        )
        if st.button("✏️ تعديل", key=f"modify_{key}", use_container_width=True):
            save_price_decision(
                sku=sku, product_name=product_name, our_price=our_price,
                comp_price=comp_price, suggested=suggested, decision="modified",
                final_price=new_price, strategy=strategy,
                competitor=competitor, match_score=match_score,
            )
            st.toast(f"✏️ تم تعديل السعر إلى {new_price:.2f} ر.س")
            decision = "modified"

    return decision
