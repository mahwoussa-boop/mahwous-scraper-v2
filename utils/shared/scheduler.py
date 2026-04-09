"""
scrapers/scheduler.py — جدولة الكشط التلقائي v1.0
═══════════════════════════════════════════════════
يشغّل async_scraper.py كـ Orphan Process تلقائياً وفق الجدول المضبوط.

الجدول الافتراضي: كل 12 ساعة
يمكن تغييره عبر متغير البيئة: SCRAPE_INTERVAL_HOURS

الحالة محفوظة في: data/scheduler_state.json
"""
import json
import logging
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

# ── مسارات ─────────────────────────────────────────────────────────────────
_ROOT      = Path(__file__).resolve().parent.parent
_DATA_DIR  = Path(os.environ.get("DATA_DIR", "")).resolve() if os.environ.get("DATA_DIR") else _ROOT / "data"
_STATE_FILE  = _DATA_DIR / "scheduler_state.json"
_SCRAPER_SCRIPT = _ROOT / "scrapers" / "async_scraper.py"

# ── الافتراضيات ─────────────────────────────────────────────────────────────
DEFAULT_INTERVAL_HOURS = int(os.environ.get("SCRAPE_INTERVAL_HOURS", "12"))


# ══════════════════════════════════════════════════════════════════════════
#  إدارة الحالة
# ══════════════════════════════════════════════════════════════════════════
def _load_state() -> dict:
    try:
        return json.loads(_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"enabled": False, "next_run": None, "interval_hours": DEFAULT_INTERVAL_HOURS,
                "last_run": None, "runs_count": 0}


def _save_state(state: dict) -> None:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    _STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def get_scheduler_status() -> dict:
    """يُرجع حالة المجدول للعرض في الواجهة."""
    s = _load_state()
    now = datetime.utcnow()
    if s.get("next_run"):
        try:
            nxt = datetime.fromisoformat(s["next_run"])
            remaining = nxt - now
            seconds = max(0, int(remaining.total_seconds()))
            s["remaining_seconds"] = seconds
            s["next_run_label"] = _fmt_duration(seconds)
        except Exception:
            s["remaining_seconds"] = 0
            s["next_run_label"] = "—"
    else:
        s["remaining_seconds"] = 0
        s["next_run_label"] = "—"
    return s


def _fmt_duration(seconds: int) -> str:
    if seconds <= 0:
        return "الآن"
    h, r = divmod(seconds, 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}س {m}د"
    if m:
        return f"{m}د {s}ث"
    return f"{s}ث"


def enable_scheduler(interval_hours: int = DEFAULT_INTERVAL_HOURS) -> None:
    """يُفعّل الجدولة التلقائية ويحسب أول تشغيل."""
    state = _load_state()
    state["enabled"]        = True
    state["interval_hours"] = interval_hours
    state["next_run"]       = (datetime.utcnow() + timedelta(hours=interval_hours)).isoformat()
    _save_state(state)
    logger.info("المجدول مُفعَّل — كل %d ساعة، التشغيل القادم: %s",
                interval_hours, state["next_run"])


def disable_scheduler() -> None:
    state = _load_state()
    state["enabled"]  = False
    state["next_run"] = None
    _save_state(state)
    logger.info("المجدول مُعطَّل")


def trigger_now(max_products: int = 0, concurrency: int = 8, full: bool = False) -> bool:
    """
    يُشغّل الكاشط فوراً كـ Orphan Process.
    يُحدِّث next_run في الحالة.
    full=True → يتخطى lastmod cache ويكشط كل شيء.
    """
    if not _SCRAPER_SCRIPT.exists():
        logger.error("الكاشط غير موجود: %s", _SCRAPER_SCRIPT)
        return False
    try:
        cmd = [
            sys.executable, "-m", "scrapers.async_scraper",
            "--max-products", str(max_products),
            "--concurrency", str(concurrency),
        ]
        if full:
            cmd.append("--full")
        subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            cwd=str(_ROOT),
        )
        state = _load_state()
        state["last_run"]    = datetime.utcnow().isoformat()
        state["runs_count"]  = state.get("runs_count", 0) + 1
        interval             = state.get("interval_hours", DEFAULT_INTERVAL_HOURS)
        state["next_run"]    = (datetime.utcnow() + timedelta(hours=interval)).isoformat()
        _save_state(state)
        logger.info("الكاشط انطلق — التشغيل رقم %d", state["runs_count"])
        return True
    except Exception as exc:
        logger.error("فشل تشغيل الكاشط: %s", exc)
        return False


# ══════════════════════════════════════════════════════════════════════════
#  Daemon Thread — يفحص الجدول كل دقيقة
# ══════════════════════════════════════════════════════════════════════════
_scheduler_thread: threading.Thread | None = None
_running = threading.Event()


def _scheduler_loop() -> None:
    """يعمل في خيط daemon — يفحص كل 60 ثانية إذا حان وقت الكشط."""
    logger.info("خيط المجدول بدأ")
    while _running.is_set():
        try:
            state = _load_state()
            if state.get("enabled") and state.get("next_run"):
                next_run = datetime.fromisoformat(state["next_run"])
                if datetime.utcnow() >= next_run:
                    logger.info("حان وقت الكشط التلقائي — أبدأ الآن…")
                    trigger_now(
                        max_products=state.get("max_products", 0),
                        concurrency=state.get("concurrency", 8),
                    )
        except Exception as exc:
            logger.debug("scheduler loop error: %s", exc)
        # انتظر 60 ثانية أو حتى يُلغى الـ event
        _running.wait(timeout=60)


def start_scheduler_thread() -> None:
    """
    يُشغّل خيط المجدول عند إقلاع التطبيق.
    آمن للاستدعاء المتعدد — لا يُنشئ خيطاً ثانياً.
    """
    global _scheduler_thread
    if _scheduler_thread and _scheduler_thread.is_alive():
        return
    _running.set()
    _scheduler_thread = threading.Thread(
        target=_scheduler_loop, name="scraper-scheduler", daemon=True
    )
    _scheduler_thread.start()
    logger.info("خيط المجدول بدأ (daemon)")


def stop_scheduler_thread() -> None:
    """إيقاف خيط المجدول."""
    _running.clear()
    if _scheduler_thread:
        _scheduler_thread.join(timeout=5)


# ══════════════════════════════════════════════════════════════════════════
#  Delta Detector — يُشغَّل تلقائياً بعد كل دورة كشط
# ══════════════════════════════════════════════════════════════════════════
def run_delta_detection(scraped_df: "pd.DataFrame | None" = None,
                        competitor: str = "") -> dict:
    """
    يُطلَق بعد كل دورة كشط أو رفع ملف منافس جديد.
    يُقارن الأسعار الجديدة مع المُخزَّنة ويُطلق أحداث التغيير.

    Parameters
    ----------
    scraped_df : pd.DataFrame — البيانات المكتشَفة حديثاً
    competitor : str          — اسم المنافس

    Returns
    -------
    dict — تقرير التغييرات
    """
    try:
        from engines.delta_detector import detect_deltas
        import pandas as pd

        if scraped_df is None or (hasattr(scraped_df, "empty") and scraped_df.empty):
            return {}

        report = detect_deltas(scraped_df, competitor)
        if report.get("price_changes", 0) > 0 or report.get("new_products", 0) > 0:
            logger.info(
                "Delta Detection: %d تغيير سعر، %d منتج جديد — المنافس: %s",
                report["price_changes"], report["new_products"], competitor
            )
        return report
    except Exception as exc:
        logger.error("run_delta_detection فشل: %s", exc)
        return {}
