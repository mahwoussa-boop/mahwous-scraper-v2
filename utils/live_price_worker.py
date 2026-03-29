"""
خيط خلفية يراقب تعديل final_priced_latest.csv ويعيد مزامنة لوحة SQLite دون حظر واجهة Streamlit.
"""
from __future__ import annotations

import os
import threading
import time
from typing import Optional

from utils.live_price_store import append_activity_log, sync_from_final_priced_csv

_FINAL = os.path.join("data", "final_priced_latest.csv")
_worker_lock = threading.Lock()
_worker_thread: Optional[threading.Thread] = None
_stop_event = threading.Event()
_last_mtime: float = 0.0


def _loop(poll_seconds: float) -> None:
    global _last_mtime
    append_activity_log("🖥️ بدأ مراقب الملفات الخلفي — انتظار تحديثات final_priced_latest.csv")
    while not _stop_event.is_set():
        try:
            mtime = os.path.getmtime(_FINAL)
        except OSError:
            mtime = 0.0
        if mtime > 0 and mtime != _last_mtime:
            _last_mtime = mtime
            append_activity_log("📥 اكتشاف تعديل على final_priced_latest.csv — جاري المزامنة…")
            sync_from_final_priced_csv()
        if _stop_event.wait(timeout=max(2.0, float(poll_seconds))):
            break
    append_activity_log("🛑 توقف مراقب الملفات الخلفي.")


def start_live_file_watcher(poll_seconds: float = 5.0) -> None:
    """يبدأ خيط المراقبة مرة واحدة (آمن مع الاستدعاء المتكرر)."""
    global _worker_thread
    with _worker_lock:
        if _worker_thread is not None and _worker_thread.is_alive():
            return
        _stop_event.clear()
        _worker_thread = threading.Thread(
            target=_loop,
            kwargs={"poll_seconds": poll_seconds},
            name="live_price_file_watcher",
            daemon=True,
        )
        _worker_thread.start()


def stop_live_file_watcher() -> None:
    _stop_event.set()
    t = _worker_thread
    if t is not None and t.is_alive():
        t.join(timeout=3.0)


def is_watcher_alive() -> bool:
    return _worker_thread is not None and _worker_thread.is_alive()
