"""
Background worker entrypoint (24/7).

Runs the continuous scraper service:
- sync sitemap every 2 hours
- resume-safe pending queue processing
- auto-trigger matcher + Gemini pricing pipeline
"""
from __future__ import annotations

import asyncio
import logging
import os

from utils.async_scraper import run_continuous_scraper_service, run_scraper_engine


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # إعدادات افتراضية عالية للإقلاع الأول (يمكن تجاوزها من Environment)
    os.environ.setdefault("SCRAPER_CONCURRENCY", "60")
    os.environ.setdefault("SCRAPER_PENDING_BATCH_SIZE", "1000")
    os.environ.setdefault("SCRAPER_INTERLEAVE_BATCH_SIZE", "300")
    os.environ.setdefault("SCRAPER_IDLE_POLL_SECONDS", "8")
    os.environ.setdefault("SCRAPER_MAX_URL_ATTEMPTS", "5")
    os.environ.setdefault("SCRAPER_DELAY_MIN", "0.05")
    os.environ.setdefault("SCRAPER_DELAY_MAX", "0.25")

    # 1) تشغيل كامل مرة واحدة حتى الانتهاء (يسحب كل المنافسين ويملأ CSV بالكامل)
    # 2) بعد الاكتمال يتحول إلى خدمة مستمرة تُحدّث كل ساعتين.
    async def _run_both():
        await run_scraper_engine()
        await run_continuous_scraper_service()

    asyncio.run(_run_both())


if __name__ == "__main__":
    main()

