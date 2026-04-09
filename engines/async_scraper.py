# [FIX OPT-01] re-export من المصدر الموحّد utils/shared
# بدلاً من 3 نسخ مستقلة قابلة للاختلاف — أي إصلاح في utils/shared يُطبَّق تلقائياً
from utils.shared.async_scraper import *  # noqa: F401, F403
