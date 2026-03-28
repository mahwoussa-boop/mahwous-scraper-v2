#!/bin/bash

# كود إدارة الرفع التلقائي لمشروع KIX-333
# تطوير: العقل المدبر (CTO)

# التوقف فوراً عند حدوث أي خطأ لضمان سلامة المستودع
set -e

echo "🚀 KIX-333 Mastermind: Starting GitHub Sync..."

# التحقق من وجود مستودع Git في المجلد الحالي
if [ ! -d ".git" ]; then
    echo "⚠️ Git repository not found. Initializing..."
    git init
    echo "✅ Initialized Git repository."
fi

# إضافة جميع التغييرات الجديدة للمرحلة الانتقالية
echo "📦 Staging all new updates..."
git add .

# التحقق مما إذا كانت هناك تغييرات فعلية للالتزام بها
if git diff --cached --quiet; then
    echo "✨ No changes detected. Nothing new to commit. Exiting gracefully."
    exit 0
fi

# تسجيل التغييرات برسالة احترافية تشمل الميزات الجديدة
# تم إضافة نظام المزامنة الجماعية مع Make.com وتحديثات لوحة الأداء
COMMIT_MESSAGE="Update KIX-333 Engine: Async Scraper, Smart Matcher, Profit Dashboard, and Make.com Bulk Sync Integration"

echo "📝 Committing changes: $COMMIT_MESSAGE"
git commit -m "$COMMIT_MESSAGE"

# الرفع إلى الفرع الرئيسي (تأكد من إعداد الريموت أولاً)
# ملاحظة: تم ترك سطر الدفع معطلاً كإجراء أمان، قم بتفعيله عند الربط الفعلي
# git push origin main

echo "--------------------------------------------------"
echo "✅ Done! All updates are staged and committed locally."
echo "💡 PRO TIP: Make sure your WEBHOOK_UPDATE_PRICES is in .streamlit/secrets.toml"
echo "🚀 Next step: Run 'git push origin main' to go live!"
