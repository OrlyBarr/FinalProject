#!/bin/bash
# fix_project.sh — תיקון בעיות תשתית בפרויקט
# הרץ מתוך שורש הפרויקט: bash fix_project.sh

echo "🔧 Fixing project issues..."

# 1. יצירת תיקיית consumers חסרה
if [ ! -d "consumers" ]; then
    mkdir -p consumers
    touch consumers/__init__.py
    echo "✅ Created consumers/ directory"
else
    echo "⚠️  consumers/ already exists"
fi

# 2. מחיקת הקובץ הכפול עם רווח בשם
if [ -f "Resilient pipeline.py" ]; then
    rm "Resilient pipeline.py"
    echo "✅ Removed duplicate 'Resilient pipeline.py'"
fi

# 3. תיקון MINIO_ENDPOINT ב-.env
if grep -q "MINIO_ENDPOINT=http://localhost:9000" .env 2>/dev/null; then
    sed -i 's|MINIO_ENDPOINT=http://localhost:9000|MINIO_ENDPOINT=http://minio:9000|g' .env
    echo "✅ Fixed MINIO_ENDPOINT in .env (localhost → minio)"
fi

# 4. יצירת buffer directories לרשת חוסן
mkdir -p /tmp/transit_buffer/kafka /tmp/transit_buffer/minio_pending
echo "✅ Created resilient buffer directories"

echo ""
echo "✅ All fixes applied. Now run: bash run.sh"