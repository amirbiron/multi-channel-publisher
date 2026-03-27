# 📱 Social Publisher

**פרסום אוטומטי לאינסטגרם ופייסבוק מטבלת Google Sheets**

---

## 🏗️ ארכיטקטורה

```
Render Cron Job (כל דקה, UTC)
       │
       ▼
   main.py
       │
       ├── Google Sheets API ← קריאת שורות READY שהגיע זמנן
       │
       ├── Google Drive API ← הורדת קובץ מדיה (bytes)
       │
       ├── Cloudinary ← העלאה → קבלת URL ציבורי
       │
       ├── Meta APIs
       │   ├── Instagram: /media → /media_publish
       │   └── Facebook: /photos או /videos
       │
       └── Google Sheets API ← עדכון סטטוס (POSTED / ERROR)
```

---

## 📋 מבנה הטבלה (Google Sheets)

| עמודה | דוגמה | הסבר |
|---|---|---|
| `id` | 1 | מספר ייחודי |
| `status` | READY | READY / IN_PROGRESS / POSTED / ERROR |
| `network` | IG | IG או FB |
| `post_type` | FEED | לשימוש עתידי (FEED / REELS / STORY) |
| `publish_at` | 2026-03-10 14:30 | שעון ישראל |
| `caption_ig` | הטקסט לאינסטגרם | |
| `caption_fb` | הטקסט לפייסבוק | |
| `drive_file_id` | 1AbCdEf... | File ID מ-Google Drive |
| `cloudinary_url` | (ימולא אוטומטית) | |
| `result` | (ימולא אוטומטית) | post_id / media_id |
| `error` | (ימולא אוטומטית) | הודעת שגיאה |

---

## 🚀 הגדרה ב-Render

### 1. יצירת Cron Job
- **Type**: Cron Job
- **Schedule**: `*/1 * * * *` (כל דקה)
- **Build Command**: `pip install -r requirements.txt`
- **Start Command**: `python main.py`
- **Region**: Frankfurt (הכי קרוב לישראל)

### 2. הגדרת Environment Variables
ראה `.env.example` לרשימה מלאה.

### 3. Google Service Account
1. צור Service Account ב-Google Cloud Console
2. הפעל את ה-APIs: Sheets API + Drive API
3. שתף את ה-Spreadsheet עם המייל של ה-SA (הרשאת Editor)
4. שתף את תיקיית/קבצי ה-Drive עם המייל של ה-SA (הרשאת Viewer)
5. הדבק את כל ה-JSON ב-`GOOGLE_SERVICE_ACCOUNT_JSON`

### 4. Meta (Facebook / Instagram)
1. צור App ב-Meta for Developers
2. הוסף את המוצרים: Instagram Graph API + Pages API
3. קבל טוקן ארוך טווח (System User מומלץ)
4. הרשאות נדרשות:
   - `instagram_basic`
   - `instagram_content_publish`
   - `pages_manage_posts`
   - `pages_read_engagement`

### 5. Cloudinary
1. צור חשבון חינמי ב-Cloudinary
2. העתק Cloud Name, API Key, API Secret מהדאשבורד

---

## 📁 מבנה הפרויקט

```
social-publisher/
├── main.py              # סקריפט ראשי (entry point)
├── config.py            # הגדרות + env vars + קבועים
├── google_api.py        # Google Sheets + Drive helpers
├── cloud_storage.py     # Cloudinary upload
├── meta_publish.py      # Instagram + Facebook publishing
├── requirements.txt     # תלויות Python
├── render.yaml          # Render Blueprint
├── .env.example         # דוגמה ל-env vars
└── README.md            # אתה פה
```

---

## 🔧 הרצה מקומית (לבדיקות)

```bash
# התקנת תלויות
pip install -r requirements.txt

# הגדרת env vars (או קובץ .env + python-dotenv)
export GOOGLE_SERVICE_ACCOUNT_JSON='{"type":"service_account",...}'
export SPREADSHEET_ID='...'
# ... שאר המשתנים

# הרצה
python main.py
```

---

## ⚠️ נקודות חשובות

- **Timezone**: הטבלה בשעון ישראל, הקוד ממיר ל-UTC אוטומטית
- **נעילה**: לפני פרסום הסטטוס משתנה ל-`IN_PROGRESS` — מונע כפילויות
- **Cloudinary חובה**: אינסטגרם דורש URL ציבורי (לא Drive link)
- **זיהוי סוג קובץ**: אוטומטי לפי MIME type מ-Drive (לא צריך עמודה נפרדת)
- **Fallback**: אם חסר caption_ig, ישתמש ב-caption_fb (ולהיפך)
- **מגבלת IG**: עד 100 פוסטים ב-24 שעות דרך API
