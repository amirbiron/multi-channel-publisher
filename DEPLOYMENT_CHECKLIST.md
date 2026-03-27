# צ׳קליסט דיפלוי — Multi-Channel Publisher (עליית GBP)

## לפני דיפלוי

### משתני סביבה
- [ ] `GBP_ACCOUNT_ID` מוגדר (למשל `accounts/123456789`)
- [ ] `GBP_OAUTH_CLIENT_ID` מוגדר
- [ ] `GBP_OAUTH_CLIENT_SECRET` מוגדר
- [ ] `GBP_REFRESH_TOKEN` מוגדר ותקף
- [ ] `GBP_ENABLED` מוגדר ל-`false` (feature flag — מתחילים כבוי)
- [ ] פרויקט Google Cloud מאושר לגישה ל-GBP API
- [ ] OAuth scopes כוללים `business.manage`

### Google Sheets
- [ ] עמודות חדשות נוספו לגיליון: `caption`, `caption_gbp`, `gbp_post_type`, `cta_type`, `cta_url`, `google_location_id`, `source`, `locked_at`, `processing_by`, `retry_count`, `published_channels`, `failed_channels`
- [ ] שורות IG/FB קיימות ממשיכות לעבוד (תאימות לאחור)
- [ ] סדר העמודות תואם ל-`SHEET_COLUMNS` ב-`config_constants.py`

### קוד ריוויו
- [ ] כל הקבצים החדשים נבדקו: `channels/google_business.py`, `channels/google_auth.py`, `channels/google_locations.py`, `validator.py`
- [ ] אין credentials או טוקנים hardcoded בקוד
- [ ] הודעות שגיאה לא מדליפות מידע רגיש (טוקנים, סודות)
- [ ] `LOCK_TIMEOUT_MINUTES` מוגדר כראוי (ברירת מחדל: 10)

### בדיקות
- [ ] כל הטסטים היחידתיים עוברים (`pytest tests/test_unit_core.py`)
- [ ] כל תרחישי ה-E2E עוברים (`pytest tests/test_e2e_scenarios.py`)
- [ ] כל הטסטים הקיימים עוברים — רגרסיה מלאה (`pytest`)
- [ ] בדיקת עשן ידנית: פוסט IG בלבד עדיין עובד
- [ ] בדיקת עשן ידנית: פוסט FB בלבד עדיין עובד
- [ ] בדיקת עשן ידנית: פוסט IG+FB עדיין עובד

---

## עלייה לאוויר (בשלבים)

### שלב 1: Feature Flag כבוי (דיפלוי קוד בלבד)
- [ ] דיפלוי עם `GBP_ENABLED=false`
- [ ] אימות שפרסום IG/FB עובד כרגיל (אין רגרסיה)
- [ ] אימות שערוץ GBP לא רשום כשהפלאג כבוי
- [ ] מעקב אחרי לוגים — בדיקה שאין שגיאות מנתיבי קוד חדשים
- [ ] המתנה 24 שעות בלי בעיות

### שלב 2: בדיקה פנימית של GBP
- [ ] הגדרת `GBP_ENABLED=true`
- [ ] יצירת פוסט GBP לבדיקה עם `google_location_id`
- [ ] אימות שפוסט GBP טקסט בלבד מתפרסם בהצלחה
- [ ] אימות שפוסט GBP טקסט + תמונה מתפרסם בהצלחה
- [ ] אימות שהפוסט מופיע ב-Google Business Profile
- [ ] אימות ש-CTA (אם בשימוש) מוצג נכון

### שלב 3: בדיקת ערוצים מעורבים
- [ ] יצירת פוסט IG+GBP ← אימות ששניהם מתפרסמים
- [ ] יצירת פוסט IG+FB+GBP ← אימות ששלושתם מתפרסמים
- [ ] סימולציית כשל GBP (מיקום לא תקין) ← אימות סטטוס PARTIAL
- [ ] אימות retry ל-GBP בלבד ← רק GBP נשלח מחדש, לא IG/FB
- [ ] אימות התראות טלגרם ל-PARTIAL/ERROR

### שלב 4: פרודקשן
- [ ] הפעלה עבור פוסטים אמיתיים של לקוחות
- [ ] מעקב אחרי 10 הפוסטים הראשונים ב-GBP — אחוז הצלחה
- [ ] אימות ששחרור נעילות עובד לשורות PROCESSING תקועות
- [ ] אימות שניקוי Cloudinary מטפל בשורות PARTIAL

---

## מוניטורינג אחרי דיפלוי

### 48 שעות ראשונות
- [ ] בדיקת לוגים לשגיאות GBP API (rate limits, כשלי auth)
- [ ] אימות שאין פרסומים כפולים (מנגנון נעילה עובד)
- [ ] מעקב אחרי retry_count — שורות עם ספירה מעל 3 דורשות בדיקה
- [ ] בדיקה שהתראות טלגרם נשלחות על שגיאות GBP

### שוטף
- [ ] רענון OAuth token של GBP עובד (בדיקה כל 7 ימים)
- [ ] מכסת API לא נחרגת (בדיקה ב-Google Cloud Console)
- [ ] שורות PROCESSING לא מצטברות (שחרור נעילות timeout)

---

## תוכנית Rollback

אם מתגלות בעיות:

1. **Rollback מהיר**: הגדרת `GBP_ENABLED=false` ← ערוץ GBP מוסר, IG/FB לא מושפעים
2. **Rollback מלא**: חזרה לדיפלוי הקודם — כל שורות IG/FB ממשיכות לעבוד
3. **תיקון חלקי**: אם רק ל-GBP יש בעיות, שורות עם `network=IG+FB` לא מושפעות

### אינדיקטורים ל-rollback:
- GBP API מחזיר שגיאות 4xx מתמשכות
- רענון OAuth token נכשל שוב ושוב
- לולאת שחרור נעילות (שורות מתחלפות בין PROCESSING ← READY ← PROCESSING)
- רגרסיה ב-IG/FB (כל כשל בערוצים קיימים = rollback מיידי)
