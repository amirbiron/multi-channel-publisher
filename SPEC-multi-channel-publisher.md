# מסמך אפיון: Multi-Channel Publisher

> **תאריך:** 27.03.2026
> **סטטוס:** v2 — מעודכן לאחר ביקורת + Technical Tasks מפורטים
> **פרויקט בסיס:** Social Publisher (קיים ופעיל)

---

## 1. רקע ומצב קיים

### מה קיים היום
מערכת **Social Publisher** — פאנל ווב (Flask + Google Sheets) שמנהל ומפרסם פוסטים ל-**Facebook** ו-**Instagram** דרך Meta Graph API.

**זרימה נוכחית:**
```
Google Sheets (תור פוסטים)
  → Google Drive (מדיה)
    → Cloudinary (CDN)
      → Meta Graph API (IG / FB)
```

**טכנולוגיות:**
- **Backend:** Python 3.12, Flask, Gunicorn
- **מסד נתונים:** Google Sheets (מסמך טבלאי כ-DB)
- **אחסון מדיה:** Google Drive → Cloudinary
- **API פרסום:** Meta Graph API (v21.0)
- **התראות:** Telegram Bot
- **דיפלוי:** Render (Cron Job + Web Service)
- **עיבוד מדיה:** Pillow (נרמול תמונות/וידאו)

### מערכת הלקוח הקיימת (חיצונית)
פאנל נפרד (Claude + Supabase) שמייצר תוכן באמצעות AI:
- המשתמש מכניס נושא + יעד רשת
- המערכת מייצרת תוכן מבוסס לוגיקה, סגנון לקוח, ונתוני מחקר
- **הפלט יוצא ל-Google Sheets** בפורמט מובנה

---

## 2. מטרת הפרויקט

בניית **Multi-Channel Publisher** — הרחבה של המערכת הקיימת כך ש:

1. **UI אחד אחיד** — ממשק אחד לניהול כל ערוצי הפרסום
2. **חיבור נפרד לכל פלטפורמה** — כל ערוץ עם API, קרדנשיאלס וסטטוס עצמאי
3. **סטטוס נפרד לכל ערוץ** — פוסט אחד יכול להצליח ב-FB ולהיכשל ב-Google Business
4. **ארכיטקטורה מודולרית** — הוספת ערוץ חדש = הוספת מודול, בלי לשנות את המבנה
5. **חיבור אוטומטי למערכת הקיימת** — קליטה ישירה מה-Google Sheets שמייצרת מערכת ה-AI, עם אפשרות גם להתערבות ידנית

---

## 3. ערוצי פרסום — שלב ראשון

| ערוץ | סטטוס | API |
|------|--------|-----|
| Instagram | קיים ✅ | Meta Graph API |
| Facebook Page | קיים ✅ | Meta Graph API |
| Google Business Profile | **חדש** 🆕 | Google Business Profile API |

### Google Business Profile API — סקירה
- **API:** Google My Business API / Business Profile API
- **יכולות:** פוסטים (STANDARD, EVENT, OFFER), תמונות, עדכונים
- **אימות:** OAuth 2.0 (ברירת מחדל). Service Account — רק לאחר POC שמוכיח תאימות עם החשבון העסקי הספציפי
- **מגבלות:** המכסות תלויות באישור הפרויקט ובהגדרות Google (למשל 300 QPM לחלק מה-APIs). יש לאמת אותן בפועל מול הפרויקט לפני עלייה לאוויר. פוסט מוגבל ל-1,500 תווים
- **סוגי פוסטים ב-API:** `STANDARD` (טקסט+תמונה או טקסט בלבד), `EVENT` (אירוע עם תאריכים), `OFFER` (הנחה עם קופון)
- **תמיכה בטקסט בלבד:** שדה `media` לא מסומן כחובה ב-LocalPost, כך שאפשר לפרסם פוסט `STANDARD` עם `summary` בלבד
- **Multi-location:** GBP תומך בכמה מיקומים לחשבון. שליפה דרך `accounts.locations.list`. תמיכה במיקום ברמת שורה (`google_location_id`) ולא כקונפיג גלובלי
- **הערה חשובה:** ב-UI נציג "עדכון" למשתמש, אבל בקוד נמפה ל-`STANDARD` שהוא הסוג הרשמי ב-API. אין סוג `UPDATE` ב-GBP API
- **גישת MVP:** נתחיל עם `STANDARD` בלבד. `EVENT` ו-`OFFER` דורשים שדות נוספים (event, offer) ויתווספו לאחר ייצוב

---

## 4. ארכיטקטורה — שכבת ערוצים (Channel Layer)

### 4.1 עיקרון: Channel Interface

כל ערוץ פרסום מממש ממשק אחיד:

```python
# channels/base.py
from dataclasses import dataclass

@dataclass
class PublishResult:
    """תוצאת פרסום לערוץ בודד"""
    channel: str                    # "IG", "FB", "GBP"
    success: bool
    status: str                     # "POSTED" / "ERROR" / "SKIPPED"
    platform_post_id: str | None    # מזהה הפוסט בפלטפורמה
    error_code: str | None          # "timeout", "quota_exceeded", "invalid_caption"
    error_message: str | None       # הודעה ידידותית ל-UI
    raw_response: dict | None       # תשובה גולמית מה-API (ללוגים)
    published_at: str | None        # timestamp של הפרסום

class BaseChannel:
    """ממשק בסיס לכל ערוץ פרסום"""

    CHANNEL_ID: str               # מזהה ייחודי: "IG", "FB", "GBP"
    CHANNEL_NAME: str             # שם תצוגה: "Instagram", "Google Business"
    SUPPORTED_POST_TYPES: list    # ["FEED", "REELS"] / ["STANDARD", "EVENT"]
    SUPPORTED_MEDIA_TYPES: list   # ["image", "video"] / ["image", "none"]

    def validate(self, post_data: dict) -> list[str]:
        """בדיקת תקינות לפני פרסום — מחזיר רשימת שגיאות (ריקה = תקין)"""

    def publish(self, post_data: dict) -> PublishResult:
        """פרסום בפועל — מחזיר תוצאה"""

    def get_caption_column(self) -> str:
        """שם עמודת הכיתוב בטבלה"""
```

### 4.2 מבנה תיקיות מוצע

```
Social-publisher/
├── channels/
│   ├── __init__.py
│   ├── base.py              # BaseChannel + PublishResult
│   ├── registry.py          # רישום והפעלת ערוצים
│   ├── meta_instagram.py    # IG — מעטפת ל-meta_publish.py הקיים
│   ├── meta_facebook.py     # FB — מעטפת ל-meta_publish.py הקיים
│   └── google_business.py   # GBP — חדש
├── main.py                  # עדכון — שימוש ב-registry
├── web_app.py               # עדכון — UI מרובה ערוצים
├── meta_publish.py          # ← נשאר כמו שהוא (backward compatible)
├── google_api.py            # ← נשאר + הוספת GBP functions
├── ...
```

### 4.3 Channel Registry — רישום ערוצים

```python
# channels/registry.py

class ChannelRegistry:
    """מנהל ערוצים — נקודת כניסה מרכזית"""

    _channels: dict[str, BaseChannel] = {}

    def register(self, channel: BaseChannel):
        """רישום ערוץ חדש"""

    def get(self, channel_id: str) -> BaseChannel:
        """קבלת ערוץ לפי מזהה"""

    def get_all(self) -> list[BaseChannel]:
        """כל הערוצים הרשומים"""

    def validate_channels(
        self,
        post_data: dict,
        target_channels: list[str]
    ) -> dict[str, list[str]]:
        """
        ולידציה לפני פרסום — מחזיר שגיאות לכל ערוץ.
        {"GBP": ["missing google_location_id"]}
        """

    def publish_to_channels(
        self,
        post_data: dict,
        target_channels: list[str]
    ) -> dict[str, PublishResult]:
        """
        פרסום לרשימת ערוצים. ערוץ שנכשל לא עוצר ערוצים אחרים.
        מחזיר: {"IG": PublishResult(...), "FB": PublishResult(...), "GBP": PublishResult(...)}
        """
```

**יתרון:** להוסיף ערוץ חדש בעתיד = ליצור קובץ channel חדש + לרשום אותו ב-registry.

---

## 5. מבנה הנתונים — Google Sheets

### 5.1 שינויים בטבלה

**עמודות חדשות ומעודכנות:**

| עמודה | חדש/קיים | תיאור | דוגמה |
|-------|----------|--------|--------|
| `network` | **עדכון** | תמיכה בערוצים נוספים | `IG+FB+GBP`, `GBP`, `ALL` |
| `caption` | **חדש** | כיתוב כללי — fallback לכל ערוץ | "טקסט ברירת מחדל..." |
| `caption_gbp` | **חדש** | כיתוב ייעודי ל-GBP | "עדכון חדש מהעסק..." |
| `gbp_post_type` | **חדש** | סוג פוסט ב-GBP | `STANDARD` / `EVENT` / `OFFER` |
| `google_location_id` | **חדש** | מזהה מיקום GBP ברמת שורה | `locations/987654321` |
| `cta_type` | **חדש** | סוג Call To Action (GBP) | `LEARN_MORE`, `CALL`, `BOOK` |
| `cta_url` | **חדש** | קישור CTA | `https://example.com/offer` |
| `source` | **חדש** | מקור הפוסט | `manual` / `auto` / `ai-panel` |
| `result` | **קיים** | הרחבת הפורמט לכלול גם GBP | `IG:123 \| FB:456 \| GBP:ERR:timeout` |
| `locked_at` | **חדש** | timestamp נעילה | `2026-03-27T18:00:05Z` |
| `processing_by` | **חדש** | מזהה הריצה שנעלה | `run_abc123` |
| `retry_count` | **חדש** | מספר ניסיונות | `0`, `1`, `2` |
| `published_channels` | **חדש** | ערוצים שהצליחו | `IG,FB` |
| `failed_channels` | **חדש** | ערוצים שנכשלו | `GBP` |

### 5.2 פורמט Network מורחב

```
# ערוץ בודד
IG
FB
GBP

# שילובים
IG+FB          (קיים)
IG+GBP
FB+GBP
IG+FB+GBP     (כל הערוצים)
ALL            (קיצור ל-כל הערוצים הרשומים)
```

### 5.3 פורמט תוצאה מפורט

**פורמט עמודת `result` (קיימת — הרחבה):**
```
# עמודת result — כבר קיימת, מרחיבים את הפורמט לכלול סטטוס + GBP
IG:POSTED:17841405822953 | FB:POSTED:615273820 | GBP:ERROR:quota_exceeded

# עמודת status — לוגיקה מעודכנת
POSTED       → כל הערוצים הצליחו
PARTIAL      → חלק הצליחו, חלק נכשלו (חדש! — כיום מסומן כ-ERROR עם פירוט בעמודת error)
ERROR        → כל הערוצים נכשלו
```

> **הערה:** פורמט `result` כמחרוזת מספיק ל-MVP (וכבר עובד כך עבור IG+FB), אך לא מתאים לסינון, retry לפי ערוץ, או ניתוח שגיאות בדוחות.
> **שדרוג מומלץ (post-MVP):** הוספת Sheet נפרד בשם `deliveries` — כל שורה = ניסיון פרסום לערוץ בודד, עם עמודות: `post_id`, `channel`, `status`, `platform_id`, `error`, `attempt`, `timestamp`.

### 5.4 סטטוסים פנימיים — מנגנון מניעת פרסום כפול

> **מצב קיים:** המערכת כבר משתמשת ב-`IN_PROGRESS` כסטטוס נעילה (ראו `main.py:148-155`, `config_constants.py:30`).
> **שדרוג:** שינוי שם ל-`PROCESSING`, הוספת `DRAFT` ו-`PARTIAL`, חיזוק עם `locked_at` + `processing_by` + `retry_count`.

```
DRAFT          → טיוטה, לא מוכן לפרסום (חדש!)
READY          → ממתין לפרסום
PROCESSING     → נלקח לטיפול (נעול) — מחליף את IN_PROGRESS
POSTED         → כל הערוצים הצליחו
PARTIAL        → חלק הצליחו (חדש!)
ERROR          → כל הערוצים נכשלו
```

> **מיגרציה:** יש לעדכן את `STATUS_IN_PROGRESS` ב-`config_constants.py` ל-`STATUS_PROCESSING = "PROCESSING"` ולהתאים את כל ההפניות.

**זרימה (שדרוג של הקיים):**
1. Cron קורא שורות `READY` שהגיע זמנן
2. מעדכן מיידית ל-`PROCESSING` + `locked_at` + `processing_by`
3. Re-read מהטבלה לאימות — אם סטטוס השתנה, מדלג (מנגנון קיים)
4. ולידציה לפי ערוצים מבוקשים (network, caption, media, location)
5. מפרסם לערוצים
6. מעדכן ל-`POSTED` / `PARTIAL` / `ERROR` + `published_channels` + `failed_channels`
7. שורה שנשארת `PROCESSING` מעל X דקות → timeout, חוזרת ל-`READY` (עם הגדלת `retry_count`)

---

## 6. חיבור למערכת ה-AI הקיימת

### 6.1 תרחיש: קליטה אוטומטית מ-Google Sheets

```
┌──────────────────┐     Google Sheets      ┌──────────────────────┐
│  AI Content      │  ──── כותב ל- ────►   │  Multi-Channel       │
│  Generator       │     טבלה מובנית        │  Publisher           │
│  (Claude+Supa)   │                        │  (קורא ומפרסם)       │
└──────────────────┘                        └──────────────────────┘
```

**איך זה עובד:**
1. מערכת ה-AI כותבת שורה ל-Google Sheets עם `status=READY`
2. ה-Publisher (Cron Job) קורא שורות READY שהגיע זמנן
3. מפרסם לערוצים שהוגדרו בעמודת `network`
4. מעדכן סטטוס + תוצאה

### 6.2 חוזה נתונים (Data Contract) מול מערכת ה-AI

| שדה | חובה? | דוגמה | הערות |
|---|---|---|---|
| `status` | כן | `READY` | ערכים מותרים: `READY`, `DRAFT` |
| `network` | כן | `IG+FB+GBP` | או `ALL` |
| `scheduled_time` | כן | `2026-03-27 18:00` | timezone מוסכם (Asia/Jerusalem) |
| `caption` | כן | "טקסט ברירת מחדל..." | caption כללי — fallback לכל ערוץ |
| `caption_ig` | לא | "..." | אם קיים — עוקף את `caption` עבור IG |
| `caption_fb` | לא | "..." | אם קיים — עוקף את `caption` עבור FB |
| `caption_gbp` | מותנה | "..." | חובה אם GBP ברשימת הערוצים ואין `caption` כללי |
| `media_url` / `drive_file_id` | מותנה | `https://...` | לפחות asset אחד לפוסט עם מדיה. GBP תומך גם בטקסט בלבד |
| `gbp_post_type` | מותנה | `STANDARD` | חובה אם GBP ברשימת הערוצים. ב-MVP רק `STANDARD` |
| `google_location_id` | מותנה | `locations/987654321` | חובה אם GBP ברשימת הערוצים |
| `cta_type` | לא | `LEARN_MORE` | רלוונטי ל-GBP STANDARD. Google מתעלמת מ-CTA ב-OFFER |
| `cta_url` | מותנה | `https://...` | חובה אם יש `cta_type` |
| `source` | כן | `ai-panel` | ערכים סגורים: `manual`, `auto`, `ai-panel` |

**לוגיקת fallback לכיתובים:**

> **מצב קיים:** IG משתמש ב-`caption_ig`, fallback ל-`caption_fb`. FB משתמש ב-`caption_fb`, fallback ל-`caption_ig`.
> **שדרוג:** הוספת שדה `caption` כללי כ-fallback אחיד לכל הערוצים.

1. אם יש `caption_{channel}` (למשל `caption_gbp`) → משתמשים בו
2. אחרת אם יש `caption` כללי → משתמשים בו
3. אחרת → validation error (הפוסט לא יצא לפרסום)

### 6.3 תרחיש: התערבות ידנית

הפאנל החדש תומך גם בעבודה ידנית מלאה:
- יצירת פוסט חדש ידנית
- עריכת פוסט שנוצר אוטומטית (לפני פרסום)
- שינוי ערוצי יעד
- פרסום מיידי (Publish Now)
- צפייה בתוצאות ושגיאות לכל ערוץ

---

## 7. שינויים ב-UI (פאנל ווב)

### 7.1 בחירת ערוצים

```
┌───────────────────────────────────────────────┐
│  ערוצי פרסום:                                 │
│  [✓] Instagram   [✓] Facebook   [✓] GBP      │
│                                               │
│  כיתוב כללי:     ___________________          │
│  כיתוב Instagram: ___________________  (אופ') │
│  כיתוב Facebook:  ___________________  (אופ') │
│  כיתוב GBP:       ___________________  (אופ') │
│                                               │
│  ── שדות Google Business (מוצגים רק אם GBP) ──│
│  מיקום:          [בחר מיקום ▾] 🔄             │
│  סוג פוסט GBP:   [עדכון (STANDARD) ▾]        │
│  CTA:            [ללא ▾] [URL: ___________]   │
└───────────────────────────────────────────────┘
```

### 7.2 תצוגת סטטוס מרובה ערוצים

כל פוסט מציג סטטוס לכל ערוץ בנפרד:

```
┌─────────────────────────────────────────────┐
│  פוסט #42 — "עדכון שבועי"                   │
│                                             │
│  IG  ✅ פורסם (ID: 17841405822953)          │
│  FB  ✅ פורסם (ID: 615273820)               │
│  GBP ❌ שגיאה: quota_exceeded               │
│                                             │
│  [נסה שוב GBP]  [ערוך]  [מחק]              │
└─────────────────────────────────────────────┘
```

### 7.3 פילטר לפי ערוץ

```
סינון: [הכל ▾] [IG] [FB] [GBP] [שגיאות בלבד]
```

### 7.4 ולידציה לפי יכולות ערוץ

לא כל ערוץ תומך באותן יכולות. ה-UI צריך להתאים את עצמו בזמן אמת:

| ערוץ | תמונה | וידאו | Reels | Carousel | הערות |
|------|--------|--------|-------|----------|-------|
| IG | ✅ | ✅ (כ-Reels) | ✅ | ✅ (2-10) | קיים ופועל |
| FB | ✅ | ✅ | ✅ | ⚠️ (דורש הרשאת `pages_manage_posts`) | FB carousel כרגע מפרסם רק פריט ראשון (ראו `main.py:253`) |
| GBP | ✅ | ❌ | ❌ | ❌ | חדש |

**כללים:**
- אם נבחר GBP → חסימת העלאת וידאו + הודעה למשתמש
- אם נבחר GBP עם EVENT/OFFER → פתיחת שדות תאריך / קופון / תנאים
- validation בצד הקליינט **לפני submit** (למנוע שגיאות מיותרות מצד ה-API)

---

## 8. Google Business Profile — פירוט טכני

### 8.1 קובץ חדש: `channels/google_business.py`

```python
class GoogleBusinessChannel(BaseChannel):
    CHANNEL_ID = "GBP"
    CHANNEL_NAME = "Google Business Profile"
    SUPPORTED_POST_TYPES = ["STANDARD"]       # MVP — EVENT/OFFER יתווספו בהמשך
    SUPPORTED_MEDIA_TYPES = ["image", "none"]  # תומך גם בפוסט טקסט בלבד

    def validate(self, post_data: dict) -> list[str]:
        """
        ולידציה ל-GBP:
        - google_location_id חובה
        - gbp_post_type נתמך
        - caption קיים (ישיר או fallback)
        - אם יש מדיה — רק תמונה
        """

    def publish(self, post_data: dict) -> PublishResult:
        """
        פרסום ל-Google Business Profile.
        1. קריאת google_location_id מהשורה
        2. בניית LocalPost body (summary, topicType, media אופציונלי)
        3. יצירת localPost דרך GBP API
        """
```

### 8.2 Google Business Profile API — קריאות

```python
# יצירת פוסט — location_id נקרא מהשורה בטבלה (google_location_id)
POST https://mybusiness.googleapis.com/v4/accounts/{account_id}/locations/{location_id}/localPosts

# גוף הבקשה — STANDARD עם תמונה
{
    "languageCode": "he",
    "summary": "טקסט הפוסט...",
    "media": [{
        "mediaFormat": "PHOTO",
        "sourceUrl": "https://res.cloudinary.com/..."
    }],
    "topicType": "STANDARD"
}

# גוף הבקשה — STANDARD טקסט בלבד (ללא media)
{
    "languageCode": "he",
    "summary": "טקסט הפוסט...",
    "topicType": "STANDARD"
}

# שליפת מיקומים זמינים לחשבון
GET https://mybusiness.googleapis.com/v4/accounts/{account_id}/locations
```

### 8.3 משתני סביבה חדשים

```env
# Google Business Profile — OAuth 2.0
GBP_ACCOUNT_ID=accounts/123456789
GBP_OAUTH_CLIENT_ID=...
GBP_OAUTH_CLIENT_SECRET=...
GBP_REFRESH_TOKEN=...

# הערה: google_location_id נשמר ברמת שורה בטבלה, לא כ-env var גלובלי
# כך שאפשר לפרסם לכמה מיקומים שונים
```

> **הערה לגבי אימות:** ברירת המחדל היא OAuth 2.0 עם scope `business.manage`. שימוש ב-Service Account אפשרי רק לאחר POC. תהליך הגישה ל-GBP API דורש אישור פרויקט מ-Google — פרויקט שלא אושר עלול להיות מוגבל ל-0 QPM.

> **הערה לגבי multi-location:** `GBP_ACCOUNT_ID` הוא גלובלי (חשבון אחד), אבל `google_location_id` נשמר ברמת שורה. כך הלקוח יכול לנהל כמה מיקומים מאותו פאנל.

---

## 9. Retry Policy

> **מצב קיים:** כבר קיים מנגנון retry ב-`main.py` (`_publish_with_retry`) — עד `PUBLISH_MAX_RETRIES` ניסיונות (ברירת מחדל: 3) עם exponential backoff (בסיס `PUBLISH_RETRY_DELAY`, ברירת מחדל: 5s).
> הרחבה נדרשת: סיווג שגיאות (retryable vs non-retryable), retry ידני מה-UI לערוץ ספציפי.

### 9.1 עקרון: retry פר ערוץ, לא לכל הפוסט

אם פוסט הצליח ב-IG ו-FB אבל נכשל ב-GBP — ננסה שוב רק את GBP.
(כיום המנגנון הקיים כבר תומך ב-partial success — ערוץ שנכשל לא עוצר ערוצים אחרים.)

### 9.2 סיווג שגיאות (חדש)

| סוג שגיאה | Retryable? | דוגמאות |
|-----------|------------|---------|
| שגיאת רשת / timeout | ✅ כן | `ConnectionError`, `Timeout`, `502/503/504` |
| rate limit | ✅ כן | `429`, `quota_exceeded` |
| שגיאת שרת | ✅ כן | `500`, `InternalServerError` |
| תוכן לא תקין | ❌ לא | `caption_too_long`, `invalid_media_format` |
| הרשאות חסרות | ❌ לא | `403`, `insufficient_permissions` |
| משאב לא נמצא | ❌ לא | `404`, `location_not_found` |

### 9.3 מדיניות retry אוטומטי

- **מספר ניסיונות:** עד 3 (כולל הניסיון הראשוני)
- **ביניהם:** exponential backoff — 30s, 120s, 300s
- **rate limit:** המתנה לפי `Retry-After` header אם קיים, אחרת 60s
- **לאחר 3 ניסיונות כושלים:** סימון `ERROR` + retry ידני בלבד מה-UI

### 9.4 גישת MVP

- **retry אוטומטי:** כבר קיים ב-Meta (IG/FB). יש להחיל את אותו מנגנון גם על GBP
- **retry ידני מה-UI:** חדש — כפתור "נסה שוב" לערוץ ספציפי שנכשל (ללא צורך לפרסם מחדש לערוצים שהצליחו)

---

## 10. Observability

### 10.1 לוגים מובנים

כל ניסיון פרסום ירשום:

| שדה | תיאור | דוגמה |
|------|--------|--------|
| `correlation_id` | מזהה ייחודי ל-job (כל הערוצים של אותו פוסט) | `job_20260327_180005_abc` |
| `channel` | ערוץ ספציפי | `GBP` |
| `action` | מה נעשה | `publish`, `validate`, `retry` |
| `started_at` | זמן התחלה | `2026-03-27T18:00:05Z` |
| `ended_at` | זמן סיום | `2026-03-27T18:00:07Z` |
| `status` | תוצאה | `success`, `error` |
| `error_raw` | הודעת שגיאה גולמית מה-API | `{"error": {"code": 429, ...}}` |
| `error_friendly` | הודעה ידידותית ל-UI | `חריגה ממכסת בקשות — נסה שוב מאוחר יותר` |

### 10.2 התראות Telegram

> **מצב קיים:** כבר פועל ב-`notifications.py` — שגיאת פרסום (`notify_publish_error`), הצלחה חלקית (`notify_partial_success`), בעיות שירות (`notify_health_issue`), תפוגת Meta API.

**הרחבה נדרשת:**
- הוספת `correlation_id` להודעות (מזהה job ייחודי)
- התראה ייעודית ל-GBP errors
- התראה על timeout של שורה `PROCESSING`

---

## 11. Technical Tasks — פירוט מלא

### Phase 1 — Foundation

#### Task 1: Data Contract + Google Sheets Schema
**מטרה:** הגדרת פורמט אחיד של שורה בטבלה, כך שגם המערכת הידנית וגם מערכת ה-AI יוכלו לעבוד מול אותו מבנה.
- [ ] עדכון מבנה ה-Google Sheet עם כל העמודות החדשות (סעיף 5.1)
- [ ] הגדרת ערכים מוסכמים: `network`, `status`, `source`, `gbp_post_type`
- [ ] מימוש לוגיקת fallback לכיתובים
- [ ] `google_location_id` ברמת שורה ולא כקונפיג גלובלי
- **AC:** שורות ישנות של IG/FB ממשיכות לעבוד עם fallback; אפשר לקרוא שורה ולבנות `post_data` תקין

#### Task 2: Base Channel + PublishResult + Registry
**מטרה:** שכבת abstraction אחת לכל ערוצי הפרסום.
- [ ] יצירת `channels/base.py` עם `PublishResult` + `BaseChannel`
- [ ] יצירת `channels/registry.py` עם `register`, `get`, `validate_channels`, `publish_to_channels`
- **AC:** אפשר לרשום ערוצים, לפרסם לרשימה, ולקבל result נפרד לכל ערוץ; אין תלות ישירה ב-`main.py` בערוצים ספציפיים

#### Task 3: עטיפת IG/FB הקיימים למודל ערוצים
**מטרה:** הכנסת המערכת הקיימת לארכיטקטורה החדשה בלי לשבור כלום.
- [ ] יצירת `channels/meta_instagram.py` — wrapper סביב `meta_publish.py`
- [ ] יצירת `channels/meta_facebook.py` — wrapper סביב `meta_publish.py`
- [ ] עדכון `main.py` לעבוד דרך Registry
- **AC:** פוסט IG/FB ממשיך לעבוד כמו לפני השינוי; regression test מוכיח שהזרימה לא נשברה

#### Task 4: Publish Orchestrator + Locking + Idempotency
**מטרה:** ריצה בטוחה של cron/job בלי פרסום כפול, עם תמיכה ב-PARTIAL.
- [ ] שליפת שורות `READY`, נעילה ל-`PROCESSING` + `locked_at` + `processing_by`
- [ ] Re-read לאימות (שדרוג מנגנון קיים)
- [ ] חישוב סטטוס: `POSTED` / `PARTIAL` / `ERROR`
- [ ] שחרור lock לשורה שנשארת `PROCESSING` מעל timeout
- **AC:** אותו פוסט לא מתפרסם פעמיים; PARTIAL עובד; retry לערוץ בודד לא מפרסם מחדש ערוצים שהצליחו

### Phase 2 — Google Business בסיסי

#### Task 5: Google OAuth + Location Service
**מטרה:** חיבור אמיתי ל-GBP, כולל תמיכה בכמה מיקומים.
- [ ] מודול auth: OAuth token + refresh אוטומטי
- [ ] מודול locations: `list_locations()`, `get_location()`, `validate_location_access()`
- [ ] cache קצר לרשימת מיקומים
- **AC:** המערכת מתחברת ל-Google עם OAuth; אפשר לשלוף מיקומים; `google_location_id` לא תקין → שגיאה ברורה

#### Task 6: Google Business Channel — STANDARD only
**מטרה:** GBP בפרודקשן בצורה הכי בטוחה: `STANDARD` בלבד.
- [ ] `channels/google_business.py` עם `SUPPORTED_POST_TYPES = ["STANDARD"]`
- [ ] תמיכה בטקסט בלבד (media אופציונלי)
- [ ] תמיכה בטקסט + תמונה
- [ ] קריאת `google_location_id` מהשורה
- [ ] חסימה של `EVENT`/`OFFER` בשלב זה עם הודעה ברורה
- **AC:** פוסט GBP טקסט בלבד מתפרסם; פוסט GBP עם תמונה מתפרסם; GBP בלי `google_location_id` → validation error

#### Task 7: Result Mapping + Retry per Channel
**מטרה:** הפיכת שגיאות ותוצאות למשהו שה-UI והאופרציה יכולים לעבוד איתו.
- [ ] סיווג שגיאות: retryable (timeout, 5xx, rate limit) vs non-retryable (invalid caption, missing permissions)
- [ ] retry ידני מערוץ ספציפי
- [ ] הרחבת retry אוטומטי קיים (Meta) גם ל-GBP
- [ ] עדכון `result` + `published_channels` + `failed_channels`
- **AC:** retry ל-GBP בלי לפרסם שוב IG/FB; הודעת שגיאה ידידותית + error code טכני

### Phase 3 — UI

#### Task 8: UI Form מרובה ערוצים
**מטרה:** יצירה ועריכה של פוסט רב-ערוצי מפאנל אחד.
- [ ] checkbox לערוצים: IG / FB / GBP
- [ ] שדה `caption` כללי + שדות caption אופציונליים לכל ערוץ
- [ ] שדות GBP (מוצגים רק אם GBP מסומן): מיקום, `gbp_post_type`, `cta_type`, `cta_url`
- [ ] validation בצד הקליינט לפני submit
- **AC:** אפשר ליצור פוסט ל-IG+FB+GBP; GBP בלי location → לא ניתן לשמור; שדות Google לא מפריעים לפוסט Meta בלבד

#### Task 9: UI תוצאות / שגיאות / Retry
**מטרה:** תצוגה ברורה של מה קרה בכל ערוץ.
- [ ] סטטוס נפרד לכל ערוץ בכרטיס פוסט
- [ ] כפתור retry לערוץ שנכשל
- [ ] פילטרים: הכל / IG / FB / GBP / שגיאות בלבד / partial בלבד
- **AC:** PARTIAL מוצג נכון; retry נפרד לערוצים שנכשלו; אפשר לסנן פוסטים עם כשל ב-GBP

#### Task 10: UI חיבור למיקומי Google
**מטרה:** בחירת location נוחה במקום הזנה ידנית.
- [ ] dropdown של מיקומים זמינים מ-Google
- [ ] שמירה כ-`google_location_id` בטבלה
- [ ] refresh לרשימת מיקומים
- [ ] fallback להזנה ידנית למנהלי מערכת
- **AC:** משתמש רואה מיקומים זמינים; location לא נגיש → אזהרה; אפשר לטעון מחדש בלי restart

### Phase 4 — Integrations + Hardening

#### Task 11: Cron Flow + AI Intake
**מטרה:** מערכת ה-AI ממשיכה לכתוב ל-Sheets וה-Publisher קולט ומפרסם.
- [ ] עדכון cron pipeline: פרסור `network`, fallback captions, `google_location_id`
- [ ] validator לפני publish: network תקין, caption קיים, media מתאים, GBP location תקין
- **AC:** מערכת ה-AI כותבת שורה והפובלישר קולט אוטומטית; חסר שדה חובה ל-GBP → error ברור בלי לפגוע ב-Meta

#### Task 12: Logging, Monitoring, Admin Safety
**מטרה:** תחזוקה ודיבוג אמיתיים.
- [ ] `correlation_id` לכל job
- [ ] לוג מובנה לכל publish attempt: channel, location, duration, success/error
- [ ] masking של secrets בלוגים
- [ ] הרחבת Telegram alerts ל-GBP + timeout של `PROCESSING`
- **AC:** אפשר לזהות למה פוסט נכשל ובאיזה ערוץ; אין טוקנים בלוגים

#### Task 13: Tests E2E + Deployment Checklist
**מטרה:** עלייה לפרודקשן בלי הימורים.
- [ ] Unit tests: parsing network, caption fallback, status aggregation, lock handling
- [ ] Integration tests: registry + IG/FB wrappers + GBP mock
- [ ] E2E scenarios: IG+FB בלבד, GBP text only, GBP+image, IG+FB+GBP PARTIAL, retry, location invalid
- [ ] Deployment checklist + rollout עם feature flag ל-GBP
- **AC:** כל תרחישי MVP עוברים; regression על IG/FB עובר

### Phase 5 — Optional after MVP stabilization

#### Task 14: GBP EVENT
- [ ] תמיכה ב-`gbp_post_type=EVENT` עם start/end time
- [ ] UI: שדות תאריך מוצגים רק כשנבחר EVENT
- **AC:** EVENT בלי טווח תאריכים → validation error; EVENT מתפרסם בהצלחה

#### Task 15: GBP OFFER
- [ ] תמיכה ב-`OFFER` עם coupon code, redeem URL, terms, event window
- [ ] UI: חסימת CTA רגיל ב-OFFER (Google מתעלמת מ-`callToAction` עבור OFFER)
- **AC:** OFFER בלי שדות חובה → validation error; OFFER מתפרסם ונשמר עם result תקין

---

## 12. Sprint Plan מוצע

| Sprint | Tasks | תיאור |
|--------|-------|--------|
| Sprint 1 | Task 1-4 | Foundation: schema, channel layer, IG/FB wrappers, orchestrator |
| Sprint 2 | Task 5-7 | GBP: OAuth, STANDARD publish, result mapping |
| Sprint 3 | Task 8-10 | UI: form, results, location selector |
| Sprint 4 | Task 11-13 | Integration: cron, monitoring, E2E tests |
| Sprint 5 | Task 14-15 | Optional: GBP EVENT + OFFER |

---

## 13. Definition of Done — MVP

ה-MVP נחשב מוכן כשכל התנאים מתקיימים:

- [ ] אפשר ליצור ולפרסם פוסט מאותו UI ל-IG, FB, GBP
- [ ] GBP תומך בטקסט בלבד וגם טקסט + תמונה
- [ ] תמיכה במיקום Google ברמת שורה (`google_location_id`)
- [ ] פוסט אחד יכול להסתיים ב-`PARTIAL`
- [ ] יש retry נפרד לערוץ שנכשל
- [ ] שורות AI נכנסות דרך Google Sheets בלי צורך בהתאמה ידנית
- [ ] IG/FB הישנים לא נשברו (backward compatible)
- [ ] יש לוגים טובים ודיבוג סביר
- [ ] יש rollout בטוח ל-GBP עם feature flag

---

## 14. סיכום טכני

| נושא | פרטים |
|------|--------|
| **שפה** | Python 3.12 |
| **Framework** | Flask |
| **DB** | Google Sheets (ללא שינוי) |
| **מדיה** | Google Drive → Cloudinary (ללא שינוי) |
| **APIs חדשים** | Google Business Profile API (OAuth 2.0) |
| **APIs קיימים** | Meta Graph API (IG + FB) — ללא שינוי |
| **דיפלוי** | Render (ללא שינוי) |
| **ערוצים ב-MVP** | IG, FB, GBP (STANDARD only) |
| **ערוצים עתידיים אפשריים** | LinkedIn, Twitter/X, TikTok, Pinterest |
| **GBP MVP** | STANDARD בלבד, EVENT/OFFER אחרי ייצוב |

### עיקרון מנחה
> **הוספת ערוץ חדש = קובץ Python חדש בתיקיית `channels/` + רישום ב-Registry.**
> אין צורך לשנות את `main.py`, את ה-UI, או את מבנה הטבלה (מעבר להוספת עמודת caption).

---

## 15. החלטות מוצר/טכניקה שנקבעו

1. **UI label vs API:** ב-UI מציגים "עדכון", בבקאנד ממפים ל-`STANDARD`
2. **אימות GBP:** OAuth 2.0 בלבד. Service Account רק לאחר POC
3. **Multi-location:** `google_location_id` ברמת שורה, לא env var גלובלי
4. **Retry:** פר ערוץ, לא לכל הפוסט
5. **Status:** `PROCESSING` (מחליף `IN_PROGRESS`) + `DRAFT` + `PARTIAL` חדשים
6. **GBP MVP:** רק `STANDARD`. EVENT/OFFER אחרי ייצוב
7. **טקסט בלבד:** GBP תומך בפוסט בלי תמונה

---

## 16. שאלות פתוחות לבירור עם הלקוח

1. **אימות GBP:** האם יש גישה פעילה ל-GBP API? האם הפרויקט ב-Google Cloud אושר?
2. **פורמט ה-Sheets:** האם מערכת ה-AI יכולה לעבוד לפי Data Contract (סעיף 6.2)?
3. **התראות:** האם להרחיב את התראות הטלגרם גם ל-GBP?
4. **Timezone:** האם `scheduled_time` תמיד Asia/Jerusalem?
