# שלבי פיתוח — הרחבה ל-Google Business Profile ו-LinkedIn

**WeRAI | אפריל 2026**
**מבוסס על מסמך דרישות ותנאי עסקה**

---

## סקירה כללית

המערכת הקיימת תומכת בפרסום ל-Facebook ו-Instagram דרך Meta Graph API v21.0.
הפיתוח של Google Business Profile **החל** (קוד קיים ב-`channels/google_business.py`, `channels/google_auth.py`, `channels/google_locations.py`).
LinkedIn הוא ערוץ חדש לגמרי שיש לבנות מאפס.

הארכיטקטורה המודולרית (`channels/` + `ChannelRegistry`) מאפשרת הוספת ערוצים ללא שינוי ב-`main.py`.

---

## שלב 1: השלמת Google Business Profile

**משך משוער: ~1-2 ספרינטים**
**סטטוס נוכחי: קוד בסיסי קיים, דורש השלמה ובדיקות**

### 1.1 סקירה ותיקון קוד קיים

- [ ] סקירת `channels/google_business.py` — וידוא שה-API calls תקינים מול GBP API v4
- [ ] סקירת `channels/google_auth.py` — וידוא OAuth 2.0 refresh flow עובד end-to-end
- [ ] סקירת `channels/google_locations.py` — וידוא שליפת locations עם pagination ו-caching
- [ ] וידוא שכל ה-error codes מוגדרים נכון (retryable vs non-retryable)
- [ ] וידוא שה-feature flag `GBP_ENABLED` עובד — כבוי = אין רישום, דלוק = ערוץ פעיל

### 1.2 השלמת תמיכה בפוסטים מסוג STANDARD

- [ ] פרסום טקסט בלבד — `summary` field ב-API
- [ ] פרסום טקסט + תמונה בודדת — `media` field עם `sourceUrl` מ-Cloudinary
- [ ] תמיכה ב-Call To Action אופציונלי (`cta_type` + `cta_url`)
- [ ] וולידציה: וידאו לא נתמך ב-GBP API — חסימה ברמת `validate()` עם הודעה ברורה

> **הערה:** GBP API אינו תומך בהעלאת וידאו. שורות עם וידאו ו-network שכולל GBP יקבלו שגיאת וולידציה ברורה. וידאו ב-GBP יישאר ידני.

### 1.3 אינטגרציה עם הממשק הקיים (Web Panel)

- [ ] וידוא שנקודת הקצה `/api/gbp/locations` מחזירה locations תקינים
- [ ] וידוא שבחירת location מתעדכנת בשדה `google_location_id` בגיליון
- [ ] וידוא שה-checkbox של GBP בטופס יצירת פוסט פעיל ומוצג נכון
- [ ] וידוא שתצוגת תוצאות מציגה סטטוס GBP בנפרד (POSTED / PARTIAL / ERROR)
- [ ] וידוא שפילטר network כולל שילובי GBP

### 1.4 בדיקות

- [ ] בדיקות יחידה: `tests/test_google_business.py` — publish הצלחה וכישלון (mock)
- [ ] בדיקות יחידה: `tests/test_google_auth.py` — token refresh, expiry, thread safety
- [ ] בדיקות יחידה: `tests/test_google_locations.py` — cache, pagination, validation
- [ ] בדיקות E2E: `tests/test_e2e_scenarios.py` — תרחישי GBP בלבד + GBP משולב
- [ ] בדיקות API: `tests/test_gbp_locations_api.py` — endpoint locations
- [ ] רגרסיה מלאה: `pytest` — אין שבירה של IG/FB

### 1.5 בדיקת לקוח (Pilot)

- [ ] הכנת חשבון GBP לבדיקה עם OAuth credentials
- [ ] פרסום פוסט טקסט בלבד → וידוא הופעה בפרופיל העסק
- [ ] פרסום פוסט טקסט + תמונה → וידוא הופעה עם תמונה
- [ ] פרסום משולב IG+GBP → וידוא ששניהם מתפרסמים
- [ ] פרסום משולב IG+FB+GBP → וידוא שלושתם מתפרסמים
- [ ] סימולציית כשל GBP → וידוא סטטוס PARTIAL ו-retry עובד
- [ ] וידוא התראות טלגרם נשלחות על שגיאות

### 1.6 דיפלוי ו-Rollout

- [ ] דיפלוי עם `GBP_ENABLED=false` — וידוא שאין רגרסיה
- [ ] הפעלת `GBP_ENABLED=true` — בדיקה פנימית
- [ ] פתיחה ללקוחות — מעקב אחרי 10 הפוסטים הראשונים
- [ ] מעקב שוטף: OAuth refresh, API quotas, rate limits

> **Rollback:** הגדרת `GBP_ENABLED=false` → restart → IG/FB לא מושפעים

---

## שלב 2: הוספת LinkedIn — תכנון ותשתית

**משך משוער: ~1 ספרינט**

### 2.1 הגדרת LinkedIn Developer App

- [ ] יצירת אפליקציה ב-[LinkedIn Developer Portal](https://developer.linkedin.com/)
- [ ] הגדרת OAuth 2.0 redirect URI
- [ ] בקשת permission: `w_member_social` (open permission, לא דורש אישור מיוחד)
- [ ] קבלת Client ID ו-Client Secret
- [ ] בדיקת three-legged OAuth flow ידנית — קבלת refresh token

### 2.2 תכנון מודל הנתונים

- [ ] הגדרת קבועים חדשים ב-`config_constants.py`:
  - `NETWORK_LI = "LI"`
  - שילובי network: `IG+LI`, `FB+LI`, `IG+FB+LI`, `IG+FB+GBP+LI`, `ALL`
  - `COL_CAPTION_LI = "caption_li"`
  - `COL_LI_AUTHOR_URN = "li_author_urn"` — URN של פרופיל אישי או דף ארגוני
- [ ] עדכון `VALID_NETWORKS` עם כל השילובים
- [ ] עדכון `SHEET_COLUMNS` עם עמודות חדשות
- [ ] הוספת עמודות לגיליון Google Sheets

### 2.3 משתני סביבה

- [ ] הגדרת credentials ב-`config.py`:
  ```
  LI_OAUTH_CLIENT_ID
  LI_OAUTH_CLIENT_SECRET
  LI_REFRESH_TOKEN
  LI_ENABLED (feature flag, default: false)
  ```

---

## שלב 3: הוספת LinkedIn — פיתוח מודול

**משך משוער: ~2-3 ספרינטים**

### 3.1 OAuth Token Management — `channels/linkedin_auth.py`

- [ ] יצירת `LinkedInOAuthManager` (בדומה ל-`GoogleOAuthManager`)
- [ ] תמיכה ב-OAuth 2.0 three-legged flow
- [ ] Token refresh אוטומטי עם margin לפני expiry
- [ ] Thread-safe עם double-check locking
- [ ] Lazy singleton — לא קורס אם credentials חסרים
- [ ] מתודות: `get_access_token()`, `get_auth_headers()`, `force_refresh()`

### 3.2 LinkedIn Channel — `channels/linkedin.py`

- [ ] יצירת class `LinkedInChannel(BaseChannel)`:
  - `CHANNEL_ID = "LI"`
  - `CHANNEL_NAME = "LinkedIn"`
  - `SUPPORTED_POST_TYPES = ("FEED",)`
  - `SUPPORTED_MEDIA_TYPES = ("image", "video", "none")`
  - `CAPTION_COLUMN = "caption_li"`

- [ ] מימוש `validate(post_data)`:
  - וידוא שיש `li_author_urn` (חובה — מזהה פרופיל אישי או דף ארגוני)
  - וידוא פורמט URN: `urn:li:person:{id}` או `urn:li:organization:{id}`
  - וידוא שיש caption או מדיה (לא פוסט ריק)
  - בדיקת אורך caption (מקסימום 3,000 תווים ב-LinkedIn)

- [ ] מימוש `publish(post_data)`:
  - **API Endpoint:** `POST https://api.linkedin.com/rest/posts`
  - **Headers:** `Authorization: Bearer {token}`, `LinkedIn-Version: 202401`, `Content-Type: application/json`
  - **Body — טקסט בלבד:**
    ```json
    {
      "author": "urn:li:person:{id}",
      "lifecycleState": "PUBLISHED",
      "visibility": "PUBLIC",
      "commentary": "תוכן הפוסט",
      "distribution": {
        "feedDistribution": "MAIN_FEED"
      }
    }
    ```
  - **Body — טקסט + תמונה:**
    - שלב 1: Initialize upload via `POST /rest/images?action=initializeUpload`
    - שלב 2: Upload binary to the provided `uploadUrl`
    - שלב 3: Create post with `content.media.id` = returned image URN
  - **Body — טקסט + וידאו:**
    - שלב 1: Initialize upload via `POST /rest/videos?action=initializeUpload`
    - שלב 2: Upload binary (chunked if large)
    - שלב 3: Create post with `content.media.id` = returned video URN
  - החזרת `PublishResult` עם `platform_post_id`

- [ ] טיפול בשגיאות:
  - `401 Unauthorized` → `auth_failure` (non-retryable)
  - `429 Too Many Requests` → `rate_limit` (retryable)
  - `5xx` → `http_5xx` (retryable)
  - `422 Unprocessable Entity` → `validation_error` (non-retryable)

### 3.3 רישום ב-Registry

- [ ] עדכון `channels/__init__.py`:
  ```python
  from channels.linkedin import LinkedInChannel
  
  LI_ENABLED = os.environ.get("LI_ENABLED", "false").lower() in ("true", "1", "yes")
  
  # ב-create_default_registry():
  if LI_ENABLED:
      registry.register(LinkedInChannel())
  ```

### 3.4 וולידציה — `validator.py`

- [ ] הוספת קודי שגיאה:
  - `LI_MISSING_AUTHOR_URN` — חסר URN של מחבר
  - `LI_INVALID_AUTHOR_URN` — פורמט URN לא תקין
  - `LI_CAPTION_TOO_LONG` — caption מעל 3,000 תווים
- [ ] הוספת `_validate_li()` method
- [ ] עדכון dispatch ב-`_validate_channel()`
- [ ] עדכון `_NETWORK_TO_CHANNELS` עם שילובי LI

### 3.5 מגבלות מדיה — `media_processor.py`

- [ ] הוספת קבועים:
  ```python
  LI_IMAGE_MAX_SIZE = 10_485_760       # 10 MB
  LI_VIDEO_MAX_SIZE = 209_715_200      # 200 MB
  LI_VIDEO_MAX_DURATION = 600          # 10 דקות
  LI_CAPTION_MAX_LENGTH = 3000
  ```
- [ ] הוספת `_targets_li(network)` helper
- [ ] עדכון `validate_media_pre_publish()` עם בדיקות LinkedIn

### 3.6 אינטגרציה עם Web Panel

- [ ] הוספת checkbox ל-LinkedIn בטופס יצירת/עריכת פוסט
- [ ] הוספת שדה caption_li עם מונה תווים (מקסימום 3,000)
- [ ] הוספת שדה `li_author_urn` — dropdown או text input
  - אפשרות לבחירה בין פרופיל אישי לדף ארגוני
- [ ] עדכון תצוגת תוצאות עם סטטוס LinkedIn
- [ ] עדכון פילטרים עם שילובי LI
- [ ] עדכון badges של networks בטבלה

---

## שלב 4: הוספת LinkedIn — בדיקות

**משך משוער: ~1 ספרינט**

### 4.1 בדיקות יחידה

- [ ] `tests/test_linkedin.py`:
  - CHANNEL_ID, CHANNEL_NAME, SUPPORTED_POST_TYPES
  - `validate()` — URN תקין, URN חסר, URN לא תקין, caption ארוך
  - `publish()` — mock: טקסט בלבד, טקסט+תמונה, טקסט+וידאו
  - `publish()` — mock: שגיאות API (401, 429, 500)
  - Fallback caption (caption כללי כשאין caption_li)

- [ ] `tests/test_linkedin_auth.py`:
  - Token refresh flow
  - Token expiry detection
  - Thread safety
  - Missing credentials (graceful failure)

- [ ] עדכון `tests/test_validator.py`:
  - תרחישי LinkedIn: URN validation, network combinations
  - PARTIAL scenarios עם LI

- [ ] עדכון `tests/test_media_processor.py`:
  - `_targets_li()` עם network values שונים
  - מגבלות גודל תמונה/וידאו

### 4.2 בדיקות אינטגרציה

- [ ] עדכון `tests/test_e2e_scenarios.py`:
  - פוסט LI בלבד — הצלחה
  - פוסט IG+LI — הצלחה
  - פוסט IG+FB+LI — הצלחה
  - פוסט ALL (IG+FB+GBP+LI) — הצלחה
  - כשל LI בלבד → PARTIAL
  - כשל LI + FB → PARTIAL (רק IG הצליח)

### 4.3 רגרסיה

- [ ] `pytest` מלא — אין שבירה של IG/FB/GBP
- [ ] בדיקת עשן ידנית: IG בלבד, FB בלבד, IG+FB, GBP בלבד
- [ ] וידוא שערוץ LI לא משפיע כשהפלאג כבוי

---

## שלב 5: LinkedIn — בדיקת לקוח ודיפלוי

**משך משוער: ~1 ספרינט**

### 5.1 בדיקת Pilot

- [ ] הכנת חשבון LinkedIn עם Developer App ו-OAuth credentials
- [ ] פרסום לפרופיל אישי — טקסט בלבד → וידוא הופעה בפיד
- [ ] פרסום לפרופיל אישי — טקסט + תמונה → וידוא הופעה עם תמונה
- [ ] פרסום לפרופיל אישי — טקסט + וידאו → וידוא הופעה עם וידאו
- [ ] פרסום לדף ארגוני — טקסט בלבד → וידוא (שינוי `li_author_urn` ל-organization URN)
- [ ] פרסום לדף ארגוני — טקסט + תמונה → וידוא
- [ ] פרסום משולב IG+LI → וידוא ששניהם מתפרסמים
- [ ] פרסום משולב IG+FB+GBP+LI → וידוא שכולם מתפרסמים
- [ ] סימולציית כשל LI → וידוא PARTIAL ו-retry

### 5.2 דיפלוי

- [ ] דיפלוי עם `LI_ENABLED=false` — וידוא אין רגרסיה
- [ ] הפעלת `LI_ENABLED=true` — בדיקה פנימית
- [ ] פתיחה ללקוחות — מעקב אחרי 10 הפוסטים הראשונים
- [ ] מעקב שוטף: OAuth refresh, API quotas, rate limits

### 5.3 תוכנית Rollback

- **Rollback מהיר:** `LI_ENABLED=false` → restart → LinkedIn מוסר, שאר הערוצים לא מושפעים
- **Rollback מלא:** חזרה לדיפלוי קודם
- **אינדיקטורים ל-rollback:**
  - LinkedIn API מחזיר 4xx מתמשכים
  - OAuth refresh נכשל שוב ושוב
  - רגרסיה בערוצים קיימים (IG/FB/GBP)

---

## סיכום קבצים שיש לגעת בהם

| # | קובץ | שלב GBP | שלב LinkedIn | סוג שינוי |
|---|---|:---:|:---:|---|
| 1 | `config_constants.py` | בדיקה | שינוי | קבועים: network, עמודות, post types |
| 2 | `config.py` | בדיקה | שינוי | credentials ממשתני סביבה |
| 3 | `channels/google_business.py` | השלמה | — | השלמת מימוש קיים |
| 4 | `channels/google_auth.py` | בדיקה | — | וידוא OAuth flow |
| 5 | `channels/google_locations.py` | בדיקה | — | וידוא locations service |
| 6 | `channels/linkedin_auth.py` | — | **חדש** | OAuth token management |
| 7 | `channels/linkedin.py` | — | **חדש** | LinkedIn channel class |
| 8 | `channels/__init__.py` | בדיקה | שינוי | רישום + feature flag |
| 9 | `validator.py` | בדיקה | שינוי | וולידציה ספציפית לערוץ |
| 10 | `media_processor.py` | בדיקה | שינוי | מגבלות מדיה + network helper |
| 11 | `web_app.py` | בדיקה | שינוי | API endpoints + ממשק |
| 12 | `templates/` | בדיקה | שינוי | checkbox, caption, שדות |
| 13 | `tests/` | השלמה | **חדש** + עדכון | בדיקות יחידה + אינטגרציה |

---

## תלויות חיצוניות

| פלטפורמה | תלות | סטטוס |
|---|---|---|
| Google Business Profile | Google Cloud Project עם GBP API enabled | נדרש וידוא |
| Google Business Profile | OAuth consent screen מאושר | נדרש וידוא |
| Google Business Profile | Refresh token תקף | נדרש וידוא |
| LinkedIn | LinkedIn Developer App | **צריך ליצור** |
| LinkedIn | OAuth redirect URI מוגדר | **צריך להגדיר** |
| LinkedIn | `w_member_social` permission | אוטומטי (open permission) |

---

## הערות חשובות

1. **תאימות תנאי שימוש:** המודל העסקי תואם — כל לקוח מחבר את החשבונות שלו עצמו
2. **אין שינוי ב-`main.py`:** הארכיטקטורה המודולרית של ה-Registry מבטיחה שלוגיקת הפרסום (לולאה, retry, עדכון סטטוס) עובדת אוטומטית עם כל ערוץ רשום
3. **Feature flags:** כל ערוץ חדש נכנס עם flag כבוי — דיפלוי בטוח, rollback מיידי
4. **GBP — אין וידאו:** ה-API לא תומך, וידאו נשאר ידני
5. **LinkedIn — `w_member_social`:** permission פתוח שלא דורש אישור מיוחד מ-LinkedIn
6. **LinkedIn — author URN:** תומך בפרופיל אישי (`urn:li:person:{id}`) ובדף ארגוני (`urn:li:organization:{id}`)
