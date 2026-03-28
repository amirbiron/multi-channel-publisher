# צ'קליסט — הוספת ערוץ פרסום חדש

מסמך זה מפרט את כל השינויים הנדרשים בקוד כדי להוסיף ערוץ פרסום חדש למערכת.
הדוגמאות משתמשות בערוץ דמיוני בשם **TikTok** עם מזהה `"TIK"`.

---

## סדר ביצוע מומלץ

1. קבועים ב-`config_constants.py`
2. קלאס הערוץ ב-`channels/`
3. רישום ב-`channels/__init__.py`
4. וולידציה ב-`validator.py`
5. מגבלות מדיה ב-`media_processor.py`
6. ממשק ב-`web_app.py` + תבניות
7. Google Sheet — עמודות חדשות
8. משתני סביבה ב-Render
9. טסטים

---

## 1. `config_constants.py` — קבועים

### ערך Network חדש
```python
NETWORK_TIK = "TIK"
```

### שילובים (אם צריך)
```python
NETWORK_IG_TIK = "IG+TIK"
NETWORK_FB_TIK = "FB+TIK"
# ... לפי הצורך
```

### עדכון `VALID_NETWORKS`
```python
VALID_NETWORKS = {
    NETWORK_IG, NETWORK_FB, NETWORK_GBP, NETWORK_TIK,
    NETWORK_BOTH, NETWORK_IG_GBP, NETWORK_FB_GBP,
    NETWORK_ALL_THREE, NETWORK_ALL,
    NETWORK_IG_TIK, NETWORK_FB_TIK,  # שילובים חדשים
    # ...
}
```

### עמודות Sheet חדשות (אם הערוץ צריך שדות ייחודיים)
```python
COL_CAPTION_TIK = "caption_tik"
COL_TIK_ACCOUNT_ID = "tik_account_id"  # דוגמה
```

### עדכון `SHEET_COLUMNS`
להוסיף את העמודות החדשות ברשימה, **אחרי** העמודות הקיימות של GBP ולפני `drive_file_id`.

---

## 2. `channels/tiktok.py` — קלאס הערוץ

ליצור קובץ חדש שיורש מ-`BaseChannel`:

```python
from channels.base import BaseChannel, PublishResult

class TikTokChannel(BaseChannel):
    CHANNEL_ID = "TIK"
    CHANNEL_NAME = "TikTok"
    SUPPORTED_POST_TYPES = ("FEED", "REELS")
    SUPPORTED_MEDIA_TYPES = ("video",)
    CAPTION_COLUMN = "caption_tik"

    def validate(self, post_data: dict) -> list[str]:
        """בדיקות ספציפיות לערוץ — מחזיר רשימת שגיאות (ריק = תקין)."""
        errors = []
        if not self.get_caption(post_data):
            errors.append("חסר קפשן ל-TikTok")
        if not post_data.get("cloud_urls"):
            errors.append("חסר קובץ מדיה")
        return errors

    def publish(self, post_data: dict) -> PublishResult:
        """פרסום בפועל — תמיד מחזיר PublishResult, לא זורק exception."""
        try:
            # ... קריאה ל-API של TikTok ...
            return self._make_result(
                success=True,
                platform_post_id="tiktok_post_123",
            )
        except Exception as exc:
            return self._make_result(
                success=False,
                error_code="api_error",
                error_message=str(exc),
            )
```

### כללים חשובים:
- **לעולם לא לזרוק exception** עבור שגיאות API צפויות — להחזיר `PublishResult` עם `success=False`
- להשתמש ב-`self.get_caption(post_data)` שבודק קודם את עמודת הקפשן הספציפית ואז fallback לקפשן הכללי
- הערוץ מקבל `cloud_urls` (רשימת URLs מ-Cloudinary) — **לא** בייטים גולמיים

---

## 3. `channels/__init__.py` — רישום

### הוספת import
```python
from channels.tiktok import TikTokChannel
```

### הוספת feature flag (אופציונלי אבל מומלץ)
```python
TIK_ENABLED = bool(os.environ.get("TIK_ACCESS_TOKEN"))
```

### הוספה ל-`create_default_registry()`
```python
def create_default_registry() -> ChannelRegistry:
    registry = ChannelRegistry()
    registry.register(InstagramChannel())
    registry.register(FacebookChannel())
    if GBP_ENABLED:
        registry.register(GoogleBusinessChannel())
    if TIK_ENABLED:                         # ← חדש
        registry.register(TikTokChannel())
    return registry
```

---

## 4. `validator.py` — וולידציה

### הוספת קודי שגיאה ב-`ErrorCode`
```python
TIK_CAPTION_MISSING = "TIK_CAPTION_MISSING"
TIK_VIDEO_ONLY = "TIK_VIDEO_ONLY"
```

### הוספת מיפוי network → ערוצים
לעדכן את `_NETWORK_TO_CHANNELS` עם כל השילובים החדשים.

### הוספת מתודת וולידציה
```python
def _validate_tik(self, normalized: dict) -> ChannelValidationResult:
    issues = []
    # ... בדיקות ספציפיות
    blocked = any(i.severity == "CHANNEL_BLOCK" for i in issues)
    return ChannelValidationResult(channel="TIK", approved=not blocked, issues=issues)
```

### עדכון dispatch ב-`_validate_channel()`
```python
dispatch = {
    "IG": self._validate_ig,
    "FB": self._validate_fb,
    "GBP": self._validate_gbp,
    "TIK": self._validate_tik,  # ← חדש
}
```

---

## 5. `media_processor.py` — מגבלות מדיה (וולידציה לפני פרסום)

### 5א. קבועים — מגבלות הפלטפורמה
להוסיף בקטע `Platform-specific limits`:
```python
TIK_VIDEO_MAX_SIZE = 287_309_824    # 274 MB (דוגמה)
TIK_VIDEO_MAX_DURATION = 600        # 10 דקות
TIK_VIDEO_MIN_DURATION = 3          # 3 שניות
```

### 5ב. Network helper חדש
```python
def _targets_tik(network: str) -> bool:
    """Does this network value include TikTok?"""
    if not network:
        return False
    return network == NETWORK_ALL or NETWORK_TIK in network
```

**שים לב:** ערוצים חדשים (שאינם IG/FB) צריכים להחזיר `False` כברירת מחדל כשה-network ריק.

### 5ג. Import
להוסיף `NETWORK_TIK` ל-import מ-config:
```python
from config import (
    # ... existing
    NETWORK_TIK,
)
```

### 5ד. עדכון `validate_media_pre_publish()`
להוסיף:
```python
publishes_to_tik = _targets_tik(network)
```
ולהעביר את המשתנה ל-`_validate_image_pre_publish()` ו-`_validate_video_pre_publish()`.

### 5ה. עדכון `_validate_video_pre_publish()`
להוסיף בדיקות ספציפיות לערוץ החדש:
```python
# בדיקות TikTok
if publishes_to_tik:
    if file_size > TIK_VIDEO_MAX_SIZE:
        size_mb = file_size / (1024 * 1024)
        return f"סרטון גדול מדי ל-TikTok — {size_mb:.0f}MB (מקסימום 274MB)"
    if duration is not None:
        if duration < TIK_VIDEO_MIN_DURATION:
            return f"סרטון קצר מדי ל-TikTok — {duration:.1f} שניות (מינימום 3 שניות)"
        if duration > TIK_VIDEO_MAX_DURATION:
            return f"סרטון ארוך מדי ל-TikTok — {duration/60:.1f} דקות (מקסימום 10 דקות)"
```

### 5ו. עדכון `_validate_image_pre_publish()` (אם רלוונטי)
אם הערוץ תומך בתמונות, להוסיף בדיקות בהתאם.

### 5ז. עדכון `_normalize_image()` (אם צריך לדלג על בדיקת יחס)
אם הערוץ החדש **אינו** דורש יחס גובה-רוחב ספציפי, לעדכן את תנאי `publishes_to_ig`.

---

## 6. ממשק — `web_app.py` + תבניות HTML

### טופס יצירת/עריכת פוסט
- להוסיף checkbox לערוץ החדש (כמו IG/FB/GBP)
- להוסיף שדה קפשן ספציפי (עם מונה תווים ומגבלה מתאימה)
- אם יש שדות ייחודיים (כמו location ב-GBP) — להוסיף section נפרד שמוצג בתנאי

### טבלת פוסטים
- להוסיף עמודה לקפשן הערוץ (אם רוצים להציג)
- לעדכן את תצוגת הרשתות (badges)
- לעדכן תצוגת תוצאות (status per channel)

### פילטרים
- להוסיף את הערוץ החדש לפילטר network

---

## 7. Google Sheet — עמודות חדשות

להוסיף לשורת ה-header בגיליון:
- `caption_tik` (או שם העמודה שבחרת)
- עמודות נוספות לפי הצורך

**מיקום:** אחרי עמודות GBP, לפני `drive_file_id` — לפי הסדר ב-`SHEET_COLUMNS`.

---

## 8. משתני סביבה (Render / `.env`)

```
TIK_ACCESS_TOKEN=xxx
TIK_ACCOUNT_ID=xxx
```

להוסיף ל-`config.py` כ-credentials:
```python
TIK_ACCESS_TOKEN = os.environ.get("TIK_ACCESS_TOKEN", "")
TIK_ACCOUNT_ID = os.environ.get("TIK_ACCOUNT_ID", "")
```

---

## 9. טסטים

### `tests/test_tiktok.py`
- בדיקת `CHANNEL_ID`, `CHANNEL_NAME`, `SUPPORTED_POST_TYPES`
- בדיקת `validate()` — מקרים תקינים ולא תקינים
- בדיקת `publish()` — mock ל-API עם הצלחה וכישלון

### עדכון `tests/test_media_processor.py`
- בדיקת `_targets_tik()` עם ערכי network שונים
- בדיקת `validate_media_pre_publish()` עם network שכולל את הערוץ החדש

### עדכון `tests/test_unit_core.py`
- בדיקת ValidationReport עם הערוץ החדש

---

## סיכום — רשימת קבצים שצריך לגעת בהם

| # | קובץ | סוג שינוי |
|---|---|---|
| 1 | `config_constants.py` | קבועים: network, עמודות, post types |
| 2 | `channels/tiktok.py` | **קובץ חדש** — קלאס הערוץ |
| 3 | `channels/__init__.py` | רישום + feature flag |
| 4 | `validator.py` | וולידציה ספציפית לערוץ |
| 5 | `media_processor.py` | מגבלות מדיה + network helper |
| 6 | `config.py` | credentials ממשתני סביבה |
| 7 | `web_app.py` | API endpoints (אם צריך) |
| 8 | `templates/` | ממשק: checkbox, קפשן, שדות |
| 9 | `tests/` | בדיקות יחידה |

> **הערה חשובה:** הארכיטקטורה של ה-Registry מבטיחה שלרוב **אין צורך לשנות את `main.py`** —
> הלוגיקה של הפרסום (לולאה על ערוצים, retry, עדכון סטטוס) היא גנרית ועובדת אוטומטית
> עם כל ערוץ רשום. החריג היחיד הוא אם הערוץ דורש טיפול מיוחד (כמו `google_location_id` ב-GBP).
