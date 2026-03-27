הנה ההשוואה:

## מה Google Sheets נותן לנו היום (ולמה זה עובד)

- **UI חינמי** — הלקוחה יכולה לראות ולערוך ישירות
- **אפס תחזוקה** — אין DB להקים, לגבות, לנטר
- **Audit trail מובנה** — היסטוריית גרסאות של גוגל
- **עובד** — עם כמות פוסטים סבירה (עשרות-מאות), אין בעיה אמיתית

## מתי Sheets מתחיל לכאוב

| בעיה | הסבר |
|---|---|
| **ביצועים** | `sheets_read_all_rows()` קורא את כל הטבלה בכל ריצה. עם 1,000+ שורות זה נהיה איטי (2-5 שניות per request) |
| **Rate limits** | Google Sheets API מגביל ל-60 בקשות/דקה per user. עם polling מהפאנל + cron job + cleanup — אפשר להגיע לזה |
| **Race conditions** | הנעילה (`IN_PROGRESS`) היא best-effort. אין transactions אמיתיים — שני cron jobs שרצים במקביל עלולים לתפוס את אותה שורה |
| **Query** | אי אפשר לעשות `WHERE status='READY' AND publish_at < NOW()` — צריך לטעון הכל ולסנן בזיכרון |
| **Carousel URLs** | כבר ראינו — פסיקים בתוך תאים זה hack. ב-DB היית עושה טבלת `post_media` עם שורה לכל קובץ |

## מה DB נותן

- **Queries** — `SELECT * FROM posts WHERE status='READY' AND publish_at <= NOW()` במקום לסנן 1,000 שורות בפייתון
- **Transactions** — `SELECT ... FOR UPDATE` פותר race conditions לגמרי
- **Relations** — טבלת `post_media` לקרוסלות, `post_results` לתוצאות per-network
- **ביצועים** — SQLite מחזיר תוצאות תוך מילישניות, גם עם 100K שורות
- **אין rate limits** — DB מקומי, אין API quotas

## ההמלצה שלי

**עכשיו — נישאר עם Sheets.** כל עוד יש עד כמה מאות פוסטים, זה עובד מצוין והפשטות שווה הרבה.

**נשקול מעבר כש:**
- יש 500+ שורות בגיליון והפאנל מתחיל להיות איטי
- כשנראה rate limit errors מ-Google Sheets API
- אם נצטרך חיפוש/סינון מתקדם בצד השרת

כשנצטרך — SQLite (עם Render Disk) הוא הצעד הכי פשוט. לא צריך שרת חיצוני, פשוט קובץ על הדיסק. PostgreSQL הוא לשלב אחרי זה (multi-instance, backup אוטומטי).
