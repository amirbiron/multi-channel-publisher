# Social Media Publishing API Guide

> **Extracted from:** [multi-channel-publisher](https://github.com/amirbiron/multi-channel-publisher)
> A battle-tested, production-grade publishing layer for Instagram, Facebook, and LinkedIn.
> Use this guide to integrate social publishing into any new project (e.g., a Voice-Bot).

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Authentication](#authentication)
   - [Meta (Instagram + Facebook)](#meta-instagram--facebook)
   - [LinkedIn](#linkedin)
3. [Publishing: Instagram](#publishing-instagram)
   - [Single Post (Image/Video)](#instagram-single-post)
   - [Carousel (2-10 items)](#instagram-carousel)
4. [Publishing: Facebook](#publishing-facebook)
   - [Photo Post](#facebook-photo)
   - [Video Post](#facebook-video)
   - [Reels](#facebook-reels)
   - [Multi-Photo Post](#facebook-multi-photo)
5. [Publishing: LinkedIn](#publishing-linkedin)
   - [Text-Only Post](#linkedin-text-only)
   - [Text + Image](#linkedin-text--image)
   - [Text + Video](#linkedin-text--video)
6. [Media Requirements](#media-requirements)
7. [Error Handling](#error-handling)
8. [Environment Variables](#environment-variables)
9. [Quick-Start Code Snippets](#quick-start-code-snippets)

---

## Architecture Overview

```
User Input (text/voice/image)
       │
       ▼
┌─────────────────┐
│  Media Upload    │  ← Upload to Cloudinary (or any CDN)
│  (cloud_storage) │    Returns public HTTPS URL
└───────┬─────────┘
        │
        ▼
┌─────────────────┐
│  Media Process   │  ← Normalize: JPEG compression, MP4 H.264 transcode
│  (media_proc)    │    Validate: aspect ratio, duration, file size
└───────┬─────────┘
        │
        ▼
┌─────────────────────────────────────────────┐
│           Publishing Layer                    │
│                                               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │ Instagram │  │ Facebook │  │ LinkedIn │  │
│  │ Graph API │  │ Graph API│  │ REST API │  │
│  └──────────┘  └──────────┘  └──────────┘  │
└─────────────────────────────────────────────┘
```

---

## Authentication

### Meta (Instagram + Facebook)

**Auth Method:** Long-Lived Access Tokens (Bearer tokens)

- Each client provides their own tokens via the app interface
- Tokens are passed as `access_token` parameter in every API call
- No OAuth refresh flow needed — tokens are long-lived

**Required Tokens:**
```
IG_ACCESS_TOKEN       — Instagram Graph API token
FB_PAGE_ACCESS_TOKEN  — Facebook Page token
```

**Required IDs:**
```
IG_USER_ID   — Instagram Business Account ID
FB_PAGE_ID   — Facebook Page ID
```

**API Base URL:**
```
https://graph.facebook.com/v21.0
```

### LinkedIn

**Auth Method:** OAuth 2.0 Three-Legged Flow

LinkedIn uses refresh tokens that are exchanged for short-lived access tokens.

**Token Endpoint:**
```
POST https://www.linkedin.com/oauth/v2/accessToken
```

**Refresh Request:**
```python
data = {
    "grant_type": "refresh_token",
    "refresh_token": LI_REFRESH_TOKEN,
    "client_id": LI_OAUTH_CLIENT_ID,
    "client_secret": LI_OAUTH_CLIENT_SECRET,
}
resp = requests.post("https://www.linkedin.com/oauth/v2/accessToken", data=data)
access_token = resp.json()["access_token"]
expires_in = resp.json().get("expires_in", 3600)  # seconds
```

**Required Headers for all LinkedIn API calls:**
```python
headers = {
    "Authorization": f"Bearer {access_token}",
    "LinkedIn-Version": "202401",
    "Content-Type": "application/json",
    "X-Restli-Protocol-Version": "2.0.0",
}
```

**Required Credentials:**
```
LI_OAUTH_CLIENT_ID     — LinkedIn App client ID
LI_OAUTH_CLIENT_SECRET — LinkedIn App client secret
LI_REFRESH_TOKEN       — Long-lived refresh token
```

**Permission:** `w_member_social`

**Important:** Implement token caching with a refresh margin (e.g., 5 minutes before expiry) and thread-safe locking for concurrent access.

---

## Publishing: Instagram

### Instagram Single Post

Instagram publishing is a **2-step process** (create container → publish):

#### Step 1: Create Media Container

```
POST https://graph.facebook.com/v21.0/{IG_USER_ID}/media
```

**For image:**
```python
data = {
    "image_url": cloud_url,       # Public HTTPS URL
    "caption": caption,
    "access_token": IG_ACCESS_TOKEN,
}
```

**For video (always published as REELS):**
```python
data = {
    "video_url": cloud_url,       # Public HTTPS URL
    "caption": caption,
    "media_type": "REELS",
    "access_token": IG_ACCESS_TOKEN,
}
```

**Response:** `{"id": "container_id"}`

#### Step 1.5: Wait for Container Processing

Both images and videos require processing time. Poll the container status:

```
GET https://graph.facebook.com/v21.0/{container_id}
    ?fields=status_code
    &access_token={IG_ACCESS_TOKEN}
```

**Possible statuses:**
- `FINISHED` — ready to publish
- `IN_PROGRESS` — still processing
- `ERROR` — failed (check `status` field for details)

**Recommended polling:** every 5 seconds, timeout after 300 seconds.

#### Step 2: Publish Container

```
POST https://graph.facebook.com/v21.0/{IG_USER_ID}/media_publish
```

```python
data = {
    "creation_id": container_id,
    "access_token": IG_ACCESS_TOKEN,
}
```

**Response:** `{"id": "published_media_id"}`

### Instagram Carousel

Carousel posts support 2-10 items (images and/or videos).

#### Steps:

1. **Create child containers** (no caption, with `is_carousel_item=true`):

```python
# For each item:
data = {
    "is_carousel_item": "true",
    "access_token": IG_ACCESS_TOKEN,
}
if is_video:
    data["video_url"] = cloud_url
    data["media_type"] = "VIDEO"
else:
    data["image_url"] = cloud_url

resp = requests.post(f"{BASE}/{IG_USER_ID}/media", data=data)
child_id = resp.json()["id"]
```

2. **Wait for ALL child containers** to reach `FINISHED` status

3. **Create carousel container:**
```python
data = {
    "media_type": "CAROUSEL",
    "caption": caption,
    "children": ",".join(child_ids),  # comma-separated child IDs
    "access_token": IG_ACCESS_TOKEN,
}
resp = requests.post(f"{BASE}/{IG_USER_ID}/media", data=data)
carousel_id = resp.json()["id"]
```

4. **Wait for carousel container** → **Publish**

---

## Publishing: Facebook

### Facebook Photo

```
POST https://graph.facebook.com/v21.0/{FB_PAGE_ID}/photos
```

```python
data = {
    "url": cloud_url,              # Public HTTPS URL
    "caption": caption,
    "access_token": FB_PAGE_ACCESS_TOKEN,
}
```

**Response:** `{"post_id": "...", "id": "..."}`

### Facebook Video

```
POST https://graph.facebook.com/v21.0/{FB_PAGE_ID}/videos
```

```python
data = {
    "file_url": cloud_url,
    "description": caption,        # Note: "description", not "caption"
    "access_token": FB_PAGE_ACCESS_TOKEN,
    "published": "true",
}
```

**Response:** `{"id": "video_id"}`

### Facebook Reels

Facebook Reels require a **3-step upload process:**

#### Step 1: Start Upload

```
POST https://graph.facebook.com/v21.0/{FB_PAGE_ID}/video_reels
```

```python
data = {
    "upload_phase": "start",
    "access_token": FB_PAGE_ACCESS_TOKEN,
}
```

**Response:** `{"video_id": "...", "upload_url": "..."}`

#### Step 2: Transfer Video

```python
headers = {
    "Authorization": f"OAuth {FB_PAGE_ACCESS_TOKEN}",
    "file_url": cloud_url,         # CDN URL in header
}
requests.post(upload_url, headers=headers)
```

#### Step 3: Finish & Publish

```
POST https://graph.facebook.com/v21.0/{FB_PAGE_ID}/video_reels
```

```python
data = {
    "upload_phase": "finish",
    "video_id": video_id,
    "video_state": "PUBLISHED",
    "description": caption,
    "access_token": FB_PAGE_ACCESS_TOKEN,
}
```

### Facebook Multi-Photo Post

1. **Upload each photo as unpublished:**

```python
data = {
    "url": cloud_url,
    "published": "false",
    "access_token": FB_PAGE_ACCESS_TOKEN,
}
resp = requests.post(f"{BASE}/{FB_PAGE_ID}/photos", data=data)
media_id = resp.json()["id"]
```

2. **Create feed post with all media attached:**

```python
data = {
    "message": caption,
    "access_token": FB_PAGE_ACCESS_TOKEN,
    "attached_media[0]": '{"media_fbid":"ID_1"}',
    "attached_media[1]": '{"media_fbid":"ID_2"}',
    # ... up to 10
}
requests.post(f"{BASE}/{FB_PAGE_ID}/feed", data=data)
```

---

## Publishing: LinkedIn

**API Base:** `https://api.linkedin.com/rest`

### LinkedIn Text-Only

```
POST https://api.linkedin.com/rest/posts
```

```python
body = {
    "author": "urn:li:person:{id}",       # or urn:li:organization:{id}
    "lifecycleState": "PUBLISHED",
    "visibility": "PUBLIC",
    "commentary": "Post text here",
    "distribution": {
        "feedDistribution": "MAIN_FEED",
    },
}
resp = requests.post(url, json=body, headers=auth_headers)
post_id = resp.headers.get("x-restli-id")
```

**Note:** `commentary` is optional — omit (don't send empty string) for media-only posts.

### LinkedIn Text + Image

#### Step 1: Initialize Image Upload

```
POST https://api.linkedin.com/rest/images?action=initializeUpload
```

```python
body = {
    "initializeUploadRequest": {
        "owner": "urn:li:person:{id}",
    }
}
resp = requests.post(url, json=body, headers=auth_headers)
upload_url = resp.json()["value"]["uploadUrl"]
image_urn = resp.json()["value"]["image"]
```

#### Step 2: Upload Image Binary

```python
# Download from cloud storage
img_data = requests.get(cloud_url)

# Upload to LinkedIn
requests.put(
    upload_url,
    data=img_data.content,
    headers={"Authorization": f"Bearer {access_token}"},
)
```

#### Step 3: Create Post with Image

```python
body = {
    "author": author_urn,
    "lifecycleState": "PUBLISHED",
    "visibility": "PUBLIC",
    "commentary": caption,
    "distribution": {"feedDistribution": "MAIN_FEED"},
    "content": {
        "media": {
            "id": image_urn,        # URN from step 1
        },
    },
}
```

### LinkedIn Text + Video

#### Step 1: Get File Size (HEAD request)

```python
head = requests.head(video_url, allow_redirects=True)
file_size = int(head.headers.get("Content-Length", 0))
```

#### Step 2: Initialize Video Upload

```
POST https://api.linkedin.com/rest/videos?action=initializeUpload
```

```python
body = {
    "initializeUploadRequest": {
        "owner": author_urn,
        "fileSizeBytes": file_size,
    }
}
resp = requests.post(url, json=body, headers=auth_headers)
video_urn = resp.json()["value"]["video"]
upload_instructions = resp.json()["value"]["uploadInstructions"]
```

#### Step 3: Upload Video (Chunked)

```python
video_data = requests.get(video_url)

for instruction in upload_instructions:
    chunk = video_data.content[instruction["firstByte"] : instruction["lastByte"] + 1]
    requests.put(
        instruction["uploadUrl"],
        data=chunk,
        headers={"Authorization": f"Bearer {access_token}"},
    )
```

#### Step 4: Create Post with Video

Same as image post, but use the video URN in `content.media.id`.

---

## Media Requirements

### Images

| Platform  | Max Size | Aspect Ratio        | Min Dimensions | Format     |
|-----------|----------|---------------------|----------------|------------|
| Instagram | 8 MB     | 0.8 (4:5) — 1.91:1 | 320px wide     | JPEG       |
| Facebook  | 8 MB     | No strict limit     | —              | JPEG/PNG   |
| LinkedIn  | —        | —                   | —              | image/*    |

**Recommended processing pipeline:**
1. Fix EXIF orientation
2. Convert to RGB (flatten transparency with white background)
3. Resize to 1080px width (maintain aspect ratio)
4. Compress as progressive JPEG (quality steps: 85 → 80 → 75 → 70 → 68)

### Videos

| Platform  | Max Size | Duration          | Format           | Aspect Ratio      |
|-----------|----------|-------------------|------------------|--------------------|
| Instagram | 300 MB   | 3s — 15 min       | MP4 H.264+AAC   | 0.8 — 1.91 (Feed) |
| Facebook  | 2 GB     | No strict limit   | MP4 H.264+AAC   | Flexible           |
| LinkedIn  | 200 MB   | —                 | video/*          | Flexible           |

**Recommended processing pipeline:**
1. Probe with `ffprobe` to check codec compliance
2. If already H.264+AAC → fast remux with `movflags +faststart`
3. Otherwise → full transcode: `ffmpeg -c:v libx264 -pix_fmt yuv420p -c:a aac -movflags +faststart`

### Supported MIME Types

```python
VIDEO_MIMES = {"video/mp4", "video/quicktime", "video/x-msvideo", "video/mpeg", "video/webm"}
IMAGE_MIMES = {"image/jpeg", "image/png", "image/gif", "image/webp", "image/bmp"}
```

---

## Error Handling

### Meta (Instagram + Facebook)

| HTTP Status | Meaning           | Retryable? |
|-------------|-------------------|------------|
| 400         | Bad request       | No         |
| 401/403     | Auth failure      | No         |
| 429         | Rate limit        | Yes        |
| 5xx         | Server error      | Yes        |
| Timeout     | Network timeout   | Yes        |

### LinkedIn

| HTTP Status | Error Code         | Retryable? |
|-------------|-------------------|------------|
| 401         | `auth_failure`     | No         |
| 422         | `validation_error` | No         |
| 429         | `rate_limit`       | Yes        |
| 5xx         | `http_{status}`    | Yes        |
| Timeout     | `timeout`          | Yes        |

**Important:** Check HTTP status codes **before** string-matching error messages.
A 504 Gateway Timeout contains "timeout" in its message but should be classified as `http_504`.

### Retry Strategy

```python
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds, with exponential backoff
RETRYABLE_CODES = {"timeout", "rate_limit", "api_error", "http_500", "http_502",
                   "http_503", "http_504", "http_429"}
```

---

## Environment Variables

### Required — Meta

```bash
META_API_VERSION=v21.0          # Facebook Graph API version
IG_USER_ID=123456               # Instagram Business Account ID
IG_ACCESS_TOKEN=EAA...          # Instagram long-lived token
FB_PAGE_ID=654321               # Facebook Page ID
FB_PAGE_ACCESS_TOKEN=EAA...     # Facebook Page long-lived token
```

### Required — LinkedIn

```bash
LI_ENABLED=true                 # Feature flag
LI_OAUTH_CLIENT_ID=xxx          # LinkedIn Developer App
LI_OAUTH_CLIENT_SECRET=xxx
LI_REFRESH_TOKEN=xxx            # From OAuth flow
```

### Required — Cloud Storage (Cloudinary)

```bash
# Option 1 (preferred):
CLOUDINARY_URL=cloudinary://API_KEY:API_SECRET@CLOUD_NAME

# Option 2:
CLOUDINARY_CLOUD_NAME=xxx
CLOUDINARY_API_KEY=xxx
CLOUDINARY_API_SECRET=xxx
```

### Optional

```bash
PUBLISH_MAX_RETRIES=3           # Retry attempts
PUBLISH_RETRY_DELAY=5           # Seconds between retries
FFMPEG_TIMEOUT=300              # Video transcode timeout (seconds)
```

---

## Quick-Start Code Snippets

### Minimal Instagram Publish (Python)

```python
import requests

BASE = "https://graph.facebook.com/v21.0"
TOKEN = "your-ig-token"
USER_ID = "your-ig-user-id"

def publish_to_instagram(image_url: str, caption: str) -> str:
    # Step 1: Create container
    resp = requests.post(f"{BASE}/{USER_ID}/media", data={
        "image_url": image_url,
        "caption": caption,
        "access_token": TOKEN,
    })
    container_id = resp.json()["id"]

    # Step 2: Wait for processing
    import time
    for _ in range(60):
        resp = requests.get(f"{BASE}/{container_id}", params={
            "fields": "status_code", "access_token": TOKEN,
        })
        if resp.json().get("status_code") == "FINISHED":
            break
        time.sleep(5)

    # Step 3: Publish
    resp = requests.post(f"{BASE}/{USER_ID}/media_publish", data={
        "creation_id": container_id,
        "access_token": TOKEN,
    })
    return resp.json()["id"]
```

### Minimal Facebook Publish (Python)

```python
def publish_to_facebook(image_url: str, caption: str) -> str:
    resp = requests.post(f"{BASE}/{PAGE_ID}/photos", data={
        "url": image_url,
        "caption": caption,
        "access_token": FB_TOKEN,
    })
    return resp.json().get("post_id") or resp.json().get("id")
```

### Minimal LinkedIn Publish (Python)

```python
def publish_to_linkedin(caption: str, author_urn: str, access_token: str) -> str:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "LinkedIn-Version": "202401",
        "Content-Type": "application/json",
        "X-Restli-Protocol-Version": "2.0.0",
    }
    body = {
        "author": author_urn,
        "lifecycleState": "PUBLISHED",
        "visibility": "PUBLIC",
        "commentary": caption,
        "distribution": {"feedDistribution": "MAIN_FEED"},
    }
    resp = requests.post("https://api.linkedin.com/rest/posts", json=body, headers=headers)
    return resp.headers.get("x-restli-id", "")
```

---

## Voice-Bot Integration Notes

For a WhatsApp/Telegram Voice-Bot that publishes to social media:

1. **Transcribe** voice message → text (Whisper API)
2. **Generate** post caption from text (LLM)
3. **Process media** if the user sends an image/video:
   - Upload to Cloudinary → get public URL
   - Normalize (JPEG/H.264) as needed
4. **Publish** using the APIs above
5. **Return** confirmation to the user via the bot

The publishing layer is **completely independent** of the input channel.
You can call the same publishing functions from a bot, a web panel, a CLI, or a cron job.
