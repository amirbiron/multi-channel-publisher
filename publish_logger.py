"""
publish_logger.py — Structured publish logging + secret masking.

Provides:
- generate_correlation_id(): unique ID per publish job
- PublishEventLogger: logs structured JSON for each publish attempt
- SecretMaskingFilter: logging filter that redacts tokens/keys from log output
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any


# ═══════════════════════════════════════════════════════════════
#  Correlation ID
# ═══════════════════════════════════════════════════════════════

def generate_correlation_id() -> str:
    """Generate a unique correlation ID for a publish job."""
    now = datetime.now(timezone.utc)
    short_uuid = uuid.uuid4().hex[:8]
    return f"job_{now.strftime('%Y%m%d_%H%M%S')}_{short_uuid}"


# ═══════════════════════════════════════════════════════════════
#  Structured Publish Event Logger
# ═══════════════════════════════════════════════════════════════

_publish_logger = logging.getLogger("publish-events")


class PublishEventLogger:
    """
    Logs structured events for a single publish job (one row, all channels).

    Usage:
        elog = PublishEventLogger(correlation_id="job_...", post_row_id="42")
        elog.log_channel_start("IG")
        ... publish ...
        elog.log_channel_end("IG", success=True, platform_post_id="123")
    """

    def __init__(self, correlation_id: str, post_row_id: str) -> None:
        self.correlation_id = correlation_id
        self.post_row_id = post_row_id
        self._channel_start_times: dict[str, float] = {}

    def _log(self, level: int, event: dict[str, Any]) -> None:
        """Emit a structured log line as JSON."""
        event["correlation_id"] = self.correlation_id
        event["post_row_id"] = self.post_row_id
        _publish_logger.log(level, json.dumps(event, ensure_ascii=False, default=str))

    # ── Job-level events ──────────────────────────────────────

    def log_job_start(self, channels: list[str]) -> None:
        self._log(logging.INFO, {
            "event": "job_start",
            "action": "publish",
            "channels": channels,
            "started_at": datetime.now(timezone.utc).isoformat(),
        })

    def log_job_end(self, success: bool, summary: str) -> None:
        self._log(logging.INFO, {
            "event": "job_end",
            "action": "publish",
            "success": success,
            "summary": summary,
            "ended_at": datetime.now(timezone.utc).isoformat(),
        })

    # ── Channel-level events ─────────────────────────────────

    def log_channel_start(
        self, channel: str, action: str = "publish", location_id: str = "",
    ) -> None:
        self._channel_start_times[channel] = time.monotonic()
        event: dict[str, Any] = {
            "event": "channel_start",
            "channel": channel,
            "action": action,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        if location_id:
            event["location_id"] = location_id
        self._log(logging.INFO, event)

    def log_channel_end(
        self,
        channel: str,
        *,
        success: bool,
        action: str = "publish",
        location_id: str = "",
        platform_post_id: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        start = self._channel_start_times.pop(channel, None)
        duration_ms = int((time.monotonic() - start) * 1000) if start else None

        event: dict[str, Any] = {
            "event": "channel_end",
            "channel": channel,
            "action": action,
            "ended_at": now.isoformat(),
            "duration_ms": duration_ms,
            "success": success,
        }
        if location_id:
            event["location_id"] = location_id
        if platform_post_id:
            event["platform_post_id"] = platform_post_id
        if error_code:
            event["error_code"] = error_code
        if error_message:
            event["error_message"] = error_message[:500]

        level = logging.INFO if success else logging.WARNING
        self._log(level, event)

    # ── Validation events ────────────────────────────────────

    def log_validation(
        self, channel: str, *, passed: bool, errors: list[str] | None = None,
    ) -> None:
        event: dict[str, Any] = {
            "event": "validation",
            "channel": channel,
            "action": "validate",
            "success": passed,
        }
        if errors:
            event["errors"] = errors
        level = logging.INFO if passed else logging.WARNING
        self._log(level, event)

    # ── Retry events ─────────────────────────────────────────

    def log_retry(
        self, channel: str, attempt: int, max_attempts: int, error_message: str,
    ) -> None:
        self._log(logging.WARNING, {
            "event": "retry",
            "channel": channel,
            "action": "retry",
            "attempt": attempt,
            "max_attempts": max_attempts,
            "error_message": error_message[:500],
        })


# ═══════════════════════════════════════════════════════════════
#  Secret Masking Filter
# ═══════════════════════════════════════════════════════════════

# Environment variable names that contain secrets
_SECRET_ENV_NAMES = (
    "IG_ACCESS_TOKEN",
    "FB_PAGE_ACCESS_TOKEN",
    "GOOGLE_SERVICE_ACCOUNT_JSON",
    "CLOUDINARY_API_SECRET",
    "CLOUDINARY_API_KEY",
    "CLOUDINARY_URL",
    "GBP_OAUTH_CLIENT_SECRET",
    "GBP_REFRESH_TOKEN",
    "TELEGRAM_BOT_TOKEN",
    "WEB_PANEL_SECRET",
    "WEB_PANEL_DEV_SECRET",
)

# Collect actual secret values from environment (skip empty ones)
_SECRET_VALUES: list[str] = []
for _name in _SECRET_ENV_NAMES:
    _val = os.environ.get(_name, "")
    # Only mask values that are long enough to be meaningful (avoid masking empty/short strings)
    if len(_val) >= 8:
        _SECRET_VALUES.append(_val)

# Build a regex that matches any of the secret values (longest first to avoid partial matches)
_SECRET_VALUES.sort(key=len, reverse=True)
_SECRET_PATTERN: re.Pattern | None = None
if _SECRET_VALUES:
    escaped = [re.escape(v) for v in _SECRET_VALUES]
    _SECRET_PATTERN = re.compile("|".join(escaped))

# Also match common token patterns that might leak via API responses
_TOKEN_PATTERNS = re.compile(
    r"(EAA[A-Za-z0-9]{20,})"        # Meta/Facebook access tokens
    r"|(ya29\.[A-Za-z0-9_-]{20,})"   # Google OAuth access tokens
    r"|(AIza[A-Za-z0-9_-]{30,})"     # Google API keys
)

MASK = "***REDACTED***"


def mask_secrets(text: str) -> str:
    """Replace known secret values and token patterns in text."""
    if _SECRET_PATTERN:
        text = _SECRET_PATTERN.sub(MASK, text)
    text = _TOKEN_PATTERNS.sub(MASK, text)
    return text


class SecretMaskingFilter(logging.Filter):
    """Logging filter that masks secrets in log messages."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = mask_secrets(record.msg)
        if record.args:
            if isinstance(record.args, dict):
                record.args = {k: mask_secrets(str(v)) if isinstance(v, str) else v
                               for k, v in record.args.items()}
            elif isinstance(record.args, tuple):
                record.args = tuple(
                    mask_secrets(str(a)) if isinstance(a, str) else a
                    for a in record.args
                )
        return True
