"""
test_publish_logger.py — Tests for publish_logger module.
"""

import json
import logging
import os
from unittest.mock import patch

import pytest

from publish_logger import (
    generate_correlation_id,
    PublishEventLogger,
    SecretMaskingFilter,
    mask_secrets,
)


# ═══════════════════════════════════════════════════════════════
#  generate_correlation_id
# ═══════════════════════════════════════════════════════════════

class TestGenerateCorrelationId:
    def test_format(self):
        cid = generate_correlation_id()
        assert cid.startswith("job_")
        parts = cid.split("_")
        assert len(parts) == 4  # job, date, time, uuid

    def test_unique(self):
        ids = {generate_correlation_id() for _ in range(50)}
        assert len(ids) == 50


# ═══════════════════════════════════════════════════════════════
#  PublishEventLogger
# ═══════════════════════════════════════════════════════════════

class TestPublishEventLogger:
    def _capture_logs(self, caplog):
        """Return parsed JSON events from captured log records."""
        events = []
        for record in caplog.records:
            if record.name == "publish-events":
                events.append(json.loads(record.message))
        return events

    def test_log_job_start(self, caplog):
        with caplog.at_level(logging.INFO, logger="publish-events"):
            elog = PublishEventLogger(correlation_id="job_test_123", post_row_id="42")
            elog.log_job_start(["IG", "FB"])

        events = self._capture_logs(caplog)
        assert len(events) == 1
        e = events[0]
        assert e["event"] == "job_start"
        assert e["correlation_id"] == "job_test_123"
        assert e["post_row_id"] == "42"
        assert e["channels"] == ["IG", "FB"]
        assert "started_at" in e

    def test_log_job_end(self, caplog):
        with caplog.at_level(logging.INFO, logger="publish-events"):
            elog = PublishEventLogger(correlation_id="job_test_456", post_row_id="7")
            elog.log_job_end(success=True, summary="All channels posted")

        events = self._capture_logs(caplog)
        assert len(events) == 1
        e = events[0]
        assert e["event"] == "job_end"
        assert e["success"] is True
        assert e["summary"] == "All channels posted"

    def test_log_channel_start_end_with_duration(self, caplog):
        with caplog.at_level(logging.INFO, logger="publish-events"):
            elog = PublishEventLogger(correlation_id="job_dur", post_row_id="10")
            elog.log_channel_start("GBP", location_id="locations/123")
            elog.log_channel_end(
                "GBP",
                success=True,
                location_id="locations/123",
                platform_post_id="post_abc",
            )

        events = self._capture_logs(caplog)
        assert len(events) == 2
        start_event = events[0]
        assert start_event["event"] == "channel_start"
        assert start_event["channel"] == "GBP"
        assert start_event["location_id"] == "locations/123"

        end_event = events[1]
        assert end_event["event"] == "channel_end"
        assert end_event["success"] is True
        assert end_event["duration_ms"] is not None
        assert end_event["duration_ms"] >= 0
        assert end_event["platform_post_id"] == "post_abc"

    def test_log_channel_end_error(self, caplog):
        with caplog.at_level(logging.WARNING, logger="publish-events"):
            elog = PublishEventLogger(correlation_id="job_err", post_row_id="5")
            elog.log_channel_start("IG")
            elog.log_channel_end(
                "IG",
                success=False,
                error_code="timeout",
                error_message="Connection timed out",
            )

        events = self._capture_logs(caplog)
        end_event = [e for e in events if e["event"] == "channel_end"][0]
        assert end_event["success"] is False
        assert end_event["error_code"] == "timeout"
        assert end_event["error_message"] == "Connection timed out"

    def test_log_validation(self, caplog):
        with caplog.at_level(logging.INFO, logger="publish-events"):
            elog = PublishEventLogger(correlation_id="job_val", post_row_id="3")
            elog.log_validation("GBP", passed=False, errors=["Missing location_id"])

        events = self._capture_logs(caplog)
        assert len(events) == 1
        assert events[0]["event"] == "validation"
        assert events[0]["success"] is False
        assert events[0]["errors"] == ["Missing location_id"]

    def test_log_retry(self, caplog):
        with caplog.at_level(logging.WARNING, logger="publish-events"):
            elog = PublishEventLogger(correlation_id="job_retry", post_row_id="8")
            elog.log_retry("FB", 1, 3, "rate limit exceeded")

        events = self._capture_logs(caplog)
        assert len(events) == 1
        assert events[0]["event"] == "retry"
        assert events[0]["attempt"] == 1
        assert events[0]["max_attempts"] == 3

    def test_correlation_id_in_all_events(self, caplog):
        with caplog.at_level(logging.INFO, logger="publish-events"):
            elog = PublishEventLogger(correlation_id="job_xyz", post_row_id="1")
            elog.log_job_start(["IG"])
            elog.log_channel_start("IG")
            elog.log_channel_end("IG", success=True)
            elog.log_job_end(success=True, summary="done")

        events = self._capture_logs(caplog)
        assert len(events) == 4
        for e in events:
            assert e["correlation_id"] == "job_xyz"
            assert e["post_row_id"] == "1"


# ═══════════════════════════════════════════════════════════════
#  Secret Masking
# ═══════════════════════════════════════════════════════════════

class TestMaskSecrets:
    def test_masks_meta_token(self):
        text = "Token is EAABsbCS1iZAZABCDEF1234567890abcdef"
        result = mask_secrets(text)
        assert "EAA" not in result
        assert "***REDACTED***" in result

    def test_masks_google_oauth_token(self):
        text = "Access token: ya29.a0AfB_byDK_ABCDEF1234567890"
        result = mask_secrets(text)
        assert "ya29." not in result
        assert "***REDACTED***" in result

    def test_masks_google_api_key(self):
        text = "Key: AIzaSyB1234567890abcdefghijklmnopqrst"
        result = mask_secrets(text)
        assert "AIza" not in result
        assert "***REDACTED***" in result

    def test_preserves_safe_text(self):
        text = "Row 42: Published to IG successfully"
        assert mask_secrets(text) == text

    def test_masks_env_var_values(self):
        """Test that actual env var values are masked when present."""
        fake_token = "FAKE_TOKEN_VALUE_12345678"
        with patch.dict(os.environ, {"IG_ACCESS_TOKEN": fake_token}):
            # Re-import to pick up the new env var
            import importlib
            import publish_logger
            importlib.reload(publish_logger)
            result = publish_logger.mask_secrets(f"Token: {fake_token}")
            assert fake_token not in result
            assert "***REDACTED***" in result


class TestSecretMaskingFilter:
    def test_filter_masks_message(self):
        filt = SecretMaskingFilter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="Token: EAABsbCS1iZAZABCDEF1234567890abcdef",
            args=None, exc_info=None,
        )
        filt.filter(record)
        assert "EAA" not in record.msg
        assert "***REDACTED***" in record.msg

    def test_filter_masks_args_tuple(self):
        filt = SecretMaskingFilter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="Token: %s",
            args=("EAABsbCS1iZAZABCDEF1234567890abcdef",),
            exc_info=None,
        )
        filt.filter(record)
        assert "EAA" not in record.args[0]

    def test_filter_preserves_safe_message(self):
        filt = SecretMaskingFilter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="Row 42: POSTED successfully",
            args=None, exc_info=None,
        )
        filt.filter(record)
        assert record.msg == "Row 42: POSTED successfully"

    def test_filter_returns_true(self):
        """Filter should always return True (don't suppress records)."""
        filt = SecretMaskingFilter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="test", args=None, exc_info=None,
        )
        assert filt.filter(record) is True
