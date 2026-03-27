"""
test_channels.py — tests for channels/base.py + channels/registry.py

Covers:
- PublishResult dataclass
- BaseChannel interface contract
- ChannelRegistry: register, get, validate, publish_to_channels
- Failure isolation: one channel failing doesn't stop the rest
"""

import pytest

from channels.base import BaseChannel, PublishResult
from channels.registry import ChannelRegistry


# ═══════════════════════════════════════════════════════════════
#  Dummy channels for testing
# ═══════════════════════════════════════════════════════════════

class DummyChannelOK(BaseChannel):
    CHANNEL_ID = "TEST_OK"
    CHANNEL_NAME = "Test OK Channel"
    SUPPORTED_POST_TYPES = ["FEED"]
    SUPPORTED_MEDIA_TYPES = ["image"]
    CAPTION_COLUMN = "caption_test"

    def validate(self, post_data: dict) -> list[str]:
        errors = []
        if not self.get_caption(post_data):
            errors.append("missing caption")
        return errors

    def publish(self, post_data: dict) -> PublishResult:
        return self._make_result(
            success=True,
            platform_post_id="ok_123",
            raw_response={"id": "ok_123"},
        )


class DummyChannelFail(BaseChannel):
    CHANNEL_ID = "TEST_FAIL"
    CHANNEL_NAME = "Test Fail Channel"
    SUPPORTED_POST_TYPES = ["FEED"]
    SUPPORTED_MEDIA_TYPES = ["image"]
    CAPTION_COLUMN = "caption_fail"

    def validate(self, post_data: dict) -> list[str]:
        return []

    def publish(self, post_data: dict) -> PublishResult:
        return self._make_result(
            success=False,
            error_code="api_error",
            error_message="Simulated failure",
        )


class DummyChannelExplode(BaseChannel):
    """Raises an unhandled exception in publish()."""
    CHANNEL_ID = "TEST_EXPLODE"
    CHANNEL_NAME = "Test Explode Channel"
    SUPPORTED_POST_TYPES = ["FEED"]
    SUPPORTED_MEDIA_TYPES = ["image"]
    CAPTION_COLUMN = "caption_explode"

    def validate(self, post_data: dict) -> list[str]:
        return []

    def publish(self, post_data: dict) -> PublishResult:
        raise RuntimeError("boom")


# ═══════════════════════════════════════════════════════════════
#  PublishResult
# ═══════════════════════════════════════════════════════════════

class TestPublishResult:
    def test_success_result(self):
        r = PublishResult(
            channel="IG", success=True, status="POSTED",
            platform_post_id="123",
        )
        assert r.success is True
        assert r.status == "POSTED"
        assert r.error_code is None

    def test_error_result(self):
        r = PublishResult(
            channel="FB", success=False, status="ERROR",
            error_code="timeout", error_message="Request timed out",
        )
        assert r.success is False
        assert r.error_code == "timeout"
        assert r.platform_post_id is None


# ═══════════════════════════════════════════════════════════════
#  BaseChannel
# ═══════════════════════════════════════════════════════════════

class TestBaseChannel:
    def test_validate_not_implemented(self):
        ch = BaseChannel()
        with pytest.raises(NotImplementedError):
            ch.validate({})

    def test_publish_not_implemented(self):
        ch = BaseChannel()
        with pytest.raises(NotImplementedError):
            ch.publish({})

    def test_get_caption_channel_specific(self):
        ch = DummyChannelOK()
        data = {"caption_test": "specific", "caption": "generic"}
        assert ch.get_caption(data) == "specific"

    def test_get_caption_falls_back_to_generic(self):
        ch = DummyChannelOK()
        data = {"caption": "generic"}
        assert ch.get_caption(data) == "generic"

    def test_get_caption_empty_when_nothing(self):
        ch = DummyChannelOK()
        assert ch.get_caption({}) == ""

    def test_make_result_success(self):
        ch = DummyChannelOK()
        r = ch._make_result(success=True, platform_post_id="p1")
        assert r.channel == "TEST_OK"
        assert r.status == "POSTED"
        assert r.published_at is not None

    def test_make_result_error(self):
        ch = DummyChannelFail()
        r = ch._make_result(success=False, error_code="x", error_message="y")
        assert r.channel == "TEST_FAIL"
        assert r.status == "ERROR"
        assert r.published_at is None

    def test_repr(self):
        ch = DummyChannelOK()
        assert "DummyChannelOK" in repr(ch)
        assert "TEST_OK" in repr(ch)


# ═══════════════════════════════════════════════════════════════
#  ChannelRegistry — registration
# ═══════════════════════════════════════════════════════════════

class TestRegistryRegistration:
    def test_register_and_get(self):
        reg = ChannelRegistry()
        ch = DummyChannelOK()
        reg.register(ch)
        assert reg.get("TEST_OK") is ch

    def test_get_unknown_raises_key_error(self):
        reg = ChannelRegistry()
        with pytest.raises(KeyError, match="NOPE"):
            reg.get("NOPE")

    def test_duplicate_register_raises(self):
        reg = ChannelRegistry()
        reg.register(DummyChannelOK())
        with pytest.raises(ValueError, match="already registered"):
            reg.register(DummyChannelOK())

    def test_register_no_channel_id_raises(self):
        reg = ChannelRegistry()
        ch = BaseChannel()  # CHANNEL_ID = ""
        with pytest.raises(ValueError, match="no CHANNEL_ID"):
            reg.register(ch)

    def test_get_all(self):
        reg = ChannelRegistry()
        reg.register(DummyChannelOK())
        reg.register(DummyChannelFail())
        assert len(reg.get_all()) == 2

    def test_channel_ids(self):
        reg = ChannelRegistry()
        reg.register(DummyChannelOK())
        reg.register(DummyChannelFail())
        assert reg.channel_ids == ["TEST_OK", "TEST_FAIL"]


# ═══════════════════════════════════════════════════════════════
#  ChannelRegistry — validation
# ═══════════════════════════════════════════════════════════════

class TestRegistryValidation:
    def test_valid_data_returns_empty(self):
        reg = ChannelRegistry()
        reg.register(DummyChannelOK())
        errors = reg.validate_channels(
            {"caption_test": "hi"}, ["TEST_OK"],
        )
        assert errors == {}

    def test_invalid_data_returns_errors(self):
        reg = ChannelRegistry()
        reg.register(DummyChannelOK())
        errors = reg.validate_channels({}, ["TEST_OK"])
        assert "TEST_OK" in errors
        assert "missing caption" in errors["TEST_OK"]

    def test_validates_multiple_channels(self):
        reg = ChannelRegistry()
        reg.register(DummyChannelOK())
        reg.register(DummyChannelFail())
        errors = reg.validate_channels({}, ["TEST_OK", "TEST_FAIL"])
        # DummyChannelOK fails validation (no caption), DummyChannelFail passes
        assert "TEST_OK" in errors
        assert "TEST_FAIL" not in errors


# ═══════════════════════════════════════════════════════════════
#  ChannelRegistry — publish_to_channels
# ═══════════════════════════════════════════════════════════════

class TestRegistryPublish:
    def test_all_succeed(self):
        reg = ChannelRegistry()
        reg.register(DummyChannelOK())
        results = reg.publish_to_channels({"caption_test": "hi"}, ["TEST_OK"])
        assert results["TEST_OK"].success is True
        assert results["TEST_OK"].platform_post_id == "ok_123"

    def test_all_fail(self):
        reg = ChannelRegistry()
        reg.register(DummyChannelFail())
        results = reg.publish_to_channels({}, ["TEST_FAIL"])
        assert results["TEST_FAIL"].success is False
        assert results["TEST_FAIL"].error_code == "api_error"

    def test_failure_does_not_stop_other_channels(self):
        """AC: failure in one channel must not prevent others from publishing."""
        reg = ChannelRegistry()
        reg.register(DummyChannelOK())
        reg.register(DummyChannelFail())

        results = reg.publish_to_channels(
            {"caption_test": "hi"},
            ["TEST_FAIL", "TEST_OK"],
        )

        assert results["TEST_FAIL"].success is False
        assert results["TEST_OK"].success is True

    def test_unhandled_exception_caught(self):
        """An exception in publish() should be caught and returned as ERROR."""
        reg = ChannelRegistry()
        reg.register(DummyChannelExplode())

        results = reg.publish_to_channels({}, ["TEST_EXPLODE"])
        r = results["TEST_EXPLODE"]
        assert r.success is False
        assert r.error_code == "unhandled_exception"
        assert "boom" in r.error_message

    def test_mixed_results(self):
        """Multiple channels: one succeeds, one fails, one explodes."""
        reg = ChannelRegistry()
        reg.register(DummyChannelOK())
        reg.register(DummyChannelFail())
        reg.register(DummyChannelExplode())

        results = reg.publish_to_channels(
            {"caption_test": "hi"},
            ["TEST_OK", "TEST_FAIL", "TEST_EXPLODE"],
        )

        assert len(results) == 3
        assert results["TEST_OK"].success is True
        assert results["TEST_FAIL"].success is False
        assert results["TEST_EXPLODE"].success is False

    def test_unknown_channel_raises(self):
        reg = ChannelRegistry()
        with pytest.raises(KeyError):
            reg.publish_to_channels({}, ["NOPE"])

    def test_empty_targets(self):
        reg = ChannelRegistry()
        reg.register(DummyChannelOK())
        results = reg.publish_to_channels({}, [])
        assert results == {}
