"""
registry.py — ChannelRegistry: register, validate, and publish to channels.

Usage:
    registry = ChannelRegistry()
    registry.register(InstagramChannel())
    registry.register(FacebookChannel())
    results = registry.publish_to_channels(post_data, ["IG", "FB"])
"""

from __future__ import annotations

import logging
from typing import Sequence

from channels.base import BaseChannel, PublishResult

logger = logging.getLogger(__name__)


class ChannelRegistry:
    """Central registry for all publishing channels."""

    def __init__(self) -> None:
        self._channels: dict[str, BaseChannel] = {}

    # ── registration ──────────────────────────────────────────

    def register(self, channel: BaseChannel) -> None:
        """Register a channel. Raises if CHANNEL_ID is already registered."""
        cid = channel.CHANNEL_ID
        if not cid:
            raise ValueError(f"Channel {channel!r} has no CHANNEL_ID")
        if cid in self._channels:
            raise ValueError(f"Channel '{cid}' is already registered")
        self._channels[cid] = channel
        logger.info(f"Registered channel: {cid} ({channel.CHANNEL_NAME})")

    def get(self, channel_id: str) -> BaseChannel:
        """Return channel by ID. Raises KeyError if not found."""
        try:
            return self._channels[channel_id]
        except KeyError:
            raise KeyError(
                f"Channel '{channel_id}' not registered. "
                f"Available: {list(self._channels)}"
            )

    def get_all(self) -> list[BaseChannel]:
        """Return all registered channels (insertion order)."""
        return list(self._channels.values())

    @property
    def channel_ids(self) -> list[str]:
        """List of registered channel IDs."""
        return list(self._channels)

    # ── validation ────────────────────────────────────────────

    def validate_channels(
        self,
        post_data: dict,
        target_channels: Sequence[str],
    ) -> dict[str, list[str]]:
        """
        Validate post_data for each target channel.

        Returns {channel_id: [error_messages]} — only channels with
        errors are included. Empty dict = all valid.
        """
        errors: dict[str, list[str]] = {}
        for cid in target_channels:
            channel = self.get(cid)
            channel_errors = channel.validate(post_data)
            if channel_errors:
                errors[cid] = channel_errors
        return errors

    # ── publishing ────────────────────────────────────────────

    def publish_to_channels(
        self,
        post_data: dict,
        target_channels: Sequence[str],
    ) -> dict[str, PublishResult]:
        """
        Publish to a list of channels. A failure in one channel does
        NOT stop the others.

        Returns {channel_id: PublishResult} for every target.
        """
        results: dict[str, PublishResult] = {}

        for cid in target_channels:
            channel = self.get(cid)
            logger.info(f"Publishing to {cid} ({channel.CHANNEL_NAME})...")

            try:
                result = channel.publish(post_data)
            except Exception as exc:
                logger.error(f"Channel {cid} raised: {exc}", exc_info=True)
                result = PublishResult(
                    channel=cid,
                    success=False,
                    status="ERROR",
                    error_code="unhandled_exception",
                    error_message=str(exc)[:500],
                )

            results[cid] = result

            if result.success:
                logger.info(f"{cid}: POSTED (id={result.platform_post_id})")
            else:
                logger.warning(
                    f"{cid}: {result.status} — "
                    f"{result.error_code}: {result.error_message}"
                )

        return results
