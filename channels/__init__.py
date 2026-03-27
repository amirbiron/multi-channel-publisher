"""channels — modular publishing layer for multi-channel support."""

import os

from channels.base import BaseChannel, PublishResult
from channels.registry import ChannelRegistry
from channels.meta_instagram import InstagramChannel
from channels.meta_facebook import FacebookChannel
from channels.google_business import GoogleBusinessChannel

__all__ = [
    "BaseChannel",
    "PublishResult",
    "ChannelRegistry",
    "InstagramChannel",
    "FacebookChannel",
    "GoogleBusinessChannel",
]

# Feature flag for GBP rollout — set GBP_ENABLED=true to activate
# Defaults to OFF for safe phased rollout (see DEPLOYMENT_CHECKLIST.md)
GBP_ENABLED = os.environ.get("GBP_ENABLED", "false").lower() in ("true", "1", "yes")


def create_default_registry() -> ChannelRegistry:
    """Create a registry with all currently available channels."""
    registry = ChannelRegistry()
    registry.register(InstagramChannel())
    registry.register(FacebookChannel())
    if GBP_ENABLED:
        registry.register(GoogleBusinessChannel())
    return registry
