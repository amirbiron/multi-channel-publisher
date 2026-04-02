"""channels — modular publishing layer for multi-channel support."""

import os

from channels.base import BaseChannel, PublishResult
from channels.registry import ChannelRegistry
from channels.meta_instagram import InstagramChannel
from channels.meta_facebook import FacebookChannel
from channels.google_business import GoogleBusinessChannel
from channels.linkedin import LinkedInChannel

__all__ = [
    "BaseChannel",
    "PublishResult",
    "ChannelRegistry",
    "InstagramChannel",
    "FacebookChannel",
    "GoogleBusinessChannel",
    "LinkedInChannel",
]

# Feature flags — set to "true" to activate
GBP_ENABLED = os.environ.get("GBP_ENABLED", "false").lower() in ("true", "1", "yes")
LI_ENABLED = os.environ.get("LI_ENABLED", "false").lower() in ("true", "1", "yes")


def create_default_registry() -> ChannelRegistry:
    """Create a registry with all currently available channels."""
    registry = ChannelRegistry()
    registry.register(InstagramChannel())
    registry.register(FacebookChannel())
    if GBP_ENABLED:
        registry.register(GoogleBusinessChannel())
    if LI_ENABLED:
        registry.register(LinkedInChannel())
    return registry
