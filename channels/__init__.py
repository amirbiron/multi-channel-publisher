"""channels — modular publishing layer for multi-channel support."""

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


def create_default_registry() -> ChannelRegistry:
    """Create a registry with all currently available channels."""
    registry = ChannelRegistry()
    registry.register(InstagramChannel())
    registry.register(FacebookChannel())
    registry.register(GoogleBusinessChannel())
    return registry
