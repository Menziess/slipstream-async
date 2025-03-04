"""Slipstream interfaces."""

from abc import ABCMeta, abstractmethod
from typing import Any, TypeAlias

Key: TypeAlias = str | int | float | bytes | bool


class ICodec(metaclass=ABCMeta):
    """Base class for codecs."""

    @abstractmethod
    def encode(self, obj: Any) -> bytes:
        """Serialize object."""
        raise NotImplementedError

    @abstractmethod
    def decode(self, s: bytes) -> object:
        """Deserialize object."""
        raise NotImplementedError


class ICache(metaclass=ABCMeta):
    """Base class for cache implementations."""

    @abstractmethod
    async def __call__(self, key: Key, val: Any) -> None:
        """Call cache to set item."""
        raise NotImplementedError

    @abstractmethod
    def __contains__(self, key: Key) -> bool:
        """Key exists in db."""
        raise NotImplementedError

    @abstractmethod
    def __delitem__(self, key: Key) -> None:
        """Delete item from db."""
        raise NotImplementedError

    @abstractmethod
    def __getitem__(self, key: Key | list[Key]) -> Any:
        """Get item from db or None."""
        raise NotImplementedError

    @abstractmethod
    def __setitem__(self, key: Key, val: Any) -> None:
        """Set item in db."""
        raise NotImplementedError
