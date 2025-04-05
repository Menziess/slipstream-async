"""Slipstream interfaces."""

from abc import ABCMeta, abstractmethod
from typing import Any, AsyncIterator, TypeAlias

from slipstream.utils import PubSub

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


class CacheMeta(ABCMeta):
    """Metaclass adds default functionality to ICache."""

    def __call__(cls, *args, **kwargs):
        """Adding instance variables."""
        instance = super().__call__(*args, **kwargs)
        if not hasattr(instance, '_pubsub'):
            instance._pubsub = PubSub()
        if not hasattr(instance, '_iterable_key'):
            instance._iterable_key = str(id(instance)) + 'cache'
        return instance


class ICache(metaclass=CacheMeta):
    """Base class for cache implementations.

    >>> class MyCache(ICache):
    ...     def __init__(self):
    ...         self.db = {}
    ...     def __contains__(self, key: Key) -> bool:
    ...         return key in self.db
    ...     def __delitem__(self, key: Key) -> None:
    ...         del self.db[key]
    ...     def __getitem__(self, key: Key | list[Key]) -> Any:
    ...         return self.db.get(key, None)
    ...     def __setitem__(self, key: Key, val: Any) -> None:
    ...         self.db[key] = val

    >>> cache = MyCache()
    >>> cache['prize'] = '🏆'
    >>> cache['prize']
    '🏆'
    >>> del cache['prize']
    >>> 'prize' in cache
    False
    """

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
        """Get item from db or None.

        Important:
        - This method should **not** raise a `KeyError` if key does not exist.
        - Instead, return None.
        """
        raise NotImplementedError

    @abstractmethod
    def __setitem__(self, key: Key, val: Any) -> None:
        """Set item in db."""
        raise NotImplementedError

    async def __call__(self, key: Key, val: Any) -> None:
        """Set item in db while also publishing to pubsub."""
        self.__setitem__(key, val)
        await self._pubsub.apublish(               # type: ignore[attr-defined]
            self._iterable_key, (key, val)         # type: ignore[attr-defined]
        )

    async def __aiter__(self) -> AsyncIterator[Any]:
        """Consume published updates to cache."""
        async for msg in self._pubsub.iter_topic(  # type: ignore[attr-defined]
            self._iterable_key                     # type: ignore[attr-defined]
        ):
            yield msg
