"""Slipstream interfaces."""

from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterator
from typing import Any, TypeAlias, TypeVar

from slipstream.utils import PubSub

T = TypeVar('T')

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


class SourceSinkMeta(ABCMeta):
    """Metaclass adds default source/sink functionalities."""

    def __call__(cls: type[T], *args: Any, **kwargs: Any) -> T:
        """Adding instance variables."""
        instance = super().__call__(*args, **kwargs)
        if not hasattr(instance, '_pubsub'):
            instance._pubsub = PubSub()  # noqa: SLF001
        if not hasattr(instance, '_iterable_key'):
            k = str(id(instance))
            instance._iterable_key = k + 'source_sink'  # noqa: SLF001
        return instance


class ICache(metaclass=SourceSinkMeta):
    """Base class for cache implementations.

    >>> class MyCache(ICache):
    ...     def __init__(self):
    ...         self.db = {}
    ...
    ...     def __contains__(self, key: Key) -> bool:
    ...         return key in self.db
    ...
    ...     def __delitem__(self, key: Key) -> None:
    ...         del self.db[key]
    ...
    ...     def __getitem__(self, key: Key | list[Key]) -> Any:
    ...         return self.db.get(key, None)
    ...
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

    __slots__ = ('_iterable_key', '_pubsub')

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
        await self._pubsub.apublish(
            self._iterable_key,
            (key, val),
        )

    async def __aiter__(self) -> AsyncIterator[Any]:
        """Consume published updates to cache."""
        async for msg in self._pubsub.iter_topic(
            self._iterable_key,
        ):
            yield msg
