"""Slipstream utilities."""

from asyncio import Queue
from inspect import _empty, iscoroutinefunction, signature
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    TypeAlias,
)

AsyncCallable: TypeAlias = Callable[..., Awaitable[Any]] | Callable[..., Any]


def iscoroutinecallable(o: Any) -> bool:
    """Check whether function is coroutine."""
    return iscoroutinefunction(o) or (
        hasattr(o, '__call__')
        and iscoroutinefunction(o.__call__)
    )


def get_positional_params(o: Any) -> tuple[str, ...]:
    """Return function positional parameters."""
    if hasattr(o, '__code__'):
        c = o.__code__
        positional_count = c.co_argcount - len(o.__defaults__ or ())
        return c.co_varnames[:positional_count]

    params = signature(o).parameters
    return tuple(
        param.name for param in params.values()
        if param.kind in (param.POSITIONAL_OR_KEYWORD, param.POSITIONAL_ONLY)
        and param.default is _empty
    )


def get_params(o: Any):
    """Return function parameters."""
    params = signature(o).parameters
    return tuple(params.keys())


class Singleton(type):
    """Maintain a single instance of a class."""

    _instances: dict['Singleton', Any] = {}

    def __call__(cls, *args: Any, **kwargs: Any):
        """Apply metaclass singleton action."""
        if cls not in cls._instances:
            cls._instances[cls] = super(
                Singleton,
                cls
            ).__call__(*args, **kwargs)
        instance = cls._instances[cls]
        if hasattr(instance, '__update__'):
            instance.__update__(*args, **kwargs)
        return instance


class PubSub(metaclass=Singleton):
    """Singleton publish subscribe pattern class."""

    _topics: dict[str, list[AsyncCallable]] = {}

    def subscribe(self, topic: str, listener: AsyncCallable) -> None:
        """Subscribe callable to topic."""
        if topic not in self._topics:
            self._topics[topic] = []
        self._topics[topic].append(listener)

    def unsubscribe(self, topic: str, listener: AsyncCallable) -> None:
        """Unsubscribe callable from topic."""
        if topic in self._topics:
            self._topics[topic].remove(listener)
            if not self._topics[topic]:
                del self._topics[topic]

    def publish(
        self,
        topic: str,
        *args: Any,
        **kwargs: Any
    ) -> None:
        """Publish message to subscribers of topic."""
        if topic not in self._topics:
            return
        for listener in self._topics[topic]:
            listener(*args, **kwargs)

    async def apublish(
        self,
        topic: str,
        *args: Any,
        **kwargs: Any
    ) -> None:
        """Publish message to subscribers of topic."""
        if topic not in self._topics:
            return
        for listener in self._topics[topic]:
            if iscoroutinecallable(listener):
                await listener(*args, **kwargs)
            else:
                listener(*args, **kwargs)

    async def iter_topic(self, topic: str) -> AsyncIterator[Any]:
        """Asynchronously iterate over messages published to a topic."""
        queue: Queue[Any] = Queue()

        self.subscribe(topic, queue.put_nowait)

        try:
            while True:
                yield await queue.get()
        finally:
            self.unsubscribe(topic, queue.put_nowait)
