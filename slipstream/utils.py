"""Slipstream utilities."""

import logging
from asyncio import Queue
from inspect import iscoroutinefunction, signature
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    TypeAlias,
)

logger = logging.getLogger(__name__)

AsyncCallable: TypeAlias = Callable[..., Awaitable[Any]] | Callable[..., Any]


def iscoroutinecallable(o: Any) -> bool:
    """Check whether function is coroutine."""
    return iscoroutinefunction(o) or (
        hasattr(o, '__call__')
        and iscoroutinefunction(o.__call__)
    )


def get_params_names(o: Any):
    """Return function parameters."""
    parameters = signature(o).parameters.values()
    return getattr(parameters, 'mapping')


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
