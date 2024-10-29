"""Slipstream utilities."""

import logging
from asyncio import gather, iscoroutinefunction
from inspect import signature
from typing import Any, Callable

logger = logging.getLogger(__name__)


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

    def __init__(cls, name, bases, dct):
        """Perform checks before instantiation."""
        if '__update__' not in dct:
            raise TypeError('Expected __update__.')

    def __call__(cls, *args, **kwargs):
        """Apply metaclass singleton action."""
        if cls not in cls._instances:
            cls._instances[cls] = super(
                Singleton, cls).__call__(*args, **kwargs)
        instance = cls._instances[cls]
        instance.__update__(*args, **kwargs)
        return instance


class PubSub:
    """Basic publish subscribe pattern."""

    def __init__(self):
        self._topics: dict[str, list[Callable]] = {}

    def subscribe(self, topic: str, listener: Callable) -> None:
        if topic not in self._topics:
            self._topics[topic] = []
        self._topics[topic].append(listener)

    def unsubscribe(self, topic: str, listener: Callable) -> None:
        if topic in self._topics:
            self._topics[topic].remove(listener)
            if not self._topics[topic]:
                del self._topics[topic]

    async def publish(self, topic: str, *args, **kwargs) -> None:
        if topic not in self._topics:
            return
        tasks = []
        for listener in self._topics[topic]:
            if iscoroutinecallable(listener):
                tasks.append(listener(*args, **kwargs))
            else:
                listener(*args, **kwargs)
            if tasks:
                await gather(*tasks)
