"""Slipstream utilities."""

import logging
from asyncio import Queue
from datetime import datetime, timedelta
from inspect import iscoroutinefunction, signature
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Optional,
    TypeAlias,
)

from slipstream.interfaces import ICache, Key

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


class Checkpoint:
    """Pulse the heartbeat of dependency stream to handle downtimes.

    Call `heartbeat` with the event time in the dependency stream.
    Call `check_pulse` in the dependent stream with the event time
    and relevant state such as topic offsets or actual messages.

    When the `downtime_check` observes a downtime using the
    `downtime_threshold`, the `downtime_callback` is called
    and `is_down` is set to `True`.

    When a message is received in the dependency stream, the
    `recovery_callback` is called and `is_down` is set to
    `False` again.
    """

    def __init__(
        self,
        cache: Optional[ICache] = None,
        cache_key: Key = '_',
        downtime_threshold: Any = timedelta(minutes=10),
        downtime_check: Optional[Callable] = None,
        recovery_check: Optional[Callable] = None,
        downtime_callback: Optional[Callable] = None,
        recovery_callback: Optional[Callable] = None
    ):
        """Create instance that tracks downtime of dependency streams.

        If `cache` and `cache_key` are set, the checkpoint information
        is saved and loaded in the cache under `cache_key`.
        """
        self._cache = cache
        self._cache_key = cache_key
        self._downtime_threshold = downtime_threshold
        self._downtime_check = downtime_check or self._default_downtime_check
        self._recovery_check = recovery_check or self._default_recovery_check
        self._downtime_callback = downtime_callback
        self._recovery_callback = recovery_callback
        self.is_down = False

        if cached := self.load():
            self.state = cached['state']
            self.checkpoint_state = cached['checkpoint_state']
        else:
            self.state = None
            self.checkpoint_state = None

    def __iter__(self):
        """Get relevant values when dict is called."""
        yield from {
            k: v for k, v in vars(self).items()
            if not k.startswith('_')
        }.items()

    def save(self):
        """Save dict representation in cache if cache is provided."""
        if not self._cache:
            return
        self._cache[self._cache_key] = dict(self)

    def load(self):
        """Load dict representation from cache if cache is provided."""
        if self._cache:
            return self._cache[self._cache_key]

    def heartbeat(self, timestamp: datetime):
        """Update checkpoint to latest state.

        Call this function whenever a message is processed in the
        dependency stream. Provide the event timestamp that is
        used to restart the dependent stream when the
        recovered dependency stream has caught up.
        """
        self.checkpoint_state = self.state
        self.checkpoint_timestamp = timestamp
        self.save()
        if self.is_down:
            if self._recovery_check(self):
                if self._recovery_callback:
                    self._recovery_callback()
                self.is_down = False

    def check_pulse(self, state, timestamp: datetime):
        """Update state that can be used as checkpoint.

        Call this function whenever a message is processed in the
        dependent stream. Provide relevant state that can be
        used to move back in time to reprocess faulty events
        that were sent out during the downtime.
        """
        self.state = state
        self.state_timestamp = timestamp
        self.save()
        if not self.checkpoint_timestamp:
            return
        if downtime := self._downtime_check(self):
            if self._downtime_callback:
                self._downtime_callback()
            self.is_down = True
            return downtime

    @staticmethod
    def _default_downtime_check(c: 'Checkpoint'):
        """Determine dependency downtime by comparing event timestamps.

        This behavior can be overridden by passing a callable to
        `downtime_check` that takes a `Checkpoint` object.
        """
        diff = c.state_timestamp - c.checkpoint_timestamp
        if diff > c._downtime_threshold:
            return diff
        return None

    @staticmethod
    def _default_recovery_check(c: 'Checkpoint'):
        """Determine dependency has caught up by comparing event timestamps.

        This behavior can be overridden by passing a callable to
        `recovery_check` that takes a `Checkpoint` object.
        """
        return c.checkpoint_timestamp >= c.state_timestamp

    def __repr__(self) -> str:
        """Represent checkpoint."""
        return str(dict(self))
