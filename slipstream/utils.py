"""Slipstream utilities."""

from asyncio import Condition, Queue
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from enum import Enum
from inspect import iscoroutinefunction, signature
from typing import (
    Any,
    ClassVar,
    ParamSpec,
    TypeAlias,
    TypeVar,
)

T = TypeVar('T')
P = ParamSpec('P')

AsyncCallable: TypeAlias = Callable[P, T | Awaitable[T]]
Pipe: TypeAlias = Callable[[AsyncIterable[Any]], AsyncIterable[Any]]


class Signal(Enum):
    """Signals can be exchanged with streams.

    SENTINEL represents an absent yield value
    PAUSE    represents the signal to pause stream
    RESUME   represents the signal to resume stream
    """

    SENTINEL = 0
    PAUSE = 1
    RESUME = 2
    STOP = 3


async def awaitable(x: Any) -> Any:
    """Convert into awaitable."""
    return await x if isinstance(x, Awaitable) else x


def iscoroutinecallable(o: Any) -> bool:
    """Check whether object is coroutine."""
    call = o.__call__ if callable(o) else None  # type: ignore[attr-defined]
    return iscoroutinefunction(o) or iscoroutinefunction(call)


def get_param_names(o: Any) -> tuple[str, ...]:
    """Return function parameter names."""
    params = signature(o).parameters
    return tuple(params.keys())


class Singleton(type):
    """Maintain a single instance of a class."""

    _instances: ClassVar[dict[type, Any]] = {}

    def __call__(cls: type[T], *args: Any, **kwargs: Any) -> T:
        """Apply metaclass singleton action."""
        if cls not in Singleton._instances:
            Singleton._instances[cls] = super().__call__(*args, **kwargs)
        instance = Singleton._instances[cls]
        if hasattr(instance, '__update__'):
            instance.__update__(*args, **kwargs)
        return instance


class PubSub(metaclass=Singleton):
    """Singleton publish subscribe pattern class."""

    _topics: ClassVar[dict[str, list[AsyncCallable]]] = {}

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

    def publish(self, topic: str, *args: Any, **kwargs: Any) -> None:
        """Publish message to subscribers of topic."""
        if topic not in self._topics:
            return
        for listener in self._topics[topic]:
            listener(*args, **kwargs)

    async def apublish(self, topic: str, *args: Any, **kwargs: Any) -> None:
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


class AsyncSynchronizedGenerator:
    """Async generator that synchronizes values across copies."""

    __slots__ = ('_cond', '_copies', '_iterator', '_value')

    @property
    def value(self) -> Any:
        """Get current value the generator is holding."""
        return self._value

    def __init__(self, gen: AsyncIterable[Any]) -> None:
        """Create instance of synchronized async generator."""
        self._iterator: AsyncIterator[Any] = aiter(gen)
        self._cond: Condition = Condition()
        self._value: Any | Signal = Signal.SENTINEL
        self._copies: list[_GeneratorCopy] = []

    def __aiter__(self) -> AsyncIterator[Any]:
        """Return self as iterator."""
        return self

    async def __anext__(self) -> Any:
        """Return next value from generator if copies are ready."""
        async with self._cond:
            while any(not copy.is_ready for copy in self._copies):
                await self._cond.wait()
            try:
                self._value = await self._iterator.__anext__()
                for copy in self._copies:
                    copy.is_ready = False
            except StopAsyncIteration:
                self._value = Signal.STOP
                self._cond.notify_all()
                raise
            self._cond.notify_all()
            return self._value

    def copy(self) -> '_GeneratorCopy':
        """Create a synchronized copy of this generator."""
        copy = _GeneratorCopy(self, self._cond)
        self._copies.append(copy)
        return copy


class _GeneratorCopy:
    """Synchronized copy of an async generator."""

    __slots__ = ('_cond', '_is_ready', '_root')

    @property
    def is_ready(self) -> bool:
        """Get readiness status of copy."""
        return self._is_ready

    @is_ready.setter
    def is_ready(self, value: bool) -> None:
        """Set readiness status of copy."""
        self._is_ready = value

    def __init__(
        self,
        root: AsyncSynchronizedGenerator,
        cond: Condition,
    ) -> None:
        """Create copy of synchronized async generator."""
        self._root: AsyncSynchronizedGenerator = root
        self._cond: Condition = cond
        self._is_ready: bool = True

    def __aiter__(self) -> AsyncIterator[Any]:
        """Return self as iterator."""
        return self

    async def __anext__(self) -> Any:
        """Return next value from root generator."""
        async with self._cond:
            while self._root.value is Signal.SENTINEL or (
                self._is_ready and self._root.value is not Signal.STOP
            ):
                await self._cond.wait()
            if self._root.value is Signal.STOP:
                raise StopAsyncIteration
            self._is_ready = True
            self._cond.notify_all()
            return self._root.value
