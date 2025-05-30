"""Common testing functionalities."""

import signal
from asyncio import sleep
from collections.abc import AsyncIterable, Generator, Iterable
from contextlib import contextmanager
from typing import Any

import pytest
from testcontainers.kafka import KafkaContainer

from slipstream import Cache
from slipstream.core import Conf
from slipstream.interfaces import ICache, Key
from slipstream.utils import PubSub, Singleton


@pytest.fixture(autouse=True)
def reset_singletons():
    """Clean singletons each test."""
    if PubSub in Singleton._instances:
        pubsub = PubSub()
        for topic, listeners in dict(pubsub._topics).items():
            for listener in listeners:
                pubsub.unsubscribe(topic, listener)
        pubsub._topics = {}  # type: ignore[attr-defined]

    if Conf in Singleton._instances:
        conf = Conf()
        conf.iterables = {}  # type: ignore[attr-defined]
        conf.pipes = {}  # type: ignore[attr-defined]
        conf.exit_hooks = set()  # type: ignore[attr-defined]


@pytest.fixture
def cache() -> Generator[Cache, None]:
    """Get Cache instance that automatically cleans itself."""
    c = Cache('tests/db')
    try:
        yield c
    finally:
        c.close()
        c.destroy()


@pytest.fixture(scope='session')
def kafka():
    """Get running kafka broker."""
    kafka = KafkaContainer().with_kraft()
    try:
        kafka.start()
        yield kafka.get_bootstrap_server()
    finally:
        kafka.stop()


@pytest.fixture
def timeout():
    """Contextmanager that will stop execution of body."""

    @contextmanager
    def set_timeout(seconds: int):
        def raise_timeout(*_):
            err_msg = f'Timeout reached: {seconds}.'
            raise TimeoutError(err_msg)

        signal.signal(signal.SIGALRM, raise_timeout)
        signal.alarm(seconds)

        yield

    return set_timeout


async def iterable_to_async(it: Iterable) -> AsyncIterable:
    """Convert Iterable to AsyncIterable."""
    for msg in it:
        await sleep(0.01)
        yield msg


async def emoji():
    """Demo async iterable."""
    for emoji in '🏆📞🐟👌':
        yield emoji


class MockCache(ICache):
    """Replaces persistent RocksDict for testing purposes."""

    def __init__(self):
        """Set dict as internal store.."""
        self._store = {}

    def __contains__(self, key: Key) -> bool:
        """Key exists in db."""
        return key in self._store

    def __delitem__(self, key: Key) -> None:
        """Delete item from db."""
        del self._store[key]

    def __getitem__(self, key: Key | list[Key]) -> Any:
        """Get item from db or None."""
        return self._store.get(key, None)

    def __setitem__(self, key: Key, val: Any) -> None:
        """Set item in db."""
        self._store[key] = val


@pytest.fixture
def mock_cache():
    """Get cache for testing."""
    return MockCache()
