from asyncio import gather

import pytest
from aiokafka import AIOKafkaClient
from conftest import MockCache

from slipstream.core import Topic
from slipstream.utils import (
    PubSub,
    Singleton,
    get_params,
    get_positional_params,
    iscoroutinecallable,
)


def test_iscoroutinecallable():

    def _s():
        return True

    async def _a():
        return True

    class _A:
        async def __call__(self):
            return True

    assert not iscoroutinecallable(_s)
    assert iscoroutinecallable(_a)
    assert iscoroutinecallable(_A)


def test_get_positional_params():
    """Should return strictly positional parameter names."""
    def f(a, b, c=0, *args, d=0, **kwargs):
        pass

    c = MockCache()
    t = Topic('test')

    assert get_positional_params(f) == ('a', 'b')
    assert get_positional_params(c) == ('key', 'val')
    assert get_positional_params(t) == ('key', 'value')
    assert get_positional_params(AIOKafkaClient) == ()


def test_get_params():
    """Should return all parameter names."""
    def f(a, b, c=0, *args, d=0, **kwargs):
        pass

    c = MockCache()
    t = Topic('test')

    assert get_params(f) == ('a', 'b', 'c', 'args', 'd', 'kwargs')
    assert get_params(c) == ('key', 'val')
    assert get_params(t) == ('key', 'value', 'headers', 'kwargs')
    assert 'bootstrap_servers' in get_params(AIOKafkaClient)


def test_Singleton():
    """Should maintain a single instance of a class."""
    class MySingleton(metaclass=Singleton):
        def __update__(self):
            pass

    a = MySingleton()
    b = MySingleton()

    assert a is b


@pytest.mark.asyncio
async def test_PubSub():
    """Should succesfully send and receive data."""
    topic, count, msg = 'test_PubSub', 0, {'msg': 'hi'}

    def handler(x):
        nonlocal count
        count += 1
        assert x == msg

    PubSub().subscribe(topic, handler)

    PubSub().publish(topic, msg)
    await PubSub().apublish(topic, msg)

    async def analyze_iter_topic():
        async for x in PubSub().iter_topic(topic):
            nonlocal count
            count += 1
            assert x == msg
            break

    await gather(
        analyze_iter_topic(),
        PubSub().apublish(topic, msg),
    )

    PubSub().unsubscribe(topic, handler)

    assert topic not in PubSub._topics
    assert count == 4
