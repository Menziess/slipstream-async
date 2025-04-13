from asyncio import Condition, gather, sleep

import pytest
from aiokafka import AIOKafkaClient
from conftest import MockCache

from slipstream.core import Topic
from slipstream.utils import (
    AsyncSynchronizedGenerator,
    PubSub,
    Singleton,
    get_param_names,
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


def test_get_param_names():
    """Should return all parameter names."""
    def f(a, b, c=0, *args, d=0, **kwargs):
        pass

    c = MockCache()
    t = Topic('test')

    assert get_param_names(f) == ('a', 'b', 'c', 'args', 'd', 'kwargs')
    assert get_param_names(c) == ('key', 'val')
    assert get_param_names(t) == ('key', 'value', 'headers', 'kwargs')
    assert 'bootstrap_servers' in get_param_names(AIOKafkaClient)


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

    # Ignore topic without subscribers
    assert PubSub().publish('empty', msg) is None

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


@pytest.mark.asyncio
async def test_AsyncSynchronizedGenerator():
    """Should consume and exhaust generator."""

    async def numbers(n=1):
        for i in range(n):
            await sleep(0.01)
            yield i

    g = AsyncSynchronizedGenerator(numbers())
    gc1 = g.copy()
    gc2 = g.copy()

    # Generators are iterable
    assert aiter(g)
    assert aiter(gc1)
    assert aiter(gc2)

    # Copies readiness matches synchronization
    assert await anext(g) == 0
    assert await anext(gc1) == 0
    assert gc1._is_ready and not gc2._is_ready
    assert await anext(gc2) == 0
    assert gc1._is_ready and gc2._is_ready

    # StopAsyncIteration when generator is exhaused
    with pytest.raises(StopAsyncIteration):
        assert await anext(g)
    with pytest.raises(StopAsyncIteration):
        assert await anext(gc1)
    with pytest.raises(StopAsyncIteration):
        assert await anext(gc2)


@pytest.mark.asyncio
async def test_AsyncSynchronizedGenerator_synchronization(mocker):
    """Should synchronize generator root and copies."""

    async def numbers(n=5):
        for i in range(n):
            await sleep(0.01)
            yield i

    # Rather than blocking and waiting will raise StopAsyncIteration
    cond = mocker.AsyncMock(spec=Condition)
    cond.wait.side_effect = []

    g = AsyncSynchronizedGenerator(numbers())
    g._cond = cond
    gc1 = g.copy()
    gc2 = g.copy()

    # Should all return 0
    assert await anext(g) == 0
    assert await anext(gc1) == 0
    assert await anext(gc2) == 0

    # Progress root, which then waits for copies
    assert await anext(g) == 1
    with pytest.raises(StopAsyncIteration):
        await anext(g)

    # Progress copy 1, which waits alongside root
    assert await anext(gc1) == 1
    with pytest.raises(StopAsyncIteration):
        await anext(gc1)

    # Progress copy 2, which then waits for root
    assert await anext(gc2) == 1
    with pytest.raises(StopAsyncIteration):
        await anext(gc2)

    # Should all return 2
    assert await anext(g) == 2
    assert await anext(gc1) == 2
    assert await anext(gc2) == 2
