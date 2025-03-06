from asyncio import gather
from inspect import Parameter, _ParameterKind

import pytest

from slipstream.utils import (
    PubSub,
    Singleton,
    get_params_names,
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


def test_get_params_names():

    def my_func(*a, b=0):
        pass

    assert dict(get_params_names(my_func)) == {
        'a': Parameter('a', _ParameterKind.VAR_POSITIONAL),
        'b': Parameter('b', _ParameterKind.KEYWORD_ONLY, default=0),
    }


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
