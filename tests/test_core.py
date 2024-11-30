from typing import AsyncIterable, Callable

import pytest

from slipstream import Conf
from slipstream.core import Topic


@pytest.mark.asyncio
async def test_Conf(mocker):
    """Should distribute messages in parallel."""
    Conf().iterables = set()
    c = Conf({'group.id': 'test'})
    assert c.group_id == 'test'  # type: ignore
    assert c.iterables == set()

    async def messages():
        for emoji in '🏆':
            yield emoji

    # Register iterable
    iterable = range(1)
    iterable_key = str(id(iterable))
    iterable_item = iterable_key, messages()
    c.register_iterable(*iterable_item)
    assert c.iterables == set([iterable_item])

    # Register handler
    stub = mocker.stub(name='handler')

    async def handler(msg, **kwargs):
        stub(msg, kwargs)
    c.register_handler(iterable_key, handler)

    # Start distributing messages and confirm message was received
    await c.start(my_arg='test')
    stub.assert_called_once_with('🏆', {'my_arg': 'test'})


def test_get_iterable():
    """Should return an interable."""
    t = Topic('test', {'group.id': 'test'})
    assert isinstance(aiter(t), AsyncIterable)


def test_get_callable():
    """Should return a callable."""
    t = Topic('test', {})
    assert isinstance(t, Callable)
