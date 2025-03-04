import pytest
from conftest import iterable_to_async

from slipstream import Conf, handle, stream
from slipstream.core import PausableStream


def test_handle():
    """Should register iterable."""
    Conf().iterables = {}

    iterable = iterable_to_async(range(1))
    iterable_key = str(id(iterable))

    @handle(iterable)
    def _(msg):
        return msg

    assert isinstance(Conf().iterables[iterable_key], PausableStream)


@pytest.mark.asyncio
async def test_stream(mocker):
    """Should start distributing messages for each registered iterable."""
    Conf().iterables = {}
    spy = mocker.spy(Conf(), '_distribute_messages')

    it = iterable_to_async(range(1))
    iterable_key = str(id(it))
    Conf().register_iterable(iterable_key, it)

    assert spy.call_count == 0
    await stream()
    assert spy.call_count == 1


@pytest.mark.asyncio
async def test_kwargable_function():
    """Should try to pass kwargs to user defined handler function."""
    my_kwargs = {
        'my_kwarg': 'kwarg value'
    }
    is_kwargable = False
    is_unkwargable = False

    @handle(iterable_to_async(range(1)))
    def kwargable(msg, **kwargs):
        nonlocal is_kwargable
        is_kwargable = kwargs == my_kwargs

    @handle(iterable_to_async(range(1)))
    def unkwargable(msg):
        nonlocal is_unkwargable
        is_unkwargable = msg == 0

    await stream(**my_kwargs)

    assert is_kwargable is True
    assert is_unkwargable is True
