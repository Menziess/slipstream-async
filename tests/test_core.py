"""Core tests."""

import logging
from asyncio import gather, sleep
from collections.abc import AsyncIterable, AsyncIterator, Callable

import pytest
from conftest import emoji
from pytest_mock import MockerFixture

from slipstream import Conf
from slipstream.core import PausableStream, Topic, _sink_output, handle, stream
from slipstream.utils import Signal


@pytest.mark.asyncio
async def test_pausablestream():
    """Should consume data from iterable."""
    iterable = emoji()
    stream = PausableStream(iterable)
    assert stream.iterable == iterable
    assert stream.signal is None
    assert stream.running.is_set()

    assert stream._iterator is None
    assert aiter(stream) == stream
    assert isinstance(stream._iterator, AsyncIterator)

    assert await anext(stream) == '🏆'

    stream.send_signal(Signal.PAUSE)
    assert stream.signal is Signal.PAUSE
    assert not stream.running.is_set()


@pytest.mark.asyncio
async def test_pausablestream_asyncgenerator(mocker: MockerFixture):
    """Should propagate signal when pausing/resuming generator."""
    iterable = mocker.Mock()
    iterable.asend = mocker.AsyncMock(return_value='🏆')
    iterable.__aiter__ = mocker.Mock(return_value=iterable)

    # A generator yields values
    stream = PausableStream(iterable)
    assert aiter(stream) == stream
    assert await anext(stream) == '🏆'

    # It will also be pausable
    spy = mocker.spy(stream.running, 'wait')
    stream.send_signal(Signal.PAUSE)

    # And resumeable
    async def resume_stream():
        await sleep(0.1)
        stream.send_signal(Signal.RESUME)

    await gather(anext(stream), resume_stream())
    spy.assert_called_once()

    # Signals will be sent to the generator
    assert await anext(stream) == '🏆'
    assert iterable.asend.await_args_list == [
        mocker.call(None),
        mocker.call(Signal.PAUSE),
        mocker.call(Signal.RESUME),
    ]


@pytest.mark.asyncio
async def test_pausablestream_iterator(mocker: MockerFixture):
    """Should not propagate signal when pausing/resuming iterator."""
    iterable = mocker.Mock()
    del iterable.asend
    iterable.__anext__ = mocker.AsyncMock(return_value='🏆')
    iterable.__aiter__ = mocker.Mock(return_value=iterable)

    # A regular iterator also yields values
    stream = PausableStream(iterable)
    assert aiter(stream) == stream
    assert await anext(stream) == '🏆'

    # It will also be pausable
    spy = mocker.spy(stream.running, 'wait')
    stream.send_signal(Signal.PAUSE)

    # And resumeable
    async def resume_stream():
        await sleep(0.1)
        stream.send_signal(Signal.RESUME)

    await gather(anext(stream), resume_stream())
    spy.assert_called_once()

    # But if will not receive signals
    assert await anext(stream) == '🏆'
    iterable.__anext__.assert_called()


def test_conf_init():
    """Should set singleton initial conf."""
    Conf.__init__(Conf(), {'group.id': 'test'})
    assert Conf().conf == {'group.id': 'test'}


@pytest.mark.asyncio
async def test_conf(mocker: MockerFixture):
    """Should distribute messages in parallel."""
    c = Conf({'group.id': 'test'})
    assert c.group_id == 'test'
    assert c.iterables == {}

    # Register iterable
    iterable = range(1)
    iterable_key = str(id(iterable))
    iterable_item = iterable_key, emoji()
    c.register_iterable(*iterable_item)

    # Register handler
    stub = mocker.stub(name='handler')

    async def handler(msg, **kwargs):
        stub(msg, kwargs)

    c.register_handler(iterable_key, handler)

    # Start distributing messages and confirm message was received
    await c.start(my_arg='test')
    assert stub.call_args_list == [
        mocker.call('🏆', {'my_arg': 'test'}),
        mocker.call('📞', {'my_arg': 'test'}),
        mocker.call('🐟', {'my_arg': 'test'}),
        mocker.call('👌', {'my_arg': 'test'}),
    ]


@pytest.mark.asyncio
async def test_conf_keyboardinterrupt(mocker: MockerFixture):
    """Should not raise on KeyboardInterrupt."""
    awaitable = mocker.AsyncMock(side_effect=KeyboardInterrupt)
    mocker.patch('slipstream.core.gather', awaitable)
    c = Conf()

    # Register iterable
    iterable = range(1)
    iterable_key = str(id(iterable))
    iterable_item = iterable_key, emoji()
    c.register_iterable(*iterable_item)

    await c.start()
    awaitable.assert_awaited_once()


def test_get_iterable():
    """Should return an interable."""
    t = Topic('test', {'group.id': 'test'})
    assert isinstance(aiter(t), AsyncIterable)


def test_get_callable():
    """Should return a callable."""
    t = Topic('test', {})
    assert isinstance(t, Callable)


@pytest.mark.asyncio
async def test_call_fail(mocker: MockerFixture, caplog):
    """Should fail to produce message and log an error."""
    mock_producer = mocker.patch(
        'slipstream.core.AIOKafkaProducer',
        autospec=True,
    ).return_value
    mock_producer.send_and_wait = mocker.AsyncMock(
        side_effect=RuntimeError('Something went wrong'),
    )

    topic, key, value = 'test', '', {}
    t = Topic(topic, {})

    with pytest.raises(RuntimeError, match='Something went wrong'):
        await t(key, value)

    mock_producer.send_and_wait.assert_called_once_with(
        topic,
        key=key.encode(),
        value=value,
        headers=None,
    )

    assert f'Error while producing to Topic {topic}' in caplog.text


@pytest.mark.asyncio
async def test_aiter_fail(mocker, caplog):
    """Should fail to consume messages and log an error."""
    caplog.set_level(logging.ERROR)
    mock_consumer = mocker.patch(
        'slipstream.core.AIOKafkaConsumer',
        autospec=True,
    ).return_value
    mock_consumer.__aiter__ = mocker.Mock(side_effect=RuntimeError(''))

    topic = 'test'
    t = Topic(topic, {})

    with pytest.raises(RuntimeError, match=''):
        await next(t)

    assert f'Error while consuming from Topic {topic}' in caplog.text


@pytest.mark.asyncio
async def test_sink_output(mocker: MockerFixture):
    """Should return the output of the sink function."""

    def src():
        pass

    stub = mocker.stub(name='handler')

    def sync_f(x):
        stub(x)

    await _sink_output(src, sync_f, (1, 2))
    stub.assert_called_once_with((1, 2))
    stub.reset_mock()

    async def async_f(x):
        stub(x)

    await _sink_output(src, async_f, (1, 2))
    stub.assert_called_once_with((1, 2))
    stub.reset_mock()


@pytest.mark.asyncio
async def test_handle(mocker: MockerFixture):
    """Should decorate handler and register iterables, handlers, pipes."""

    async def number():
        yield {'val': 123}

    source = number()

    async def pipe(s):
        async for _ in s:
            yield _['val']

    async def handler(msg, **_):
        return msg

    sink = mocker.AsyncMock()

    handle(source, pipe=[pipe], sink=[sink])(handler)

    c = Conf()
    it_key = str(id(source))
    assert c.iterables[it_key].iterable == source
    pipes = list(c.pipes.items())
    assert pipes[0][1] == (it_key, (pipe,))

    await stream()

    sink.assert_called_once_with(123)
