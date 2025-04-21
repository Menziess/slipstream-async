"""Core tests."""

import logging
from asyncio import gather, sleep
from collections.abc import AsyncIterable, AsyncIterator, Callable

import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition
from conftest import emoji
from pytest_mock import MockerFixture

from slipstream import Conf
from slipstream.codecs import JsonCodec
from slipstream.core import (
    READ_FROM_END,
    READ_FROM_START,
    PausableStream,
    Topic,
    _sink_output,
    handle,
    stream,
)
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
    assert c.conf['group.id'] == 'test'
    assert c.__getattr__('group.id') == 'test'
    assert c.iterables == {}

    # Register iterable
    iterable = emoji()
    iterable_key = str(id(iterable))
    iterable_item = iterable_key, iterable
    c.register_iterable(*iterable_item)

    # Register handler
    stub = mocker.stub(name='handler')

    async def handler(msg, **kwargs):
        stub(msg, kwargs)

    c.register_handler(iterable_key, handler)

    # Register exit_hook
    await_hook = mocker.AsyncMock()
    c.register_exit_hook(await_hook)

    # Start distributing messages and confirm message was received
    await c.start(my_arg='test')
    assert stub.call_args_list == [
        mocker.call('🏆', {'my_arg': 'test'}),
        mocker.call('📞', {'my_arg': 'test'}),
        mocker.call('🐟', {'my_arg': 'test'}),
        mocker.call('👌', {'my_arg': 'test'}),
    ]

    await_hook.assert_called_once()


@pytest.mark.asyncio
async def test_conf_keyboardinterrupt(mocker: MockerFixture):
    """Should not raise on KeyboardInterrupt."""
    c = Conf()
    awaitable = mocker.AsyncMock(side_effect=KeyboardInterrupt)
    mocker.patch('slipstream.core.gather', awaitable)

    await c.start()

    awaitable.assert_called_once()


@pytest.mark.asyncio
async def test_conf_exception(mocker: MockerFixture):
    """Should raise on Exception."""
    c = Conf()
    awaitable = mocker.AsyncMock(side_effect=ValueError('test'))
    mocker.patch('slipstream.Conf._distribute_messages', awaitable)

    # Register iterable
    iterable = emoji()
    iterable_key = str(id(iterable))
    iterable_item = iterable_key, iterable
    c.register_iterable(*iterable_item)

    with pytest.raises(ValueError, match='test'):
        await c.start()

    awaitable.assert_awaited_once()


def test_get_iterable():
    """Should return an interable."""
    t = Topic('test', {'group.id': 'test'})
    assert isinstance(aiter(t), AsyncIterable)


def test_get_callable():
    """Should return a callable."""
    t = Topic('test', {'security_protocol': 'SASL_SSL'})
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
async def test_topic_seek(mocker):
    """Should seek to offset."""
    t = Topic('test')
    c = mocker.Mock(spec=AIOKafkaConsumer)
    c.partitions_for_topic.return_value = {0, 1}
    c.assignment.return_value = {
        TopicPartition('test', 0),
    }

    assert await t.admin

    with pytest.raises(RuntimeError, match='No consumer provided'):
        await t.seek(-1)

    with pytest.raises(ValueError, match='Offset must be bigger than -3'):
        await t.seek(-3, consumer=c)

    with pytest.raises(RuntimeError, match='Failed to assign'):
        await t.seek(READ_FROM_START, consumer=c, timeout=0)

    c.assignment.return_value = {
        TopicPartition('test', 0),
        TopicPartition('test', 1),
    }

    await t.seek(READ_FROM_START, consumer=c)
    await t.seek(READ_FROM_END, consumer=c)

    c.assignment.side_effect = [
        {
            TopicPartition('test', 0),
        },
        {
            TopicPartition('test', 0),
            TopicPartition('test', 1),
        },
    ]

    await t.seek({0: 0, 1: 0}, consumer=c)


@pytest.mark.parametrize('raise_error', [True, False])
@pytest.mark.asyncio
async def test_topic_get_consumer(raise_error, mocker):
    """Should get started instance of Kafka consumer."""
    t = Topic('test', codec=JsonCodec(), offset=0)
    c = mocker.AsyncMock(spec=AIOKafkaConsumer)
    mocker.patch('slipstream.core.AIOKafkaConsumer', return_value=c)

    assert await t.admin

    if raise_error:
        mocker.patch.object(
            t, 'seek', mocker.AsyncMock(side_effect=RuntimeError('oops'))
        )

    if raise_error:
        with pytest.raises(RuntimeError, match='oops'):
            await t.get_consumer()
    else:
        assert await t.get_consumer()
        c.start.assert_called_once()


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
