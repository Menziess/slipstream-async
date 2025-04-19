"""Core tests."""

import logging
from collections.abc import AsyncIterable, Callable

import pytest
from conftest import emoji
from pytest_mock import MockerFixture

from slipstream import Conf
from slipstream.core import Topic, _sink_output, handle, stream


@pytest.mark.asyncio
async def test_conf(mocker: MockerFixture) -> None:
    """Should distribute messages in parallel."""
    Conf().iterables = {}
    c = Conf({'group.id': 'test'})
    assert c.group_id == 'test'  # type: ignore[attr-defined]
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


def test_get_iterable() -> None:
    """Should return an interable."""
    t = Topic('test', {'group.id': 'test'})
    assert isinstance(aiter(t), AsyncIterable)


def test_get_callable() -> None:
    """Should return a callable."""
    t = Topic('test', {})
    assert isinstance(t, Callable)


@pytest.mark.asyncio
async def test_call_fail(mocker: MockerFixture, caplog) -> None:
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
async def test_aiter_fail(mocker, caplog) -> None:
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
async def test_sink_output(mocker: MockerFixture) -> None:
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
async def test_handle(mocker: MockerFixture) -> None:
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
