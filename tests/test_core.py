import logging
from typing import AsyncIterable, Callable

import pytest
from conftest import emoji

from slipstream import Conf
from slipstream.core import Topic


@pytest.mark.asyncio
async def test_Conf(mocker):
    """Should distribute messages in parallel."""
    Conf().iterables = {}
    c = Conf({'group.id': 'test'})
    assert c.group_id == 'test'  # type: ignore
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


def test_get_iterable():
    """Should return an interable."""
    t = Topic('test', {'group.id': 'test'})
    assert isinstance(aiter(t), AsyncIterable)


def test_get_callable():
    """Should return a callable."""
    t = Topic('test', {})
    assert isinstance(t, Callable)


@pytest.mark.asyncio
async def test_call_fail(mocker, caplog):
    """Should fail to produce message and log an error."""
    mock_producer = mocker.patch(
        'slipstream.core.AIOKafkaProducer',
        autospec=True
    ).return_value
    mock_producer.send_and_wait = mocker.AsyncMock(
        side_effect=RuntimeError('')
    )

    topic, key, value = 'test', '', {}
    t = Topic(topic, {})

    with pytest.raises(RuntimeError, match=''):
        await t(key, value)

    mock_producer.send_and_wait.assert_called_once_with(
        topic,
        key=key.encode(),
        value=value,
        headers=None
    )

    assert f'Error raised while producing to Topic {topic}' in caplog.text


@pytest.mark.asyncio
async def test_aiter_fail(mocker, caplog):
    """Should fail to consume messages and log an error."""
    caplog.set_level(logging.ERROR)
    mock_consumer = mocker.patch(
        'slipstream.core.AIOKafkaConsumer',
        autospec=True
    ).return_value
    mock_consumer.__aiter__ = mocker.Mock(
        side_effect=RuntimeError('')
    )

    topic = 'test'
    t = Topic(topic, {})

    with pytest.raises(RuntimeError, match=''):
        async for _ in t:
            break

    assert f'Error raised while consuming from Topic {topic}' in caplog.text
