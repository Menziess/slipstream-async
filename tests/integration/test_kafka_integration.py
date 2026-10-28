"""Test integration with kafka."""

from asyncio import Event, create_task, gather, sleep
from datetime import datetime as dt
from datetime import timedelta, timezone
from typing import cast

import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from conftest import iterable_to_async

from slipstream import Conf, Topic, handle, stream
from slipstream.checkpointing import Checkpoint, Dependency
from slipstream.codecs import JsonCodec
from slipstream.core import Signal

UTC = timezone.utc


@pytest.mark.asyncio
async def test_produce_consume(kafka):
    """Should be able to exchange messages with kafka."""
    t = Topic(
        'test_produce_consume',
        {
            'bootstrap_servers': kafka,
            'auto_offset_reset': 'earliest',
            'group_instance_id': 'test_produce_consume',
            'group_id': 'test_produce_consume',
        },
    )

    await t(b'a', b'a')
    await t(b'b', b'b')
    await t(b'c', b'c')

    msg = await anext(t)
    assert msg.value == 'a'

    expected = ['b', 'c']
    async for msg in t:
        assert msg.value == expected.pop(0)
        if len(expected) == 0:
            break

    assert len(expected) == 0


@pytest.mark.asyncio
async def test_checkpoint_reprocessing(kafka, timeout):
    """Should be able to reprocess erroneous data."""
    t = Topic(
        'test_checkpoint_reprocessing',
        {
            'bootstrap_servers': kafka,
            'auto_offset_reset': 'earliest',
            'group_instance_id': 'test_checkpoint_reprocessing',
            'group_id': 'test_checkpoint_reprocessing',
        },
        codec=JsonCodec(),
    )
    Conf().register_iterable(str(id(t)), t)
    pausable_stream = Conf().iterables[str(id(t))]

    weather_messages = iter(
        [
            {'timestamp': dt(2023, 1, 1, 10, tzinfo=UTC), 'value': '🌞'},
            {'timestamp': dt(2023, 1, 1, 11, tzinfo=UTC), 'value': '⛅'},
            {'timestamp': dt(2023, 1, 1, 12, tzinfo=UTC), 'value': '🌦️'},
            {'timestamp': dt(2023, 1, 1, 13, tzinfo=UTC), 'value': '🌧'},
        ],
    )
    activity_messages = iter(
        [
            {
                'timestamp': dt(2023, 1, 1, 10, 30, tzinfo=UTC),
                'value': 'swimming',
            },  # 🌞
            {
                'timestamp': dt(2023, 1, 1, 11, 30, tzinfo=UTC),
                'value': 'walking home',
            },  # ⛅
            {
                'timestamp': dt(2023, 1, 1, 12, 30, tzinfo=UTC),
                'value': 'shopping',
            },  # 🌦️
            {
                'timestamp': dt(2023, 1, 1, 13, 10, tzinfo=UTC),
                'value': 'lunch',
            },  # 🌧
        ],
    )

    async def next_weather():
        weather = next(weather_messages)
        await c.heartbeat(weather['timestamp'])
        return weather['value']

    async def next_activity():
        msg = await anext(t)
        if not (activity := msg.value):
            err_msg = 'Missing message value.'
            raise RuntimeError(err_msg)
        ts = dt.strptime(
            activity['timestamp'],
            '%Y-%m-%d %H:%M:%S%z',
        ).astimezone(UTC)
        return (
            await c.check_pulse(ts, **{str(msg.partition): msg.offset}),
            activity['value'],
        )

    async def recovery_callback(_: Checkpoint, d: Dependency) -> None:
        offset = cast('dict[str, int]', d.checkpoint_state)
        await t.seek({int(p): o for p, o in offset.items()})

    c = Checkpoint(
        t,
        Dependency(
            'd',
            iterable_to_async(weather_messages),
            downtime_threshold=timedelta(hours=1),
            marker='timestamp',
        ),
        name='c',
        marker='timestamp',
        on_recovery=recovery_callback,
    )

    with timeout(seconds=5):
        # Send all activities to Kafka
        for activity in activity_messages:
            await t(None, activity)

        # '🌞'
        assert await next_weather() == '🌞'

        # ('🌞', 'swimming')
        assert await next_activity() == (None, 'swimming')

        # '⛅'
        assert await next_weather() == '⛅'

        # ('⛅', 'walking home')
        assert await next_activity() == (None, 'walking home')

        # ('⛅', 'shopping')        <- wrongly enriched with stale data
        # Downtime of 5400s is detected, pausing activity stream
        assert await next_activity() == (timedelta(seconds=5400), 'shopping')
        assert c.dependencies['d'].is_down is True
        assert pausable_stream.signal == Signal.PAUSE

        # '🌦️'                      <- the weather stream recovers
        assert await next_weather() == '🌦️'

        # '🌧'                      <- the weather stream catches up
        assert await next_weather() == '🌧'
        assert c.dependencies['d'].is_down is False
        assert pausable_stream.signal == Signal.RESUME

        # ('🌦️', 'shopping')        <- correction
        # ('🌧', 'lunch')
        assert await next_activity() == (None, 'shopping')
        assert await next_activity() == (None, 'lunch')


@pytest.mark.asyncio
async def test_topic_pause_poll_keeps_group_and_offset(kafka, timeout):
    """Should stay in the group while paused and not skip records."""
    t = Topic(
        'test_pause_poll_group',
        {
            'bootstrap_servers': kafka,
            'auto_offset_reset': 'earliest',
            'group_id': 'test_pause_poll_group',
            'group_instance_id': 'test_pause_poll_group',
            'enable_auto_commit': False,
            'max_poll_interval_ms': 10000,
            'session_timeout_ms': 6000,
            'heartbeat_interval_ms': 2000,
        },
    )
    await t(b'k', b'a')
    await t(b'k', b'b')
    await t(b'k', b'c')
    with timeout(seconds=40):
        assert (await t.asend(None)).value == 'a'
        for _ in range(5):
            assert await t.asend(Signal.PAUSE) is Signal.SENTINEL
        assert (await t.asend(Signal.RESUME)).value == 'b'
        assert (await t.asend(None)).value == 'c'


def _topic(kafka: str, name: str, **conf) -> Topic:
    topic = Topic(
        name,
        {
            'bootstrap_servers': kafka,
            'auto_offset_reset': 'earliest',
            'group_id': name,
            **conf,
        },
    )
    topic.commit_handled = True
    return topic


@pytest.mark.asyncio
async def test_message_in_flight_is_redelivered_after_shutdown(kafka, timeout):
    """Should not skip the message that was being handled on shutdown."""
    name = 'test_in_flight_redelivered'
    t = _topic(kafka, name)
    for value in (b'a', b'b', b'c'):
        await t(b'k', value)

    with timeout(seconds=60):
        assert (await anext(t)).value == 'a'
        await t.exit_hook()

        t = _topic(kafka, name)
        assert (await anext(t)).value == 'a'
        assert (await anext(t)).value == 'b'
        await t.exit_hook()

        t = _topic(kafka, name)
        assert (await anext(t)).value == 'b'
        await t.exit_hook()


@pytest.mark.asyncio
async def test_handler_error_does_not_commit_failed_message(kafka, timeout):
    """Should process the failed message again after the stream crashed."""
    name = 'test_handler_error_not_committed'
    t = _topic(kafka, name)
    for value in (b'a', b'b'):
        await t(b'k', value)
    seen = []

    @handle(t)
    def handler(msg):
        seen.append(msg.value)
        if msg.value == 'b':
            err_msg = 'poison'
            raise ValueError(err_msg)

    with timeout(seconds=60), pytest.raises(ValueError, match='poison'):
        await stream()

    assert seen == ['a', 'b']

    seen.clear()
    t = _topic(kafka, name)
    assert (await anext(t)).value == 'b'
    await t.exit_hook()


@pytest.mark.asyncio
async def test_produce_timeout_when_broker_stops_responding(
    kafka, kafka_container, timeout
):
    """Should raise instead of waiting forever on an idempotent producer."""
    t = _topic(
        kafka,
        'test_produce_timeout',
        enable_idempotence=True,
        produce_timeout=3,
        request_timeout_ms=3000,
    )
    container = kafka_container.get_wrapped_container()

    with timeout(seconds=60):
        await t(b'k', b'before')
        container.pause()
        try:
            with pytest.raises(RuntimeError, match='not acknowledged'):
                await t(b'k', b'during')
        finally:
            container.unpause()
        await t.exit_hook()


@pytest.mark.asyncio
async def test_rebalance_does_not_lose_messages(kafka, timeout):
    """Should deliver every message when a second consumer joins."""
    name = 'test_rebalance'
    admin = AIOKafkaAdminClient(bootstrap_servers=kafka)
    await admin.start()
    await admin.create_topics([NewTopic(name, 2, 1)])
    await admin.close()

    a = _topic(kafka, name, session_timeout_ms=6000)
    for partition in (0, 1):
        for i in range(3):
            await a(b'k', f'{partition}-{i}'.encode(), partition=partition)
    expected = {f'{p}-{i}' for p in (0, 1) for i in range(3)}

    seen: dict[str, set[str]] = {'a': set(), 'b': set()}
    done = Event()

    async def consume(t: Topic, who: str) -> None:
        async for msg in t:
            seen[who].add(cast('str', msg.value))
            if seen['a'] | seen['b'] == expected and seen['b']:
                done.set()
            await sleep(0.05)

    with timeout(seconds=90):
        first = await anext(a)
        seen['a'].add(cast('str', first.value))
        b = _topic(kafka, name, session_timeout_ms=6000)
        tasks = [create_task(consume(a, 'a')), create_task(consume(b, 'b'))]
        await done.wait()
        for task in tasks:
            task.cancel()
        await gather(*tasks, return_exceptions=True)
        await a.exit_hook()
        await b.exit_hook()

        assert seen['a'] | seen['b'] == expected
        assert seen['b']


@pytest.mark.asyncio
async def test_idle_stream_commits_handled_messages(kafka, timeout):
    """Should commit the last messages of a burst without a new message."""
    name = 'test_idle_commit'
    t = _topic(kafka, name, auto_commit_interval_ms=500)
    await t(b'k', b'a')
    await t(b'k', b'b')

    with timeout(seconds=60):
        assert (await anext(t)).value == 'a'
        waiting = create_task(anext(t))
        assert (await waiting).value == 'b'
        waiting = create_task(anext(t))
        await sleep(2)

        reader = AIOKafkaConsumer(bootstrap_servers=kafka, group_id=name)
        await reader.start()
        try:
            committed = await reader.committed(TopicPartition(name, 0))
        finally:
            await reader.stop()
        waiting.cancel()
        await t.exit_hook()

    assert committed == 2
