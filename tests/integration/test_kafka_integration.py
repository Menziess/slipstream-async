"""Test integration with kafka."""

from datetime import datetime as dt
from datetime import timedelta
from typing import cast

import pytest
from conftest import iterable_to_async

from slipstream import Conf, Topic
from slipstream.checkpointing import Checkpoint, Dependency
from slipstream.codecs import JsonCodec
from slipstream.core import Signal


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

    msg = await next(t)
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
            {'timestamp': dt(2023, 1, 1, 10), 'value': '🌞'},
            {'timestamp': dt(2023, 1, 1, 11), 'value': '⛅'},
            {'timestamp': dt(2023, 1, 1, 12), 'value': '🌦️'},
            {'timestamp': dt(2023, 1, 1, 13), 'value': '🌧'},
        ]
    )
    activity_messages = iter(
        [
            {'timestamp': dt(2023, 1, 1, 10, 30), 'value': 'swimming'},  # 🌞
            {
                'timestamp': dt(2023, 1, 1, 11, 30),
                'value': 'walking home',
            },  # ⛅
            {'timestamp': dt(2023, 1, 1, 12, 30), 'value': 'shopping'},  # 🌦️
            {'timestamp': dt(2023, 1, 1, 13, 10), 'value': 'lunch'},  # 🌧
        ]
    )

    async def next_weather():
        weather = next(weather_messages)
        await c.heartbeat(weather['timestamp'])
        return weather['value']

    async def next_activity():
        msg = await next(t)
        if not (activity := msg.value):
            raise RuntimeError('Missing message value.')
        ts = dt.strptime(activity['timestamp'], '%Y-%m-%d %H:%M:%S')
        return (
            await c.check_pulse(ts, **{str(msg.partition): msg.offset}),
            activity['value'],
        )

    async def recovery_callback(c: Checkpoint, d: Dependency) -> None:
        offset = cast(dict[str, int], d.checkpoint_state)
        await t.seek({int(p): o for p, o in offset.items()})

    c = Checkpoint(
        'c',
        t,
        [
            Dependency(
                'd',
                iterable_to_async(weather_messages),
                downtime_threshold=timedelta(hours=1),
            )
        ],
        recovery_callback=recovery_callback,
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
