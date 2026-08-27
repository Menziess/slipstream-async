"""Checkpointing tests."""

from asyncio import Event, create_task, sleep
from datetime import datetime, timedelta, timezone

import pytest
from conftest import emoji, iterable_to_async

from slipstream.checkpointing import (
    Checkpoint,
    Dependency,
    coerce_marker,
    consumer_at_end,
    consumer_lag_downtime,
    consumer_lag_recovery,
)
from slipstream.core import Conf, Signal, Topic, handle, stream

UTC = timezone.utc


@pytest.fixture
def dependency():
    """Dependency instance."""
    return Dependency('emoji', emoji())


@pytest.fixture
def checkpoint(mock_cache):
    """Checkpoint instance."""

    async def dependent():
        yield {
            'event_timestamp': datetime(2025, 1, 1, 10, tzinfo=UTC),
        }

    async def dependency():
        yield {
            'event_timestamp': datetime(2025, 1, 1, 10, tzinfo=UTC),
        }

    dep = Dependency('dependency', dependency())

    return Checkpoint(
        dependent(),
        dep,
        name='test',
        cache=mock_cache,
        marker='event_timestamp',
    )


def test_dependency_init(dependency):
    """Should properly initialize dependency."""
    assert dependency.name == 'emoji'
    assert dependency.checkpoint_state is None
    assert dependency.checkpoint_marker is None
    assert isinstance(dependency.downtime_threshold, timedelta)
    assert dependency.is_down is False


def test_dependency_save_and_load(mock_cache, dependency):
    """Should save and load dependency using cache."""
    checkpoint_state = {'offset': 1}
    checkpoint_marker = datetime(2025, 1, 1, 10, tzinfo=UTC)
    dependency.save(
        mock_cache,
        '_prefix_',
        checkpoint_state,
        checkpoint_marker,
    )

    loaded_dep = Dependency('emoji', iterable_to_async([]))
    loaded_dep.load(mock_cache, '_prefix_')

    assert loaded_dep.checkpoint_state == checkpoint_state
    assert loaded_dep.checkpoint_marker == checkpoint_marker


@pytest.mark.asyncio
async def test_default_downtime_check(dependency):
    """Should check for datetime diff surpassing threshold."""
    checkpoint = Checkpoint(
        iterable_to_async([]),
        dependency,
        name='test',
        marker='timestamp',
    )

    checkpoint.state_marker = 'not-datetime'
    dependency.checkpoint_marker = 'not-datetime'
    with pytest.raises(TypeError, match='Expecting either `datetime`'):
        dependency._default_downtime_check(checkpoint, dependency)

    checkpoint.state_marker = datetime(2025, 1, 1, 11, tzinfo=UTC)
    dependency.checkpoint_marker = datetime(2025, 1, 1, 10, tzinfo=UTC)
    downtime = dependency._default_downtime_check(checkpoint, dependency)
    assert isinstance(downtime, timedelta)
    assert downtime == timedelta(hours=1)


@pytest.mark.asyncio
async def test_default_recovery_check(dependency):
    """Should check surpassing datetime is true."""
    checkpoint = Checkpoint(
        iterable_to_async([]),
        dependency,
        name='test',
        marker='timestamp',
    )

    checkpoint.state_marker = 'not-datetime'
    dependency.checkpoint_marker = 'not-datetime'
    with pytest.raises(TypeError, match='Expecting either `datetime`'):
        dependency._default_recovery_check(checkpoint, dependency)

    checkpoint.state_marker = datetime(2025, 1, 1, 10, tzinfo=UTC)
    dependency.checkpoint_marker = datetime(2025, 1, 1, 11, tzinfo=UTC)
    recovered = dependency._default_recovery_check(checkpoint, dependency)
    assert recovered is True


def test_checkpoint_init(checkpoint):
    """Should properly initialize checkpoint."""
    assert checkpoint.name == 'test'
    assert isinstance(checkpoint.dependencies, dict)
    assert 'dependency' in checkpoint.dependencies
    assert checkpoint.state == {}
    assert checkpoint.state_marker is None


@pytest.mark.asyncio
async def test_heartbeat_single_dependency(checkpoint):
    """Should correctly update dependency data."""
    marker = datetime(2025, 1, 1, 10, 30, tzinfo=UTC)
    await checkpoint.heartbeat(marker)

    with pytest.raises(KeyError):
        await checkpoint.heartbeat(marker, 'not-existing')

    dep = checkpoint['dependency']
    assert dep.checkpoint_marker == marker
    assert dep.checkpoint_state == checkpoint.state


@pytest.mark.asyncio
async def test_checkpoint_state_is_snapshot(checkpoint):
    """Should not mutate a saved checkpoint on later pulses."""
    first = datetime(2025, 1, 1, 10, tzinfo=UTC)
    await checkpoint.check_pulse(first, offset=0)
    await checkpoint.heartbeat(first)

    await checkpoint.check_pulse(first + timedelta(minutes=1), offset=1)

    assert checkpoint['dependency'].checkpoint_state == {'offset': 0}


@pytest.mark.asyncio
async def test_heartbeat_multiple_dependencies_error(checkpoint):
    """Should warn about missing argument."""
    checkpoint.dependencies['extra'] = Dependency(
        'extra',
        iterable_to_async([]),
    )
    with pytest.raises(ValueError, match='`dependency_name` must be provided'):
        await checkpoint.heartbeat(
            datetime(2025, 1, 1, 10, tzinfo=UTC),
        )


@pytest.mark.asyncio
async def test_heartbeat_with_dependency_name(checkpoint):
    """Should correctly update dependency data."""
    checkpoint.dependencies['extra'] = Dependency(
        'extra',
        iterable_to_async([]),
    )
    marker = datetime(2025, 1, 1, 10, 30, tzinfo=UTC)
    await checkpoint.heartbeat(marker, 'dependency')

    dep = checkpoint['dependency']
    assert dep.checkpoint_marker == marker
    assert checkpoint['extra'].checkpoint_marker is None


@pytest.mark.asyncio
async def test_check_pulse_initial_state(checkpoint):
    """Should update dependency and checkpoint data."""
    marker = datetime(2025, 1, 1, 10, tzinfo=UTC)
    await checkpoint.check_pulse(marker, offset=0)

    dep = checkpoint['dependency']
    assert dep.checkpoint_marker == marker
    assert dep.checkpoint_state == {'offset': 0}
    assert checkpoint.state_marker == marker


@pytest.mark.asyncio
async def test_check_pulse_downtime_detected(checkpoint, mocker):
    """Should detect downtime and pause dependent stream."""
    c = Conf()
    mock_iterable = mocker.MagicMock()
    dependent_key = str(id(checkpoint.dependent))
    c.register_iterable(dependent_key, mock_iterable)
    pausable_stream = c.iterables[dependent_key]
    assert pausable_stream.signal is None

    await checkpoint.check_pulse(
        datetime(2025, 1, 1, 10, tzinfo=UTC),
        offset=0,
    )

    downtime = await checkpoint.check_pulse(
        datetime(2025, 1, 1, 10, 30, tzinfo=UTC),
        offset=1,
    )

    # Downtime observed, dependent paused
    assert downtime == timedelta(minutes=30)
    assert downtime == {'dependency': timedelta(minutes=30)}
    assert checkpoint['dependency'].is_down is True
    assert pausable_stream.signal is Signal.PAUSE


@pytest.mark.asyncio
async def test_check_heartbeat_downtime_recovered(checkpoint, mocker):
    """Should detect recovery and resume dependent stream."""
    c = Conf()
    mock_iterable = mocker.MagicMock()
    dependent_key = str(id(checkpoint.dependent))
    c.register_iterable(dependent_key, mock_iterable)
    pausable_stream = c.iterables[dependent_key]
    assert pausable_stream.signal is None

    # If no dependency data has ever come in yet, use the first
    # pulse as a checkpoint_marker
    await checkpoint.check_pulse(
        datetime(2025, 1, 1, 10, tzinfo=UTC),
        offset=0,
    )
    await checkpoint.check_pulse(
        datetime(2025, 1, 1, 11, tzinfo=UTC),
        offset=1,
    )

    # Even though no dependency data has come in yet, it's already
    # marked as down using the fact that the dependent stream has
    # processed one hour of data
    assert checkpoint['dependency'].is_down is True
    assert pausable_stream.signal is Signal.PAUSE

    # When data does come in, and it's late (or still catching up)
    # we can observe this in the latency info
    latency_info = await checkpoint.heartbeat(
        datetime(2025, 1, 1, 10, 30, tzinfo=UTC),
    )
    assert latency_info.get('is_late') is True

    # Latency info shows that the dependency stream has caught up
    latency_info = await checkpoint.heartbeat(
        datetime(2025, 1, 1, 11, 1, tzinfo=UTC),
    )
    assert latency_info.get('is_late') is False

    # Recovery observed, dependent resumed
    assert checkpoint['dependency'].is_down is False
    assert pausable_stream.signal is Signal.RESUME


@pytest.mark.parametrize('is_async', [True, False])
@pytest.mark.asyncio
async def test_custom_callbacks(is_async, checkpoint, mocker):
    """Check custom callbacks properly called."""
    if is_async:
        downtime_callback = mocker.AsyncMock()
        recovery_callback = mocker.AsyncMock()
    else:
        downtime_callback = mocker.Mock()
        recovery_callback = mocker.Mock()

    checkpoint._downtime_callback = downtime_callback
    checkpoint._recovery_callback = recovery_callback

    # Trigger downtime
    await checkpoint.check_pulse(
        datetime(2025, 1, 1, 10, tzinfo=UTC),
        state={'offset': 0},
    )
    await checkpoint.check_pulse(
        datetime(2025, 1, 1, 11, tzinfo=UTC),
        state={'offset': 1},
    )
    downtime_callback.assert_called_once_with(
        checkpoint,
        checkpoint['dependency'],
    )

    # Trigger recovery
    await checkpoint.heartbeat(
        datetime(2025, 1, 1, 11, 1, tzinfo=UTC),
    )
    recovery_callback.assert_called_once_with(
        checkpoint,
        checkpoint['dependency'],
    )


@pytest.mark.parametrize('is_async', [True, False])
@pytest.mark.asyncio
async def test_custom_checks(is_async, mock_cache, mocker):
    """Check custom check functions called."""
    if is_async:
        downtime_check = mocker.AsyncMock(return_value=timedelta(hours=1))
        recovery_check = mocker.AsyncMock(return_value=timedelta(hours=1))
    else:
        downtime_check = mocker.Mock(return_value=timedelta(hours=1))
        recovery_check = mocker.Mock(return_value=timedelta(hours=1))

    async def messages():
        yield {
            'event_timestamp': datetime(2025, 1, 1, 10, tzinfo=UTC),
        }

    dependency = Dependency(
        'dependency',
        messages(),
        downtime_check=downtime_check,
        recovery_check=recovery_check,
    )

    async def dependent():
        yield {
            'event_timestamp': datetime(2025, 1, 1, 10, tzinfo=UTC),
        }

    checkpoint = Checkpoint(
        dependent(),
        dependency,
        name='test',
        cache=mock_cache,
        marker='event_timestamp',
    )

    # Trigger downtime
    await checkpoint.check_pulse(
        datetime(2025, 1, 1, 10, tzinfo=UTC),
        state={'offset': 0},
    )
    assert dependency.is_down is True
    await checkpoint.check_pulse(
        datetime(2025, 1, 1, 11, tzinfo=UTC),
        state={'offset': 1},
    )
    downtime_check.assert_called()

    # Trigger recovery
    await checkpoint.heartbeat(
        datetime(2025, 1, 1, 11, 1, tzinfo=UTC),
    )
    recovery_check.assert_called()
    assert dependency.is_down is False


def test_repr(checkpoint):
    """Should print representation without crashing."""
    assert str(checkpoint)


@pytest.mark.asyncio
async def test_check_pulse_reports_earlier_down_dependency(mock_cache):
    """A later healthy dependency must not hide an earlier downtime."""

    async def always_down(_c, _d):
        return timedelta(minutes=30)

    async def always_up(_c, _d):
        return None

    down = Dependency(
        'down',
        iterable_to_async([]),
        downtime_check=always_down,
    )
    up = Dependency(
        'up',
        iterable_to_async([]),
        downtime_check=always_up,
    )
    checkpoint = Checkpoint(
        iterable_to_async([]),
        down,
        up,
        name='test',
        cache=mock_cache,
        marker='timestamp',
    )

    downtime = await checkpoint.check_pulse(
        datetime(2025, 1, 1, 10, tzinfo=UTC),
    )

    assert downtime == {'down': timedelta(minutes=30)}
    assert down.is_down is True
    assert up.is_down is False


@pytest.mark.asyncio
async def test_check_pulse_recovers_without_heartbeat(mock_cache, mocker):
    """Pulse re-checks recovery so a timer can resume without Kafka traffic."""
    down = True

    def downtime_check(_c, _d):
        return timedelta(seconds=1) if down else None

    def recovery_check(_c, _d):
        return not down

    dependency = Dependency(
        'dependency',
        iterable_to_async([]),
        downtime_check=downtime_check,
        recovery_check=recovery_check,
    )
    checkpoint = Checkpoint(
        iterable_to_async([]),
        dependency,
        name='test',
        cache=mock_cache,
        marker='timestamp',
    )
    c = Conf()
    mock_iterable = mocker.MagicMock()
    dependent_key = str(id(checkpoint.dependent))
    c.register_iterable(dependent_key, mock_iterable)

    first = await checkpoint.check_pulse(datetime(2025, 1, 1, 10, tzinfo=UTC))
    assert first == timedelta(seconds=1)
    assert first == {'dependency': timedelta(seconds=1)}
    assert dependency.is_down is True
    assert c.iterables[dependent_key].signal is Signal.PAUSE

    down = False
    second = await checkpoint.check_pulse(
        datetime(2025, 1, 1, 10, 0, 1, tzinfo=UTC),
    )
    assert second is None
    assert dependency.is_down is False
    assert c.iterables[dependent_key].signal is Signal.RESUME


@pytest.mark.asyncio
async def test_consumer_at_end_no_consumer():
    """Missing consumer is not ready."""
    assert await consumer_at_end(None) is None


@pytest.mark.asyncio
async def test_consumer_at_end_empty_assignment(mocker):
    """Unassigned consumer is not ready."""
    consumer = mocker.MagicMock()
    consumer.assignment.return_value = set()
    assert await consumer_at_end(consumer) is None


@pytest.mark.asyncio
async def test_consumer_at_end_probe_error(mocker):
    """Assignment probe failure is not ready."""
    consumer = mocker.MagicMock()
    consumer.assignment.side_effect = AttributeError('no assignment')
    assert await consumer_at_end(consumer) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('position', 'expected'),
    [(0, False), (1, True), (2, True)],
)
async def test_consumer_at_end_positions(position, expected, mocker):
    """Caught up when every assigned position is at or past the snapshot."""
    part = object()
    consumer = mocker.MagicMock()
    consumer.assignment.return_value = {part}
    consumer.end_offsets = mocker.AsyncMock(return_value={part: 1})
    consumer.position = mocker.AsyncMock(return_value=position)
    assert await consumer_at_end(consumer) is expected


@pytest.mark.asyncio
async def test_consumer_lag_checks_require_confirmed_end(mocker):
    """Lag checks treat anything but a confirmed end as down."""
    topic = mocker.MagicMock()
    topic.consumer = None
    dependency = Dependency('dep', topic)
    checkpoint = Checkpoint(
        iterable_to_async([]),
        dependency,
        name='test',
        marker='timestamp',
    )

    assert await consumer_lag_downtime(checkpoint, dependency) == timedelta(
        seconds=1
    )
    assert await consumer_lag_recovery(checkpoint, dependency) is False

    part = object()
    consumer = mocker.MagicMock()
    consumer.assignment.return_value = {part}
    consumer.end_offsets = mocker.AsyncMock(return_value={part: 1})
    consumer.position = mocker.AsyncMock(return_value=1)
    topic.consumer = consumer

    assert await consumer_lag_downtime(checkpoint, dependency) is None
    assert await consumer_lag_recovery(checkpoint, dependency) is True


@pytest.mark.asyncio
async def test_consumer_lag_checks_accept_raw_consumer(mocker):
    """A raw consumer (no .consumer wrapper) is still probed."""
    part = object()
    consumer = mocker.Mock(spec=['assignment', 'end_offsets', 'position'])
    consumer.assignment.return_value = {part}
    consumer.end_offsets = mocker.AsyncMock(return_value={part: 1})
    consumer.position = mocker.AsyncMock(return_value=1)
    dependency = Dependency('dep', consumer)
    checkpoint = Checkpoint(
        iterable_to_async([]),
        dependency,
        name='test',
        marker='timestamp',
    )

    assert await consumer_lag_downtime(checkpoint, dependency) is None
    assert await consumer_lag_recovery(checkpoint, dependency) is True


@pytest.mark.asyncio
async def test_check_pulse_two_deps_one_recovers_stays_paused(
    mock_cache, mocker
):
    """Resume only when every dependency has recovered."""
    first_down = True

    def first_downtime(_c, _d):
        return timedelta(seconds=1) if first_down else None

    def first_recovery(_c, _d):
        return not first_down

    def always_down(_c, _d):
        return timedelta(seconds=2)

    first = Dependency(
        'first',
        iterable_to_async([]),
        downtime_check=first_downtime,
        recovery_check=first_recovery,
    )
    second = Dependency(
        'second',
        iterable_to_async([]),
        downtime_check=always_down,
    )
    checkpoint = Checkpoint(
        iterable_to_async([]),
        first,
        second,
        name='test',
        cache=mock_cache,
        marker='timestamp',
    )
    c = Conf()
    mock_iterable = mocker.MagicMock()
    dependent_key = str(id(checkpoint.dependent))
    c.register_iterable(dependent_key, mock_iterable)

    first_pulse = await checkpoint.check_pulse(
        datetime(2025, 1, 1, 10, tzinfo=UTC),
    )
    assert first_pulse == {
        'first': timedelta(seconds=1),
        'second': timedelta(seconds=2),
    }
    assert c.iterables[dependent_key].signal is Signal.PAUSE

    first_down = False
    second_pulse = await checkpoint.check_pulse(
        datetime(2025, 1, 1, 10, 0, 1, tzinfo=UTC),
    )
    assert second_pulse == {'second': timedelta(seconds=2)}
    assert first.is_down is False
    assert second.is_down is True
    assert c.iterables[dependent_key].signal is Signal.PAUSE


@pytest.mark.asyncio
async def test_check_pulse_does_not_refire_downtime_callback(mock_cache):
    """Already-down pulses must not re-run the downtime callback."""
    calls = []

    def downtime_check(_c, _d):
        return timedelta(seconds=1)

    def on_down(_c, _d):
        calls.append(_d.name)

    dependency = Dependency(
        'dependency',
        iterable_to_async([]),
        downtime_check=downtime_check,
    )
    checkpoint = Checkpoint(
        iterable_to_async([]),
        dependency,
        name='test',
        cache=mock_cache,
        marker='timestamp',
        on_downtime=on_down,
    )
    marker = datetime(2025, 1, 1, 10, tzinfo=UTC)
    await checkpoint.check_pulse(marker)
    await checkpoint.check_pulse(marker + timedelta(seconds=1))
    assert calls == ['dependency']


@pytest.mark.asyncio
async def test_check_pulse_stays_truthy_in_hysteresis_band(mock_cache):
    """Still down but not currently over threshold stays truthy."""
    over_threshold = True

    def downtime_check(_c, _d):
        return timedelta(minutes=11) if over_threshold else None

    def recovery_check(_c, _d):
        return False

    dependency = Dependency(
        'dependency',
        iterable_to_async([]),
        downtime_check=downtime_check,
        recovery_check=recovery_check,
    )
    checkpoint = Checkpoint(
        iterable_to_async([]),
        dependency,
        name='test',
        cache=mock_cache,
        marker='timestamp',
    )
    t0 = datetime(2025, 1, 1, 10, tzinfo=UTC)
    over = await checkpoint.check_pulse(t0)
    assert over == timedelta(minutes=11)
    assert dependency.is_down is True

    over_threshold = False
    band = await checkpoint.check_pulse(t0 + timedelta(minutes=1))
    assert band is not True
    assert band == True  # noqa: E712
    assert band == {'dependency': True}
    assert dependency.is_down is True


@pytest.mark.asyncio
async def test_resume_callback_fires_once_on_overlap(mock_cache):
    """Overlapping pulse and heartbeat recovery invoke the callback once."""
    entered = 0
    release = Event()
    calls: list[str] = []

    async def downtime_check(_c, _d):
        return None

    async def recovery_check(_c, _d):
        nonlocal entered
        entered += 1
        await release.wait()
        return True

    def on_resume(_c, _d):
        calls.append(_d.name)

    dependency = Dependency(
        'dependency',
        iterable_to_async([]),
        downtime_check=downtime_check,
        recovery_check=recovery_check,
    )
    dependency.is_down = True
    checkpoint = Checkpoint(
        iterable_to_async([]),
        dependency,
        name='test',
        cache=mock_cache,
        marker='timestamp',
        on_recovery=on_resume,
    )
    checkpoint._awaiting_resume = True
    marker = datetime(2025, 1, 1, 10, tzinfo=UTC)
    pulse = create_task(checkpoint.check_pulse(marker))
    beat = create_task(checkpoint.heartbeat(marker))
    for _ in range(50):
        if entered >= 2:
            break
        await sleep(0)
    release.set()
    await pulse
    await beat
    assert calls == ['dependency']


def test_coerce_marker_prefers_payload_timestamp():
    """Should treat marker='timestamp' as the payload field on a record."""
    event = datetime(2025, 1, 1, 10, tzinfo=UTC)
    pick = coerce_marker('timestamp')
    assert pick is not None
    rec = type(
        'Rec',
        (),
        {
            'timestamp': 1_700_000_000_000,
            'partition': 0,
            'value': {'timestamp': event},
        },
    )()
    assert pick(rec) == event


def test_for_handler_without_checkpoint():
    """Should raise when no Checkpoint was bound to the handler."""

    def orphan(_msg):
        return None

    with pytest.raises(KeyError, match='No checkpoint bound'):
        Checkpoint.for_handler(orphan)


@pytest.mark.asyncio
async def test_handle_depends_on_heartbeats_and_pulses():
    """Should heartbeat the leader and pulse the dependent."""

    async def weather():
        yield {'timestamp': datetime(2025, 1, 1, 10, tzinfo=UTC)}

    async def activity():
        await sleep(0.05)
        yield {'timestamp': datetime(2025, 1, 1, 10, 5, tzinfo=UTC)}

    weather_s, activity_s = weather(), activity()
    seen = []

    @handle(weather_s)
    def weather_handler(msg):
        return msg

    @handle(Checkpoint(activity_s, weather_s, marker='timestamp'))
    def activity_handler(msg):
        seen.append(msg)

    await stream()

    c = Checkpoint.for_handler(activity_handler)
    assert activity_handler.checkpoint is c  # type: ignore[attr-defined]
    dep = next(iter(c.dependencies.values()))
    assert dep.checkpoint_marker == datetime(2025, 1, 1, 10, tzinfo=UTC)
    assert c.state_marker == datetime(2025, 1, 1, 10, 5, tzinfo=UTC)
    assert dep.is_down is False
    assert seen == [{'timestamp': datetime(2025, 1, 1, 10, 5, tzinfo=UTC)}]


@pytest.mark.asyncio
async def test_handle_uses_dependency_marker_field():
    """Should heartbeat the leader field named on Dependency."""
    event = datetime(2025, 1, 1, 10, tzinfo=UTC)
    other = datetime(2025, 1, 1, 9, tzinfo=UTC)

    async def weather():
        yield {
            'timestamp': other,
            'last_updated_at': event,
        }

    async def activity():
        await sleep(0.05)
        yield {'timestamp': datetime(2025, 1, 1, 10, 5, tzinfo=UTC)}

    weather_s, activity_s = weather(), activity()

    @handle(weather_s)
    def weather_handler(msg):
        return msg

    @handle(
        Checkpoint(
            activity_s,
            dependencies=Dependency(
                'weather',
                weather_s,
                marker='last_updated_at',
            ),
            marker='timestamp',
        )
    )
    def activity_handler(msg):
        return msg

    await stream()

    dep = Checkpoint.for_handler(activity_handler)['weather']
    assert dep.checkpoint_marker == event


@pytest.mark.asyncio
async def test_handle_depends_on_detects_downtime():
    """Should pause the dependent when event-time lag exceeds threshold."""

    async def weather():
        yield {'timestamp': datetime(2025, 1, 1, 10, tzinfo=UTC)}

    async def activity():
        await sleep(0.05)
        yield {'timestamp': datetime(2025, 1, 1, 11, tzinfo=UTC)}

    weather_s, activity_s = weather(), activity()

    @handle(weather_s)
    def weather_handler(_msg):
        return None

    @handle(Checkpoint(activity_s, weather_s, marker='timestamp'))
    def activity_handler(_msg):
        return None

    await stream()

    c = Checkpoint.for_handler(activity_handler)
    dep = next(iter(c.dependencies.values()))
    assert dep.is_down is True
    key = str(id(activity_s))
    assert Conf().iterables[key].signal is Signal.PAUSE


@pytest.mark.asyncio
async def test_handle_depends_on_recovery_callback():
    """Should invoke on_recovery after the leader catches up."""
    recovered = []

    async def weather():
        yield {'timestamp': datetime(2025, 1, 1, 10, tzinfo=UTC)}
        await sleep(0.08)
        yield {'timestamp': datetime(2025, 1, 1, 11, 1, tzinfo=UTC)}

    async def activity():
        await sleep(0.04)
        yield {'timestamp': datetime(2025, 1, 1, 11, tzinfo=UTC)}

    weather_s, activity_s = weather(), activity()

    @handle(weather_s)
    def weather_handler(_msg):
        return None

    @handle(
        Checkpoint(
            activity_s,
            weather_s,
            marker='timestamp',
            on_recovery=lambda _c, d: recovered.append(d.name),
        )
    )
    def activity_handler(_msg):
        return None

    await stream()
    assert recovered


@pytest.mark.asyncio
async def test_handle_timer_depends_on_topic_uses_lag():
    """Should use consumer-lag checks and not pause a non-Topic dependent."""
    topic = Topic('weather', {'bootstrap_servers': 'localhost:9092'})

    async def ticks():
        yield 1

    ticks_s = ticks()

    @handle(
        Checkpoint(
            ticks_s,
            topic,
            marker=lambda _msg: datetime(2025, 1, 1, tzinfo=UTC),
        )
    )
    def tick(_msg):
        return None

    c = Checkpoint.for_handler(tick)
    assert c.pause_dependent is False
    assert 'weather' in c.dependencies
    dep = c['weather']
    assert dep.downtime_check is consumer_lag_downtime
    assert dep.recovery_check is consumer_lag_recovery


@pytest.mark.asyncio
async def test_handle_depends_on_rejects_self():
    """Should reject a source that depends on itself."""

    async def msgs():
        yield 1

    src = msgs()
    with pytest.raises(ValueError, match='cannot depend on its dependent'):
        Checkpoint(src, src, marker='timestamp')


def test_checkpoint_named_dependencies():
    """Should accept one or many dependencies via the named argument."""

    async def msgs():
        yield 1

    dependent, weather, traffic = msgs(), msgs(), msgs()
    one = Checkpoint(dependent, dependencies=weather, marker='timestamp')
    assert len(one.dependencies) == 1

    two = Checkpoint(
        msgs(),
        dependencies=[weather, traffic],
        name='join',
        marker='timestamp',
    )
    assert len(two.dependencies) == 2


def test_handle_without_checkpoint_source():
    """Should leave handlers without a Checkpoint unbound."""

    async def msgs():
        yield 1

    @handle(msgs())
    def plain(_msg):
        return None

    with pytest.raises(KeyError, match='No checkpoint bound'):
        Checkpoint.for_handler(plain)


@pytest.mark.asyncio
async def test_check_pulse_skips_seed_for_custom_check(mock_cache):
    """Should not copy the dependent clock into a lag-style dependency."""

    def never_datetime(_c, _d):
        return None

    dependency = Dependency(
        'leader',
        iterable_to_async([]),
        downtime_check=never_datetime,
        recovery_check=lambda _c, _d: True,
    )
    checkpoint = Checkpoint(
        iterable_to_async([]),
        dependency,
        name='test',
        cache=mock_cache,
        marker='timestamp',
        pause_dependent=False,
    )
    await checkpoint.check_pulse(datetime(2025, 1, 1, 11, tzinfo=UTC))
    assert dependency.checkpoint_marker is None


def test_coerce_marker_field_name():
    """Should look up a named field on dicts and record payloads."""
    pick = coerce_marker('event_time')
    assert pick is not None
    ts = datetime(2025, 1, 1, 10, tzinfo=UTC)
    assert pick({'event_time': ts}) == ts

    rec = type('Rec', (), {'value': {'event_time': ts}})()
    assert pick(rec) == ts
    compact = coerce_marker('timestamp')
    assert compact({'timestamp': '20230101T100000Z'}) == datetime(
        2023, 1, 1, 10, tzinfo=UTC
    )
    with pytest.raises(KeyError, match='not found'):
        pick({})
    with pytest.raises(TypeError, match='callable or field name'):
        coerce_marker(1)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_handle_passes_downtime_argument():
    """Should pass pulse result as downtime when the handler accepts it."""
    seen = []

    async def weather():
        yield {'timestamp': datetime(2025, 1, 1, 10, tzinfo=UTC)}

    async def activity():
        await sleep(0.05)
        yield {'timestamp': datetime(2025, 1, 1, 11, tzinfo=UTC)}

    weather_s, activity_s = weather(), activity()

    @handle(weather_s)
    def weather_handler(_msg):
        return None

    @handle(
        Checkpoint(
            activity_s,
            weather_s,
            downtime_threshold=timedelta(minutes=10),
            marker='timestamp',
        )
    )
    def activity_handler(_msg, downtime=None, checkpoint=None):
        seen.append(downtime)
        assert checkpoint is not None
        assert checkpoint.downtime == downtime

    await stream()
    assert seen
    assert seen[0] == timedelta(hours=1)
    assert list(seen[0].values()) == [timedelta(hours=1)]
