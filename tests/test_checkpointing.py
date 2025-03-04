from datetime import datetime, timedelta

import pytest
from conftest import emoji, iterable_to_async

from slipstream.checkpointing import Checkpoint, Dependency
from slipstream.core import Signal


@pytest.fixture
def dependency():
    """A typical dependency object."""
    return Dependency('emoji', emoji())


@pytest.fixture
def checkpoint(mock_cache):
    """A typical checkpoint object."""

    async def dependent():
        yield {'event_timestamp': datetime(2025, 1, 1, 10)}

    async def dependency():
        yield {'event_timestamp': datetime(2025, 1, 1, 10)}

    dep = Dependency('dependency', dependency())

    return Checkpoint('test', dependent(), [dep], cache=mock_cache)


def test_dependency_init(dependency):
    """Should properly initialize dependency."""
    assert dependency.name == 'emoji'
    assert dependency.checkpoint_state is None
    assert dependency.checkpoint_marker is None
    assert isinstance(dependency._downtime_threshold, timedelta)
    assert dependency.is_down is False


def test_dependency_save_and_load(mock_cache, dependency):
    """Should save and load dependency using cache."""
    checkpoint_state = {'offset': 1}
    checkpoint_marker = datetime(2025, 1, 1, 10)
    dependency.save(mock_cache, '_prefix_',
                    checkpoint_state, checkpoint_marker)

    loaded_dep = Dependency('emoji', iterable_to_async([]))
    loaded_dep.load(mock_cache, '_prefix_')

    assert loaded_dep.checkpoint_state == checkpoint_state
    assert loaded_dep.checkpoint_marker == checkpoint_marker


@pytest.mark.asyncio
async def test_default_downtime_check(dependency):
    """Should check for datetime diff surpassing threshold."""
    checkpoint = Checkpoint('test', iterable_to_async([]), [dependency])
    checkpoint.state_marker = datetime(2025, 1, 1, 11)
    dependency.checkpoint_marker = datetime(2025, 1, 1, 10)

    downtime = dependency._default_downtime_check(checkpoint, dependency)
    assert isinstance(downtime, timedelta)
    assert downtime == timedelta(hours=1)


@pytest.mark.asyncio
async def test_default_recovery_check(dependency):
    """Should check surpassing datetime is true."""
    checkpoint = Checkpoint('test', iterable_to_async([]), [dependency])
    checkpoint.state_marker = datetime(2025, 1, 1, 10)
    dependency.checkpoint_marker = datetime(2025, 1, 1, 11)

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
    marker = datetime(2025, 1, 1, 10, 30)
    await checkpoint.heartbeat(marker)

    dep = checkpoint['dependency']
    assert dep.checkpoint_marker == marker
    assert dep.checkpoint_state == checkpoint.state


@pytest.mark.asyncio
async def test_heartbeat_multiple_dependencies_error(checkpoint):
    """Should warn about missing argument."""
    checkpoint.dependencies['extra'] = Dependency(
        'extra', iterable_to_async([]))
    with pytest.raises(ValueError, match='`dependency_name` must be provided'):
        await checkpoint.heartbeat(datetime(2025, 1, 1, 10))


@pytest.mark.asyncio
async def test_heartbeat_with_dependency_name(checkpoint):
    """Should correctly update dependency data."""
    checkpoint.dependencies['extra'] = Dependency(
        'extra', iterable_to_async([]))
    marker = datetime(2025, 1, 1, 10, 30)
    await checkpoint.heartbeat(marker, 'dependency')

    dep = checkpoint['dependency']
    assert dep.checkpoint_marker == marker
    assert checkpoint['extra'].checkpoint_marker is None


@pytest.mark.asyncio
async def test_check_pulse_initial_state(checkpoint):
    """Should update dependency and checkpoint data."""
    marker = datetime(2025, 1, 1, 10)
    await checkpoint.check_pulse(marker, offset=0)

    dep = checkpoint['dependency']
    assert dep.checkpoint_marker == marker
    assert dep.checkpoint_state == {'offset': 0}
    assert checkpoint.state_marker == marker


@pytest.mark.asyncio
async def test_check_pulse_downtime_detected(checkpoint, mocker):
    """Should detect downtime and pause dependent stream."""
    mock_iterable = mocker.MagicMock()
    mocker.patch(
        'slipstream.core.Conf.iterables',
        {str(id(checkpoint.dependent)): mock_iterable}
    )

    await checkpoint.check_pulse(datetime(2025, 1, 1, 10), offset=0)

    downtime = await checkpoint.check_pulse(
        datetime(2025, 1, 1, 10, 30), offset=1)

    # Downtime observed, dependent paused
    assert isinstance(downtime, timedelta)
    assert checkpoint['dependency'].is_down is True
    mock_iterable.send_signal.assert_called_with(Signal.PAUSE)


@pytest.mark.asyncio
async def test_check_heartbeat_downtime_recovered(checkpoint, mocker):
    """Should detect recovery and resume dependent stream."""
    mock_iterable = mocker.MagicMock()
    mocker.patch(
        'slipstream.core.Conf.iterables',
        {str(id(checkpoint.dependent)): mock_iterable}
    )

    await checkpoint.check_pulse(datetime(2025, 1, 1, 10), offset=0)
    await checkpoint.check_pulse(datetime(2025, 1, 1, 11), offset=1)
    assert checkpoint['dependency'].is_down is True

    conf_mock = mocker.patch('slipstream.Conf', autospec=True)
    conf_mock.return_value.iterables = {
        str(id(checkpoint.dependent)): mocker.MagicMock()
    }

    # Recover dependency stream
    await checkpoint.heartbeat(datetime(2025, 1, 1, 11, 1))

    # Recovery observed, dependent resumed
    assert checkpoint['dependency'].is_down is False
    mock_iterable.send_signal.assert_called_with(Signal.RESUME)


@pytest.mark.asyncio
async def test_custom_callbacks(checkpoint, mocker):
    """Check custom callbacks properly called."""
    downtime_callback = mocker.AsyncMock()
    recovery_callback = mocker.AsyncMock()
    checkpoint._downtime_callback = downtime_callback
    checkpoint._recovery_callback = recovery_callback

    # Trigger downtime
    await checkpoint.check_pulse(datetime(2025, 1, 1, 10), state={'offset': 0})
    await checkpoint.check_pulse(datetime(2025, 1, 1, 11), state={'offset': 1})
    downtime_callback.assert_called_once_with(
        checkpoint, checkpoint['dependency'])

    # Trigger recovery
    await checkpoint.heartbeat(datetime(2025, 1, 1, 11, 1))
    recovery_callback.assert_called_once_with(
        checkpoint, checkpoint['dependency'])
