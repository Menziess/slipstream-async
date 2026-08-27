"""Slipstream checkpointing."""

import logging
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Generator,
)
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Any, ClassVar

from slipstream.core import Conf, Signal
from slipstream.interfaces import ICache
from slipstream.utils import AsyncCallable, awaitable

try:
    from aiokafka.errors import KafkaError

    _PROBE_ERRORS: tuple[type[BaseException], ...] = (
        KafkaError,
        AttributeError,
        TypeError,
    )
except ImportError:  # pragma: no cover
    _PROBE_ERRORS = (AttributeError, TypeError)

_logger = logging.getLogger(__name__)


STATE_NAME = 'state'
STATE_MARKER_NAME = 'state_marker'
CHECKPOINT_STATE_NAME = 'checkpoint_state'
CHECKPOINT_MARKER_NAME = 'checkpoint_marker'
CHECKPOINTS_NAME = 'checkpoints'


class Downtime(dict):
    """Per-dependency pulse result.

    Falsy when empty. A single entry compares equal to that value, so
    ``downtime == timedelta(...)`` still holds for one leader.
    """

    def __eq__(self, other: object) -> bool:
        """Match a dict, or the sole value when only one leader is down."""
        if isinstance(other, dict):
            return dict.__eq__(self, other)
        if len(self) == 1:
            return next(iter(self.values())) == other
        return NotImplemented


def _dependency_consumer(d: 'Dependency') -> Any:
    """Resolve a Topic wrapper or a raw aiokafka consumer."""
    wrapped = getattr(d.dependency, 'consumer', None)
    if wrapped is not None:
        return wrapped
    raw = d.dependency
    if hasattr(raw, 'assignment'):
        return raw
    return None


async def consumer_at_end(consumer: Any) -> bool | None:
    """Whether a Kafka consumer is at the end of its assignment.

    Returns None when the consumer is missing, unassigned, or the
    probe fails. Caught up is ``position >= end_offset`` so a
    partition that advanced after ``end_offsets`` was snapshotted
    is not treated as lag.

    Args:
        consumer: An aiokafka consumer, or None.

    Returns:
        True when every assigned partition is at or past the
        snapshot, False when any assigned partition still has
        lag, None when readiness cannot be confirmed.
    """
    if consumer is None:
        return None
    try:
        assignment = consumer.assignment()
    except _PROBE_ERRORS:
        return None
    if not assignment:
        return None
    try:
        end_offsets = await consumer.end_offsets(assignment)
        positions = {p: await consumer.position(p) for p in assignment}
    except _PROBE_ERRORS:
        return None
    return all(positions[p] >= end_offsets[p] for p in assignment)


async def consumer_lag_downtime(
    _c: 'Checkpoint',
    d: 'Dependency',
) -> timedelta | None:
    """Treat a dependency as down until its consumer is at the end.

    Use as ``downtime_check`` when the dependent is a timer (or
    any source that must not emit from stale local cache). Missing
    or unassigned consumers are also down. Pair with
    ``consumer_lag_recovery``; the default event-time recovery
    check will not resume a timer after the first-pulse seed.
    """
    if await consumer_at_end(_dependency_consumer(d)) is True:
        return None
    return timedelta(seconds=1)


async def consumer_lag_recovery(_c: 'Checkpoint', d: 'Dependency') -> bool:
    """Recover only when the dependency consumer is at the end.

    Pair with ``consumer_lag_downtime``. Accepts a ``Topic`` or a
    raw aiokafka consumer as ``d.dependency``.
    """
    return await consumer_at_end(_dependency_consumer(d)) is True


class Dependency:
    """Track the dependent stream state to recover from downtime.

    The dependency name should not be changed once created,
    it is used to persist the dependency in the cache.

    >>> async def emoji():
    ...     for emoji in '🏆📞🐟👌':
    ...         yield emoji
    >>> Dependency('emoji', emoji())
    {'checkpoint_state': None, 'checkpoint_marker': None}
    """

    @property
    def downtime_check(
        self,
    ) -> AsyncCallable[['Checkpoint', 'Dependency'], Any]:
        """Is called when downtime is detected."""
        return self._downtime_check

    @property
    def recovery_check(
        self,
    ) -> AsyncCallable[['Checkpoint', 'Dependency'], bool]:
        """Is called when downtime is resolved."""
        return self._recovery_check

    def __init__(
        self,
        name: str,
        dependency: AsyncIterable[Any],
        downtime_threshold: Any = timedelta(minutes=10),
        downtime_check: AsyncCallable[['Checkpoint', 'Dependency'], Any]
        | None = None,
        recovery_check: AsyncCallable[['Checkpoint', 'Dependency'], bool]
        | None = None,
        marker: Callable[[Any], Any] | str | None = None,
    ) -> None:
        """Initialize dependency for checkpointing."""
        self.name = name
        self.dependency = dependency
        self.checkpoint_state = None
        self.checkpoint_marker = None
        self.downtime_threshold = downtime_threshold
        self._downtime_check = downtime_check or self._default_downtime_check
        self._recovery_check = recovery_check or self._default_recovery_check
        self.marker = coerce_marker(marker) if marker is not None else None
        self.is_down = False

    def uses_event_time_seed(self) -> bool:
        """Whether a silent leader should inherit the dependent marker."""
        check = self._downtime_check
        default = self._default_downtime_check
        return check is default or getattr(check, '__func__', None) is default

    def use_consumer_lag(self) -> None:
        """Compare Kafka consumer lag instead of event times."""
        if self.uses_event_time_seed():
            self._downtime_check = consumer_lag_downtime
            self._recovery_check = consumer_lag_recovery

    def _checkpoint_key(self, cache_key_prefix: str) -> str:
        """Cache key prefix for this dependency."""
        return f'{cache_key_prefix}{self.name}_'

    def save(
        self,
        cache: ICache,
        cache_key_prefix: str,
        checkpoint_state: Any,
        checkpoint_marker: datetime,
    ) -> None:
        """Save checkpoint state to cache."""
        key = self._checkpoint_key(cache_key_prefix)
        cache[key + CHECKPOINT_STATE_NAME] = checkpoint_state
        cache[key + CHECKPOINT_MARKER_NAME] = checkpoint_marker

    def load(self, cache: ICache, cache_key_prefix: str) -> None:
        """Load checkpoint state from cache."""
        key = self._checkpoint_key(cache_key_prefix)
        self.checkpoint_state = cache[key + CHECKPOINT_STATE_NAME]
        self.checkpoint_marker = cache[key + CHECKPOINT_MARKER_NAME]

    @staticmethod
    def _require_datetime_markers(
        c: 'Checkpoint',
        d: 'Dependency',
        check: str,
    ) -> tuple[datetime, datetime]:
        """Return both markers or raise if either is not a datetime."""
        marker, checkpoint = c.state_marker, d.checkpoint_marker
        if isinstance(marker, datetime) and isinstance(checkpoint, datetime):
            return marker, checkpoint
        err_msg = (
            'Expecting either `datetime` markers in heartbeat and '
            f'check_pulse, or a custom {check} in dependency, '
            f'got; {marker} and {checkpoint}'
        )
        raise TypeError(err_msg)

    @staticmethod
    def _default_downtime_check(
        c: 'Checkpoint',
        d: 'Dependency',
    ) -> timedelta | None:
        """Determine dependency downtime by comparing event timestamps.

        This behavior can be overridden by passing a callable to
        `downtime_check` that takes a `Checkpoint` object.
        """
        marker, checkpoint = Dependency._require_datetime_markers(
            c, d, 'downtime_check'
        )
        diff = marker - checkpoint
        if diff > d.downtime_threshold:
            return diff
        return None

    @staticmethod
    def _default_recovery_check(c: 'Checkpoint', d: 'Dependency') -> bool:
        """Determine dependency has caught up by comparing event timestamps.

        This behavior can be overridden by passing a callable to
        `recovery_check` that takes a `Checkpoint` object.
        """
        marker, checkpoint = Dependency._require_datetime_markers(
            c, d, 'recovery_check'
        )
        return checkpoint > marker

    def __iter__(self) -> Generator[tuple[str, Any | None], None, None]:
        """Get relevant values when dict is called."""
        yield from (
            {
                CHECKPOINT_STATE_NAME: self.checkpoint_state,
                CHECKPOINT_MARKER_NAME: self.checkpoint_marker,
            }.items()
        )

    def __repr__(self) -> str:
        """Represent checkpoint."""
        return str(dict(self))


class Checkpoint:
    """One dependent stream and the dependency streams it must not outrun.

    Pass the checkpoint to ``@handle``. The library heartbeats dependencies,
    pulses the dependent, and pauses it when event time falls behind.

    >>> async def emoji():
    ...     for emoji in '🏆📞🐟👌':
    ...         yield emoji
    >>> dependent, dependency = emoji(), emoji()
    >>> dependent_checkpoint = Checkpoint(
    ...     dependent,
    ...     dependencies=Dependency('dependency', dependency),
    ...     marker=lambda msg: msg,
    ...     name='dependent',
    ... )
    >>> from slipstream import handle
    >>> @handle(dependent_checkpoint)
    ... async def dependent_handler(msg, checkpoint=None):
    ...     yield msg

    If no cache is provided, the checkpoint lasts only for this process.
    """

    _by_handler: ClassVar[dict[Callable[..., Any], 'Checkpoint']] = {}

    def __init__(
        self,
        dependent: AsyncIterable[Any],
        *leaders: Any,
        dependencies: Any = None,
        name: str | None = None,
        on_downtime: Callable[['Checkpoint', Dependency], Any] | None = None,
        on_recovery: Callable[['Checkpoint', Dependency], Any] | None = None,
        cache: ICache | None = None,
        cache_key_prefix: str = '_',
        pause_dependent: bool | None = None,
        downtime_threshold: timedelta | None = None,
        marker: Callable[[Any], Any] | str,
    ) -> None:
        """Create a checkpoint for ``dependent`` against its dependencies.

        ::

            Checkpoint(
                activity,
                dependencies=[weather, traffic],
                marker='timestamp',
            )
        """
        built, pause = _parse_checkpoint_args(
            dependent,
            _collect_leaders(leaders, dependencies),
            pause_dependent=pause_dependent,
            downtime_threshold=downtime_threshold,
        )
        self.name = _checkpoint_name(name, dependent)
        self.dependent = dependent
        self.dependencies = {d.name: d for d in built}
        if id(self.dependent) in {id(d.dependency) for d in built}:
            err_msg = 'Checkpoint cannot depend on its dependent stream.'
            raise ValueError(err_msg)
        self.pause_dependent = pause
        self.marker = coerce_marker(marker)
        for dep in self.dependencies.values():
            if dep.marker is None:
                dep.marker = self.marker
        self.downtime: Any | None = None
        self._cache = cache
        self._cache_key = f'{cache_key_prefix}_{self.name}_'
        self._downtime_callback = on_downtime
        self._recovery_callback = on_recovery
        self._awaiting_resume = False

        self.state = {}
        self.state_marker = None

        if self._cache:
            self.state = self._cache[f'{self._cache_key}_{STATE_NAME}'] or {}
            self.state_marker = self._cache[
                f'{self._cache_key}_{STATE_MARKER_NAME}'
            ]
            for dependency in self.dependencies.values():
                dependency.load(self._cache, self._cache_key)

    def __aiter__(self) -> AsyncIterator[Any]:
        """Iterate the dependent stream."""
        return self.dependent.__aiter__()

    async def heartbeat(
        self,
        marker: datetime | Any,
        dependency_name: str | None = None,
    ) -> dict:
        """Update checkpoint to latest state.

        Args:
            marker (datetime | Any): Typically the event timestamp that is
                compared to the event timestamp of a dependent stream.
            dependency_name (str, optional): Required when there are multiple
                dependencies to specify which one the heartbeat is for.
        """
        if dependency_name:
            if not (dependency := self.dependencies.get(dependency_name)):
                err_msg = 'Dependency does not exist.'
                raise KeyError(err_msg)
        elif len(self.dependencies) == 1:
            dependency = next(iter(self.dependencies.values()))
        else:
            err_msg = (
                'Argument `dependency_name` must be provided '
                'for checkpoint with multiple dependencies.'
            )
            raise ValueError(err_msg)

        self._save_checkpoint(dependency, self.state, marker)

        if dependency.is_down:
            if await awaitable(dependency.recovery_check(self, dependency)):
                dependency.is_down = False
            await self._resume_if_cleared(dependency)

        return {
            'is_late': dependency.is_down,
            'dependent_marker': self.state_marker,
            'dependency_marker': dependency.checkpoint_marker,
        }

    async def check_pulse(
        self,
        marker: datetime | Any,
        **kwargs: Any,
    ) -> Any | None:
        """Update state that can be used as checkpoint.

        Args:
            marker (datetime | Any): Typically the event timestamp that is
                compared to the event timestamp of a dependency stream.
            kwargs (Any): Any information that can be used for reprocessing any
                incorrect data that was sent out during downtime of a
                dependency stream, stored in `state`.

        Returns:
            None when every dependency is healthy. Otherwise a
            :class:`Downtime` map of name → check (or ``True`` if still
            down but not over threshold). One leader still compares
            equal to its timedelta / ``True``.
        """
        self._save_state(marker, **kwargs)

        reported = Downtime()
        for dependency in self.dependencies.values():
            down_now = await self._pulse_dependency(dependency)
            if down_now:
                reported[dependency.name] = down_now
            elif dependency.is_down:
                reported[dependency.name] = True

        self.downtime = reported or None
        return self.downtime

    async def _pulse_dependency(self, dependency: Dependency) -> Any | None:
        """Update one dependency from the current pulse.

        Returns:
            A truthy downtime value when this dependency is down.
        """
        if (
            not dependency.checkpoint_marker
            and dependency.uses_event_time_seed()
        ):
            self._save_checkpoint(
                dependency,
                self.state,
                self.state_marker,
            )

        down_now = await awaitable(dependency.downtime_check(self, dependency))
        if down_now:
            if not dependency.is_down:
                log_msg = (
                    f'Downtime of dependency "{dependency.name}" detected'
                )
                _logger.info(log_msg)
                await self._pause_dependent()
                self._awaiting_resume = True
                if self._downtime_callback:
                    await awaitable(self._downtime_callback(self, dependency))
                dependency.is_down = True
            return down_now

        if dependency.is_down and await awaitable(
            dependency.recovery_check(self, dependency)
        ):
            dependency.is_down = False
            await self._resume_if_cleared(dependency)
        return None

    def _signal_dependent(self, signal: Signal) -> None:
        """Pause or resume the dependent iterable when configured to do so."""
        key, c = str(id(self.dependent)), Conf()
        if self.pause_dependent and key in c.iterables:
            c.iterables[key].send_signal(signal)

    async def _pause_dependent(self) -> None:
        """Pause the dependent iterable when configured to do so."""
        self._signal_dependent(Signal.PAUSE)

    async def _resume_if_cleared(self, dependency: Dependency) -> None:
        """Resume the dependent stream when no dependency is still down."""
        if any(_.is_down for _ in self.dependencies.values()):
            return
        if not self._awaiting_resume:
            return
        self._awaiting_resume = False
        _logger.debug(
            f'Dependency "{dependency.name}" downtime resolved',
        )
        self._signal_dependent(Signal.RESUME)
        if self._recovery_callback:
            await awaitable(self._recovery_callback(self, dependency))

    def _save_state(self, state_marker: datetime | Any, **kwargs: Any) -> None:
        """Save state of the stream (to cache).

        Event-time markers only move forward so an out-of-order
        partition cannot rewind the clock and pause the other source.
        """
        self.state.update(**kwargs)
        self.state_marker = _later_marker(self.state_marker, state_marker)
        if not self._cache:
            return
        self._cache[f'{self._cache_key}_{STATE_NAME}'] = self.state
        self._cache[f'{self._cache_key}_{STATE_MARKER_NAME}'] = (
            self.state_marker
        )

    def _save_checkpoint(
        self,
        dependency: Dependency,
        checkpoint_state: Any,
        checkpoint_marker: datetime | Any,
    ) -> None:
        """Save state of the dependency checkpoint (to cache).

        Markers only move forward (see ``_save_state``).
        """
        marker = _later_marker(dependency.checkpoint_marker, checkpoint_marker)
        dependency.checkpoint_state = dict(checkpoint_state)
        dependency.checkpoint_marker = marker
        if not self._cache:
            return
        dependency.save(
            self._cache,
            self._cache_key,
            dependency.checkpoint_state,
            checkpoint_marker,
        )

    def on_downtime(
        self,
        callback: Callable[['Checkpoint', Dependency], Any],
    ) -> 'Checkpoint':
        """Set the callback invoked when a dependency first goes down."""
        self._downtime_callback = callback
        return self

    def on_recovery(
        self,
        callback: Callable[['Checkpoint', Dependency], Any],
    ) -> 'Checkpoint':
        """Set the callback invoked when every dependency has recovered."""
        self._recovery_callback = callback
        return self

    @classmethod
    def for_handler(cls, handler: Callable[..., Any]) -> 'Checkpoint':
        """Return the checkpoint bound by ``@handle(Checkpoint(...))``."""
        try:
            return cls._by_handler[handler]
        except KeyError:
            err_msg = 'No checkpoint bound to this handler.'
            raise KeyError(err_msg) from None

    @classmethod
    def bind_handler(
        cls,
        handler: Callable[..., Any],
        checkpoint: 'Checkpoint',
    ) -> None:
        """Record the checkpoint created for a ``@handle`` wrapper."""
        cls._by_handler[handler] = checkpoint
        handler.checkpoint = checkpoint  # type: ignore[attr-defined]

    def __getitem__(self, key: str) -> Dependency:
        """Get dependency from dependencies."""
        return self.dependencies[key]

    def __repr__(self) -> str:
        """Represent checkpoint."""
        return str(
            {
                STATE_NAME: self.state,
                STATE_MARKER_NAME: self.state_marker,
                CHECKPOINTS_NAME: {
                    dependency.name: dict(dependency)
                    for dependency in self.dependencies.values()
                },
            },
        )


def _later_marker(old: Any, new: Any) -> Any:
    """Keep the high-water marker; ignore incomparable types."""
    if old is None:
        return new
    try:
        return max(old, new)
    except TypeError:
        return new


def _as_datetime(value: Any) -> datetime | None:
    """Parse a datetime or a common timestamp string."""
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        pass
    try:
        return datetime.strptime(  # noqa: DTZ007
            value, '%Y-%m-%d %H:%M:%S'
        )
    except ValueError:
        pass
    try:
        return datetime.strptime(value, '%Y%m%dT%H%M%SZ').replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None


def coerce_marker(
    marker: Callable[[Any], Any] | str,
) -> Callable[[Any], Any]:
    """Accept a callable or a field name."""
    if callable(marker):
        return marker
    if not isinstance(marker, str):
        err_msg = 'marker must be a callable or field name.'
        raise TypeError(err_msg)
    key = marker

    def _pick(msg: Any) -> Any:
        if isinstance(msg, dict) and key in msg:
            return _as_datetime(msg[key]) or msg[key]
        payload = getattr(msg, 'value', None)
        if isinstance(payload, dict) and key in payload:
            return _as_datetime(payload[key]) or payload[key]
        if not isinstance(msg, dict) and hasattr(msg, key):
            val = getattr(msg, key)
            return _as_datetime(val) or val
        err_msg = f'marker field {key!r} not found'
        raise KeyError(err_msg)

    return _pick


def _pulse_state(msg: Any) -> dict[str, Any]:
    """Snapshot Topic partition offsets when present."""
    if hasattr(msg, 'partition') and hasattr(msg, 'offset'):
        return {str(msg.partition): msg.offset}
    return {}


def _is_topic(it: Any) -> bool:
    """Whether ``it`` is a Kafka Topic (optional extra)."""
    from slipstream.core import Topic, aiokafka_available

    return bool(aiokafka_available) and isinstance(it, Topic)


def _raw_dependency_name(d: AsyncIterable[Any]) -> str:
    """Prefer a Topic name; otherwise the object id."""
    name = getattr(d, 'name', None)
    if isinstance(name, str) and name:
        return name
    return str(id(d))


def _dependency_names(
    deps: tuple[AsyncIterable[Any], ...],
) -> list[str]:
    """Return unique names, suffixing with id when a name is reused."""
    raw = [_raw_dependency_name(d) for d in deps]
    if len(raw) == len(set(raw)):
        return raw
    return [f'{name}_{id(d)}' for name, d in zip(raw, deps, strict=True)]


def _unwrap_source(item: Any) -> Any:
    """Use the source bound on a ``@handle`` wrapper when present."""
    source = getattr(item, 'source', None)
    if source is not None:
        return source
    return item


def _checkpoint_name(
    name: str | None,
    stream: AsyncIterable[Any],
) -> str:
    """Resolve the cache key name for a checkpoint."""
    if name:
        return name
    topic = getattr(stream, 'name', None)
    if isinstance(topic, str) and topic:
        return topic
    return 'checkpoint'


def _collect_leaders(
    positional: tuple[Any, ...],
    named: Any,
) -> tuple[Any, ...]:
    """Combine *leaders with ``dependencies=`` without iterating a source."""
    items = list(positional)
    if named is None:
        return tuple(items)
    if isinstance(named, list | tuple):
        return (*items, *named)
    return (*items, named)


def _coerce_leader(
    item: Any,
    downtime_threshold: timedelta | None,
) -> Dependency:
    """Turn a stream, handler, or Dependency into a Dependency."""
    if isinstance(item, Dependency):
        return item
    source = _unwrap_source(item)
    raw = _raw_dependency_name(source)
    extra: dict[str, Any] = {}
    if downtime_threshold is not None:
        extra['downtime_threshold'] = downtime_threshold
    return Dependency(raw, source, **extra)


def _parse_checkpoint_args(
    dependent: AsyncIterable[Any],
    leaders: tuple[Any, ...],
    pause_dependent: bool | None,
    downtime_threshold: timedelta | None,
) -> tuple[list[Dependency], bool]:
    """Build Dependency objects and choose whether to pause."""
    stream = dependent
    built = [_coerce_leader(item, downtime_threshold) for item in leaders]
    names = [d.name for d in built]
    if len(names) != len(set(names)):
        built = [
            Dependency(
                f'{d.name}_{id(d.dependency)}',
                d.dependency,
                downtime_check=d.downtime_check,
                recovery_check=d.recovery_check,
                downtime_threshold=d.downtime_threshold,
                marker=d.marker,
            )
            for d in built
        ]
    use_lag = (
        not _is_topic(stream)
        and bool(built)
        and all(_is_topic(d.dependency) for d in built)
    )
    if use_lag:
        for d in built:
            d.use_consumer_lag()
    pause = (not use_lag) if pause_dependent is None else pause_dependent
    return built, pause


def bind_checkpoint(
    f: Callable[..., Any],
    handler: Callable[..., Awaitable[Any]],
    checkpoint: Checkpoint,
) -> Callable[..., Awaitable[Any]]:
    """Heartbeat leaders and pulse the dependent around a handler."""

    @wraps(f)
    async def _pulsed(msg: Any, **kwargs: Any) -> Any:
        downtime = await checkpoint.check_pulse(
            checkpoint.marker(msg),
            **_pulse_state(msg),
        )
        return await handler(
            msg,
            downtime=downtime,
            checkpoint=checkpoint,
            **kwargs,
        )

    c = Conf()
    for dep in checkpoint.dependencies.values():
        key = str(id(dep.dependency))
        if key not in c.iterables:
            c.register_iterable(key, dep.dependency)
        dep_name = dep.name

        async def _heartbeat(
            msg: Any,
            _name: str = dep_name,
            _marker: Callable[[Any], Any] = dep.marker or checkpoint.marker,
            **_kwargs: Any,
        ) -> None:
            await checkpoint.heartbeat(_marker(msg), _name)

        c.register_handler(key, _heartbeat)

    Checkpoint.bind_handler(_pulsed, checkpoint)
    return _pulsed
