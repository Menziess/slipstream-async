"""Slipstream checkpointing."""

import logging
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Generator,
)
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, ClassVar

from slipstream.core import Conf, Signal
from slipstream.interfaces import ICache
from slipstream.utils import AsyncCallable, awaitable

_logger = logging.getLogger(__name__)


def _validate_state_markers(
    dependencies: list['Dependency'],
    state: Callable[[Any, dict[str, Any]], dict[str, Any]] | None,
    marker: Callable[..., Any] | str | None,
) -> None:
    """Require explicit dependency markers for a state-aware marker."""
    if not state or not callable(marker):
        return
    missing = [
        dependency.name for dependency in dependencies if not dependency.marker
    ]
    if missing:
        err_msg = (
            'Dependencies need their own marker when the checkpoint '
            f'marker uses state: {missing}'
        )
        raise ValueError(err_msg)


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
        marker: Callable[..., Any] | str | None = None,
        state: Callable[[Any, dict[str, Any]], dict[str, Any]] | None = None,
    ) -> None:
        """Initialize dependency for checkpointing."""
        self.name = name
        self.dependency = dependency
        self.checkpoint_state: Any = None
        self.checkpoint_marker: Any = None
        self.downtime_threshold = downtime_threshold
        self._downtime_check = downtime_check or self._default_downtime_check
        self._recovery_check = recovery_check or self._default_recovery_check
        self.marker = marker
        self.state_extractor = state
        self.is_down = False

    def uses_default_downtime_check(self) -> bool:
        """Return whether first-pulse event-time seeding applies."""
        check = self._downtime_check
        default = self._default_downtime_check
        return check is default or getattr(check, '__func__', None) is default

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
    def _default_downtime_check(
        c: 'Checkpoint',
        d: 'Dependency',
    ) -> timedelta | None:
        """Determine dependency downtime by comparing event timestamps.

        This behavior can be overridden by passing a callable to
        `downtime_check` that takes a `Checkpoint` object.
        """
        diff = c.state_marker - d.checkpoint_marker
        if diff > d.downtime_threshold:
            return diff
        return None

    @staticmethod
    def _default_recovery_check(c: 'Checkpoint', d: 'Dependency') -> bool:
        """Determine dependency has caught up by comparing event timestamps.

        This behavior can be overridden by passing a callable to
        `recovery_check` that takes a `Checkpoint` object.
        """
        return d.checkpoint_marker > c.state_marker

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
    """Track one stream against the streams it depends on.

    >>> async def emoji():
    ...     for emoji in '🏆📞🐟👌':
    ...         yield emoji
    >>> dependent, dependency = emoji(), emoji()
    >>> checkpoint = Checkpoint(
    ...     dependent,
    ...     Dependency('dependency', dependency),
    ...     name='dependent',
    ... )

    Pass a marker and bind the checkpoint to call ``heartbeat`` and
    ``check_pulse`` automatically:

    >>> checkpoint = Checkpoint(
    ...     dependent,
    ...     Dependency('dependency', dependency, marker='timestamp'),
    ...     name='dependent',
    ...     marker='timestamp',
    ... )
    >>> from slipstream import handle
    >>> @handle(checkpoint)
    ... async def dependent_handler(msg, checkpoint=None):
    ...     yield msg

    If no cache is provided, the checkpoint lasts only for this process.
    """

    _by_handler: ClassVar[dict[Callable[..., Any], 'Checkpoint']] = {}

    def __init__(
        self,
        dependent: AsyncIterable[Any],
        *leaders: AsyncIterable[Any] | Dependency,
        dependencies: AsyncIterable[Any]
        | Dependency
        | list[AsyncIterable[Any] | Dependency]
        | None = None,
        name: str | None = None,
        on_downtime: Callable[['Checkpoint', Dependency], Any] | None = None,
        on_recovery: Callable[['Checkpoint', Dependency], Any] | None = None,
        cache: ICache | None = None,
        cache_key_prefix: str = '_',
        pause_dependent: bool | None = None,
        downtime_threshold: timedelta | None = None,
        marker: Callable[..., Any] | str | None = None,
        state: Callable[[Any, dict[str, Any]], dict[str, Any]] | None = None,
    ) -> None:
        """Create a checkpoint for ``dependent`` against its dependencies.

        ::

            Checkpoint(
                activity,
                Dependency('weather', weather),
                name='activity',
            )
        """
        items = [*leaders]
        if isinstance(dependencies, list):
            items.extend(dependencies)
        elif dependencies is not None:
            items.append(dependencies)
        built = []
        for item in items:
            if isinstance(item, Dependency):
                built.append(item)
            elif downtime_threshold is None:
                built.append(Dependency(str(id(item)), item))
            else:
                built.append(
                    Dependency(
                        str(id(item)),
                        item,
                        downtime_threshold=downtime_threshold,
                    )
                )
        names = [dependency.name for dependency in built]
        if len(names) != len(set(names)):
            err_msg = 'Dependency names must be unique.'
            raise ValueError(err_msg)
        self.name = name or getattr(dependent, 'name', 'checkpoint')
        self.dependent = dependent
        self.dependencies = {d.name: d for d in built}
        if id(self.dependent) in {id(d.dependency) for d in built}:
            err_msg = 'Checkpoint cannot depend on its dependent stream.'
            raise ValueError(err_msg)
        _validate_state_markers(built, state, marker)
        self.pause_dependent = (
            True if pause_dependent is None else pause_dependent
        )
        self.marker = marker
        self.state_extractor = state
        self.downtime: Any | None = None
        self._cache = cache
        self._cache_key = f'{cache_key_prefix}_{self.name}_'
        self._downtime_callback = on_downtime
        self._recovery_callback = on_recovery
        self._awaiting_resume = False

        self.state = {}
        self.state_marker: Any = None

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
        checkpoint_state: Any = None,
    ) -> dict:
        """Update checkpoint to latest state.

        Args:
            marker (datetime | Any): Typically the event timestamp that is
                compared to the event timestamp of a dependent stream.
            dependency_name (str, optional): Required when there are multiple
                dependencies to specify which one the heartbeat is for.
            checkpoint_state: Complete caller-selected dependency state.
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

        self._save_checkpoint(
            dependency,
            self.state if checkpoint_state is None else checkpoint_state,
            marker,
        )

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
        checkpoint_state: Any = None,
        **kwargs: Any,
    ) -> Any | None:
        """Update state that can be used as checkpoint.

        Args:
            marker (datetime | Any): Typically the event timestamp that is
                compared to the event timestamp of a dependency stream.
            checkpoint_state: Complete caller-selected dependent state.
            kwargs (Any): Any information that can be used for reprocessing any
                incorrect data that was sent out during downtime of a
                dependency stream, stored in `state`.

        Returns:
            None when every dependency is healthy. Otherwise a
            :class:`Downtime` map of name → check (or ``True`` if still
            down but not over threshold). One leader still compares
            equal to its timedelta / ``True``.
        """
        self._save_state(marker, checkpoint_state, **kwargs)

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
            dependency.checkpoint_marker is None
            and dependency.uses_default_downtime_check()
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

    def _save_state(
        self,
        state_marker: datetime | Any,
        checkpoint_state: dict[str, Any] | None,
        **kwargs: Any,
    ) -> None:
        """Save state of the stream (to cache).

        Markers only move forward so another partition cannot rewind the
        checkpoint.
        """
        if checkpoint_state is None:
            self.state.update(**kwargs)
        else:
            self.state = dict(checkpoint_state)
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

        Markers only move forward so another partition cannot rewind the
        checkpoint.
        """
        checkpoint_marker = _later_marker(
            dependency.checkpoint_marker,
            checkpoint_marker,
        )
        dependency.checkpoint_state = dict(checkpoint_state)
        dependency.checkpoint_marker = checkpoint_marker
        if not self._cache:
            return
        dependency.save(
            self._cache,
            self._cache_key,
            dependency.checkpoint_state,
            checkpoint_marker,
        )

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

    @classmethod
    def for_handler(cls, handler: Callable[..., Any]) -> 'Checkpoint':
        """Return the checkpoint bound by ``@handle``."""
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
        """Record a checkpoint binding."""
        cls._by_handler[handler] = checkpoint
        handler.checkpoint = checkpoint  # type: ignore[attr-defined]


def _later_marker(old: Any, new: Any) -> Any:
    """Keep the high-water marker."""
    return new if old is None else max(old, new)


def _marker_value(marker: Callable[[Any], Any] | str, msg: Any) -> Any:
    """Apply only the marker policy supplied by the caller."""
    if callable(marker):
        return marker(msg)
    try:
        return msg[marker]
    except TypeError:
        return msg.value[marker]


def bind_checkpoint(
    f: Callable[..., Any],
    handler: Callable[..., Awaitable[Any]],
    checkpoint: Checkpoint,
) -> Callable[..., Awaitable[Any]]:
    """Heartbeat dependencies and pulse the dependent around a handler."""
    if checkpoint.marker is None:
        err_msg = 'A marker is required when binding a checkpoint to handle.'
        raise ValueError(err_msg)
    checkpoint_marker = checkpoint.marker

    @wraps(f)
    async def _pulsed(msg: Any, **kwargs: Any) -> Any:
        state = (
            checkpoint.state_extractor(msg, dict(checkpoint.state))
            if checkpoint.state_extractor
            else None
        )
        marker = (
            checkpoint_marker(msg, state)
            if checkpoint.state_extractor and callable(checkpoint_marker)
            else _marker_value(checkpoint_marker, msg)
        )
        downtime = await checkpoint.check_pulse(
            marker,
            checkpoint_state=state,
        )
        return await handler(
            msg,
            downtime=downtime,
            checkpoint=checkpoint,
            **kwargs,
        )

    c = Conf()
    for dependency in checkpoint.dependencies.values():
        key = str(id(dependency.dependency))
        if key not in c.iterables:
            c.register_iterable(key, dependency.dependency)
        marker = dependency.marker or checkpoint_marker

        async def _heartbeat(
            msg: Any,
            _name: str = dependency.name,
            _dependency: Dependency = dependency,
            _marker: Callable[..., Any] | str = marker,
            **_kwargs: Any,
        ) -> None:
            state = (
                _dependency.state_extractor(
                    msg,
                    dict(_dependency.checkpoint_state or {}),
                )
                if _dependency.state_extractor
                else None
            )
            marker_value = (
                _marker(msg, state)
                if _dependency.state_extractor and callable(_marker)
                else _marker_value(_marker, msg)
            )
            await checkpoint.heartbeat(
                marker_value,
                _name,
                checkpoint_state=state,
            )

        c.register_handler(key, _heartbeat)

    Checkpoint.bind_handler(_pulsed, checkpoint)
    return _pulsed
