"""Slipstream checkpointing."""

import logging
from collections.abc import AsyncIterable, Callable, Generator
from datetime import datetime, timedelta
from typing import Any

from slipstream.core import Conf, Signal
from slipstream.interfaces import ICache
from slipstream.utils import AsyncCallable, awaitable, iscoroutinecallable

_logger = logging.getLogger(__name__)


STATE_NAME = 'state'
STATE_MARKER_NAME = 'state_marker'
CHECKPOINT_IS_DOWN = 'checkpoint_is_down'
CHECKPOINT_STATE_NAME = 'checkpoint_state'
CHECKPOINT_MARKER_NAME = 'checkpoint_marker'
CHECKPOINTS_NAME = 'checkpoints'


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
    ) -> None:
        """Initialize dependency for checkpointing."""
        self.name = name
        self.dependency = dependency
        self.checkpoint_state = None
        self.checkpoint_marker = None
        self.downtime_threshold = downtime_threshold
        self._downtime_check = downtime_check or self._default_downtime_check
        self._recovery_check = recovery_check or self._default_recovery_check
        self.is_down = False

    def save_is_down(
        self, cache: ICache, cache_key_prefix: str, is_down: bool
    ) -> None:
        """Save is_down checkpoint state to cache."""
        key = f'{cache_key_prefix}{self.name}_'
        cache[key + CHECKPOINT_IS_DOWN] = is_down

    def save(
        self,
        cache: ICache,
        cache_key_prefix: str,
        checkpoint_state: Any,
        checkpoint_marker: datetime,
    ) -> None:
        """Save checkpoint state to cache."""
        key = f'{cache_key_prefix}{self.name}_'
        cache[key + CHECKPOINT_STATE_NAME] = checkpoint_state
        cache[key + CHECKPOINT_MARKER_NAME] = checkpoint_marker

    def load(self, cache: ICache, cache_key_prefix: str) -> None:
        """Load checkpoint state from cache."""
        key = f'{cache_key_prefix}{self.name}_'
        self.is_down = cache[key + CHECKPOINT_IS_DOWN]
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
        if not (
            isinstance(c.state_marker, datetime)
            and isinstance(d.checkpoint_marker, datetime)
        ):
            err_msg = (
                'Expecting either `datetime` markers in heartbeat and '
                'check_pulse, or a custom downtime_check in dependency, '
                f'got; {c.state_marker} and {d.checkpoint_marker}'
            )
            raise TypeError(err_msg)

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
        if not (
            isinstance(c.state_marker, datetime)
            and isinstance(d.checkpoint_marker, datetime)
        ):
            err_msg = (
                'Expecting either `datetime` markers in heartbeat and '
                'check_pulse, or a custom recovery_check in dependency, '
                f'got; {c.state_marker} and {d.checkpoint_marker}'
            )
            raise TypeError(err_msg)

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
    """Pulse the heartbeat of dependency streams to handle downtimes.

    A checkpoint consists of a dependent stream and dependency streams.

    >>> async def emoji():
    ...     for emoji in '🏆📞🐟👌':
    ...         yield emoji

    >>> dependent, dependency = emoji(), emoji()

    The checkpoint and dependency names should not be changed once created,
    they are used to persist the checkpoint in the cache.

    >>> c = Checkpoint(
    ...     'dependent',
    ...     dependent=dependent,
    ...     dependencies=[Dependency('dependency', dependency)],
    ... )

    Checkpoints automatically handle pausing of dependent streams
    when they are bound to user handler functions (using `handle`):

    >>> from slipstream import handle

    >>> @handle(dependent)
    ... async def dependent_handler(msg):
    ...     await c.check_pulse(marker=msg.value['event_timestamp'])
    ...     yield msg.key, msg.value

    >>> @handle(dependency)
    ... async def dependency_handler(msg):
    ...     await c.heartbeat(msg.value['event_timestamp'])
    ...     yield msg.key, msg.value

    On the first pulse check, no message might have been received
    from `dependency` yet. Therefore the dependency checkpoint is
    updated with the initial state and marker of the
    dependent stream:

    >>> from asyncio import run

    >>> run(c.check_pulse(marker=datetime(2025, 1, 1, 10), offset=8))
    >>> c['dependency'].checkpoint_marker
    datetime.datetime(2025, 1, 1, 10, 0)

    When a message is received in `dependency`, send a heartbeat
    with its event time, which can be compared with the
    dependent event times to check for downtime:

    >>> run(c.heartbeat(datetime(2025, 1, 1, 10, 30)))
    {'is_late': False, ...}

    When the pulse is checked after a while, it's apparent that no
    dependency messages have been received for 30 minutes:

    >>> run(c.check_pulse(marker=datetime(2025, 1, 1, 11), offset=9))
    datetime.timedelta(seconds=1800)

    Because the downtime surpasses the default `downtime_threshold`,
    the dependent stream will be paused (and resumed when the
    recovery check succeeds). Callbacks can be provided for
    additional custom behavior.

    As the dependency stream recovers, it has to "catch up" with the
    the dependent stream first. Until then, the dependent stream
    stays paused, and the dependency stream is marked as down.

    >>> run(c.heartbeat(datetime(2025, 1, 1, 10, 45)))
    {'is_late': True, ...}

    >>> run(c.heartbeat(datetime(2025, 1, 1, 11, 1)))
    {'is_late': False, ...}

    If no cache is provided, the checkpoint lifespan will be limited
    to that of the application runtime.
    """

    def __init__(
        self,
        name: str,
        dependent: AsyncIterable[Any],
        dependencies: list[Dependency],
        downtime_callback: Callable[['Checkpoint', Dependency], Any]
        | None = None,
        recovery_callback: Callable[['Checkpoint', Dependency], Any]
        | None = None,
        cache: ICache | None = None,
        cache_key_prefix: str = '_',
        pause_dependent: bool = True,
    ) -> None:
        """Create instance that tracks downtime of dependency streams."""
        self.name = name
        self.dependent = dependent
        self.dependencies: dict[str, Dependency] = {
            dependency.name: dependency for dependency in dependencies
        }
        self.pause_dependent = pause_dependent
        self._cache = cache
        self._cache_key = f'{cache_key_prefix}_{name}_'
        self._downtime_callback = downtime_callback
        self._recovery_callback = recovery_callback

        self.state = {}
        self.state_marker = None

        # Load checkpoint state from cache
        if self._cache:
            self.state = self._cache[f'{self._cache_key}_{STATE_NAME}'] or {}
            self.state_marker = self._cache[
                f'{self._cache_key}_{STATE_MARKER_NAME}'
            ]
            for dependency in self.dependencies.values():
                dependency.load(self._cache, self._cache_key)

        # Pause dependent if any dependency was down
        # Callbacks will not be called at startup of the app
        for dependency in self.dependencies.values():
            if not dependency.is_down:
                continue
            log_msg = f'Downtime of dependency "{dependency.name}" detected'
            _logger.debug(log_msg)
            key, c = str(id(self.dependent)), Conf()
            if self.pause_dependent and key in c.iterables:
                c.iterables[key].send_signal(Signal.PAUSE)

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
                self._save_dependency_is_down(dependency, False)

            if not any(_.is_down for _ in self.dependencies.values()):
                _logger.debug(
                    f'Dependency "{dependency.name}" downtime resolved',
                )
                key, c = str(id(self.dependent)), Conf()
                if self.pause_dependent and key in c.iterables:
                    c.iterables[key].send_signal(Signal.RESUME)
                if self._recovery_callback:
                    if iscoroutinecallable(self._recovery_callback):
                        await self._recovery_callback(self, dependency)
                    else:
                        self._recovery_callback(self, dependency)

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
            Any: Typically the timedelta between the last state_marker and
                the checkpoint_marker since the stream went down.
        """
        self._save_state(marker, **kwargs)

        downtime = None

        for dependency in self.dependencies.values():
            # When the dependency stream hasn't had any message yet
            # set the checkpoint to the very first available state
            if not dependency.checkpoint_marker:
                self._save_checkpoint(
                    dependency,
                    self.state,
                    self.state_marker,
                )

            # Trigger on the first dependency that is down and
            # pause the dependent stream
            if downtime := await awaitable(
                dependency.downtime_check(self, dependency)
            ):
                log_msg = (
                    f'Downtime of dependency "{dependency.name}" detected'
                )
                _logger.debug(log_msg)
                key, c = str(id(self.dependent)), Conf()
                if self.pause_dependent and key in c.iterables:
                    c.iterables[key].send_signal(Signal.PAUSE)
                if self._downtime_callback:
                    if iscoroutinecallable(self._downtime_callback):
                        await self._downtime_callback(self, dependency)
                    else:
                        self._downtime_callback(self, dependency)
                self._save_dependency_is_down(dependency, True)

        if any(_.is_down for _ in self.dependencies.values()):
            return downtime
        return None

    def _save_state(self, state_marker: datetime | Any, **kwargs: Any) -> None:
        """Save state of the stream (to cache)."""
        self.state.update(**kwargs)
        self.state_marker = state_marker
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
        """Save state of the dependency checkpoint (to cache)."""
        dependency.checkpoint_state = checkpoint_state
        dependency.checkpoint_marker = checkpoint_marker
        if not self._cache:
            return
        dependency.save(
            self._cache,
            self._cache_key,
            checkpoint_state,
            checkpoint_marker,
        )

    def _save_dependency_is_down(
        self, dependency: Dependency, is_down: bool
    ) -> None:
        """Save is_down state of the dependency checkpoint (to cache)."""
        dependency.is_down = is_down
        if not self._cache:
            return
        dependency.save_is_down(self._cache, self._cache_key, is_down)

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
