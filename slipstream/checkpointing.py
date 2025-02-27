"""Slipstream checkpointing."""

import logging
from datetime import datetime, timedelta
from typing import (
    Any,
    AsyncIterable,
    Callable,
    Optional,
)

from slipstream.core import Conf, Signal
from slipstream.interfaces import ICache

logger = logging.getLogger(__name__)


class Dependency:
    """Track the dependent stream state to recover from downtime."""

    def __init__(
        self,
        name: str,
        stream: AsyncIterable[Any],
        downtime_threshold: Any = timedelta(minutes=10),
        downtime_check: Optional[Callable[[
            'Checkpoint', 'Dependency'], Any]] = None,
        recovery_check: Optional[Callable[[
            'Checkpoint', 'Dependency'], bool]] = None,
    ):
        """Initialize dependency for checkpointing."""
        self.name = name
        self.stream = stream
        self.checkpoint_state = None
        self.checkpoint_timestamp = None
        self._downtime_threshold = downtime_threshold
        self._downtime_check = downtime_check or self._default_downtime_check
        self._recovery_check = recovery_check or self._default_recovery_check
        self.is_down = False

    def __iter__(self):
        """Get relevant values when dict is called."""
        yield from ({
            'checkpoint_state': self.checkpoint_state,
            'checkpoint_timestamp': self.checkpoint_timestamp,
        }.items())

    def save(
        self,
        cache: ICache,
        cache_key_prefix: str,
        checkpoint_state: Any,
        checkpoint_timestamp: datetime
    ):
        """Save checkpoint state to cache."""
        key = f'{cache_key_prefix}{self.name}_'
        cache[key + 'checkpoint_state'] = checkpoint_state
        cache[key + 'checkpoint_timestamp'] = checkpoint_timestamp

    def load(self, cache: ICache, cache_key_prefix: str) -> None:
        """Load checkpoint state from cache."""
        key = f'{cache_key_prefix}{self.name}_'
        self.checkpoint_state = cache[key + 'checkpoint_state']
        self.checkpoint_timestamp = cache[key + 'checkpoint_timestamp']

    @staticmethod
    def _default_downtime_check(
        c: 'Checkpoint',
        d: 'Dependency'
    ) -> Optional[timedelta]:
        """Determine dependency downtime by comparing event timestamps.

        This behavior can be overridden by passing a callable to
        `downtime_check` that takes a `Checkpoint` object.
        """
        if not (
            isinstance(c.state_timestamp, datetime)
            and isinstance(d.checkpoint_timestamp, datetime)
        ):
            return None

        diff = c.state_timestamp - d.checkpoint_timestamp
        if diff > d._downtime_threshold:
            return diff
        return None

    @staticmethod
    def _default_recovery_check(c: 'Checkpoint', d: 'Dependency') -> bool:
        """Determine dependency has caught up by comparing event timestamps.

        This behavior can be overridden by passing a callable to
        `recovery_check` that takes a `Checkpoint` object.
        """
        if not (
            isinstance(c.state_timestamp, datetime)
            and isinstance(d.checkpoint_timestamp, datetime)
        ):
            return False

        return d.checkpoint_timestamp >= c.state_timestamp


class Checkpoint:
    """Pulse the heartbeat of dependency stream to handle downtimes.

    Call `heartbeat` with the event time in the dependency stream.
    Call `check_pulse` in the dependent stream with the event time
    and relevant state such as topic offsets or actual messages.

    When the `downtime_check` observes a downtime using the
    `downtime_threshold`, the `downtime_callback` is called
    and `is_down` is set to `True`.

    When a message is received in the dependency stream, the
    `recovery_callback` is called and `is_down` is set to
    `False` again.
    """

    def __init__(
        self,
        name: str,
        stream: AsyncIterable[Any],
        dependencies: list[Dependency],
        downtime_callback: Optional[Callable[[
            'Checkpoint', Dependency], Any]] = None,
        recovery_callback: Optional[Callable[[
            'Checkpoint', Dependency], Any]] = None,
        cache: Optional[ICache] = None,
        cache_key_prefix: str = '_',
    ):
        """Create instance that tracks downtime of dependency streams.

        If `cache` and `cache_key` are set, the checkpoint information
        is saved and loaded in the cache under `cache_key`.
        """
        self.name = name
        self.stream = stream
        self.dependencies: dict[str, Dependency] = {
            dependency.name: dependency
            for dependency in dependencies
        }
        self._cache = cache
        self._cache_key = f'{cache_key_prefix}_{name}_'
        self._downtime_callback = downtime_callback
        self._recovery_callback = recovery_callback

        self.state = None
        self.state_timestamp = None

        # Load checkpoint state from cache
        if self._cache:
            self.state = self._cache[
                f'{self._cache_key}_state']
            self.state_timestamp = self._cache[
                f'{self._cache_key}_state_timestamp']
            for dependency in self.dependencies.values():
                dependency.load(self._cache, self._cache_key)

    def __iter__(self):
        """Get relevant values when dict is called."""
        yield from ({
            'state': self.state,
            'state_timestamp': self.state_timestamp,
            'checkpoints': {
                dependency.name: dict(dependency)
                for dependency in self.dependencies.values()
            }
        }.items())

    def save_state(self, state: Any, state_timestamp: datetime) -> None:
        """Save state of the stream (to cache)."""
        self.state = state
        self.state_timestamp = state_timestamp
        if not self._cache:
            return
        self._cache[f'{self._cache_key}_state'] = state
        self._cache[f'{self._cache_key}_state_timestamp'] = state_timestamp

    def save_checkpoint(
        self,
        dependency: Dependency,
        checkpoint_state: Any,
        checkpoint_timestamp: datetime
    ) -> None:
        """Save state of the dependency checkpoint (to cache)."""
        dependency.checkpoint_state = checkpoint_state
        dependency.checkpoint_timestamp = checkpoint_timestamp
        if not self._cache:
            return
        dependency.save(
            self._cache, self._cache_key,
            checkpoint_state, checkpoint_timestamp)

    def heartbeat(
        self,
        timestamp: datetime | Any,
        dependency_name: Optional[str] = None,
    ) -> None:
        """Update checkpoint to latest state.

        Call this function whenever a message is processed in the
        dependency stream. Provide the event timestamp that is
        used to restart the dependent stream when the
        recovered dependency stream has caught up.
        """
        if dependency_name and not (
            dependency := self.dependencies.get(dependency_name)
        ):
            raise KeyError('Dependency does not exist.')
        elif len(self.dependencies) == 1:
            dependency = next(iter(self.dependencies.values()))
        else:
            raise ValueError(
                'Argument `dependency_name` must be provided '
                'for checkpoint with multiple dependencies.'
            )

        self.save_checkpoint(dependency, self.state, timestamp)

        if dependency.is_down:
            if dependency._recovery_check(self, dependency):
                dependency.is_down = False
            else:
                return

            if not any(_.is_down for _ in self.dependencies.values()):
                logger.debug(
                    f'Downtime restored of dependency "{dependency.name}", '
                    f'resuming stream "{self.name}".'
                )
                key = str(id(self.stream))
                Conf().iterables[key].send_signal(Signal.RESUME)
                if self._recovery_callback:
                    self._recovery_callback(self, dependency)

    def check_pulse(self, state, timestamp: datetime) -> Optional[Any]:
        """Update state that can be used as checkpoint.

        Call this function whenever a message is processed in the
        dependent stream. Provide relevant state that can be
        used to move back in time to reprocess faulty events
        that were sent out during the downtime.
        """
        self.save_state(state, timestamp)

        downtime = None

        for dependency in self.dependencies.values():

            # When the dependency stream hasn't had any message yet
            # set the checkpoint to the very first available state
            if not dependency.checkpoint_timestamp:
                self.save_checkpoint(dependency, state, timestamp)

            # Trigger on the first dependency that is down and
            # pause the dependent stream
            if downtime := dependency._downtime_check(self, dependency):
                logger.debug(
                    f'Downtime detected of dependency "{dependency.name}", '
                    f'pausing stream "{self.name}".'
                )
                key = str(id(self.stream))
                Conf().iterables[key].send_signal(Signal.PAUSE)
                if self._downtime_callback:
                    self._downtime_callback(self, dependency)
                dependency.is_down = True

        if any(_.is_down for _ in self.dependencies.values()):
            return downtime

    def __repr__(self) -> str:
        """Represent checkpoint."""
        return str(dict(self))
