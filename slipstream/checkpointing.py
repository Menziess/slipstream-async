"""Slipstream checkpointing."""

from datetime import datetime, timedelta
from typing import (
    Any,
    AsyncIterable,
    Callable,
    Optional,
)

from slipstream.core import Conf, Signal
from slipstream.interfaces import ICache, Key


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
        stream: AsyncIterable[Any],
        dependencies: list[AsyncIterable[Any]],
        cache: Optional[ICache] = None,
        cache_key: Key = '_',
        downtime_threshold: Any = timedelta(minutes=10),
        downtime_check: Optional[Callable] = None,
        recovery_check: Optional[Callable] = None,
        downtime_callback: Optional[Callable] = None,
        recovery_callback: Optional[Callable] = None
    ):
        """Create instance that tracks downtime of dependency streams.

        If `cache` and `cache_key` are set, the checkpoint information
        is saved and loaded in the cache under `cache_key`.
        """
        self.stream = stream
        self.dependencies = dependencies
        self._cache = cache
        self._cache_key = cache_key
        self._downtime_threshold = downtime_threshold
        self._downtime_check = downtime_check or self._default_downtime_check
        self._recovery_check = recovery_check or self._default_recovery_check
        self._downtime_callback = downtime_callback
        self._recovery_callback = recovery_callback
        self.is_down = False

        if cached := self.load():
            self.state = cached['state']
            self.state_timestamp = cached['state_timestamp']
            self.checkpoint_state = cached['checkpoint_state']
            self.checkpoint_timestamp = cached['checkpoint_timestamp']
        else:
            self.state = None
            self.state_timestamp = None
            self.checkpoint_state = None
            self.checkpoint_timestamp = None

    def __iter__(self):
        """Get relevant values when dict is called."""
        yield from ({
            'state': self.state,
            'state_timestamp': self.state_timestamp,
            'checkpoint_state': self.checkpoint_state,
            'checkpoint_timestamp': self.checkpoint_timestamp,
        }.items())

    def save(self):
        """Save dict representation in cache if cache is provided."""
        if not self._cache:
            return
        self._cache[self._cache_key] = dict(self)

    def load(self):
        """Load dict representation from cache if cache is provided."""
        if self._cache:
            return self._cache[self._cache_key]

    def heartbeat(self, timestamp: datetime):
        """Update checkpoint to latest state.

        Call this function whenever a message is processed in the
        dependency stream. Provide the event timestamp that is
        used to restart the dependent stream when the
        recovered dependency stream has caught up.
        """
        self.checkpoint_state = self.state
        self.checkpoint_timestamp = timestamp
        self.save()
        if self.is_down:
            if self._recovery_check(self):
                key = str(id(self.stream))
                Conf().iterables[key].send_signal(Signal.RESUME)
                if self._recovery_callback:
                    self._recovery_callback()
                self.is_down = False

    def check_pulse(self, state, timestamp: datetime):
        """Update state that can be used as checkpoint.

        Call this function whenever a message is processed in the
        dependent stream. Provide relevant state that can be
        used to move back in time to reprocess faulty events
        that were sent out during the downtime.
        """
        if not self.checkpoint_timestamp:
            # When the dependency stream hasn't had any message yet
            # set the checkpoint to the very first available state
            self.checkpoint_state = state
            self.checkpoint_timestamp = timestamp

        self.state = state
        self.state_timestamp = timestamp
        self.save()

        if downtime := self._downtime_check(self):
            key = str(id(self.stream))
            Conf().iterables[key].send_signal(Signal.PAUSE)
            if self._downtime_callback:
                self._downtime_callback()
            self.is_down = True
            return downtime

    @staticmethod
    def _default_downtime_check(c: 'Checkpoint'):
        """Determine dependency downtime by comparing event timestamps.

        This behavior can be overridden by passing a callable to
        `downtime_check` that takes a `Checkpoint` object.
        """
        if not (
            isinstance(c.state_timestamp, datetime)
            and isinstance(c.checkpoint_timestamp, datetime)
        ):
            return None

        diff = c.state_timestamp - c.checkpoint_timestamp
        if diff > c._downtime_threshold:
            return diff
        return None

    @staticmethod
    def _default_recovery_check(c: 'Checkpoint'):
        """Determine dependency has caught up by comparing event timestamps.

        This behavior can be overridden by passing a callable to
        `recovery_check` that takes a `Checkpoint` object.
        """
        if not (
            isinstance(c.state_timestamp, datetime)
            and isinstance(c.checkpoint_timestamp, datetime)
        ):
            return None

        return c.checkpoint_timestamp >= c.state_timestamp

    def __repr__(self) -> str:
        """Represent checkpoint."""
        return str(dict(self))
