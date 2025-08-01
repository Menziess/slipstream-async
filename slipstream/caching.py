"""Slipstream caching."""

import os
from asyncio import Lock
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from types import TracebackType
from typing import Any, TypeVar

from slipstream.interfaces import ICache, Key

rocksdict_available = False

try:
    import rocksdict  # noqa: F401

    rocksdict_available = True
except ImportError:  # pragma: no cover
    pass

__all__ = [
    'MB',
    'MINUTES',
    'Cache',
]


MB = 1024 * 1024
MINUTES = 60
T = TypeVar('T')

if rocksdict_available:
    from rocksdict import (
        AccessType,
        ColumnFamily,
        CompactOptions,
        DBCompactionStyle,
        DBCompressionType,
        FifoCompactOptions,
        IngestExternalFileOptions,
        Options,
        Rdict,
        RdictIter,
        ReadOptions,
        Snapshot,
        WriteBatch,
        WriteOptions,
    )
    from rocksdict.rocksdict import (
        RdictColumns,
        RdictEntities,
        RdictItems,
        RdictKeys,
        RdictValues,
    )

    class Cache(ICache):
        """Create a RocksDB database in the specified folder.

        >>> cache = Cache('db/mycache')  # doctest: +SKIP

        The cache instance acts as a callable to store data:

        >>> cache('key', {'msg': 'Hello World!'})  # doctest: +SKIP
        >>> cache['key']  # doctest: +SKIP
        {'msg': 'Hello World!'}
        """

        def __init__(
            self,
            path: str,
            options: Options | None = None,
            column_families: dict[str, Options] | None = None,
            access_type: AccessType | None = None,
            target_table_size: int = 25 * MB,
            number_of_locks: int = 16,
        ) -> None:
            """Create instance that holds rocksdb reference.

            This configuration setup optimizes for low disk usage.
            (25mb per table).
            The oldest records may be removed during compaction.

            https://congyuwang.github.io/RocksDict/rocksdict.html
            """
            self.name = path
            self._number_of_locks = number_of_locks
            self._locks = [Lock() for _ in range(self._number_of_locks)]
            options = options or self._default_options(target_table_size)
            access_type = access_type or AccessType.read_write()
            column_families = (
                column_families
                or dict.fromkeys(Rdict.list_cf(path, options), options)
                if Path(path + '/CURRENT').exists()
                else {}
            )
            self.db = Rdict(path, options, column_families, access_type)

        @staticmethod
        def _default_options(target_table_size: int) -> Options:
            options = Options()
            compaction_options = FifoCompactOptions()
            compaction_options.max_table_files_size = target_table_size
            options.create_if_missing(create_if_missing=True)
            options.set_max_background_jobs(os.cpu_count() or 2)
            options.increase_parallelism(os.cpu_count() or 2)
            options.set_log_file_time_to_roll(30 * MINUTES)
            options.set_keep_log_file_num(1)
            options.set_max_log_file_size(int(0.1 * MB))
            options.set_max_manifest_file_size(MB)
            options.set_fifo_compaction_options(compaction_options)
            options.set_compaction_style(DBCompactionStyle.fifo())
            options.set_level_zero_file_num_compaction_trigger(4)
            options.set_level_zero_slowdown_writes_trigger(6)
            options.set_level_zero_stop_writes_trigger(8)
            options.set_max_write_buffer_number(2)
            options.set_write_buffer_size(1 * MB)
            options.set_target_file_size_base(256 * MB)
            options.set_max_bytes_for_level_base(1024 * MB)
            options.set_max_bytes_for_level_multiplier(4.0)
            options.set_compression_type(DBCompressionType.lz4())
            options.set_delete_obsolete_files_period_micros(10 * 1000)
            return options

        @asynccontextmanager
        async def _get_lock(self, key: Key) -> AsyncGenerator[Lock, None]:
            """Get lock from a pool of locks based on key."""
            index = hash(key) % self._number_of_locks
            async with (lock := self._locks[index]):
                yield lock

        def __contains__(self, key: Key) -> bool:
            """Key exists in db."""
            return key in self.db

        def __delitem__(self, key: Key) -> None:
            """Delete item from db."""
            del self.db[key]

        def __getitem__(self, key: Key | list[Key]) -> Any:
            """Get item from db or None."""
            try:
                return self.db[key]
            except KeyError:
                pass

        def __setitem__(self, key: Key, val: Any) -> None:
            """Set item in db."""
            self.db[key] = val

        @asynccontextmanager
        async def transaction(self, key: Key) -> AsyncGenerator['Cache', None]:
            """Lock the db entry while using the context manager.

            >>> async with cache.transaction('fish'):  # doctest: +SKIP
            ...     cache['fish'] = '🐟'

            - This works for asynchronous code (not multi-threading/processing)
            - While locked, other transactions on the same key will block
            - Actions outside of transaction blocks ignore ongoing transactions
            - Reads aren't limited by ongoing transactions
            """
            async with self._get_lock(key):
                yield self

        def __enter__(self) -> 'Cache':
            """Contextmanager."""
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: TracebackType | None,
        ) -> None:
            """Exit contextmanager."""
            self.close()

        def set_dumps(self, dumps: Callable[[Any], bytes]) -> None:
            """Set custom dumps function."""
            return self.db.set_dumps(dumps)

        def set_loads(self, dumps: Callable[[bytes], Any]) -> None:
            """Set custom loads function."""
            return self.db.set_loads(dumps)

        def set_read_options(self, read_opt: ReadOptions) -> None:
            """Set custom read options."""
            return self.db.set_read_options(read_opt)

        def set_write_options(self, write_opt: WriteOptions) -> None:
            """Set custom write options."""
            return self.db.set_write_options(write_opt)

        def put(
            self,
            key: Key,
            value: Any,
            write_opt: WriteOptions | None = None,
        ) -> None:
            """Put item in database using key."""
            return self.db.put(key, value, write_opt)

        def get(
            self,
            key: Key | list[Key],
            default: T = None,
            read_opt: ReadOptions | None = None,
        ) -> Any | T:
            """Get item from database by key."""
            return self.db.get(key, default, read_opt)

        def put_entity(
            self,
            key: Key,
            names: list[Any],
            values: list[Any],
            write_opt: WriteOptions | None = None,
        ) -> None:
            """Put wide-column in database using key.

            >>> cache.put_entity('key', ['a', 'b'], [1, 2])  # doctest: +SKIP
            """
            return self.db.put_entity(key, names, values, write_opt)

        def get_entity(
            self,
            key: Key | list[Key],
            default: Any = None,
            read_opt: ReadOptions | None = None,
        ) -> list[tuple[Any, Any]] | None:
            """Get wide-column from database by key.

            >>> cache.get_entity('key')  # doctest: +SKIP
            [('a', 1), ('b', 2)]
            """
            return self.db.get_entity(key, default, read_opt)

        def delete(
            self,
            key: Key,
            write_opt: WriteOptions | None = None,
        ) -> None:
            """Delete item from database."""
            return self.db.delete(key, write_opt)

        def key_may_exist(
            self,
            key: Key,
            fetch: bool = False,
            read_opt: ReadOptions | None = None,
        ) -> bool | tuple[bool, Any]:
            """Check if a key exist without performing IO operations."""
            return self.db.key_may_exist(key, fetch, read_opt)

        def iter(self, read_opt: ReadOptions | None = None) -> RdictIter:
            """Get iterable."""
            return self.db.iter(read_opt)

        def items(
            self,
            backwards: bool = False,
            from_key: str | int | float | bytes | bool | None = None,
            read_opt: ReadOptions | None = None,
        ) -> RdictItems:
            """Get tuples of key-value pairs."""
            return self.db.items(backwards, from_key, read_opt)

        def keys(
            self,
            backwards: bool = False,
            from_key: str | int | float | bytes | bool | None = None,
            read_opt: ReadOptions | None = None,
        ) -> RdictKeys:
            """Get keys."""
            return self.db.keys(backwards, from_key, read_opt)

        def values(
            self,
            backwards: bool = False,
            from_key: Key | None = None,
            read_opt: ReadOptions | None = None,
        ) -> RdictValues:
            """Get values."""
            return self.db.values(backwards, from_key, read_opt)

        def columns(
            self,
            backwards: bool = False,
            from_key: Key | None = None,
            read_opt: ReadOptions | None = None,
        ) -> RdictColumns:
            """Get values as widecolumns."""
            return self.db.columns(backwards, from_key, read_opt)

        def entities(
            self,
            backwards: bool = False,
            from_key: Key | None = None,
            read_opt: ReadOptions | None = None,
        ) -> RdictEntities:
            """Get keys and entities."""
            return self.db.entities(backwards, from_key, read_opt)

        def ingest_external_file(
            self,
            paths: list[str],
            opts: IngestExternalFileOptions | None = None,
        ) -> None:
            """Load list of SST files into current column family."""
            opts = opts or IngestExternalFileOptions()
            return self.db.ingest_external_file(paths, opts)

        def get_column_family(self, name: str) -> Rdict:
            """Get column family by name."""
            return self.db.get_column_family(name)

        def get_column_family_handle(self, name: str) -> ColumnFamily:
            """Get column family handle by name."""
            return self.db.get_column_family_handle(name)

        def drop_column_family(self, name: str) -> None:
            """Drop column family by name."""
            return self.db.drop_column_family(name)

        def create_column_family(
            self,
            name: str,
            options: Options | None = None,
        ) -> Rdict:
            """Craete column family."""
            options = options or Options()
            return self.db.create_column_family(name, options)

        def delete_range(
            self,
            begin: Key,
            end: Key,
            write_opt: WriteOptions | None = None,
        ) -> None:
            """Delete database items, excluding end."""
            return self.db.delete_range(begin, end, write_opt)

        def snapshot(self) -> Snapshot:
            """Create snapshot of current column family."""
            return self.db.snapshot()

        def path(self) -> str:
            """Get current database path."""
            return self.db.path()

        def set_options(self, options: dict[str, str]) -> None:
            """Set options for current column family."""
            return self.db.set_options(options)

        def property_value(self, name: str) -> str | None:
            """Get property by name from current column family."""
            return self.db.property_value(name)

        def property_int_value(self, name: str) -> int | None:
            """Get property as int by name from current column family."""
            return self.db.property_int_value(name)

        def latest_sequence_number(self) -> int:
            """Get sequence number of the most recent transaction."""
            return self.db.latest_sequence_number()

        def try_catch_up_with_primary(self) -> None:
            """Try to catch up with the primary by reading log files."""
            self.db.try_catch_up_with_primary()

        def cancel_all_background(self, wait: bool = True) -> None:
            """Request stopping background work."""
            self.db.cancel_all_background(wait)

        def list_cf(
            self,
            path: str,
            options: Options | None = None,
        ) -> list[str]:
            """List column families."""
            options = options or Options()
            return self.db.list_cf(path, options)

        def live_files(self) -> list[dict[str, Any]]:
            """Get list of all table files with their level, start/end key."""
            return self.db.live_files()

        def compact_range(
            self,
            begin: Key | None,
            end: Key | None,
            compact_opt: CompactOptions | None = None,
        ) -> None:
            """Run manual compaction on range for the current column family."""
            compact_opt = compact_opt or CompactOptions()
            return self.db.compact_range(begin, end, compact_opt)

        def close(self) -> None:
            """Flush memory to disk, and drop the current column family."""
            return self.db.close()

        def write(
            self,
            write_batch: WriteBatch,
            write_opt: WriteOptions | None = None,
        ) -> None:
            """Write a batch."""
            self.db.write(write_batch, write_opt)

        def flush(self, wait: bool = True) -> None:
            """Manually flush the current column family."""
            return self.db.flush(wait)

        def flush_wal(self, sync: bool = True) -> None:
            """Manually flush the WAL buffer."""
            return self.db.flush_wal(sync)

        def repair(self, path: str, options: Options | None = None) -> None:
            """Repair the database."""
            options = options or Options()
            self.db.repair(path, options)

        def destroy(self, options: Options | None = None) -> None:
            """Delete the database."""
            options = options or Options()
            return Rdict.destroy(self.name, options)
