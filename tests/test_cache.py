"""Cache tests."""

from asyncio import gather, sleep
from collections.abc import AsyncIterable, Callable
from tempfile import TemporaryDirectory

import pytest
from rocksdict import (
    DbClosedError,
    Options,
    ReadOptions,
    SstFileWriter,
    WriteBatch,
    WriteOptions,
)

from slipstream.caching import Cache


@pytest.mark.serial
@pytest.mark.parametrize(
    ('key', 'val', 'updated'),
    [
        (b'123', 'a', 'b'),
        ('123', 'b', 'c'),
        (True, 'c', 'd'),
        (123, 'd', 'e'),
    ],
)
def test_crud(key, val, updated, cache):
    """Test create/read/update/delete."""
    cache[key] = val
    assert key in cache
    assert cache[key] == val
    cache[key] = updated
    assert cache[key] == updated
    del cache[key]
    assert cache[key] is None


def test_contextmanager_cache():
    """Should automatically close cache after use."""
    key, val = 'x', 'y'
    cache = Cache('tests/db')
    try:
        with cache as c:
            c[key] = val
            assert key in cache
            assert cache[key] == val
            del cache[key]
            assert cache[key] is None

        with pytest.raises(DbClosedError):
            cache[key]
    finally:
        cache.destroy()


def test_cancel_all_background():
    """Should cancel background tasks."""
    with TemporaryDirectory(dir='tests') as tmp:
        cache = Cache(tmp)
        cache.snapshot()
        cache.cancel_all_background()

        with pytest.raises(Exception, match='Shutdown in progress'):
            cache.close()


def test_repair():
    """Should attempt to repair table."""
    with TemporaryDirectory(dir='tests') as tmp:
        cache = Cache(tmp)
        cache['key'] = 123
        cache.close()
        cache.repair(tmp)


def test_get_callable(cache):
    """Should return a callable."""
    assert isinstance(cache, Callable)


@pytest.mark.asyncio
async def test_transaction(cache):
    """Should lock db entry within with block."""
    key = '123'

    async def transaction(val):
        async with cache.transaction(key):
            entry = cache[key] or ''
            await sleep(0.01)
            await cache(key, entry + val)

    await gather(transaction('a'), transaction('b'))
    assert cache[key] == 'ab'

    await gather(transaction('b'), transaction('a'))
    assert cache[key] == 'abba'


def test_iterability(cache):
    """Should be iterable and get cache updates."""
    cache[123] = 123
    it = cache.iter()
    it.seek_to_first()

    assert it.valid()
    while it.valid():
        assert it.key() == 123
        assert it.value() == 123
        it.next()

    assert list(cache.keys()) == [123]
    assert list(cache.values()) == [123]
    assert list(cache.items()) == [(123, 123)]
    assert isinstance(aiter(cache), AsyncIterable)


def test_wrapper_methods(cache):
    """Should propagate."""
    cache.set_dumps(lambda _: bytes(_))
    cache.set_loads(lambda _: _.decode())

    cache.set_read_options(ReadOptions())
    cache.set_write_options(WriteOptions())

    # Basic operations
    cache.put('key', 123)
    assert cache.get('key') == 123

    cache.put_entity('entity', ['a', 'b'], [1, 2])
    assert cache.get_entity('entity') == [('a', 1), ('b', 2)]

    cache.delete('key')
    assert cache.get('key', default=456) == 456

    assert cache.key_may_exist('key') is False

    assert list(cache.columns(from_key='entity')) == [[('a', 1), ('b', 2)]]

    assert list(cache.entities(from_key='entity')) == [
        ('entity', [('a', 1), ('b', 2)]),
    ]

    path = cache.path()
    assert path == 'tests/db'

    # Create column family
    assert cache.create_column_family('my-fam')
    assert cache.list_cf(path) == ['default', 'my-fam']
    assert cache.get_column_family_handle('my-fam')
    assert cache.get_column_family('my-fam')
    cache.drop_column_family('my-fam')
    assert cache.list_cf(path) == ['default']

    # Write file and flush
    assert cache.live_files() == []
    cache.write(WriteBatch())
    cache.flush()
    cache.flush_wal()
    assert cache.live_files() != []

    # Write external file and ingest
    with TemporaryDirectory(dir=path) as tmp:
        dump = tmp + '/dump'
        fw = SstFileWriter(Options())
        fw.open(dump)
        fw['key'] = 123
        fw.finish()

        cache.ingest_external_file([dump])

    # Delete range
    assert cache['key'] == 123
    cache.delete_range(begin='j', end='k')  # [begin, end)
    assert cache['key'] == 123
    cache.delete_range(begin='k', end='l')
    assert cache.get('key') is None

    # Other functionalities
    assert cache.property_value('rocksdb.live-sst-files-size')
    assert cache.property_value('rocksdb.background-errors') == '0'
    cache.set_options({'table_factory.filter_policy.bloom_before_level': '3'})
    assert cache.latest_sequence_number()
