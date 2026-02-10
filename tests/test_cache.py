"""Cache tests."""

from asyncio import gather, sleep
from collections.abc import AsyncIterable, Callable
from tempfile import TemporaryDirectory

import pytest
from rocksdict import (
    AccessType,
    DbClosedError,
    Options,
    ReadOptions,
    SstFileWriter,
    WriteBatch,
    WriteOptions,
)

from slipstream.caching import Cache, Proxy


@pytest.mark.asyncio
async def test_proxy():
    """Should receive and send messages via pubsub."""
    p = Proxy()
    assert p._pubsub is not None
    assert p._iterable_key is not None

    result = None

    async def send():
        await p('test')

    async def receive():
        nonlocal result
        async for item in p:
            result = item
            break

    await gather(receive(), send())
    assert result == 'test'


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


def test_secondary_db():
    """Should automatically close cache after use."""
    with TemporaryDirectory(dir='tests') as tmp:
        first_path, second_path = tmp + '/first', tmp + '/secondary'
        first = Cache(first_path)
        secondary = Cache(
            first_path, access_type=AccessType.secondary(second_path)
        )
        with first, secondary as s:
            s.try_catch_up_with_primary()

        first.destroy()
        secondary.destroy()


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
    cache.compact_range(begin='k', end='l')
    cache.delete_range(begin='k', end='l')
    assert cache.get('key') is None

    # Other functionalities
    assert cache.property_value('rocksdb.live-sst-files-size')
    assert cache.property_int_value('rocksdb.background-errors') == 0
    cache.set_options({'table_factory.filter_policy.bloom_before_level': '3'})
    assert cache.latest_sequence_number()


def test_keys_prefix(cache):
    """Should stop iteration when key no longer matches prefix."""
    cache['user:1'] = 'alice'
    cache['user:2'] = 'bob'
    cache['user:3'] = 'charlie'
    cache['order:1'] = 'order1'
    cache['order:2'] = 'order2'

    assert list(cache.keys(prefix='user:')) == ['user:1', 'user:2', 'user:3']

    assert list(cache.keys(prefix='order:')) == ['order:1', 'order:2']

    assert len(list(cache.keys())) == 5


def test_items_prefix(cache):
    """Should stop iteration when key no longer matches prefix."""
    cache['user:1'] = 'alice'
    cache['user:2'] = 'bob'
    cache['order:1'] = 'order1'

    assert list(cache.items(prefix='user:')) == [
        ('user:1', 'alice'),
        ('user:2', 'bob'),
    ]

    assert list(cache.items(prefix='order:')) == [('order:1', 'order1')]


def test_values_prefix(cache):
    """Should stop iteration when key no longer matches prefix."""
    cache['user:1'] = 'alice'
    cache['user:2'] = 'bob'
    cache['order:1'] = 'order1'

    assert list(cache.values(prefix='user:')) == ['alice', 'bob']

    assert list(cache.values(prefix='order:')) == ['order1']


def test_entities_prefix(cache):
    """Should stop iteration when key no longer matches prefix."""
    cache.put_entity('user:1', ['name', 'age'], ['alice', 30])
    cache.put_entity('user:2', ['name', 'age'], ['bob', 25])
    cache.put_entity('order:1', ['item', 'qty'], ['book', 2])

    result = list(cache.entities(prefix='user:'))
    assert len(result) == 2
    assert result[0][0] == 'user:1'
    assert result[1][0] == 'user:2'

    result = list(cache.entities(prefix='order:'))
    assert len(result) == 1
    assert result[0][0] == 'order:1'


def test_columns_prefix(cache):
    """Should stop iteration when key no longer matches prefix."""
    cache.put_entity('user:1', ['name', 'age'], ['alice', 30])
    cache.put_entity('user:2', ['name', 'age'], ['bob', 25])
    cache.put_entity('order:1', ['item', 'qty'], ['book', 2])

    result = list(cache.columns(prefix='user:'))
    assert len(result) == 2

    result = list(cache.columns(prefix='order:'))
    assert len(result) == 1


def test_prefix_with_from_key(cache):
    """Should respect from_key when prefix is also provided."""
    cache['user:1'] = 'alice'
    cache['user:2'] = 'bob'
    cache['user:3'] = 'charlie'
    cache['order:1'] = 'order1'

    result = list(cache.keys(from_key='user:2', prefix='user:'))
    assert result == ['user:2', 'user:3']


def test_prefix_no_matches(cache):
    """Should return empty when no keys match prefix."""
    cache['user:1'] = 'alice'
    cache['user:2'] = 'bob'

    assert list(cache.keys(prefix='order:')) == []
    assert list(cache.items(prefix='order:')) == []
    assert list(cache.values(prefix='order:')) == []


def test_existing_iteration_unchanged(cache):
    """Should maintain existing behavior when prefix is not provided."""
    cache[123] = 'numeric'
    cache['abc'] = 'string'

    assert 123 in list(cache.keys())
    assert 'abc' in list(cache.keys())
    assert (123, 'numeric') in list(cache.items())
    assert 'numeric' in list(cache.values())
