from asyncio import gather, sleep
from typing import AsyncIterable, Callable

import pytest
from rocksdict import DbClosedError

from slipstream.caching import Cache


@pytest.mark.serial
@pytest.mark.parametrize('key,val,updated', [
    (b'123', 'a', 'b'),
    ('123', 'b', 'c'),
    (True, 'c', 'd'),
    (123, 'd', 'e'),
])
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


def test_get_callable(cache):
    """Should return a callable."""
    assert isinstance(cache, Callable)


@pytest.mark.asyncio
async def test_transaction(cache):
    """Test transaction."""
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
    """Test iterability."""
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
