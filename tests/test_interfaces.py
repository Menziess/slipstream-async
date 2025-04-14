
from asyncio import gather
from typing import Any

import pytest

from slipstream.interfaces import ICache, ICodec, Key


def test_ICodec():
    """Should be usable as interface."""
    class Codec(ICodec):

        def encode(self, obj: Any):
            return str(obj).encode()

        def decode(self, s: bytes):
            return s.decode()

    c = Codec()

    assert c.encode('a') == b'a'
    assert c.decode(b'a') == 'a'


@pytest.mark.asyncio
async def test_ICache():
    """Should be usable as interface."""
    class Cache(ICache):

        def __init__(self):
            self.db = {}

        def __contains__(self, key: Key) -> bool:
            return key in self.db

        def __delitem__(self, key: Key) -> None:
            del self.db[key]

        def __getitem__(self, key: Key | list[Key]) -> Any:
            return self.db.get(key, None)

        def __setitem__(self, key: Key, val: Any) -> None:
            self.db[key] = val

    c = Cache()
    c['prize'] = '🏆'
    assert c['prize'] == '🏆'
    assert 'prize' in c
    del c['prize']
    assert 'prize' not in c

    count, msg = 0, ('msg', 'hi')

    async def analyze_iter_cache():
        async for x in c:
            nonlocal count
            count += 1
            assert x == msg
            break

    await gather(
        analyze_iter_cache(),
        c(*msg),
    )

    assert count == 1
