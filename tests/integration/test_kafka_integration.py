"""Test integration with kafka."""

import pytest

from slipstream import Topic


@pytest.mark.asyncio
async def test_produce_consume(kafka):
    """Should be able to exchange messages with kafka."""
    t = Topic('test_produce_consume', {
        'bootstrap_servers': kafka,
        'auto_offset_reset': 'earliest',
        'group_instance_id': 'test_produce_consume',
        'group_id': 'test_produce_consume',
    })

    await t('a'.encode(), 'a'.encode())
    # await t('b'.encode(), 'b'.encode())
    # await t('c'.encode(), 'c'.encode())

    msg = await next(t)
    assert msg.value == b'a'

    # expected = [b'b', b'c']
    # async for msg in t:
    #     assert msg.value == expected.pop(0)
    #     if len(expected) == 0:
    #         break

    # assert len(expected) == 0
