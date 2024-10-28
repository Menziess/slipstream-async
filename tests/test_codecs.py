from slipstream.codecs import JsonCodec


def test_JsonCodec():
    """Should both serialize and deserialize messages."""
    c = JsonCodec()
    serialized = b'{"msg": "hi"}'
    unserialized = {'msg': 'hi'}
    assert c.encode(unserialized) == serialized
    assert c.decode(serialized) == unserialized
