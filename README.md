[![Test Python Package](https://github.com/Menziess/slipstream-async/actions/workflows/triggered-tests.yml/badge.svg)](https://github.com/Menziess/slipstream-async/actions/workflows/triggered-tests.yml)
[![Documentation Status](https://readthedocs.org/projects/slipstream/badge/?version=latest)](https://slipstream.readthedocs.io/en/latest/?badge=latest)
[![PyPI Downloads](https://img.shields.io/pypi/dm/slipstream-async.svg)](https://pypi.org/project/slipstream-async/)

# Slipstream

<img src="https://raw.githubusercontent.com/menziess/slipstream/master/res/logo.png" width="25%" height="25%" align="right" />

Slipstream provides a data-flow model to simplify development of stateful streaming applications.

```sh
pip install slipstream-async
```

```py
from asyncio import run

from slipstream import handle, stream


async def messages():
    for emoji in '🏆📞🐟👌':
        yield emoji


@handle(messages(), sink=[print])
def handle_message(msg):
    yield f'Hello {msg}!'


if __name__ == '__main__':
    run(stream())
```

```sh
Hello 🏆!
Hello 📞!
Hello 🐟!
Hello 👌!
```

## Usage

Async `iterables` are sources, (async) `callables` are sinks.

Decorate handler functions using `handle`, then run `stream` to start processing:

<img src="https://raw.githubusercontent.com/menziess/slipstream/master/res/demo.gif" />

Multiple sources and sinks can be provided to establish many-to-many relations between them.
The 4 emoji's were printed using the callable `print`.

## Quickstart

Install `aiokafka` (latest) along with slipstream:

```sh
pip install slipstream-async[kafka]
```

Spin up a local Kafka broker with [docker-compose.yml](docker-compose.yml), using `localhost:29091` to connect:

```sh
docker compose up broker -d
```

Follow the docs and set up a Kafka connection: [slipstream.readthedocs.io](https://slipstream.readthedocs.io).

## Features

- [`slipstream.handle`](slipstream/__init__.py): bind streams (iterables) and sinks (callables) to user defined handler functions
- [`slipstream.stream`](slipstream/__init__.py): start streaming
- [`slipstream.Topic`](slipstream/core.py): consume from (iterable), and produce to (callable) kafka using [**aiokafka**](https://aiokafka.readthedocs.io/en/stable/index.html)
- [`slipstream.Cache`](slipstream/caching.py): store data to disk using [**rocksdict**](https://congyuwang.github.io/RocksDict/rocksdict.html)
- [`slipstream.Conf`](slipstream/core.py): set global kafka configuration (can be overridden per topic)
- [`slipstream.codecs.JsonCodec`](slipstream/codecs.py): serialize and deserialize json messages
- [`slipstream.checkpointing.Checkpoint`](slipstream/checkpointing.py): recover from stream downtimes
