[![Test Python Package](https://github.com/Menziess/slipstream/actions/workflows/python-test.yml/badge.svg)](https://github.com/Menziess/slipstream/actions/workflows/python-test.yml) [![Documentation Status](https://readthedocs.org/projects/slipstream/badge/?version=latest)](https://slipstream.readthedocs.io/en/latest/?badge=latest) [![Downloads](https://static.pepy.tech/personalized-badge/slipstream?period=month&units=international_system&left_color=grey&right_color=brightgreen&left_text=downloads/month)](https://pepy.tech/project/slipstream)

# Slipstream

<img src="./res/logo.png" width="25%" height="25%" align="right" />

Slipstream provides a data-flow model to simplify development of stateful streaming applications.

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

![demo](./res/demo.gif)

Multiple sources and sinks can be provided to establish many-to-many relations between them.
The 4 emoji's were printed using the callable `print`.

## Quickstart

Install slipstream and run the demo snippet above:

```sh
pip install slipstream
```

Spin up a local Kafka broker with [docker-compose.yml](docker-compose.yml), using `localhost:29091` to connect:

```sh
pip install slipstream[kafka]

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
