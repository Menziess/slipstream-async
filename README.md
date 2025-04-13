[![Test Python Package](https://github.com/Menziess/slipstream-async/actions/workflows/triggered-tests.yml/badge.svg)](https://github.com/Menziess/slipstream-async/actions/workflows/triggered-tests.yml)
[![Documentation Status](https://readthedocs.org/projects/slipstream/badge/?version=latest)](https://slipstream.readthedocs.io/en/latest/?badge=latest)
[![PyPI Downloads](https://img.shields.io/pypi/dm/slipstream-async.svg)](https://pypi.org/project/slipstream-async/)

# Slipstream

<img src="https://raw.githubusercontent.com/menziess/slipstream/master/docs/source/_static/logo.png" width="25%" height="25%" align="right" />

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

Slipstream components interoperate with basic python building blocks:

- `Any`-thing can be passed around as data
- Any `Callable` may be used as a sink
- `AsyncIterables` act as sources
- Parallelize through `handle`

<img src="https://raw.githubusercontent.com/menziess/slipstream/master/docs/source/_static/demo.gif" />

A many-to-many relation is established by passing multiple sources / sinks.

## Quickstart

Install Slipstream along with `aiokafka` (latest):

```sh
pip install slipstream-async[kafka]
```

Spin up a local Kafka broker with [docker-compose.yml](docker-compose.yml), using `localhost:29091` to connect:

```sh
docker compose up broker -d
```

Copy-paste [this snippet](https://slipstream.readthedocs.io/en/stable/getting_started.html#kafka).

## Features

- [`slipstream.handle`](https://slipstream.readthedocs.io/en/stable/slipstream.html#slipstream.handle): bind streams (iterables) and sinks (callables) to user defined handler functions
- [`slipstream.stream`](https://slipstream.readthedocs.io/en/stable/slipstream.html#slipstream.stream): start streaming
- [`slipstream.Topic`](https://slipstream.readthedocs.io/en/stable/slipstream.html#slipstream.core.Topic): consume from (iterable), and produce to (callable) kafka using [**aiokafka**](https://aiokafka.readthedocs.io/en/stable/index.html)
- [`slipstream.Cache`](https://slipstream.readthedocs.io/en/stable/slipstream.html#slipstream.Cache): store data to disk using [**rocksdict**](https://rocksdict.github.io/RocksDict/rocksdict.html)
- [`slipstream.Conf`](https://slipstream.readthedocs.io/en/stable/slipstream.html#slipstream.Conf): set global kafka configuration (can be overridden per topic)
- [`slipstream.codecs.JsonCodec`](https://slipstream.readthedocs.io/en/stable/autoapi/slipstream/codecs/index.html#slipstream.codecs.JsonCodec): serialize and deserialize json messages
- [`slipstream.checkpointing.Checkpoint`](https://slipstream.readthedocs.io/en/stable/autoapi/slipstream/checkpointing/index.html#slipstream.checkpointing.Checkpoint): recover from stream downtimes
