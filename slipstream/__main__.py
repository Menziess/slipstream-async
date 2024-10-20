"""Main module."""

import asyncio
from asyncio import run
from time import sleep, strftime

from slipstream import Topic, handle, stream

t1 = Topic(
    bootstrap_servers='localhost:29091',
    topic='demo1',
    group_id='demo1'
)
t2 = Topic(
    bootstrap_servers='localhost:29091',
    topic='demo2',
    group_id='demo2'
)


def timer(interval=1.0):
    while True:
        yield strftime('%H:%M:%S')
        sleep(interval)


async def atimer(interval=1.0):
    while True:
        yield strftime('%H:%M:%S')
        await asyncio.sleep(interval)


@handle(atimer())
def handler0(msg):
    print(msg)


@handle(t1)
def handler1(msg):
    print(msg)


@handle(t2)
def handler2(msg):
    print(msg)


if __name__ == '__main__':
    try:
        run(stream())
    except KeyboardInterrupt:
        pass
