"""Common testing functionalities."""

import signal
from contextlib import contextmanager
from typing import Iterator

from pytest import fixture
from testcontainers.kafka import KafkaContainer

from slipstream import Cache

KAFKA_CONTAINER = 'confluentinc/cp-kafka:latest'


@fixture
def cache() -> Iterator[Cache]:
    """Get Cache instance that automatically cleans itself."""
    c = Cache('tests/db')
    try:
        yield c
    finally:
        c.close()
        c.destroy()


@fixture(scope='session')
def kafka():
    """Get running kafka broker."""
    kafka = KafkaContainer(KAFKA_CONTAINER)
    kafka.start()
    yield kafka.get_bootstrap_server()
    kafka.stop()


@fixture
def timeout():
    """Contextmanager that will stop execution of body."""
    @contextmanager
    def set_timeout(seconds: int):
        def raise_timeout(*_):
            raise TimeoutError(f'Timeout reached: {seconds}.')

        def start_timeout():
            signal.signal(signal.SIGALRM, raise_timeout)
            signal.alarm(seconds)
        yield start_timeout()
    return set_timeout
