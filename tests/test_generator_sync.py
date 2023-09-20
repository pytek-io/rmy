import pytest

from rmy.session import ASYNC_GENERATOR_OVERFLOWED_MESSAGE
from tests.conftest import (
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    TestObject,
    check_exception,
    check_remote_exception,
    generate_proxies_sync,
)
from tests.utils_sync import enumerate, scoped_iter, sleep


def test_async_generator(proxy_sync: TestObject):
    for i, value in enumerate(proxy_sync.count.eval(10)):
        assert i == value


def test_sync_generator(proxy_sync: TestObject):
    for i, value in enumerate(proxy_sync.count_sync.eval(10)):
        assert i == value


def test_async_generator_exception(proxy_sync: TestObject):
    with check_remote_exception() as exception:
        with scoped_iter(proxy_sync.async_generator_exception.eval(exception)) as stream:
            for i, value in enumerate(stream):
                assert i == value


def test_explicit_close():
    actual = TestObject()
    with generate_proxies_sync(actual) as proxies:
        with proxies() as proxy:
            with scoped_iter(proxy.count.eval(100)) as numbers:
                for i in numbers:
                    if i == 3:
                        break
    sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS + 1)
    assert actual.finally_called
    # the current value should be 3 since the producer is slower than the consumer
    assert actual.current_value == 3


def test_close_on_drop():
    actual = TestObject()
    with generate_proxies_sync(actual) as proxies:
        with proxies() as proxy:
            numbers = proxy.count.eval(100)
            for i in numbers:
                if i == 3:
                    break
            numbers = None
            sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
            assert actual.finally_called
            # the current value should be 3 since the producer is slower than the consumer
            assert actual.current_value == 3


def test_overflow(proxy_sync: TestObject):
    with check_exception(OverflowError(ASYNC_GENERATOR_OVERFLOWED_MESSAGE)):
        with scoped_iter(proxy_sync.count_to_infinity_nowait.eval()) as numbers:
            for i in numbers:
                sleep(0.1)


def test_remote_generator_pull_decorator(proxy_sync: TestObject):
    with check_exception(OverflowError(ASYNC_GENERATOR_OVERFLOWED_MESSAGE)):
        for i, value in enumerate(proxy_sync.remote_generator_unsynced.eval()):
            sleep(0.1)
            assert i == value
            if i == 3:
                break
