import pytest
from rmy.session import ASYNC_GENERATOR_OVERFLOWED_MESSAGE
from tests.utils import (
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    TestObject,
    check_exception,
    check_remote_exception,
    create_proxy_object_sync,
    receive_test_object_sync,
)
from tests.utils_sync import enumerate, scoped_iter, sleep


@receive_test_object_sync
def test_async_generator(proxy: TestObject):
    for i, value in enumerate(proxy.count.eval(10)):
        assert i == value


@receive_test_object_sync
def test_sync_generator(proxy: TestObject):
    for i, value in enumerate(proxy.count_sync.eval(10)):
        assert i == value


@receive_test_object_sync
def test_async_generator_exception(proxy: TestObject):
    with check_remote_exception() as exception:
        with scoped_iter(proxy.async_generator_exception.eval(exception)) as stream:
            for i, value in enumerate(stream):
                assert i == value


@receive_test_object_sync
def test_explicit_close(proxy: TestObject):
    with scoped_iter(proxy.count.eval(100)) as numbers:
        for i in numbers:
            if i == 3:
                break
    sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS + 1)
    assert proxy.get_finally_called.eval()
    # the current value should be 3 since the producer is slower than the consumer
    assert proxy.get_current_value.eval() == 3


@receive_test_object_sync
def test_close_on_drop(proxy: TestObject):
    numbers = proxy.count.eval(100)
    for i in numbers:
        if i == 3:
            break
    numbers = None
    sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
    assert proxy.get_finally_called.eval()
    # the current value should be 3 since the producer is slower than the consumer
    assert proxy.get_current_value.eval() == 3


@receive_test_object_sync
def test_overflow(proxy: TestObject):
    with check_exception(OverflowError(ASYNC_GENERATOR_OVERFLOWED_MESSAGE)):
        with scoped_iter(proxy.count_to_infinity_nowait.eval()) as numbers:
            for i in numbers:
                sleep(0.1)


@receive_test_object_sync
def test_remote_generator_pull_decorator(proxy: TestObject):
    with check_exception(OverflowError(ASYNC_GENERATOR_OVERFLOWED_MESSAGE)):
        for i, value in enumerate(proxy.remote_generator_unsynced.eval()):
            sleep(0.1)
            assert i == value
            if i == 3:
                break
