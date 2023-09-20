import pytest

from rmy.session import ASYNC_GENERATOR_OVERFLOWED_MESSAGE
from tests.conftest import (
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    TestObject,
    check_exception,
    check_remote_exception,
    generate_proxies_async,
)
from tests.utils_async import enumerate, scoped_iter, sleep


pytestmark = pytest.mark.anyio


async def test_async_generator(proxy_async: TestObject):
    async for i, value in enumerate(proxy_async.count.wait(10)):
        assert i == value


async def test_sync_generator(proxy_async: TestObject):
    async for i, value in enumerate(proxy_async.count_sync.wait(10)):
        assert i == value


async def test_async_generator_exception(proxy_async: TestObject):
    with check_remote_exception() as exception:
        async with scoped_iter(proxy_async.async_generator_exception.wait(exception)) as stream:
            async for i, value in enumerate(stream):
                assert i == value


async def test_explicit_close():
    actual = TestObject()
    async with generate_proxies_async(actual) as proxies:
        async with proxies() as proxy:
            async with scoped_iter(proxy.count.wait(100)) as numbers:
                async for i in numbers:
                    if i == 3:
                        break
    await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS + 1)
    assert actual.finally_called
    # the current value should be 3 since the producer is slower than the consumer
    assert actual.current_value == 3


async def test_close_on_drop():
    actual = TestObject()
    async with generate_proxies_async(actual) as proxies:
        async with proxies() as proxy:
            numbers = proxy.count.wait(100)
            async for i in numbers:
                if i == 3:
                    break
            numbers = None
            await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
            assert actual.finally_called
            # the current value should be 3 since the producer is slower than the consumer
            assert actual.current_value == 3


async def test_overflow(proxy_async: TestObject):
    with check_exception(OverflowError(ASYNC_GENERATOR_OVERFLOWED_MESSAGE)):
        async with scoped_iter(proxy_async.count_to_infinity_nowait.wait()) as numbers:
            async for i in numbers:
                await sleep(0.1)


async def test_remote_generator_pull_decorator(proxy_async: TestObject):
    with check_exception(OverflowError(ASYNC_GENERATOR_OVERFLOWED_MESSAGE)):
        async for i, value in enumerate(proxy_async.remote_generator_unsynced.wait()):
            await sleep(0.1)
            assert i == value
            if i == 3:
                break
