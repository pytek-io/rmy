import pytest
from rmy.session import ASYNC_GENERATOR_OVERFLOWED_MESSAGE
from tests.utils import (
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    TestObject,
    check_exception,
    check_remote_exception,
    create_proxy_object_async,
)
from tests.utils_async import enumerate, scoped_iter, sleep


pytestmark = pytest.mark.anyio


async def test_async_generator():
    async with create_proxy_object_async(TestObject()) as proxy:
        async for i, value in enumerate(proxy.count.wait(10)):
            assert i == value


async def test_sync_generator():
    async with create_proxy_object_async(TestObject()) as proxy:
        async for i, value in enumerate(proxy.count_sync.wait(10)):
            assert i == value


async def test_async_generator_exception():
    async with create_proxy_object_async(TestObject()) as proxy:
        with check_remote_exception() as exception:
            async with scoped_iter(proxy.async_generator_exception.wait(exception)) as stream:
                async for i, value in enumerate(stream):
                    assert i == value


async def test_explicit_close():
    async with create_proxy_object_async(TestObject()) as proxy:
        async with scoped_iter(proxy.count.wait(100)) as numbers:
            async for i in numbers:
                if i == 3:
                    break
        await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS + 1)
        assert await proxy.get_finally_called.wait()
        # the current value should be 3 since the producer is slower than the consumer
        assert await proxy.get_current_value.wait() == 3


async def test_close_on_drop():
    async with create_proxy_object_async(TestObject()) as proxy:
        numbers = proxy.count.wait(100)
        async for i in numbers:
            if i == 3:
                break
        numbers = None
        await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert await proxy.get_finally_called.wait()
        # the current value should be 3 since the producer is slower than the consumer
        assert await proxy.get_current_value.wait() == 3


async def test_overflow():
    async with create_proxy_object_async(TestObject()) as proxy:
        with check_exception(OverflowError(ASYNC_GENERATOR_OVERFLOWED_MESSAGE)):
            async with scoped_iter(proxy.count_to_infinity_nowait.wait()) as numbers:
                async for i in numbers:
                    await sleep(0.1)


async def test_remote_generator_pull_decorator():
    async with create_proxy_object_async(TestObject()) as proxy:
        with check_exception(OverflowError(ASYNC_GENERATOR_OVERFLOWED_MESSAGE)):
            async for i, value in enumerate(proxy.remote_generator_unsynced.wait()):
                await sleep(0.1)
                assert i == value
                if i == 3:
                    break
