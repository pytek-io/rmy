import pytest

from tests.utils import (
    ASYNC_GENERATOR_OVERFLOWED_MESSAGE,
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    RemoteObject,
    create_proxy_object_async,
    test_exception,
)
from tests.utils_async import enumerate, scoped_iter, sleep


pytestmark = pytest.mark.anyio


async def test_async_generator():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        async for i, value in enumerate(proxy.count(10)):
            assert i == value


async def test_sync_generator():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        async for i, value in enumerate(proxy.count_sync(10)):
            assert i == value


async def test_async_generator_exception():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        with test_exception() as exception:
            async with scoped_iter(proxy.async_generator_exception(exception)) as stream:
                async for i, value in enumerate(stream):
                    assert i == value


async def test_early_exit():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        async with scoped_iter(proxy.count(100)) as numbers:
            async for i in numbers:
                if i == 3:
                    break
        await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS + 1)
        assert await proxy.finally_called
        # the current value should be 3 since the producer is slower than the consumer
        assert await proxy.current_value == 3


async def test_overflow():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        with pytest.raises(Exception) as e_info:
            async with scoped_iter(proxy.count_to_infinity_nowait()) as numbers:
                async for i in numbers:
                    await sleep(0.1)
        assert e_info.value.args[0] == ASYNC_GENERATOR_OVERFLOWED_MESSAGE


async def test_remote_generator_pull_decorator():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        async for i, value in enumerate(proxy.remote_generator_pull_synced()):
            await sleep(0.1)
            assert i == value
            if i == 3:
                break
