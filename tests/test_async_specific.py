import anyio
import anyio.abc
import asyncio
import asyncstdlib as astd
import pytest

from rmy import RemoteGeneratorPull, RemoteGeneratorPush
from rmy.session import RemoteAwaitable

from tests.utils import (
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    TestObject,
    async_generator,
    create_proxy_object_async,
)
from tests.utils_async import scoped_iter, sleep


pytestmark = pytest.mark.anyio


async def test_async_generator_cancellation():
    async with create_proxy_object_async(TestObject()) as proxy:
        async with anyio.create_task_group():
            with anyio.move_on_after(1):
                async with scoped_iter(proxy.count.wait(100)) as numbers:
                    async for i in numbers:
                        pass
        await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert await proxy.get_finally_called.wait()


async def test_coroutine_cancellation():
    async with create_proxy_object_async(TestObject()) as proxy:
        task = asyncio.create_task(proxy.sleep_forever.wait())
        await asyncio.sleep(1)
        if not task.done():
            task.cancel()
        await asyncio.sleep(.1)
        await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert await proxy.get_ran_tasks.wait() == 1


async def test_coroutine_time_out():
    async with create_proxy_object_async(TestObject()) as proxy:
        async with anyio.create_task_group():
            with anyio.move_on_after(1):
                await proxy.sleep_forever.wait()
        await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert await proxy.get_ran_tasks.wait() == 1


async def test_remote_generator_pull():
    """Checking that the remote generator pull behaves like as a pass through locally."""
    for i, value in enumerate(RemoteGeneratorPull(range(10))):
        assert i == value

    async for i, value in astd.enumerate(RemoteGeneratorPull(async_generator(10))):
        assert i == value


async def test_remote_generator_push():
    """Checking that the remote generator push behaves like as a pass through locally."""
    with pytest.raises(TypeError):
        for i, value in enumerate(RemoteGeneratorPush(range(10))):
            assert i == value

    async for i, value in astd.enumerate(RemoteGeneratorPush(async_generator(10))):
        assert i == value


async def test_remote_coroutine():
    """Checking that the remote awaitable behaves like as a pass through locally."""

    async def coroutine():
        return 1

    assert await RemoteAwaitable(coroutine()) == 1


async def test_async_nested_generators():
    async with create_proxy_object_async(TestObject()) as proxy:
        [test] = await proxy.nested_coroutine.wait()
        assert await test == 1


async def test_remote_object_from_context():
    async with create_proxy_object_async(TestObject()) as proxy:
        async with proxy.remote_object_from_context.wait("test") as proxy_server_test:
            assert await proxy_server_test.echo_coroutine.wait(1) == 1
