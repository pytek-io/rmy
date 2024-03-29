import asyncio

import anyio
import anyio.abc
import asyncstdlib as astd
import pytest

from rmy import RemoteGeneratorPull, RemoteGeneratorPush
from rmy.session import RemoteAwaitable
from tests.conftest import ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS, TestObject, async_generator
from tests.utils_async import scoped_iter, sleep


pytestmark = pytest.mark.anyio


async def test_async_generator_cancellation(proxy_async: TestObject):
    async with anyio.create_task_group():
        with anyio.move_on_after(1):
            async with scoped_iter(proxy_async.count.wait(100)) as numbers:
                async for i in numbers:
                    pass
    await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
    assert await proxy_async.get_finally_called.wait()


async def test_coroutine_cancellation(proxy_async: TestObject):
    task = asyncio.create_task(proxy_async.sleep_forever.wait())
    await asyncio.sleep(1)
    if not task.done():
        task.cancel()
    await asyncio.sleep(0.1)
    await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
    assert await proxy_async.get_ran_tasks.wait() == 1


async def test_coroutine_time_out(proxy_async: TestObject):
    async with anyio.create_task_group():
        with anyio.move_on_after(1):
            await proxy_async.sleep_forever.wait()
    await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
    assert await proxy_async.get_ran_tasks.wait() == 1


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


async def test_async_nested_generators(proxy_async: TestObject):
    [test] = await proxy_async.nested_coroutine.wait()
    assert await test == 1


async def test_remote_object_from_context(proxy_async: TestObject):
    async with proxy_async.remote_object_from_context.wait("test") as proxy_server_test:
        assert await proxy_server_test.echo_coroutine.wait(1) == 1
