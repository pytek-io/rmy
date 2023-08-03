import anyio
import anyio.abc
import asyncstdlib as astd
import pytest

from rmy import RemoteAwaitable, RemoteGeneratorPull, RemoteGeneratorPush
from tests.utils import (
    ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS,
    RemoteObject,
    async_generator,
    create_proxy_object_async,
)
from tests.utils_async import scoped_iter, sleep


pytestmark = pytest.mark.anyio


async def test_async_generator_cancellation():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        async with anyio.create_task_group():
            with anyio.move_on_after(1):
                async with scoped_iter(proxy.count.wait(100)) as numbers:
                    async for i in numbers:
                        pass
        await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert await proxy.getattr_async("finally_called")


async def test_coroutine_cancellation():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        async with anyio.create_task_group() as task_group:

            async def cancellable_task(task_status: anyio.abc.TaskStatus):
                task_status.started()
                await proxy.sleep_forever.wait()

            await task_group.start(cancellable_task)
            await sleep(0.1)
            task_group.cancel_scope.cancel()
        await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert await proxy.getattr_async("ran_tasks") == 1


async def test_coroutine_time_out():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        async with anyio.create_task_group():
            with anyio.move_on_after(1):
                await proxy.sleep_forever.wait()
        await sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert await proxy.getattr_async("ran_tasks") == 1


# async def test_set_attribute():
#     async with create_proxy_object_async(RemoteObject("test")) as proxy:
#         new_value = "new_value"
#         with pytest.raises(AttributeError):
#             proxy.attribute = new_value


async def test_remote_generator_pull():
    """Checking that the remote generator pull behaves like as a pass through locally."""
    for i, value in enumerate(RemoteGeneratorPull(range(10))):
        assert i == value

    async for i, value in astd.enumerate(RemoteGeneratorPull(async_generator(10))):
        assert i == value


async def test_remote_generator_push():
    """Checking that the remote generator pull behaves like as a pass through locally."""
    with pytest.raises(TypeError):
        for i, value in enumerate(RemoteGeneratorPush(range(10))):
            assert i == value

    async for i, value in astd.enumerate(RemoteGeneratorPush(async_generator(10))):
        assert i == value


async def test_remote_coroutine():
    """Checking that the remote generator pull behaves like as a pass through locally."""

    async def coroutine():
        return 1

    assert await RemoteAwaitable(coroutine()) == 1


async def test_async_nested_generators():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        [test] = await proxy.nested_coroutine.wait()
        assert await test == 1
