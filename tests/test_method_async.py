import pytest

from tests.utils import (
    ERROR_MESSAGE,
    TestObject,
    check_remote_exception,
    receive_test_object_async,
)


pytestmark = pytest.mark.anyio


@receive_test_object_async
async def test_async_method(proxy: TestObject):
    value = "test"
    returned_value = await proxy.echo_coroutine.wait(value)
    assert returned_value is not value
    assert returned_value == value


@receive_test_object_async
async def test_async_method_exception(proxy: TestObject):
    with check_remote_exception() as exception:
        await proxy.throw_exception_coroutine.wait(exception)


@receive_test_object_async
async def test_sync_method(proxy: TestObject):
    value = "test"
    returned_value = await proxy.echo_sync.wait(value)
    assert returned_value is not value
    assert returned_value == value


@receive_test_object_async
async def test_sync_method_exception(proxy: TestObject):
    with check_remote_exception() as exception:
        await proxy.throw_exception_coroutine.wait(exception)


@receive_test_object_async
async def test_remote_object_arg(proxy: TestObject):
    assert proxy is await proxy.echo_coroutine.wait(proxy)


@receive_test_object_async
async def test_async_context(proxy: TestObject):
    async with proxy.async_context_manager.wait("test") as result:
        assert result == "test"
    assert await proxy.get_current_value.wait() == 1
