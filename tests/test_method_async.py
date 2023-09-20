import pytest

from tests.conftest import ERROR_MESSAGE, TestObject, check_remote_exception


pytestmark = pytest.mark.anyio


async def test_async_method(proxy_async: TestObject):
    value = "test"
    returned_value = await proxy_async.echo_coroutine.wait(value)
    assert returned_value is not value
    assert returned_value == value


async def test_async_method_exception(proxy_async: TestObject):
    with check_remote_exception() as exception:
        await proxy_async.throw_exception_coroutine.wait(exception)


async def test_sync_method(proxy_async: TestObject):
    value = "test"
    returned_value = await proxy_async.echo_sync.wait(value)
    assert returned_value is not value
    assert returned_value == value


async def test_sync_method_exception(proxy_async: TestObject):
    with check_remote_exception() as exception:
        await proxy_async.throw_exception_coroutine.wait(exception)


async def test_remote_object_arg(proxy_async: TestObject):
    assert proxy_async is await proxy_async.echo_coroutine.wait(proxy_async)


async def test_async_context(proxy_async: TestObject):
    async with proxy_async.async_context_manager.wait("test") as result:
        assert result == "test"
    assert await proxy_async.get_current_value.wait() == 1
