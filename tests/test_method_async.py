import pytest

from tests.utils import (
    ERROR_MESSAGE,
    RemoteObject,
    check_remote_exception,
    create_proxy_object_async,
)


pytestmark = pytest.mark.anyio


async def test_async_method():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        value = "test"
        returned_value = await proxy.echo_coroutine.rma(value)
        assert returned_value is not value
        assert returned_value == value


async def test_async_method_exception():
    with check_remote_exception() as exception:
        async with create_proxy_object_async(RemoteObject()) as proxy:
            await proxy.throw_exception_coroutine.rma(exception)


async def test_sync_method():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        value = "test"
        returned_value = await proxy.echo_sync.rma(value)
        assert returned_value is not value
        assert returned_value == value


async def test_sync_method_exception():
    with check_remote_exception() as exception:
        async with create_proxy_object_async(RemoteObject()) as proxy:
            await proxy.throw_exception_coroutine.rma(exception)


async def test_remote_object_arg():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        assert proxy is await proxy.echo_coroutine.rma(proxy)


async def test_async_context():
    async with create_proxy_object_async(RemoteObject()) as proxy:
        async with proxy.async_context_manager.rma("test") as result:
            assert result == "test"
        assert await proxy.getattr_async("current_value") == 1
