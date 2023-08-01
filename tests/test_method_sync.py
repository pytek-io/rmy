import pytest

from tests.utils import ERROR_MESSAGE, RemoteObject, check_exception, create_proxy_object_sync


def test_async_method():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        value = "test"
        returned_value = proxy.echo_coroutine.rms(value)
        assert returned_value is not value
        assert returned_value == value


def test_async_method_exception():
    with check_exception() as exception:
        with create_proxy_object_sync(RemoteObject()) as proxy:
            proxy.throw_exception_coroutine.rms(exception)


def test_sync_method():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        value = "test"
        returned_value = proxy.echo_sync.rms(value)
        assert returned_value is not value
        assert returned_value == value


def test_sync_method_exception():
    with check_exception() as exception:
        with create_proxy_object_sync(RemoteObject()) as proxy:
            proxy.throw_exception_coroutine.rms(exception)


def test_remote_object_arg():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        assert proxy is proxy.echo_coroutine.rms(proxy)


def test_async_context():
    test = RemoteObject()
    with create_proxy_object_sync(test) as proxy:
        assert test is not proxy
        print(id(test), id(proxy))
        with proxy.async_context_manager.rms("test") as result:
            assert result == "test"
        assert proxy.getattr_sync("current_value") == 1


# async def test_sync_context():
#     async with create_proxy_object_async(RemoteObject()) as proxy:
#         with proxy.sync_context_manager.rms("test") as result:
#             assert result == "test"
#         assert proxy.current_value == 1
