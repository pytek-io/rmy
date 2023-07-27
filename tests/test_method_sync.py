import pytest

from tests.utils import ERROR_MESSAGE, RemoteObject, create_proxy_object_sync, test_exception




def test_async_method():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        value = "test"
        returned_value = proxy.echo_coroutine(value)
        assert returned_value is not value
        assert returned_value == value


def test_async_method_exception():
    with test_exception() as exception:
        with create_proxy_object_sync(RemoteObject()) as proxy:
            proxy.throw_exception_coroutine(exception)


def test_sync_method():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        value = "test"
        returned_value = proxy.echo_sync(value)
        assert returned_value is not value
        assert returned_value == value


def test_sync_method_exception():
    with test_exception() as exception:
        with create_proxy_object_sync(RemoteObject()) as proxy:
            proxy.throw_exception_coroutine(exception)


def test_remote_object_arg():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        assert proxy is proxy.echo_coroutine(proxy)


def test_async_context():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        with proxy.async_context_manager("test") as result:
            assert result == "test"
        assert proxy.current_value == 1
