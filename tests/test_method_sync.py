import pytest

from tests.utils import (
    ERROR_MESSAGE,
    RemoteObject,
    check_remote_exception,
    create_proxy_object_sync,
)


def test_async_method():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        value = "test"
        returned_value = proxy.echo_coroutine.wait(value)
        assert returned_value is not value
        assert returned_value == value


def test_async_method_exception():
    with check_remote_exception() as exception:
        with create_proxy_object_sync(RemoteObject()) as proxy:
            proxy.throw_exception_coroutine.wait(exception)


def test_sync_method():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        value = "test"
        returned_value = proxy.echo_sync.wait(value)
        assert returned_value is not value
        assert returned_value == value


def test_sync_method_exception():
    with check_remote_exception() as exception:
        with create_proxy_object_sync(RemoteObject()) as proxy:
            proxy.throw_exception_coroutine.wait(exception)


def test_remote_object_arg():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        assert proxy is proxy.echo_coroutine.wait(proxy)


def test_async_context():
    with create_proxy_object_sync(RemoteObject()) as proxy:
        with proxy.async_context_manager.eval("test") as result:
            assert result == "test"
        assert proxy.getattr_sync("current_value") == 1
