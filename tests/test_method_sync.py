import pytest

from tests.utils import (
    ERROR_MESSAGE,
    TestObject,
    check_remote_exception,
    create_proxy_object_sync,
)


def test_async_method():
    with create_proxy_object_sync(TestObject()) as proxy:
        value = "test"
        returned_value = proxy.echo_coroutine.eval(value)
        assert returned_value is not value
        assert returned_value == value


def test_async_method_exception():
    with check_remote_exception() as exception:
        with create_proxy_object_sync(TestObject()) as proxy:
            proxy.throw_exception_coroutine.eval(exception)


def test_sync_method():
    with create_proxy_object_sync(TestObject()) as proxy:
        value = "test"
        returned_value = proxy.echo_sync.eval(value)
        assert returned_value is not value
        assert returned_value == value


def test_sync_method_exception():
    with check_remote_exception() as exception:
        with create_proxy_object_sync(TestObject()) as proxy:
            proxy.throw_exception_coroutine.eval(exception)


def test_remote_object_arg():
    with create_proxy_object_sync(TestObject()) as proxy:
        assert proxy is proxy.echo_coroutine.eval(proxy)


def test_async_context():
    with create_proxy_object_sync(TestObject()) as proxy:
        with proxy.async_context_manager.eval("test") as result:
            assert result == "test"
        assert proxy.get_current_value.eval() == 1
