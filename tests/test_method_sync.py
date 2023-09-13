import pytest

from tests.utils import (
    ERROR_MESSAGE,
    TestObject,
    check_remote_exception,
    create_proxy_object_sync,
    receive_test_object_sync,
)


@receive_test_object_sync
def test_async_method(proxy: TestObject):
    value = "test"
    returned_value = proxy.echo_coroutine.eval(value)
    assert returned_value is not value
    assert returned_value == value


@receive_test_object_sync
def test_async_method_exception(proxy: TestObject):
    with check_remote_exception() as exception:
        proxy.throw_exception_coroutine.eval(exception)


@receive_test_object_sync
def test_sync_method(proxy: TestObject):
    value = "test"
    returned_value = proxy.echo_sync.eval(value)
    assert returned_value is not value
    assert returned_value == value


@receive_test_object_sync
def test_sync_method_exception(proxy: TestObject):
    with check_remote_exception() as exception:
        proxy.throw_exception_coroutine.eval(exception)


@receive_test_object_sync
def test_remote_object_arg(proxy: TestObject):
    assert proxy is proxy.echo_coroutine.eval(proxy)


@receive_test_object_sync
def test_async_context(proxy: TestObject):
    with proxy.async_context_manager.eval("test") as result:
        assert result == "test"
    assert proxy.get_current_value.eval() == 1
