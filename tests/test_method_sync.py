import pytest

from tests.conftest import ERROR_MESSAGE, TestObject, check_remote_exception


def test_async_method(proxy_sync: TestObject):
    value = "test"
    returned_value = proxy_sync.echo_coroutine.eval(value)
    assert returned_value is not value
    assert returned_value == value


def test_async_method_exception(proxy_sync: TestObject):
    with check_remote_exception() as exception:
        proxy_sync.throw_exception_coroutine.eval(exception)


def test_sync_method(proxy_sync: TestObject):
    value = "test"
    returned_value = proxy_sync.echo_sync.eval(value)
    assert returned_value is not value
    assert returned_value == value


def test_sync_method_exception(proxy_sync: TestObject):
    with check_remote_exception() as exception:
        proxy_sync.throw_exception_coroutine.eval(exception)


def test_remote_object_arg(proxy_sync: TestObject):
    assert proxy_sync is proxy_sync.echo_coroutine.eval(proxy_sync)


def test_async_context(proxy_sync: TestObject):
    with proxy_sync.async_context_manager.eval("test") as result:
        assert result == "test"
    assert proxy_sync.get_current_value.eval() == 1
