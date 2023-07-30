import pytest
from tests.utils import create_proxy_object_sync, RemoteObject

def test_fetch_attribute():
    value = "test"
    with create_proxy_object_sync(RemoteObject(value)) as proxy:
        returned_value = proxy.attribute
        assert returned_value is not value
        assert returned_value == value


def test_attribute_error():
    with create_proxy_object_sync(RemoteObject("test")) as proxy:
        with pytest.raises(AttributeError):
            proxy.dummy


def test_set_attribute():
    with create_proxy_object_sync(RemoteObject("test")) as proxy:
        new_value = "new_value"
        proxy.attribute = new_value
        returned_value = proxy.attribute
        assert returned_value is not new_value
        assert returned_value == new_value
