import pytest
from tests.utils import (
    create_proxy_object_sync,
    RemoteObject,
)


def test_attribute():
    value = "test"
    with create_proxy_object_sync(RemoteObject(value)) as proxy:
        returned_value = proxy.attribute
        assert returned_value is not value
        assert returned_value == value


def test_non_existent_attribute():
    with create_proxy_object_sync(RemoteObject("test")) as proxy:
        with pytest.raises(AttributeError):
            proxy.dummy
