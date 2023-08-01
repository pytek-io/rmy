import pytest

from tests.utils import RemoteObject, create_proxy_object_async


pytestmark = pytest.mark.anyio


async def test_fetch_attribute():
    value = "test"
    async with create_proxy_object_async(RemoteObject(value)) as proxy:
        returned_value = await proxy.getattr_async("attribute")
        assert returned_value is not value
        assert returned_value == value


async def test_attribute_error():
    async with create_proxy_object_async(RemoteObject("test")) as proxy:
        with pytest.raises(AttributeError):
            await proxy.dummy


async def test_set_attribute():
    async with create_proxy_object_async(RemoteObject("test")) as proxy:
        new_value = "new_value"
        await proxy.setattr_async("attribute", new_value)
        returned_value = await proxy.getattr_async("attribute")
        assert returned_value is not new_value
        assert returned_value == new_value
