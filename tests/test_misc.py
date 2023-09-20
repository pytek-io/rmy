import pickle

import anyio
import pytest

from rmy.session import Session
from tests.conftest import ERROR_MESSAGE, TestObject, create_test_connection


pytestmark = pytest.mark.anyio


def transform(value):
    return pickle.loads(pickle.dumps(value))


async def test_serialization():
    c1, c2 = create_test_connection("first", "second")
    async with anyio.create_task_group() as task_group:
        s1 = Session(c1, task_group, {})
        initial = TestObject()
        remote = transform(initial)
        local = transform(remote)
        assert initial is not remote
        assert initial is local
        c1.close()
