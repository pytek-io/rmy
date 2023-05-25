import tempfile
from functools import partial
import anyio
import anyio.abc
import sys
import contextlib
import pytest
from typing import AsyncIterator, List, Tuple


from fountainhead.client import AsyncClient
from fountainhead.server import OVERRIDE_ERROR_MESSAGE, Server

from tests.utils import create_test_environment_core, ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS

if sys.version_info < (3, 10):
    from asyncstdlib import anext


@contextlib.asynccontextmanager
async def create_test_environment(
    nb_clients: int = 1,
) -> AsyncIterator[Tuple[Server, List[AsyncClient]]]:
    with tempfile.TemporaryDirectory() as event_folder:
        async with create_test_environment_core(partial(Server, event_folder), nb_clients) as (
            server,
            clients,
        ):
            yield server, [AsyncClient(client) for client in clients]


@pytest.mark.anyio
async def test_read_and_write():
    async with create_test_environment() as (_server, (client,)):
        topic, original_value = "topic/subtopic", [123]
        time_stamp = await client.write_event(topic, original_value)
        returned_value = await client.read_event(topic, time_stamp)
        assert original_value is not returned_value
        assert original_value == returned_value


@pytest.mark.anyio
async def test_override():
    """Checking override fails if override flag is not set, succeed otherwise."""
    async with create_test_environment() as (_server, (client,)):
        topic, original_value = "topic/subtopic", [123]
        time_stamp = await client.write_event(topic, original_value)
        new_value = "hello world"
        with pytest.raises(Exception) as e_info:
            await client.write_event(topic, new_value, time_stamp)
        assert e_info.value.args[0] == OVERRIDE_ERROR_MESSAGE
        new_time_stamp = await client.write_event(topic, new_value, time_stamp, override=True)
        assert new_time_stamp == time_stamp
        returned_value = await client.read_event(topic, time_stamp)
        assert new_value is not returned_value
        assert new_value == returned_value


@pytest.mark.anyio
async def test_subscription():
    async with create_test_environment(2) as (server, (client1, client2)):
        topic = "topic/subtopic"
        async with client2.read_events(topic) as events:
            for i in range(10):
                value = {"value": i}
                time_stamp = await client1.write_event(topic, value)
                event_time_stamp, event_value = await anext(events)
                assert event_time_stamp == time_stamp
                assert event_value is not value
                assert value == event_value
        await anyio.sleep(ENOUGH_TIME_TO_COMPLETE_ALL_PENDING_TASKS)
        assert not server.subscriptions[topic]