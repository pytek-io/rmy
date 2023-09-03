from __future__ import annotations
import asyncio
import contextlib
from itertools import count
from typing import Any

import anyio
import anyio.abc
import asyncstdlib

from .abc import Connection
from .common import cancel_task_group_on_signal, scoped_insert
from .connection import TCPConnection
from .session import DEFAULT_SERVER_OBJECT_ID, RemoteObject, Session


class Server:
    def __init__(self) -> None:
        self.client_sessions = {}
        self.client_session_id = count()
        self.object_id = count()
        self.common_objects = {}

    def register_object(self, remote_object: RemoteObject, object_id=DEFAULT_SERVER_OBJECT_ID):
        RemoteObject.init(remote_object, session=None, is_proxy=False, object_id=object_id)
        self.common_objects[object_id] = remote_object

    @contextlib.asynccontextmanager
    async def on_new_connection(self, connection: Connection):
        async with anyio.create_task_group() as session_task_group:
            client_session = Session(connection, session_task_group, self.common_objects)
            with scoped_insert(self.client_sessions, next(self.client_session_id), client_session):
                async with asyncstdlib.closing(client_session):
                    yield client_session


async def _serve_tcp(port: int, server: Server):
    async def on_new_tcp_connection(reader, writer):
        async with server.on_new_connection(
            TCPConnection(reader, writer, throw_on_eof=False)
        ) as client_core:
            await client_core.process_messages()

    async with await asyncio.start_server(on_new_tcp_connection, "localhost", port) as tcp_server:
        await tcp_server.serve_forever()


async def handle_signals(main, *args, **kwargs):
    async with anyio.create_task_group() as task_group:
        task_group.start_soon(cancel_task_group_on_signal, task_group)
        task_group.start_soon(main, *args, **kwargs)


async def start_tcp_server(port: int, server_object: Any = None):
    server = Server()
    server.register_object(server_object)
    await handle_signals(_serve_tcp, port, server)


def run_tcp_server(port: int, server_object: Any):
    anyio.run(start_tcp_server, port, server_object)
