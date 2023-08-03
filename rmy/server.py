from __future__ import annotations

import asyncio
import contextlib
from itertools import count
from typing import Any

import anyio
import anyio.abc
import asyncstdlib

from .abc import Connection
from .session import Session
from .common import cancel_task_group_on_signal, scoped_insert
from .connection import TCPConnection


class Server:
    def __init__(self, server_object: Any) -> None:
        self.server_object = server_object
        self.client_sessions = {}
        self.client_session_id = count()
        self.object_id = count()

    @contextlib.asynccontextmanager
    async def on_new_connection(self, connection: Connection):
        async with anyio.create_task_group() as session_task_group:
            client_session = Session(connection, session_task_group)
            client_session.register_object(self.server_object)
            with scoped_insert(self.client_sessions, next(self.client_session_id), client_session):
                async with asyncstdlib.closing(client_session):
                    yield client_session


async def _serve_tcp(port: int, server_object: Any):
    server = Server(server_object)

    async def on_new_connection_raw(reader, writer):
        async with server.on_new_connection(
            TCPConnection(reader, writer, throw_on_eof=False)
        ) as client_core:
            await client_core.process_messages()

    async with await asyncio.start_server(on_new_connection_raw, "localhost", port) as tcp_server:
        await tcp_server.serve_forever()


async def handle_signals(main, *args, **kwargs):
    async with anyio.create_task_group() as task_group:
        task_group.start_soon(cancel_task_group_on_signal, task_group)
        task_group.start_soon(main, *args, **kwargs)


async def start_tcp_server(port: int, server_object: Any):
    await handle_signals(_serve_tcp, port, server_object)


def run_tcp_server(port: int, server_object: Any):
    anyio.run(start_tcp_server, port, server_object)
