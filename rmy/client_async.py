import contextlib
from typing import TYPE_CHECKING, AsyncIterator

import anyio
import anyio.abc

from .abc import Connection
from .common import cancel_task_on_exit
from .connection import connect_to_tcp_server
from .session import Session, SERVER_OBJECT_ID, current_session


class AsyncClient:
    def __init__(self, session: Session) -> None:
        self.session: Session = session

    async def fetch_object_local(self, object_id: int = SERVER_OBJECT_ID):
        return await self.session.fetch_object_local(object_id)


@contextlib.asynccontextmanager
async def create_session(connection: Connection) -> AsyncIterator[Session]:
    async with anyio.create_task_group() as task_group:
        session = Session(connection, task_group)
        current_session.set(session)
        async with cancel_task_on_exit(session.process_messages):
            yield session


@contextlib.asynccontextmanager
async def create_async_client(connection: Connection) -> AsyncIterator[AsyncClient]:
    async with create_session(connection) as session:
        yield AsyncClient(session)


@contextlib.asynccontextmanager
async def connect_session(host_name: str, port: int) -> AsyncIterator[Session]:
    async with connect_to_tcp_server(host_name, port) as connection:
        async with create_session(connection) as session:
            yield session


@contextlib.asynccontextmanager
async def connect(host_name: str, port: int) -> AsyncIterator[AsyncClient]:
    async with connect_to_tcp_server(host_name, port) as connection:
        async with create_async_client(connection) as client:
            yield client
