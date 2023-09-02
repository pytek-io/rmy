import contextlib
import traceback
from typing import TYPE_CHECKING, AsyncIterator, Type, TypeVar

import anyio
import anyio.abc

from .abc import Connection
from .common import cancel_task_on_exit
from .connection import connect_to_tcp_server
from .session import DEFAULT_SERVER_OBJECT_ID, BaseRemoteObject, Session, current_session


T = TypeVar("T")


class AsyncClient:
    def __init__(self, session: Session) -> None:
        self.session: Session = session

    async def fetch_remote_object(
        self, object_class: Type[T], object_id=DEFAULT_SERVER_OBJECT_ID
    ) -> T:
        return await self.session.fetch_remote_object(object_class, object_id)


@contextlib.asynccontextmanager
async def create_session(connection: Connection) -> AsyncIterator[Session]:
    try:
        async with anyio.create_task_group() as task_group:
            session = Session(connection, task_group, {})
            current_session.set(session)
            async with cancel_task_on_exit(session.process_messages):
                yield session
    except Exception:
        traceback.print_exc()
        raise


@contextlib.asynccontextmanager
async def connect_session(host_name: str, port: int) -> AsyncIterator[Session]:
    async with connect_to_tcp_server(host_name, port) as connection:
        async with create_session(connection) as session:
            yield session


@contextlib.asynccontextmanager
async def create_async_client(host_name: str, port: int) -> AsyncIterator[AsyncClient]:
    async with connect_session(host_name, port) as session:
        yield AsyncClient(session)
