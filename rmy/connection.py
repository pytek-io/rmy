import contextlib
import struct
from asyncio.streams import StreamReader, StreamWriter, open_connection
import sys

if sys.version_info < (3, 8):
    from asyncio.streams import IncompleteReadError
else:
    from asyncio.exceptions import IncompleteReadError
from pickle import dumps, loads
from typing import Any, Tuple, Callable

import asyncstdlib

from .abc import Connection


FORMAT = "Q"
SIZE_LENGTH = 8


class TCPConnection(Connection):
    def __init__(
        self,
        reader: StreamReader,
        writer: StreamWriter,
        throw_on_eof=True,
    ):
        self.reader = reader
        self.writer = writer
        self.dumps = dumps
        self.loads = loads
        self.throw_on_eof = throw_on_eof
        self._closing = False

    def set_dumps(self, dumps: Callable[[Any], bytes]):
        self.dumps = dumps

    def set_loads(self, loads: Callable[[bytes], Any]):
        self.loads = loads

    def send_nowait(self, message: Tuple[Any, ...]) -> int:
        message_as_bytes = self.dumps(message)
        message_size = len(message_as_bytes)
        self.writer.write(struct.pack(FORMAT, message_size) + message_as_bytes)
        return message_size

    async def send(self, message: Tuple[Any, ...]) -> int:
        message_size = self.send_nowait(message)
        try:
            await self.writer.drain()
            return message_size
        except ConnectionResetError:
            if self.throw_on_eof:
                raise
        return 0

    async def drain(self):
        await self.writer.drain()

    def close(self):
        self._closing = True
        self.writer.close()
        self.reader.feed_eof()

    async def __anext__(self) -> Any:
        try:
            length = struct.unpack(FORMAT, await self.reader.readexactly(SIZE_LENGTH))[0]
            return self.loads(await self.reader.readexactly(length))
        except (IncompleteReadError, ConnectionResetError, BrokenPipeError):
            if self.throw_on_eof:
                if not self._closing and self.reader.at_eof():
                    raise RuntimeError("Connection closed.")
                raise
            else:
                raise StopAsyncIteration()

    async def aclose(self):
        self._closing = True
        self.writer.close()
        await self.wait_closed()

    async def wait_closed(self):
        await self.writer.wait_closed()


@contextlib.asynccontextmanager
async def connect_to_tcp_server(host_name: str, port: int):
    reader, writer = await open_connection(host_name, port)
    connection = TCPConnection(reader, writer, False)
    async with asyncstdlib.closing(connection):
        yield connection
