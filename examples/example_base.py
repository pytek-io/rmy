import argparse
from typing import TypeVar

import rmy
import asyncio

T = TypeVar("T")


async def run_main_async(object_class, port: int, main_async):
    async with rmy.create_async_client("localhost", port) as client:
        o = await client.fetch_remote_object(object_class, 0)
        await main_async(o)


def main(demo_object, main_sync, main_async=None):
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument("--server", action="store_true", help="run as server")
    parser.add_argument("--port", default="8081", help="TCP port to use")
    parser.add_argument("--async", action="store_true", help="run async version")
    args = parser.parse_args()
    if args.server:
        rmy.run_tcp_server(args.port, demo_object)
    else:
        if args.__dict__["async"]:
            asyncio.run(run_main_async(type(demo_object), args.port, main_async))
        else:
            with rmy.create_sync_client("localhost", args.port) as client:
                o = client.fetch_remote_object(type(demo_object))
                main_sync(o)
