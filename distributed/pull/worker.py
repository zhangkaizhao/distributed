import asyncio
import hashlib
import multiprocessing
from typing import Any, Dict, Generator, List, Optional, Tuple, Union, cast

import aiohttp

from distributed import config as default_config
from distributed import constants


class NoMoreFiles(Exception):
    """No more files"""


class WaitForTimeoutFile(Exception):
    """Wait for processing timeout file from master"""


class Worker(multiprocessing.Process):
    def __init__(self, name: str, config: Optional[Any] = None):
        super().__init__()
        self.name = name
        self.config = config if config is not None else default_config

        self.master_addr = self.config.MASTER
        self.master_endpoint = "http://{0}:{1}".format(*self.master_addr)
        self.worker_register_endpoint = (
            f"{self.master_endpoint}/worker_register"
        )
        self.get_file_endpoint = f"{self.master_endpoint}/get_file"
        self.report_file_hash_endpoint = (
            f"{self.master_endpoint}/update_file_hash"
        )

        self.chunk_size = self.config.CHUNK_SIZE

    async def worker_register(self) -> bool:
        async with aiohttp.ClientSession() as session:
            headers = {constants.HTTP_HEADER_X_WORKER_NAME: self.name}
            async with session.post(
                self.worker_register_endpoint, headers=headers
            ) as resp:
                # TODO should be 200 OK
                # print(resp.status)
                return resp.status == 200

    async def fetch_file_and_calculate_hash(self) -> Tuple[str, str]:
        async with aiohttp.ClientSession() as session:
            headers = {constants.HTTP_HEADER_X_WORKER_NAME: self.name}
            async with session.get(
                self.get_file_endpoint, headers=headers
            ) as resp:
                # TODO should be 200 OK or 202 Accepted or 204 No Content
                # print(resp.status)
                if resp.status == 202:
                    raise WaitForTimeoutFile
                elif resp.status == 204:
                    raise NoMoreFiles
                else:
                    file_path = resp.headers.get(
                        constants.HTTP_HEADER_X_FILE_PATH
                    )
                    h = hashlib.new("sha1")
                    while True:
                        chunk = await resp.content.read(self.chunk_size)
                        if not chunk:
                            break
                        h.update(chunk)
                    file_hash = h.hexdigest()
                    return file_path, file_hash

    async def report_file_hash(self, file_path: str, file_hash: str) -> bool:
        async with aiohttp.ClientSession() as session:
            payload = {"file_path": file_path, "file_hash": file_hash}
            headers = {constants.HTTP_HEADER_X_WORKER_NAME: self.name}
            async with session.put(
                self.report_file_hash_endpoint, data=payload, headers=headers
            ) as resp:
                # TODO should be 200 OK
                # print(resp.status)
                return resp.status == 200

    async def work(self) -> None:
        file_path, file_hash = await self.fetch_file_and_calculate_hash()
        await self.report_file_hash(file_path, file_hash)

    def run(self) -> None:
        asyncio.run(self.worker_register())
        while True:
            try:
                asyncio.run(self.work())
            except WaitForTimeoutFile:
                asyncio.run(
                    asyncio.sleep(
                        self.config.WORKER_WAIT_FOR_TIMEOUT_FILE_SECONDS
                    )
                )
            except NoMoreFiles:
                break


def main(worker_name: str, processes_number: int) -> None:
    processes = []
    for _ in range(processes_number):
        process = Worker(worker_name)
        process.start()
        processes.append(process)

    for process in processes:
        process.join()


if __name__ == "__main__":
    import argparse

    worker_number = multiprocessing.cpu_count()
    parser = argparse.ArgumentParser(description="distributed worker")
    parser.add_argument("--name", default="worker")
    parser.add_argument("--processes", default=worker_number)
    args = parser.parse_args()

    main(args.name, args.processes)
    print("done")
