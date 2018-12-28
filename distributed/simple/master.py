import asyncio
from typing import Any, Awaitable, Callable, List, Optional

import aiohttp

from distributed import config as default_config
from distributed.home import Home


class _Worker:
    def __init__(self, host: str, port: int, master: "Master") -> None:
        self.host = host
        self.port = port
        self.master = master
        self.job_endpoint = f"http://{host}:{port}/new_file"

    async def _process_job(self, file_path: str) -> str:
        async with aiohttp.ClientSession() as session:
            full_path = self.master.home.get_full_path(file_path)
            files = {"file": open(full_path, "rb")}
            async with session.post(self.job_endpoint, data=files) as resp:
                # TODO should be 200 OK
                # print(resp.status)
                return await resp.text()

    async def start_task(self) -> None:
        while not self.master.files_queue.empty():
            file_path = await self.master.files_queue.get()
            # if bad worker, put file_path back to queue
            try:
                result = await self._process_job(file_path)
            except aiohttp.ClientConnectionError as ex:
                print(ex)
                print(f"{self.job_endpoint} is unreachable.")
                self.master.files_queue.task_done()
                # put file_path back to queue
                await self.master.files_queue.put(file_path)
                # remove worker
                # self.master.workers.remove(self)
                # exit worker task
                break
            else:
                self.master.home.set_file_hash(file_path, result)
                self.master.files_queue.task_done()


class Master:
    def __init__(self, home_dir: str, config: Optional[Any] = None) -> None:
        self.home = Home(home_dir)
        self.config = config if config is not None else default_config

        print("loading home files...")
        self.home.load_files()
        print("home files loaded.")

        print("preparing home files queue...")
        self.files_queue: asyncio.Queue[str] = asyncio.Queue()
        for file_path in self.home.files:
            self.files_queue.put_nowait(file_path)
        print("home files queue prepared.")

        print("creating workers...")
        self.workers: List[_Worker] = []
        for host, port in self.config.WORKERS:
            worker = _Worker(host, port, self)
            self.workers.append(worker)
        print("workers created.")

        self._worker_tasks: List[
            asyncio.Task[Callable[[_Worker], Awaitable[None]]]
        ] = []

    async def _start_worker_tasks(self) -> None:
        print("preparing worker tasks...")
        for worker in self.workers:
            task = asyncio.create_task(worker.start_task())
            self._worker_tasks.append(task)
        print("worker tasks prepared.")

    async def run(self) -> None:
        await self._start_worker_tasks()
        await asyncio.gather(*self._worker_tasks)

        for path, hash_ in self.home.files.items():
            print(f"{path} -> {hash_}")

        print(f"state: {self.home.state}")


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="distributed master")
    parser.add_argument("--home")
    args = parser.parse_args()

    home_dir = args.home or os.getcwd()
    master = Master(home_dir)
    asyncio.run(master.run())
    print("done")
