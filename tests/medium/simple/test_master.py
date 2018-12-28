import asyncio
import multiprocessing
import os
import os.path
import random
import tempfile
import unittest

from aiohttp import web
from aiohttp.test_utils import unused_port

from distributed.simple.master import Master
import distributed.simple.worker as simple_worker

from ...util import calc_file_hash


def start_worker_server(app, port):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    web.run_app(app, port=port)


class MasterTestCase(unittest.TestCase):
    def setUp(self):
        port1 = unused_port()
        port2 = unused_port()
        worker_app1 = simple_worker.init()
        worker_app2 = simple_worker.init()
        process1 = multiprocessing.Process(
            target=start_worker_server, args=(worker_app1, port1)
        )
        process2 = multiprocessing.Process(
            target=start_worker_server, args=(worker_app2, port2)
        )
        process1.start()
        process2.start()
        self._port1 = port1
        self._port2 = port2
        self._processes = [process1, process2]

    def tearDown(self):
        for process in self._processes:
            process.terminate()

    def test_task(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        with tempfile.TemporaryDirectory() as home_dir:
            dir1_path = os.path.join(home_dir, "var/dir1")
            dir2_path = os.path.join(home_dir, "var/dir2")
            dir3_path = os.path.join(home_dir, "var/dir2/dir3")
            os.makedirs(dir1_path)
            os.makedirs(dir3_path)
            file1_path = os.path.join(dir1_path, "file1")
            file2_path = os.path.join(dir2_path, "file2")
            file3_path = os.path.join(dir2_path, "file3.ext")
            file4_path = os.path.join(dir3_path, "file4")
            files = {}
            for file_path in (file1_path, file2_path, file3_path, file4_path):
                size = random.randint(2 * 1024 * 1024, 10 * 1024 * 1024)
                chunk = os.urandom(size)
                files[file_path] = calc_file_hash(chunk)
                with open(file_path, "wb") as f:
                    f.write(chunk)

            class FakeConfig:
                MASTER = ("127.0.0.1", 8000)
                WORKERS = [
                    ("127.0.0.1", self._port1),
                    ("127.0.0.1", self._port2),
                ]
                CHUNK_SIZE = 256 * 1024
                WORKER_PROCESSING_TIMEOUT_SECONDS = 4
                WORKER_WAIT_FOR_TIMEOUT_FILE_SECONDS = 4

            master = Master(home_dir, config=FakeConfig)
            loop.run_until_complete(master.run())
            got_files = master.home.files
            expected_files = {
                master.home._get_rel_path(path): hash_
                for path, hash_ in files.items()
            }
            assert got_files == expected_files

    def test_task_while_worker_server_unreachable(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        port3 = unused_port()

        with tempfile.TemporaryDirectory() as home_dir:
            dir1_path = os.path.join(home_dir, "var/dir1")
            dir2_path = os.path.join(home_dir, "var/dir2")
            dir3_path = os.path.join(home_dir, "var/dir2/dir3")
            os.makedirs(dir1_path)
            os.makedirs(dir3_path)
            file1_path = os.path.join(dir1_path, "file1")
            file2_path = os.path.join(dir2_path, "file2")
            file3_path = os.path.join(dir2_path, "file3.ext")
            file4_path = os.path.join(dir3_path, "file4")
            files = {}
            for file_path in (file1_path, file2_path, file3_path, file4_path):
                size = random.randint(2 * 1024 * 1024, 10 * 1024 * 1024)
                chunk = os.urandom(size)
                files[file_path] = calc_file_hash(chunk)
                with open(file_path, "wb") as f:
                    f.write(chunk)

            class FakeConfig:
                MASTER = ("127.0.0.1", 8000)
                WORKERS = [("127.0.0.1", port3)]
                CHUNK_SIZE = 256 * 1024
                WORKER_PROCESSING_TIMEOUT_SECONDS = 4
                WORKER_WAIT_FOR_TIMEOUT_FILE_SECONDS = 4

            master = Master(home_dir, config=FakeConfig)
            loop.run_until_complete(master.run())
            got_files = master.home.files
            expected_files = {
                master.home._get_rel_path(path): None
                for path, hash_ in files.items()
            }
            assert got_files == expected_files

    def test_task_with_ignoring_unreachable_worker_server(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        port3 = unused_port()

        with tempfile.TemporaryDirectory() as home_dir:
            dir1_path = os.path.join(home_dir, "var/dir1")
            dir2_path = os.path.join(home_dir, "var/dir2")
            dir3_path = os.path.join(home_dir, "var/dir2/dir3")
            os.makedirs(dir1_path)
            os.makedirs(dir3_path)
            file1_path = os.path.join(dir1_path, "file1")
            file2_path = os.path.join(dir2_path, "file2")
            file3_path = os.path.join(dir2_path, "file3.ext")
            file4_path = os.path.join(dir3_path, "file4")
            files = {}
            for file_path in (file1_path, file2_path, file3_path, file4_path):
                size = random.randint(2 * 1024 * 1024, 10 * 1024 * 1024)
                chunk = os.urandom(size)
                files[file_path] = calc_file_hash(chunk)
                with open(file_path, "wb") as f:
                    f.write(chunk)

            class FakeConfig:
                MASTER = ("127.0.0.1", 8000)
                WORKERS = [
                    ("127.0.0.1", self._port1),
                    ("127.0.0.1", self._port2),
                    ("127.0.0.1", port3),
                ]
                CHUNK_SIZE = 256 * 1024
                WORKER_PROCESSING_TIMEOUT_SECONDS = 4
                WORKER_WAIT_FOR_TIMEOUT_FILE_SECONDS = 4

            master = Master(home_dir, config=FakeConfig)
            loop.run_until_complete(master.run())
            got_files = master.home.files
            expected_files = {
                master.home._get_rel_path(path): hash_
                for path, hash_ in files.items()
            }
            assert got_files == expected_files
