import os.path
import unittest

from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

from distributed import constants
import distributed.pull.master as pull_master

from ...util import gen_random_file_hash


class MasterHTTPTestCase(AioHTTPTestCase):
    async def get_application(self):
        home_dir = os.path.dirname(__file__)
        return pull_master.init(home_dir)

    @unittest_run_loop
    async def test_worker_register(self):
        headers = {constants.HTTP_HEADER_X_WORKER_NAME: "worker-1"}
        resp = await self.client.post("/worker_register", headers=headers)
        self.assertEqual(resp.status, 200)

        master = self.app["master"]
        got_current_worker_names = [w.name for w in master.current_workers]
        expected_current_worker_names = ["worker-1"]
        self.assertSequenceEqual(
            got_current_worker_names, expected_current_worker_names
        )

    @unittest_run_loop
    async def test_worker_register_while_missing_worker_name_header(self):
        resp = await self.client.post("/worker_register")
        self.assertEqual(resp.status, 400)

        master = self.app["master"]
        got_current_worker_names = [w.name for w in master.current_workers]
        expected_current_worker_names = []
        self.assertSequenceEqual(
            got_current_worker_names, expected_current_worker_names
        )

    @unittest_run_loop
    async def test_worker_register_while_conflict(self):
        headers = {constants.HTTP_HEADER_X_WORKER_NAME: "worker-1"}
        resp = await self.client.post("/worker_register", headers=headers)
        self.assertEqual(resp.status, 200)

        headers = {constants.HTTP_HEADER_X_WORKER_NAME: "worker-1"}
        resp = await self.client.post("/worker_register", headers=headers)
        self.assertEqual(resp.status, 409)

        master = self.app["master"]
        got_current_worker_names = [w.name for w in master.current_workers]
        expected_current_worker_names = ["worker-1"]
        self.assertSequenceEqual(
            got_current_worker_names, expected_current_worker_names
        )

    @unittest_run_loop
    async def test_worker_register_for_different_workers(self):
        headers = {constants.HTTP_HEADER_X_WORKER_NAME: "worker-1"}
        resp = await self.client.post("/worker_register", headers=headers)
        self.assertEqual(resp.status, 200)

        headers = {constants.HTTP_HEADER_X_WORKER_NAME: "worker-2"}
        resp = await self.client.post("/worker_register", headers=headers)
        self.assertEqual(resp.status, 200)

        headers = {constants.HTTP_HEADER_X_WORKER_NAME: "worker-3"}
        resp = await self.client.post("/worker_register", headers=headers)
        self.assertEqual(resp.status, 200)

        master = self.app["master"]
        got_current_worker_names = [w.name for w in master.current_workers]
        expected_current_worker_names = ["worker-1", "worker-2", "worker-3"]
        self.assertSequenceEqual(
            got_current_worker_names, expected_current_worker_names
        )

    @unittest_run_loop
    async def test_get_file(self):
        pass

    @unittest_run_loop
    async def test_update_file_hash(self):
        pass

    @unittest_run_loop
    async def test_home_status(self):
        # XXX mocked
        master = self.app["master"]
        master.home.files = {
            "var/file1": None,
            "var/dir1/file1": gen_random_file_hash(),
            "var/dir2/file2": None,
        }

        resp = await self.client.get("/home_status")
        self.assertEqual(resp.status, 200)
        got_status = await resp.json()
        expected_status = {"state": {"total": 3, "finished": 1}}
        self.assertDictEqual(got_status, expected_status)

    @unittest_run_loop
    async def test_home_status_with_optional_include_files(self):
        # XXX mocked
        _files = {
            "var/file1": None,
            "var/dir1/file1": gen_random_file_hash(),
            "var/dir2/file2": None,
        }
        master = self.app["master"]
        master.home.files = _files

        params = {"include_files": 1}
        resp = await self.client.get("/home_status", params=params)
        self.assertEqual(resp.status, 200)
        got_status = await resp.json()
        expected_status = {
            "state": {"total": 3, "finished": 1},
            "files": _files,
        }
        self.assertDictEqual(got_status, expected_status)


if __name__ == "__main__":
    unittest.main()
