import os
import random
import tempfile
import unittest

from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

import distributed.simple.worker as simple_worker

from ...util import calc_file_hash


class WorkerHTTPTestCase(AioHTTPTestCase):
    async def get_application(self):
        return simple_worker.init()

    @unittest_run_loop
    async def test_new_file(self):
        files = {"file": open(__file__, "rb")}
        resp = await self.client.post("/new_file", data=files)
        got_resp_text = await resp.text()
        expected_resp_text = calc_file_hash(open(__file__, "rb").read())
        self.assertEqual(resp.status, 200)
        self.assertEqual(got_resp_text, expected_resp_text)

    @unittest_run_loop
    async def test_new_file_with_empty_file(self):
        with tempfile.TemporaryFile() as fp:
            files = {"file": fp}
            resp = await self.client.post("/new_file", data=files)
            got_resp_text = await resp.text()
            expected_resp_text = calc_file_hash(b"")
            self.assertEqual(resp.status, 200)
            self.assertEqual(got_resp_text, expected_resp_text)

    @unittest_run_loop
    async def test_new_file_with_big_file(self):
        with tempfile.TemporaryFile() as fp:
            size = random.randint(2 * 1024 * 1024, 10 * 1024 * 1024)
            chunk = os.urandom(size)
            fp.write(chunk)
            fp.seek(0)

            files = {"file": fp}
            resp = await self.client.post("/new_file", data=files)
            got_resp_text = await resp.text()
            expected_resp_text = calc_file_hash(chunk)
            self.assertEqual(resp.status, 200)
            self.assertEqual(got_resp_text, expected_resp_text)


if __name__ == "__main__":
    unittest.main()
