import unittest

from distributed.home import Home

from ..util import gen_random_file_hash


class HomeTest(unittest.TestCase):
    def setUp(self):
        home_dir = "/does/not/exist"
        self.home = Home(home_dir)

    def test_set_file_hash(self):
        file1_hash = gen_random_file_hash()
        file2_hash = gen_random_file_hash()
        self.home.files = {
            "var/file1": None,
            "var/dir1/file1": file1_hash,
            "var/dir2/file2": None,
        }
        self.home.set_file_hash("var/dir2/file2", file2_hash)
        got_files = self.home.files
        expected_files = {
            "var/file1": None,
            "var/dir1/file1": file1_hash,
            "var/dir2/file2": file2_hash,
        }
        self.assertDictEqual(got_files, expected_files)

    def test_get_full_path(self):
        path = "var/file1"
        got_path = self.home.get_full_path(path)
        self.assertEqual(got_path, "/does/not/exist/var/file1")

    def test_finished(self):
        self.home.files = {
            "var/file1": None,
            "var/dir1/file1": None,
            "var/dir2/file2": None,
        }
        self.assertFalse(self.home.finished)

        self.home.files = {
            "var/file1": None,
            "var/dir1/file1": gen_random_file_hash(),
            "var/dir2/file2": None,
        }
        self.assertFalse(self.home.finished)

        self.home.files = {
            "var/file1": gen_random_file_hash(),
            "var/dir1/file1": gen_random_file_hash(),
            "var/dir2/file2": gen_random_file_hash(),
        }
        self.assertTrue(self.home.finished)

    def test_state(self):
        self.home.files = {
            "var/file1": None,
            "var/dir1/file1": None,
            "var/dir2/file2": None,
        }
        expected_state = {"total": 3, "finished": 0}
        self.assertDictEqual(self.home.state, expected_state)

        self.home.files = {
            "var/file1": None,
            "var/dir1/file1": gen_random_file_hash(),
            "var/dir2/file2": None,
        }
        expected_state = {"total": 3, "finished": 1}
        self.assertDictEqual(self.home.state, expected_state)

        self.home.files = {
            "var/file1": gen_random_file_hash(),
            "var/dir1/file1": gen_random_file_hash(),
            "var/dir2/file2": gen_random_file_hash(),
        }
        expected_state = {"total": 3, "finished": 3}
        self.assertDictEqual(self.home.state, expected_state)


if __name__ == "__main__":
    unittest.main()
