import os
import os.path
import random
import tempfile
import unittest

from distributed.home import Home


class HomeTest(unittest.TestCase):
    def test_load_files(self):
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
            for file_path in (file1_path, file2_path, file3_path, file4_path):
                with open(file_path, "wb") as f:
                    f.write(os.urandom(random.randint(0, 10)))

            home = Home(home_dir)
            home.load_files()
            got_files = home.files
            expected_files = {
                "var/dir1/file1": None,
                "var/dir2/file2": None,
                "var/dir2/file3.ext": None,
                "var/dir2/dir3/file4": None,
            }
            self.assertDictEqual(got_files, expected_files)


if __name__ == "__main__":
    unittest.main()
