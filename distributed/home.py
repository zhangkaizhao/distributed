import os
import os.path
from typing import Dict, Optional


class Home:
    def __init__(self, home_dir: str) -> None:
        self.home_dir = home_dir
        self._files_loaded = False
        self.files: Dict[str, Optional[str]] = {}

    def _load_files(self, dir_: str) -> None:
        for entry in os.scandir(dir_):
            if entry.is_file():
                path = self._get_rel_path(os.path.join(dir_, entry.path))
                self.files[path] = None
            elif entry.is_dir():
                # max recursive?
                self._load_files(entry.path)

    def _get_rel_path(self, full_path: str) -> str:
        return full_path[len(self.home_dir) + 1 :]

    def load_files(self) -> None:
        if self._files_loaded:
            raise Exception("files loaded")

        self._load_files(self.home_dir)
        self._files_loaded = True

    def set_file_hash(self, path: str, hash_: str) -> None:
        self.files[path] = hash_

    def get_full_path(self, path: str) -> str:
        return os.path.join(self.home_dir, path)

    def file_exists(self, path: str) -> bool:
        return path in self.files

    @property
    def finished(self) -> bool:
        """Check if all files have hash set"""
        for f in self.files:
            if self.files[f] is None:
                return False
        return True

    @property
    def state(self) -> Dict[str, int]:
        """Report files hash set state"""
        result = {"total": len(self.files), "finished": 0}
        for f in self.files:
            if self.files[f] is not None:
                result["finished"] += 1
        return result
