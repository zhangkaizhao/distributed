import asyncio
import time
from typing import Any, Dict, Generator, List, Optional, Tuple, Union, cast

from aiohttp import web
from aiohttp.web_fileresponse import FileResponse

from distributed.home import Home
from distributed import config as default_config
from distributed import constants


class _Worker:
    def __init__(self, name: str, config: Any) -> None:
        self.name = name
        self.config = config

        # for calculating processing speed
        self._total_file_size: int = 0
        self._total_processing_seconds: float = 0
        self._total_files: int = 0
        # file_path -> (file_size, start_at)
        self._current_processing_files: Dict[str, Tuple[int, float]] = {}

    @property
    def processing_speed(self) -> Union[int, float]:
        """processing speed per byte"""
        if self._total_file_size == 0:
            return 0
        else:
            return self._total_processing_seconds / self._total_file_size

    @property
    def processing_speed_per_file(self) -> Union[int, float]:
        """processing speed per file"""
        if self._total_files == 0:
            return 0
        else:
            return self._total_processing_seconds / self._total_files

    def new_processing_file(self, file_path: str, file_size: int) -> None:
        if file_path in self._current_processing_files:
            print(
                f"file(path={file_path}) is in current processing files"
                " already. override it."
            )
        self._current_processing_files[file_path] = (file_size, time.time())

    def file_processing_finished(self, file_path: str) -> None:
        if file_path in self._current_processing_files:
            # pop from current processing files
            file_size, start_at = self._current_processing_files.pop(file_path)
            processing_seconds = time.time() - start_at
            if processing_seconds < 0:
                print(
                    f"processing start time of file(path={file_path}) is later"
                    " than now. ignore it."
                )
            else:
                # update processing speed
                self._total_file_size += file_size
                self._total_processing_seconds += time.time() - start_at
                self._total_files += 1
        else:
            print(
                f"file(path={file_path}) is not in current processing files."
                " ignore it."
            )

    def estimate_file_processing_seconds(
        self, file_size: int
    ) -> Union[int, float]:
        return file_size * self.processing_speed

    def iter_timeout_files(self) -> Generator[Tuple[str, float], None, None]:
        # calculate based on worker's processing speed
        timeout_before = (
            time.time() - self.config.WORKER_PROCESSING_TIMEOUT_SECONDS
        )
        for (
            file_path,
            (file_size, start_at),
        ) in self._current_processing_files.items():
            estimated_processing_seconds = self.estimate_file_processing_seconds(
                file_size
            )
            expected_finished_at = start_at + estimated_processing_seconds
            if expected_finished_at < timeout_before:
                yield file_path, start_at

    @property
    def timeout_files(self) -> List[Tuple[str, float]]:
        return list(self.iter_timeout_files())

    def get_one_timeout_file(self) -> Optional[Tuple[str, float]]:
        for timeout_file in self.iter_timeout_files():
            return timeout_file
        return None

    def get_earlist_timeout_file(self) -> Optional[Tuple[str, float]]:
        result = None
        for file_path, start_at in self.iter_timeout_files():
            if result is None or start_at < result[1]:
                result = file_path, start_at
        return result

    def has_processing_files(self) -> bool:
        return bool(len(self._current_processing_files))

    def remove_current_processing_file(self, file_path: str) -> bool:
        return self._current_processing_files.pop(file_path, None) is not None


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

        self.current_workers: List[_Worker] = []

    def _get_worker(self, worker_name: str) -> Optional[_Worker]:
        for worker in self.current_workers:
            if worker.name == worker_name:
                return worker
        return None

    def worker_register(self, worker_name: str) -> bool:
        if self._get_worker(worker_name) is not None:
            return False

        worker = _Worker(worker_name, self.config)
        self.current_workers.append(worker)
        print(f"worker(name={worker_name}) registered.")
        return True

    def worker_registered(self, worker_name: str) -> bool:
        return self._get_worker(worker_name) is not None

    def new_processing_file(self, worker_name: str, file_path: str) -> None:
        full_path = self.home.get_full_path(file_path)
        file_size = os.path.getsize(full_path)

        # worker should be registered before this
        worker = self._get_worker(worker_name)
        worker = cast(_Worker, worker)
        worker.new_processing_file(file_path, file_size)

    def file_processing_finished(
        self, worker_name: str, file_path: str, file_hash: str
    ) -> None:
        self.home.set_file_hash(file_path, file_hash)

        # worker should be registered before this
        worker = self._get_worker(worker_name)
        worker = cast(_Worker, worker)
        worker.file_processing_finished(file_path)

    def iter_timeout_files(
        self
    ) -> Generator[Tuple[str, str, float], None, None]:
        for worker in self.current_workers:
            for file_path, start_at in worker.iter_timeout_files():
                yield worker.name, file_path, start_at

    @property
    def timeout_files(self) -> List[Tuple[str, str, float]]:
        return list(self.iter_timeout_files())

    def get_one_timeout_file(self) -> Optional[Tuple[str, str, float]]:
        for timeout_file in self.iter_timeout_files():
            return timeout_file
        else:
            return None

    def get_earlist_timeout_file(self) -> Optional[Tuple[str, str, float]]:
        result = None
        for worker_name, file_path, start_at in self.iter_timeout_files():
            if result is None or start_at < result[1]:
                result = worker_name, file_path, start_at
        return result

    def has_processing_files(self) -> bool:
        for worker in self.current_workers:
            if worker.has_processing_files():
                return True
        return False

    def remove_current_processing_file(
        self, worker_name: str, file_path: str
    ) -> None:
        # worker should be registered before this
        worker = self._get_worker(worker_name)
        worker = cast(_Worker, worker)
        worker.remove_current_processing_file(file_path)


def _get_worker_name(request: web.Request) -> str:
    """Get worker name from request header"""
    worker_name = request.headers.get(constants.HTTP_HEADER_X_WORKER_NAME)
    if not worker_name:
        raise web.HTTPBadRequest(text="missing worker name header")
    return worker_name


def _ensure_worker_registered(master: Master, worker_name: str) -> None:
    """Ensure worker registered already"""
    if not master.worker_registered(worker_name):
        raise web.HTTPBadRequest(text="worker not registered yet")


async def worker_register(request: web.Request) -> web.Response:
    """For worker to register itself

    request headers:

    * X-DISTRIBUTED-WORKER-NAME: worker's name, worker should been registered
                                 already. required

    responses:

    * 200 OK: successful
    * 400 Bad Request: missing worker name header
    * 409 Conflict: worker has already registered
    """
    worker_name = _get_worker_name(request)

    master = request.app["master"]
    if not master.worker_register(worker_name):
        # already registered
        raise web.HTTPConflict(text="worker has already registered")

    return web.Response()


async def get_file(request: web.Request) -> FileResponse:
    """For worker to pull file for calculating file hash task

    request headers:

    * X-DISTRIBUTED-WORKER-NAME: worker's name, worker should been registered
                                 already. required

    responses:

    * 200 OK: file response
    * 202 Accepted: wait for processing files timeout and make request again
    * 204 No Content: no more files
    * 400 Bad Request: missing worker name header
    * 400 Bad Request: worker not registered yet
    """
    master = request.app["master"]

    worker_name = _get_worker_name(request)
    _ensure_worker_registered(master, worker_name)

    try:
        file_path = master.files_queue.get_nowait()
    except asyncio.QueueEmpty:
        # try to get timeout task
        timeout_file = master.get_earlist_timeout_file()
        if file_path:
            _worker_name, file_path, _ = timeout_file
            # remove old processing file
            master.remove_current_processing_file(_worker_name, file_path)
        else:
            # try to find out any processing files
            if not master.has_processing_files():
                # no more files
                raise web.HTTPNoContent()
            else:
                # tell workers to wait until processing files timeout
                raise web.HTTPAccepted(
                    text=(
                        "wait for processing files timeout and make request"
                        " again"
                    )
                )

    master.new_processing_file(worker_name, file_path)
    master.files_queue.task_done()

    full_path = master.home.get_full_path(file_path)
    headers = {constants.HTTP_HEADER_X_FILE_PATH: file_path}
    return FileResponse(full_path, headers=headers)


async def update_file_hash(request: web.Request) -> web.Response:
    """For worker to report file hash

    request headers:

    * X-DISTRIBUTED-WORKER-NAME: worker's name, worker should been registered
                                 already. required
    * X-DISTRIBUTED-FILE-PATH: file path. required

    responses:

    * 200 OK: file hash updated
    * 400 Bad Request: missing worker name header
    * 400 Bad Request: missing file_path parameter
    * 400 Bad Request: missing file_hash parameter
    * 404 Bad Request: file not found
    """
    master = request.app["master"]

    worker_name = _get_worker_name(request)
    _ensure_worker_registered(master, worker_name)

    form = await request.post()
    file_path = form.get("file_path")
    if not file_path:
        raise web.HTTPBadRequest(text="missing file_path parameter")
    file_hash = form.get("file_hash")
    if not file_hash:
        raise web.HTTPBadRequest(text="missing file_hash parameter")

    if master.home.file_exists(file_path):
        master.file_processing_finished(worker_name, file_path, file_hash)
    else:
        raise web.HTTPNotFound(text="file not found")

    return web.Response()


async def home_status(request: web.Request) -> web.Response:
    """Reporting home files status

    request query parameters:

    * include_files: whether including files with hashes. optional

    responses:

    * 200 OK: json response of status
    """
    master = request.app["master"]
    state = master.home.state
    outdict = {"state": state}
    if request.query.get("include_files"):
        outdict["files"] = master.home.files
    return web.json_response(outdict)


def init(home_dir) -> web.Application:
    app = web.Application()
    app["master"] = Master(home_dir)
    app.add_routes(
        [
            web.post("/worker_register", worker_register),
            web.get("/get_file", get_file),
            web.put("/update_file_hash", update_file_hash),
            web.get("/home_status", home_status),
        ]
    )
    return app


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="distributed master server")
    parser.add_argument("--home")
    parser.add_argument("--path")
    parser.add_argument("--port")
    args = parser.parse_args()

    home_dir = args.home or os.getcwd()
    app = init(home_dir)

    web.run_app(app, path=args.path, port=args.port)
