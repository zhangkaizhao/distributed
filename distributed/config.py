MASTER = ("127.0.0.1", 8000)

WORKERS = [
    ("127.0.0.1", 8001),
    ("127.0.0.1", 8002),
    ("127.0.0.1", 8003),
    ("127.0.0.1", 8004),
]

CHUNK_SIZE = 256 * 1024

# master checks if a file processing timeout in worker
WORKER_PROCESSING_TIMEOUT_SECONDS = 4
# workers wait for file processing file timeout from master to get a file
WORKER_WAIT_FOR_TIMEOUT_FILE_SECONDS = 4
