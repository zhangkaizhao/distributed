import hashlib
import os
import random

from distributed import config


def calc_file_hash(chunk: bytes) -> str:
    return hashlib.new("sha1", chunk).hexdigest()


def gen_random_file_hash() -> str:
    size = random.randint(0, config.CHUNK_SIZE * 3)
    chunk = os.urandom(size)
    return calc_file_hash(chunk)
