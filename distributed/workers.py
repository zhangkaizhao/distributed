import time

import psutil


class WorkerState:
    cpu_percent: int = 0


class Workers:
    HEARTBEAT = 10  # seconds

    def __init__(self, heartbeat=None):
        self.heartbeat = self.HEARTBEAT if heartbeat is None else heartbeat
        self.workers = {}

    def update_worker_state(self, worker_addr, state):
        now = time.time()
        self.workers[worker_addr] = (now, state)

    def select_worker_addr(self):
        """Select latest low load worker"""
        active_after = time.time() - self.heartbeat
        active_worker_states = []
        for worker_addr in self.workers:
            time_, state = self.workers[worker_addr]
            if time_ < active_after:
                active_worker_states.append((worker_addr, state))
        if active_worker_states:
            active_worker_states.sort(key=lambda x: x[1].cpu_percent)
            return active_worker_states[0][0]
        return None


def collect_worker_state():
    return {"cpu_percent": psutil.cpu_percent()}
