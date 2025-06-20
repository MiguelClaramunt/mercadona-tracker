import queue
import sqlite3
import threading

from mercatracker import db as db_module


class DatabaseManager(threading.Thread):
    """
    A dedicated thread to handle all database write operations sequentially.
    """

    def __init__(self, db_path):
        super().__init__(daemon=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.queue = queue.Queue()
        self.stop_event = threading.Event()

    def run(self):
        while not (self.stop_event.is_set() and self.queue.empty()):
            try:
                item = self.queue.get(timeout=1)
            except queue.Empty:
                continue
            if item["type"] == "dump":
                db_module.write_dump(self.conn, item["data"])
            elif item["type"] == "set_cache":
                params = item["data"]
                db_module.write_set_cache(
                    self.conn,
                    params["ymd"],
                    params["supermarket_id"],
                    params["set_name"],
                    params["set_data"],
                )
            self.queue.task_done()
        self.conn.close()

    def enqueue_dump(self, parameters: dict):
        """Enqueue a dump write operation."""
        self.queue.put({"type": "dump", "data": parameters})

    def enqueue_set_cache(
        self, ymd: int, supermarket_id: int, set_name: str, set_data: set
    ):
        """Enqueue a set cache write operation."""
        self.queue.put(
            {
                "type": "set_cache",
                "data": {
                    "ymd": ymd,
                    "supermarket_id": supermarket_id,
                    "set_name": set_name,
                    "set_data": set_data,
                },
            }
        )

    def shutdown(self):
        """Signal to stop after processing all queued operations."""
        self.stop_event.set()
        self.join()
