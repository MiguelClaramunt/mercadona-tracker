# import libsql_experimental as libsql
import os
import signal
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from tqdm import tqdm

from mercatracker import db
from mercatracker.api import (
    ConsumProductSchema,
    MercadonaProductSchema,
    TemporaryRestrictionException,
)
from mercatracker.config import Config
from mercatracker.db_manager import DatabaseManager
from mercatracker.sethandler import SetHandler

stop_event = threading.Event()


def _handle_exit(signum, frame):
    print(f"Received exit signal ({signum}), will stop after current batch...")
    stop_event.set()


signal.signal(signal.SIGINT, _handle_exit)
signal.signal(signal.SIGTERM, _handle_exit)
if hasattr(signal, "SIGBREAK"):
    # Handle Ctrl-Break on Windows
    signal.signal(signal.SIGBREAK, _handle_exit)

BATCH_SIZE = 16  # increased for higher concurrency
config = Config().load()


def fetch_product_data(request_handler, id_, params):
    product = request_handler(id=id_, params=params)
    product.request()
    return product


def process_batch(request_handler, ids_batch, params):
    # use batch size for thread pool to maximize throughput
    with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
        futures = [
            executor.submit(fetch_product_data, request_handler, id_, params)
            for id_ in ids_batch
        ]
        return [f.result() for f in as_completed(futures)]


def scrape_supermarket(
    conn, product_schema_class, sh, supermarket_params, position, db_manager
):
    supermarket = product_schema_class(request_sitemap=True)
    db_params = {
        "supermarket": product_schema_class.get_name(),
        "params": supermarket_params,
    }
    # ensure supermarket record exists (INSERT OR IGNORE semantics)
    db.write_supermarket_params(conn, db_params)
    supermarket_id = db.fetch_supermarket_id(conn, db_params)

    sh.supermarket_id = supermarket_id
    sh.ymd = supermarket.lastmod
    sh.load_cache()

    if not sh.s or not sh.c:
        s = product_schema_class(request_sitemap=True).scrape_ids()
        sh.reset_set("c", "w")
        sh.update_set("s", s, reset=True)
        # enqueue set_cache writes
        for name, data in sh.sets.items():
            db_manager.enqueue_set_cache(sh.ymd, sh.supermarket_id, name, data)

    if supermarket.lastmod != db.get_lastmod(conn, supermarket_id):
        s = supermarket.scrape_ids()
        sh.reset_set("c", "w")
        sh.update_set("s", s, reset=True)
        # enqueue set_cache writes
        for name, data in sh.sets.items():
            db_manager.enqueue_set_cache(sh.ymd, sh.supermarket_id, name, data)
    else:
        w = db.get_processed_ids(conn, supermarket_id, supermarket.lastmod)
        sh.update_set("w", w, reset=True)

    # Process IDs
    ids_to_process = list(sh.s - sh.w - sh.c)
    if not ids_to_process:
        return sh

    with tqdm(
        total=len(ids_to_process),
        desc=product_schema_class.get_name(),
        ncols=0,
        position=position,
        leave=True,
    ) as pbar:
        for i in range(0, len(ids_to_process), BATCH_SIZE):
            if stop_event.is_set():
                print(
                    "Stop signal received, exiting current scrape_supermarket batch loop..."
                )
                break
            batch_ids = ids_to_process[i : i + BATCH_SIZE]
            results = process_batch(product_schema_class, batch_ids, supermarket_params)
            valid_requests = [r for r in results if r.response.status_code == 200]
            written_ids_batch = []
            if valid_requests:
                # enqueue dump writes
                for prod in valid_requests:
                    db_manager.enqueue_dump(
                        {
                            "id": prod.id,
                            "ymd": supermarket.lastmod,
                            "content": prod.to_dump(),
                            "supermarket_id": supermarket_id,
                        }
                    )
                    written_ids_batch.append(prod.id)
                pbar.update(len(written_ids_batch))
            # update sets after each batch
            sh.update_set({"w": written_ids_batch, "c": batch_ids})
    return sh


if __name__ == "__main__":
    # start database manager thread
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mercadona.db")
    db_manager = DatabaseManager(db_path)
    db_manager.start()
    sh = SetHandler()
    sh.init_set("s", "w", "c")  # scraped, written, checked

    scrape_targets = [
        {
            "request_handler": MercadonaProductSchema,
            "params": '{"lang":"es","wh":"vlc1"}',
        },
        {
            "request_handler": ConsumProductSchema,
            "params": '{"shippingZoneId":"0D"}',
        },
    ]

    def scrape_target(target, position, db_manager):
        local_conn = sqlite3.connect(db_path, check_same_thread=False)
        local_sh = SetHandler(conn=local_conn)
        local_sh.init_set("s", "w", "c")
        try:
            while not stop_event.is_set():
                try:
                    local_sh = scrape_supermarket(
                        local_conn,
                        target["request_handler"],
                        local_sh,
                        target["params"],
                        position,
                        db_manager,
                    )
                    remaining_ids = local_sh.s - local_sh.w - local_sh.c
                    if not remaining_ids:
                        # enqueue set_cache writes on completion
                        for name, data in local_sh.sets.items():
                            db_manager.enqueue_set_cache(
                                local_sh.ymd, local_sh.supermarket_id, name, data
                            )
                        print(
                            f"All {target['request_handler'].get_name()} IDs processed."
                        )
                        break
                except requests.exceptions.SSLError:
                    secs = 120
                    print(f"Encountered SSLError. Retrying in {secs // 60} minutes...")
                    time.sleep(secs)
                except TemporaryRestrictionException as e:
                    secs = 300
                    print(f"{e} Retrying in {secs // 60} minutes...")
                    time.sleep(secs)
                except KeyboardInterrupt:
                    print(
                        "KeyboardInterrupt caught, breaking out of scrape_target loop..."
                    )
                    break
        finally:
            # on exit, enqueue cache writes before closing DB
            if stop_event.is_set():
                for name, data in local_sh.sets.items():
                    db_manager.enqueue_set_cache(
                        local_sh.ymd, local_sh.supermarket_id, name, data
                    )
                print("Shutdown initiated. Exiting target.")
            local_conn.close()

    with ThreadPoolExecutor(max_workers=len(scrape_targets)) as executor:
        futures = [
            executor.submit(scrape_target, target, pos, db_manager)
            for pos, target in enumerate(scrape_targets)
        ]
        for future in as_completed(futures):
            future.result()

    # shutdown database manager after all tasks
    db_manager.shutdown()
