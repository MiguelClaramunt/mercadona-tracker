# import libsql_experimental as libsql
import os
import sqlite3
import sys
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
from mercatracker.sethandler import SetHandler

BATCH_SIZE = 8
config = Config().load()


def fetch_product_data(request_handler, id_, params):
    product = request_handler(id=id_, params=params)
    product.request()
    return product


def process_batch(request_handler, ids_batch, params):
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(fetch_product_data, request_handler, id_, params)
            for id_ in ids_batch
        ]
        return [f.result() for f in as_completed(futures)]


def scrape_supermarket(conn, product_schema_class, sh, supermarket_params):
    supermarket = product_schema_class(request_sitemap=True)
    db_params = {
        "supermarket": product_schema_class.get_name(),
        "params": supermarket_params,
    }
    supermarket_id = db.fetch_supermarket_id(conn, db_params)
    if not supermarket_id:
        db.write_supermarket_params(conn, supermarket_params)
        supermarket_id = db.fetch_supermarket_id(conn, supermarket_params)

    sh.supermarket_id = supermarket_id
    sh.ymd = supermarket.lastmod
    sh.load_cache()

    if not sh.s or not sh.c:
        s = product_schema_class(request_sitemap=True).scrape_ids()
        sh.reset_set("c", "w")
        sh.update_set("s", s, reset=True)
        sh.save_cache()

    if supermarket.lastmod != db.get_lastmod(conn, supermarket_id):
        s = supermarket.scrape_ids()
        sh.reset_set("c", "w")
        sh.update_set("s", s, reset=True)
        sh.save_cache()
    else:
        w = db.get_processed_ids(conn, supermarket_id, supermarket.lastmod)
        sh.update_set("w", w, reset=True)

    # Process IDs
    ids_to_process = list(sh.s - sh.w - sh.c)
    if not ids_to_process:
        return sh

    with tqdm(
        total=len(ids_to_process), desc=product_schema_class.get_name(), ncols=0
    ) as pbar:
        for i in range(0, len(ids_to_process), BATCH_SIZE):
            batch_ids = ids_to_process[i : i + BATCH_SIZE]
            results = process_batch(product_schema_class, batch_ids, supermarket_params)
            valid_requests = [r for r in results if r.response.status_code == 200]
            written_ids_batch = []
            for product in valid_requests:
                try:
                    db.write_dump(
                        conn,
                        {
                            "id": product.id,
                            "ymd": supermarket.lastmod,
                            "content": product.to_dump(),
                            "supermarket_id": supermarket_id,
                        },
                    )
                except sqlite3.IntegrityError:
                    continue
                written_ids_batch.append(product.id)
                pbar.update()

            sh.update_set(
                {
                    "w": written_ids_batch,
                    "c": batch_ids,
                }
            )
    return sh


if __name__ == "__main__":
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mercadona.db")
    conn = sqlite3.connect(db_path)
    sh = SetHandler(conn=conn)
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

    for target in scrape_targets:
        sh.reset_set("s", "w", "c")

        while True:
            try:
                sh = scrape_supermarket(
                    conn, target["request_handler"], sh, target["params"]
                )
                remaining_ids = sh.s - sh.w - sh.c
                if not remaining_ids:
                    sh.save_cache()
                    print(f"All {target['request_handler'].get_name()} IDs processed.")
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
                sh.save_cache()
                print("Interrupted by user. Closing DB.")
                conn.close()
                sys.exit(130)
