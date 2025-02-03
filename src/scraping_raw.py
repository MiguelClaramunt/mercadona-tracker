import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from tqdm import tqdm

from mercatracker import db, logging
from mercatracker.api import (
    ConsumProductSchema,
    MercadonaProductSchema,
    TemporaryRestrictionException,
)
from mercatracker.config import Config
from mercatracker.sethandler import SetHandler

BATCH_SIZE = 10
config = Config().load()

def fetch_product_data(request_handler, id_, params):
    product = request_handler(id=id_, params=params)
    product.request()
    return product

def process_batch(request_handler, ids_batch, params):
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(fetch_product_data, request_handler, id_, params)
            for id_ in ids_batch
        ]
        return [f.result() for f in as_completed(futures)]

def scrape_supermarket(conn, request_handler_class, sh, supermarket_params):
    soup = request_handler_class(request_sitemap=True)
    supermarket_id = db.query_database(conn, supermarket_params)
    if not supermarket_id:
        db.write_supermarket_params(conn, supermarket_params)
        supermarket_id = db.query_database(conn, supermarket_params)

    sh.supermarket_id = supermarket_id
    sh.ymd = soup.lastmod
    sh.load_cache()

    if soup.lastmod != db.get_lastmod(conn, supermarket_id):
        logging.soup(soup.lastmod)
        scraped_ids = soup.scrape_ids()
        sh.reset_set("checked_ids", "written_ids")
        sh.update_set("scraped_ids", scraped_ids, reset=True)
        sh.save_cache()
    else:
        written_ids = db.get_processed_ids(conn, supermarket_id, soup.lastmod)
        sh.update_set("written_ids", written_ids, reset=True)

    # Process IDs
    ids_to_process = list(sh.scraped_ids - sh.written_ids - sh.checked_ids)
    if not ids_to_process:
        return sh

    with tqdm(total=len(ids_to_process)) as pbar:
        for i in range(0, len(ids_to_process), BATCH_SIZE):
            batch_ids = ids_to_process[i : i + BATCH_SIZE]
            results = process_batch(request_handler_class, batch_ids, supermarket_params)
            valid_requests = [r for r in results if r.response.status_code == 200]
            written_ids_batch = []
            for product in valid_requests:
                try:
                    db.write_dump(
                        conn,
                        {
                            "id": product.id,
                            "ymd": soup.lastmod,
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
                    "written_ids": written_ids_batch,
                    "checked_ids": batch_ids,
                }
            )
    return sh

if __name__ == "__main__":
    conn = sqlite3.connect("/home/miguel/git/mercadona-tracker/src/mercadona.db")
    sh = SetHandler(conn=conn)
    sh.init_set("scraped_ids", "written_ids", "checked_ids")

    scrape_targets = [
        {
            "request_handler": MercadonaProductSchema,
            "params": {
                "supermarket": "mercadona",
                "params": '{"lang":"es","wh":"vlc1"}',
            },
        },
        {
            "request_handler": ConsumProductSchema,
            "params": {
                "supermarket": "consum",
                "params": '{"shippingZoneId":"0D"}',
            },
        },
    ]

    for target in scrape_targets:
        sh.reset_set("scraped_ids", "written_ids", "checked_ids")

        while True:
            try:
                sh = scrape_supermarket(conn, target["request_handler"], sh, target["params"])
                remaining_ids = sh.scraped_ids - sh.written_ids - sh.checked_ids
                if not remaining_ids:
                    sh.save_cache()
                    print(f"All {target['params']['supermarket']} IDs processed.")
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