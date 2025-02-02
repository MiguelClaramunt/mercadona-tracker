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


def fetch_product_data(
    request_handler: MercadonaProductSchema | ConsumProductSchema,
    id: str,
    params: dict[str, str],
):
    product = request_handler(id=id, params=params)
    product.request()
    return product


def process_batch(request_handler, ids_batch: list[str], params: dict[str, str]):
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(fetch_product_data, request_handler, id, params)
            for id in ids_batch
        ]
        results = [future.result() for future in as_completed(futures)]
    return results


def scrape_mercadona(
    conn: sqlite3.Connection,
    request_handler: MercadonaProductSchema,
    sh: SetHandler,
    supermarket_params: dict[str, str],
):
    soup = request_handler(request_sitemap=True)
    # Ensure we have a supermarket_id in DB
    supermarket_id = db.query_database(conn, supermarket_params)
    if not supermarket_id:
        db.write_supermarket_params(conn, supermarket_params)
        supermarket_id = db.query_database(conn, supermarket_params)

    # Update our sethandler's IDs
    sh.supermarket_id = supermarket_id
    sh.ymd = soup.lastmod
    sh.load_cache()

    lastmod_in_db = db.get_lastmod(conn)
    if soup.lastmod != lastmod_in_db:
        logging.soup(soup.lastmod)
        scraped_ids = soup.scrape_ids()
        sh.reset_set("checked_ids", "written_ids")
        sh.update_set("scraped_ids", scraped_ids, reset=True)
        sh.save_cache()
    else:
        # If we already have that lastmod, we just load from DB
        scraped_ids = sh.scraped_ids
        written_ids = db.get_processed_ids(conn, soup.lastmod)
        sh.update_set("written_ids", written_ids, reset=True)

    ids_to_process = list(sh.scraped_ids - sh.written_ids - sh.checked_ids)
    total_ids = len(ids_to_process)
    if not total_ids:
        return sh

    with tqdm(total=total_ids) as pbar:
        for i in range(0, total_ids, BATCH_SIZE):
            batch_ids = ids_to_process[i : i + BATCH_SIZE]
            results = process_batch(
                request_handler=request_handler,
                ids_batch=batch_ids,
                params=supermarket_params,
            )
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
    # We create a handler with default 0/0 for ymd/supermarket_id; they get updated later
    sh = SetHandler(conn=conn)
    sh.init_set("scraped_ids", "written_ids", "checked_ids")

    while True:
        try:
            sh = scrape_mercadona(
                conn=conn,
                request_handler=MercadonaProductSchema,
                sh=sh,
                supermarket_params={
                    "supermarket": "mercadona",
                    "params": '{"lang":"es","wh":"vlc1"}',
                },
            )
            remaining_ids = sh.scraped_ids - sh.written_ids - sh.checked_ids
            if not remaining_ids:
                sh.save_cache()
                print(
                    f"All IDs have been processed, {len(sh.written_ids)} ids written."
                )
                break
        except requests.exceptions.SSLError:
            secs = 60 * 2
            print(f"Encountered SSLError. Retrying in {secs // 60} minutes...")
            time.sleep(secs)
        except TemporaryRestrictionException as e:
            secs = 60 * 5
            print(f"{e} Retrying in {secs // 60} minutes...")
            time.sleep(secs)
        except KeyboardInterrupt:
            sh.save_cache()
            print("Interrupted by user. Closing connection to database.")
            conn.close()
            sys.exit(130)

    sh.save_cache()
    conn.close()
