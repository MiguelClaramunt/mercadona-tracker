# import libsql_experimental as libsql
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from mercatracker import db, logging
from mercatracker.api import ProductSchema
from mercatracker.config import Config
from mercatracker.scraper import Soup
from tqdm import tqdm

BATCH_SIZE = 40

config = Config().load()


def fetch_product_data(id, params):
    product = ProductSchema(id=id, params=params)
    product.request()
    return product  # return ProductSchema instance


def process_batch(ids_batch):
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(fetch_product_data, id, config.params_api)
            for id in ids_batch
        ]
        results = [future.result() for future in as_completed(futures)]
    return results


def main(conn: sqlite3.Connection):
    soup = Soup(url=config.url_sitemap).request()
    lastmod = soup.scrape_lastmod()

    lastmod_db, ymd_id = db.get_lastmod(conn)
    if lastmod != str(lastmod_db):
        db.write_lastmod(conn, lastmod)
        logging.soup(lastmod)

    all_ids = soup.scrape_ids()
    processed_ids = db.get_processed_ids(conn, ymd_id)

    # set intersection (all ids minus the processed ones present in db)
    ids = list(set(all_ids) - set(processed_ids))
    invalid_requests = []

    with tqdm(total=len(ids)) as pbar:
        for i in range(0, len(ids), BATCH_SIZE):
            batch_ids = ids[i : i + BATCH_SIZE]
            results = process_batch(batch_ids)

            invalid_requests += [
                result.id for result in results if result.response.status_code != 200
            ]

            # write only valid requests to db
            valid_requests = [
                result for result in results if result.response.status_code == 200
            ]

            for product in valid_requests:
                db.write_dump(
                    conn,
                    {
                        "id": product.id,
                        "ymd_id": ymd_id,
                        "content": product.to_dump(),
                    },
                )
                pbar.update(1)

    return set(ids), set(invalid_requests)


if __name__ == "__main__":
    conn = sqlite3.connect("/home/miguel/git/mercadona-tracker/src/mercadona.db")

    ids, invalid_requests = {}, {" "}
    while ids != invalid_requests:
        try:
            ids, invalid_requests = main(conn=conn)
            if ids == invalid_requests:
                conn.close()
                break
        except requests.exceptions.SSLError:
            # logging.error(f"SSLError occurred: {e}")
            time.sleep(10)
        except KeyboardInterrupt:
            conn.close()
            try:
                sys.exit(130)
            except SystemExit:
                os._exit(130)

    conn.close()
