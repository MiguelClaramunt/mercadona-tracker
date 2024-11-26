import concurrent.futures
import json

# import libsql_experimental as libsql
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from tqdm import tqdm

from mercatracker import db, logging
from mercatracker.api import ProductSchema
from mercatracker.config import Config
from mercatracker.scraper import Soup

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
    lastmod = soup.get_lastmod()

    # scrape and write ids in db if they are not present
    if lastmod != str(db.get_lastmod(conn)):
        all_ids = soup.get_ids()
        db.write_scraped_ids(
            conn,
            (
                lastmod,
                json.dumps(all_ids),
            ),
        )
    else: # retrieve scraped ids from db
        all_ids = db.get_scraped_ids(conn, lastmod)

    processed_ids = db.get_processed_ids(conn, lastmod)
    # set intersection (all ids minus the processed ones present in db)
    ids = list(set(all_ids) - set(processed_ids))
    if not processed_ids:
        logging.soup(lastmod)
    invalid_requests = []

    ### SINGLE THREAD PROCESSING ###

    # with tqdm(total=len(ids)) as pbar:
    #     try:
    #         for id in ids:
    #             try:
    #                 product = api.ProductSchema(id=id, params=config.params_api)
    #                 response = product.request()

    #                 if response.status_code == 200:
    #                     db.write_dump(
    #                         conn,
    #                         (
    #                             id,
    #                             str(product.decode()),
    #                             lastmod,
    #                         ),
    #                     )
    #                     pbar.update(1)

    #                 elif response.status_code == 410:
    #                     continue

    #             except requests.exceptions.SSLError:
    #                 continue

    #     except KeyboardInterrupt:
    #         conn.close()
    #         try:
    #             sys.exit(130)
    #         except SystemExit:
    #             os._exit(130)

    ### MULTI THREAD PROCESSING ###

    with tqdm(total=len(ids)) as pbar:
        for i in range(0, len(ids), BATCH_SIZE):
            batch_ids = ids[i:i + BATCH_SIZE]
            results = process_batch(batch_ids)

            invalid_requests += [
                result.id for result in results if result.response.status_code != 200
            ]

            # write only valid requests to db
            valid_requests = [
                result for result in results if result.response.status_code == 200
            ]

            for product in valid_requests:
                dump = str(product.to_json())  # convert product dict to string
                db.write_dump(
                    conn,
                    (product.id, dump, lastmod, hash(dump)),
                )
                pbar.update(1)
    
    conn.close()
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
