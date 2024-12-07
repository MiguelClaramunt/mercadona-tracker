import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from tqdm import tqdm

from mercatracker import db, logging
from mercatracker.api import ProductSchema
from mercatracker.config import Config
from mercatracker.scraper import Soup, TemporaryRestrictionException
from mercatracker.sethandler import SetHandler

BATCH_SIZE = 40
config = Config().load()


def fetch_product_data(id, params):
    product = ProductSchema(id=id, params=params)
    product.request()
    return product


def process_batch(ids_batch):
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(fetch_product_data, id, config.params_api)
            for id in ids_batch
        ]
        results = [future.result() for future in as_completed(futures)]
    return results


def main(conn: sqlite3.Connection, sh: SetHandler):
    soup = Soup(url=config.url_sitemap).request()
    lastmod = soup.scrape_lastmod()

    if lastmod != db.get_lastmod(conn):
        logging.soup(lastmod)
        scraped_ids = soup.scrape_ids()

        sh.reset_set("checked_ids", "written_ids")
        sh.update_set("scraped_ids", scraped_ids, reset=True)
        sh.save_cache()
    else:
        scraped_ids = sh.scraped_ids
        written_ids = db.get_processed_ids(conn, lastmod)
        sh.update_set("written_ids", written_ids, reset=True)

    ids_to_process = list(sh.scraped_ids - sh.written_ids - sh.checked_ids)
    total_ids = len(ids_to_process)

    if not total_ids:
        return sh

    with tqdm(total=total_ids) as pbar:
        for i in range(0, total_ids, BATCH_SIZE):
            batch_ids = ids_to_process[i : i + BATCH_SIZE]
            results = process_batch(batch_ids)

            # write only valid requests to db
            valid_requests = [
                result for result in results if result.response.status_code == 200
            ]
            # print([product.id for product in valid_requests])
            written_ids_batch = []
            for product in valid_requests:
                db.write_dump(
                    conn,
                    {
                        "id": product.id,
                        "ymd": lastmod,
                        "content": product.to_dump(),
                    },
                )
                written_ids_batch.append(product.id)
                pbar.update(1)

            sh.update_set(
                {
                    "written_ids": written_ids_batch,
                    "checked_ids": batch_ids,
                }
            )
            sh.save_cache()

    return sh


if __name__ == "__main__":
    conn = sqlite3.connect("/home/miguel/git/mercadona-tracker/src/mercadona.db")

    sh = SetHandler(cache_file="set_cache.pkl")
    sh.init_set("scraped_ids", "written_ids", "checked_ids")

    while True:
        try:
            sh = main(conn=conn, sh=sh)
            remaining_ids = sh.scraped_ids - sh.written_ids - sh.checked_ids

            if not remaining_ids:
                print("All IDs have been processed or marked invalid.")
                break

        except requests.exceptions.SSLError:
            secs = 60
            print(f"Encountered SSLError. Retrying in {secs // 60} minutes...")
            time.sleep(secs)
        except TemporaryRestrictionException as e:
            secs = 60 * 3
            print(f"An error occurred: /{e}. Retrying in {secs // 60} minutes...")
            time.sleep(secs)
        except KeyboardInterrupt:
            print("Interrupted by user. Closing connection to database.")
            conn.close()
            sys.exit(130)

    conn.close()
