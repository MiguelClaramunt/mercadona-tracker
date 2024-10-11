import concurrent
import json

# import libsql_experimental as libsql
import sqlite3

from tqdm import tqdm

from mercatracker import api, db, logging
from mercatracker.config import Config
from mercatracker.scraper import Soup

BATCH_SIZE = 50

config = Config().load()


def fetch_product_data(id, params):
    product = api.ProductSchema(id=id, params=params)
    product.request()  # Perform the request
    return product  # Return the ProductSchema instance


def process_batch(ids_batch):
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(fetch_product_data, id, config.params_api)
            for id in ids_batch
        ]
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]
    return results


def main():
    conn = sqlite3.connect("/home/miguel/git/mercadona-tracker/src/mercadona.db")

    soup = Soup(url=config.url_sitemap).request()
    lastmod = soup.get_lastmod()

    if lastmod != db.get_lastmod(conn):
        all_ids = soup.get_ids()
        db.write_scraped_ids(
            conn,
            (
                lastmod,
                json.dumps(all_ids),
            ),
        )

    else:
        all_ids = db.get_scraped_ids(conn, lastmod)

    ids = set(all_ids) - set(db.get_processed_ids(conn, lastmod))
    logging.soup(lastmod)

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

    ids_ = list(ids)
    invalid_requests = []

    with tqdm(total=len(ids_)) as pbar:
        for i in range(0, len(ids_), BATCH_SIZE):
            batch_ids = ids_[i:i + BATCH_SIZE]
            results = process_batch(batch_ids)

            # update tqdm's total counter
            invalid_requests.append(
                [result for result in results if result.response.status_code != 200]
            )
            pbar.total = len(ids) - len(invalid_requests[0])
            pbar.refresh()

            # write only valid requests to db
            valid_requests = [
                result for result in results if result.response.status_code == 200
            ]

            for product in valid_requests:
                db.write_dump(
                    conn,
                    (
                        product.id,
                        str(product.decode()),
                        lastmod,
                    ),
                )
                pbar.update(1)


if __name__ == "__main__":
    main()
