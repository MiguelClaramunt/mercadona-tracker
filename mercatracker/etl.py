import glob
from os import path
from pathlib import Path

import pandas as pd
import sqlite3

from mercatracker import api, globals, processing, reporting, scraping
import utils
from utils.hasher import Hasher


config = globals.load_dotenv(
    dotenv_shared=".env.shared",
)


def scrape_items(ids: list[list[str]], filename: str) -> None:
    checked, all_ids = ids
    for batch in utils.batch_split(all_ids, batch_size=500):
        if not batch <= checked:
            for id in batch:
                if id not in checked and id:
                    response = api.call(id)
                    if response.status_code == 200:
                        df = processing.process(
                            response, columns=config["ITEMS_COLUMNS"]
                        )
                        processing.df2csv(
                            df,
                            filename,
                            columns=config["ITEMS_CSV_COLUMNS"],
                        )
                        checked.add(id)
                    if response.status_code == 410:
                        ids.pop(ids.index(id))
                else:
                    continue


def get_lastmod_csv(path: str) -> int:
    return pd.read_csv(path, usecols=["ymd"]).max().values[0]


def get_lastmod_sql(table: str, col: str, conn: sqlite3.Connection) -> int:
    query = f"""SELECT max({col}) FROM {table}"""
    return pd.read_sql(query, conn).values[0][0]


def get_recent_files(pathname: str, lastmod: int) -> list[str]:
    files = glob.glob(pathname)

    return [file for file in files if scraping.search(file, r"(\d{8})") > str(lastmod)]


def read_multiple(paths: list[str]) -> pd.DataFrame:
    return pd.concat((pd.read_csv(file) for file in paths), ignore_index=True)


def flatten_levels(
    df: pd.DataFrame,
    cols_original: list[list[str]] = config["_CATEGORIES_JER_COLUMNS"],
    cols_to_rename: list[str] = config["CATEGORIES_JER_COLUMNS"],
) -> pd.DataFrame:
    level, name, ymd = cols_to_rename
    return pd.concat(
        (
            df[col].rename(
                columns={
                    df[col].columns[0]: level,
                    df[col].columns[1]: name,
                    df[col].columns[2]: ymd,
                }
            )
            for col in cols_original
        )
    )


def generate_categories(paths: str) -> pd.DataFrame:
    return flatten_levels(
        read_multiple(paths).dropna(subset=config["CATEGORIES"]),
        config["_CATEGORIES_JER_COLUMNS"],
        config["CATEGORIES_JER_COLUMNS"],
    )


def update_dataframe(
    path: str,
    columns_to_select: list[str],
    columns_to_hash: list[str],
    subset_duplicates: list[str],
) -> pd.DataFrame:
    """Updates the previously processed file concatenating new information

    Args:
        path (str): Full path with extension
        columns_to_select (list[str]): _description_
        columns_to_hash (list[str]): _description_
    """

    lastmod = get_lastmod_csv(path)
    recent_files = get_recent_files("./items/*.csv", lastmod=lastmod)

    if Path(path).stem == "categories":
        df_updated = Hasher(
            generate_categories(recent_files)[columns_to_select],
            columns_to_hash=columns_to_hash,
            target_column_name="hash",
        ).hash()
    else:
        df_updated = Hasher(
            read_multiple(recent_files)[columns_to_select],
            columns_to_hash=columns_to_hash,
            target_column_name="hash",
        ).hash()

    df_current = pd.read_csv(path)

    return (
        pd.concat((df_current, df_updated))
        .drop_duplicates(subset=subset_duplicates, keep="last")
        .reset_index(drop=True)
    )


def update_database(
    table: str,
    conn: sqlite3.Connection,
    columns_to_select: list[str],
    columns_to_hash: list[str],
    subset_duplicates: list[str],
) -> pd.DataFrame:
    """Updates the previously processed file concatenating new information

    Args:
        path (str): Full path with extension
        columns_to_select (list[str]): _description_
        columns_to_hash (list[str]): _description_
    """

    lastmod = get_lastmod_sql(table=table, col="ymd", conn=conn)
    recent_files = get_recent_files("./items/*.csv", lastmod=lastmod)

    if table == "categories":
        read_multiple(recent_files)[columns_to_select],
    else:
        df_updated = Hasher(
            read_multiple(recent_files)[columns_to_select],
            columns_to_hash=columns_to_hash,
            target_column_name="hash",
        ).hash()

    query = f"""SELECT * FROM {table};"""

    df_current = pd.read_sql(query, conn)

    return (
        pd.concat((df_current, df_updated))
        .drop_duplicates(subset=subset_duplicates, keep="last")
        .reset_index(drop=True)
    )
