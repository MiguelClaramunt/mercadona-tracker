import glob
import sqlite3
from pathlib import Path

import pandas as pd

from mercatracker import api, globals, io, processing, scraping
from mercatracker.hasher import Hasher

config = globals.load_dotenv(
    dotenv_shared=".env.shared",
)


def scrape_items(ids: list[list[str]], filename: str) -> tuple[str, str]:
    checked, all_ids = ids
    for batch in processing.batch_split(all_ids, batch_size=500):
        if not batch <= checked:
            for id in batch:
                if id not in checked and id:
                    response = api.call(id)
                    if response.status_code == 200:
                        df = processing.process(
                            response, columns=config["_ITEMS_COLUMNS"]
                        )
                        io.df2csv(
                            df,
                            filename,
                            columns=config["ITEMS_COLUMNS"],
                        )
                        checked.add(id)
                    if response.status_code == 410:
                        all_ids.pop(all_ids.index(id))
    # return (checked, all_ids)


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
    cols_final: list[list[str]] = config["_CATEGORIES_COLUMNS"],
    cols_to_rename: list[str] = config["_CATEGORIES_JER"],
) -> pd.DataFrame:
    level, name, ymd = cols_final
    return pd.concat(
        (
            df[col].rename(
                columns={
                    df[col].columns[0]: level,
                    df[col].columns[1]: name,
                    df[col].columns[2]: ymd,
                }
            )
            for col in cols_to_rename
        )
    )


def generate_categories(paths: str) -> pd.DataFrame:
    return flatten_levels(
        read_multiple(paths).dropna(subset=config["CATEGORIES"]),
        config["_CATEGORIES_COLUMNS"],
        config["_CATEGORIES_JER"],
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
        ).hash()
    else:
        df_updated = Hasher(
            read_multiple(recent_files)[columns_to_select],
            columns_to_hash=columns_to_hash,
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

    if not recent_files:
        return pd.DataFrame(columns=columns_to_select)

    if table == "categories":
        df_updated = Hasher(
            generate_categories(recent_files)[columns_to_select],
            columns_to_hash=columns_to_hash,
        ).hash()
    else:
        df_updated = Hasher(
            read_multiple(recent_files)[columns_to_select],
            columns_to_hash=columns_to_hash,
        ).hash()

    query = f"""SELECT * FROM {table};"""

    df_current = pd.read_sql(query, conn)

    return (
        get_new_rows(df_current, df_updated)
        .drop_duplicates(subset=subset_duplicates, keep="last")
        .reset_index(drop=True)
    )


def get_new_rows(df_current: pd.DataFrame, df_updated: pd.DataFrame) -> pd.DataFrame:
    return df_updated[~df_updated.hash.isin(df_current.hash)]
