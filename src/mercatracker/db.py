import json
import sqlite3


def create_dumps_table(conn: sqlite3.Connection):
    conn.execute("""
                 CREATE TABLE IF NOT EXISTS dumps (
                    id TEXT,
                    ymd INT,
                    content TEXT,
                    PRIMARY KEY (id, ymd)
                 );""")
    conn.commit()


def create_ids_scraped_table(conn: sqlite3.Connection):
    conn.execute("""
                 CREATE TABLE IF NOT EXISTS ids_scraped (
                    ymd TEXT,
                    ids TEXT,
                    PRIMARY KEY (ymd)
                 );""")
    conn.commit()


def write_dump(conn: sqlite3.Connection, parameters: tuple):
    create_dumps_table(conn)
    cur = conn.cursor()
    cur.execute("""INSERT INTO dumps(id, content, ymd) VALUES(?,?,?)""", parameters)
    conn.commit()


def write_scraped_ids(conn: sqlite3.Connection, parameters: tuple):
    create_ids_scraped_table(conn)
    cur = conn.cursor()
    cur.execute("""INSERT INTO ids_scraped(ymd, ids) VALUES(?,?)""", parameters)
    conn.commit()


def get_lastmod(conn: sqlite3.Connection) -> str:
    create_ids_scraped_table(conn)
    conn.row_factory = lambda cursor, row: row[0]
    return conn.execute("""SELECT max(ymd) FROM ids_scraped""").fetchone()


def get_processed_ids(conn: sqlite3.Connection, lastmod: str):
    conn.row_factory = lambda cursor, row: row[0]
    return conn.execute("""SELECT id FROM dumps WHERE ymd=?""", (lastmod,)).fetchall()


def get_scraped_ids(conn: sqlite3.Connection, lastmod: str):
    ids = conn.execute("""SELECT ids FROM ids_scraped WHERE ymd=?""", (lastmod,)).fetchone()
    return json.loads(ids)


def select_from_table(conn: sqlite3.Connection, column_name: str, table_name: str):
    conn.row_factory = lambda cursor, row: row[0]
    return conn.execute("""SELECT {} FROM {}""".format(column_name, table_name))
