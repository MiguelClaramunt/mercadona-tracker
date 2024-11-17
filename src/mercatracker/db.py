import json
import sqlite3


def create_dumps_table(conn: sqlite3.Connection):
    conn.execute("""
                 CREATE TABLE IF NOT EXISTS dumps (
                    id TEXT,
                    ymd INT,
                    content TEXT,
                    hash INT,
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
    cur.execute("""INSERT INTO dumps(id, content, ymd, hash) VALUES(?,?,?,?)""", parameters)
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


def get_scraped_ids(conn, ymd):
    result = conn.execute("SELECT ids FROM ids_scraped WHERE ymd = ?", (ymd,)).fetchone()
    if result:
        return json.loads(result[0])  # Extract the string from the tuple
    return []


def select_from_table(conn: sqlite3.Connection, column_name: str, table_name: str):
    conn.row_factory = lambda cursor, row: row[0]
    return conn.execute("""SELECT {} FROM {}""".format(column_name, table_name))
