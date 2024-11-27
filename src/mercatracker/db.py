import json
import sqlite3
from typing import Any


def dict_to_query_components(input_dict: dict[str, Any]) -> tuple[str, str, Any]:
    columns = ", ".join(input_dict.keys())
    placeholders = ",".join("?" * len(input_dict))
    values = tuple(input_dict.values())
    return (columns, placeholders, values)


def dict_to_query(input_dict: dict[str, Any]) -> tuple[str, str]:
    columns, placeholders, values = dict_to_query_components(input_dict)
    query = f"INSERT INTO dumps({columns}) VALUES({placeholders})"
    return (query, values)


def create_dumps_table(conn: sqlite3.Connection):
    conn.execute("""
                 CREATE TABLE IF NOT EXISTS "dumps" (
                    "id" TEXT,
                    "ymd" INTEGER,
                    "content" TEXT,
                    "hash" INTEGER, 
                    `ymd_id` INTEGER REFERENCES `dates_scraped`(`ymd_id`),
                    PRIMARY KEY (id, ymd_id))
                 ;""")
    
    # Check if the trigger already exists
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='trigger' AND name='remove_consecutive_duplicates';
    """)
    trigger_exists = cursor.fetchone()

    if not trigger_exists:
        # Create a trigger to remove consecutive duplicates
        # Duplicates are considered consecutive if they have same {id, content} than the previous ymd_id
        conn.execute("""
                    CREATE TRIGGER remove_consecutive_duplicates
                    AFTER INSERT ON dumps
                    BEGIN
                        DELETE FROM dumps
                        WHERE rowid = NEW.rowid AND EXISTS (
                            SELECT 1 FROM dumps
                            WHERE id = NEW.id
                            AND ymd_id = (
                                SELECT MAX(ymd_id) FROM dumps
                                WHERE id = NEW.id AND ymd_id < NEW.ymd_id
                            )
                            AND content = NEW.content
                        );
                    END;
                    """)
    conn.commit()


def create_ids_scraped_table(conn: sqlite3.Connection):
    conn.execute("""
                 CREATE TABLE IF NOT EXISTS ids_scraped (
                    ymd TEXT,
                    ids TEXT,
                    PRIMARY KEY (ymd)
                 );""")
    conn.commit()


def create_dates_scraped_table(conn: sqlite3.Connection):
    conn.execute("""
                 CREATE TABLE IF NOT EXISTS dates_scraped (
                    ymd INTEGER,
                    ymd_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    UNIQUE (ymd, ymd_id)
                 );""")
    conn.commit()


def write_dump(conn: sqlite3.Connection, parameters: dict):
    create_dumps_table(conn)
    cur = conn.cursor()
    query, values = dict_to_query(parameters)
    cur.execute(query, values)
    conn.commit()


def write_lastmod(conn: sqlite3.Connection, lastmod: int):
    create_dates_scraped_table(conn)
    cur = conn.cursor()
    cur.execute("""INSERT INTO dates_scraped(ymd) VALUES(?)""", (lastmod,))
    conn.commit()


def write_scraped_ids(conn: sqlite3.Connection, parameters: tuple):
    create_ids_scraped_table(conn)
    cur = conn.cursor()
    cur.execute("""INSERT INTO ids_scraped(ymd, ids) VALUES(?,?)""", parameters)
    conn.commit()


def get_lastmod(conn: sqlite3.Connection) -> tuple[int, int]:
    create_dates_scraped_table(conn)
    return conn.execute("""SELECT max(ymd), ymd_id FROM dates_scraped""").fetchone()


def get_processed_ids(conn: sqlite3.Connection, ymd_id: str | int) -> list[str]:
    create_dumps_table(conn)
    conn.row_factory = lambda cursor, row: row[0]
    return conn.execute(
        """SELECT id FROM dumps WHERE ymd_id=?""", (int(ymd_id),)
    ).fetchall()


def get_scraped_ids(conn, ymd):
    create_dumps_table(conn)
    result = conn.execute("SELECT ids FROM ids_scraped WHERE ymd = ?", (ymd,)).fetchone()
    if result:
        return json.loads(result)  # Extract the string from the tuple
    return []


def select_from_table(conn: sqlite3.Connection, column_name: str, table_name: str):
    conn.row_factory = lambda cursor, row: row[0]
    return conn.execute("""SELECT {} FROM {}""".format(column_name, table_name))
