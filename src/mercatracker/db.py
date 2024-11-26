import json
import sqlite3


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
    create_dumps_table(conn)
    conn.row_factory = lambda cursor, row: row[0]
    return conn.execute("""SELECT id FROM dumps WHERE ymd=?""", (lastmod,)).fetchall()


def get_scraped_ids(conn, ymd):
    create_dumps_table(conn)
    result = conn.execute("SELECT ids FROM ids_scraped WHERE ymd = ?", (ymd,)).fetchone()
    if result:
        return json.loads(result)  # Extract the string from the tuple
    return []


def select_from_table(conn: sqlite3.Connection, column_name: str, table_name: str):
    conn.row_factory = lambda cursor, row: row[0]
    return conn.execute("""SELECT {} FROM {}""".format(column_name, table_name))
