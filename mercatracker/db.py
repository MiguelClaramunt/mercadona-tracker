import json
from typing import Any


def dict_to_query_components(input_dict: dict[str, Any]) -> tuple[str, str, Any]:
    columns = ", ".join(input_dict.keys())
    placeholders = ",".join("?" * len(input_dict))
    values = tuple(input_dict.values())
    return (columns, placeholders, values)


def dict_to_query(input_dict: dict[str, Any]) -> tuple[str, Any]:
    columns, placeholders, values = dict_to_query_components(input_dict)
    query = f"INSERT INTO dumps({columns}) VALUES({placeholders})"
    return (query, values)


def init_dumps_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS "dumps" (
            "id" TEXT,
            "ymd" INTEGER,
            "content" TEXT,
            "supermarket_id" INTEGER,
            PRIMARY KEY (id, ymd, supermarket_id),
            FOREIGN KEY ("supermarket_id") REFERENCES "supermarkets"("id")
        );
    """)
    # ensure index for querying by supermarket and ymd
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_dumps_market_ymd
        ON "dumps"(supermarket_id, ymd);
    """)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='trigger' AND name='remove_consecutive_duplicates';
    """)
    trigger_exists = cursor.fetchone()
    if not trigger_exists:
        conn.execute("""
            CREATE TRIGGER remove_consecutive_duplicates
            AFTER INSERT ON dumps
            BEGIN
                DELETE FROM dumps
                WHERE rowid = NEW.rowid
                  AND EXISTS (
                    SELECT 1 FROM dumps
                    WHERE id = NEW.id
                      AND ymd = (
                        SELECT MAX(ymd)
                        FROM dumps
                        WHERE id = NEW.id AND ymd < NEW.ymd
                      )
                      AND content = NEW.content
                  );
            END;
        """)
    conn.commit()


def write_dump(conn, parameters: dict):
    init_dumps_table(conn)
    cur = conn.cursor()
    query, values = dict_to_query(parameters)
    cur.execute(query, values)
    conn.commit()


def init_supermarkets_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS "supermarkets" (
            "id" INTEGER PRIMARY KEY AUTOINCREMENT, 
            "supermarket" TEXT,
            "params" TEXT, 
            UNIQUE ("id", "supermarket", "params")
        );
    """)
    conn.commit()


def write_supermarket_params(conn, parameters: dict):
    init_supermarkets_table(conn)
    cur = conn.cursor()
    query = """
        INSERT OR IGNORE INTO supermarkets (supermarket, params) 
        VALUES (:supermarket, :params)
    """
    cur.execute(query, parameters)
    conn.commit()


def get_lastmod(conn, supermarket_id: int) -> int:
    init_dumps_table(conn)
    conn.row_factory = lambda cursor, row: row[0]
    return conn.execute(
        """SELECT max(ymd) FROM dumps WHERE supermarket_id = ?""", (supermarket_id,)
    ).fetchone()


def get_processed_ids(conn, supermarket_id: int, ymd: int) -> list[str]:
    init_dumps_table(conn)
    conn.row_factory = lambda cursor, row: row[0]
    return conn.execute(
        "SELECT id FROM dumps WHERE ymd = ? AND supermarket_id = ?",
        (ymd, supermarket_id),
    ).fetchall()


def select_from_table(conn, column_name: str, table_name: str):
    conn.row_factory = lambda cursor, row: row[0]
    return conn.execute(f"SELECT {column_name} FROM {table_name}")


def fetch_supermarket_id(conn, criteria) -> int:
    init_supermarkets_table(conn)
    query = 'SELECT "id" FROM "supermarkets" WHERE '
    conditions = [f'"{key}" = ?' for key in criteria]
    query += " AND ".join(conditions)
    cursor = conn.execute(query, tuple(criteria[k] for k in criteria))
    if (result := cursor.fetchone()) is None:
        return False
    if isinstance(result, tuple):
        return result[0]
    return result


def init_set_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS set_cache (
            ymd INTEGER,
            supermarket_id INTEGER,
            set_name TEXT,
            data TEXT,
            PRIMARY KEY (ymd, supermarket_id, set_name),
            FOREIGN KEY (supermarket_id) REFERENCES supermarkets(id)
        );
    """)
    conn.commit()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='trigger' AND name='delete_older_set_cache_entries'
    """)
    trigger_exists = cursor.fetchone()
    if not trigger_exists:
        conn.execute("""
            CREATE TRIGGER delete_older_set_cache_entries
            AFTER INSERT ON set_cache
            BEGIN
                DELETE FROM set_cache
                WHERE supermarket_id = NEW.supermarket_id
                  AND set_name = NEW.set_name
                  AND ymd < NEW.ymd;
            END;
        """)
    conn.commit()


def write_set_cache(
    conn,
    ymd: int,
    supermarket_id: int,
    set_name: str,
    set_data: set,
):
    init_set_table(conn)
    cur = conn.cursor()
    data_json = json.dumps(list(set_data))
    query = """
        INSERT OR REPLACE INTO set_cache (ymd, supermarket_id, set_name, data)
        VALUES (?, ?, ?, ?)
    """
    cur.execute(query, (ymd, supermarket_id, set_name, data_json))
    conn.commit()


def load_set_cache(conn, ymd: int, supermarket_id: int) -> dict[str, set]:
    init_set_table(conn)
    conn.row_factory = None
    cur = conn.cursor()
    query = """
        SELECT set_name, data FROM set_cache
        WHERE ymd = ? AND supermarket_id = ?
    """
    cur.execute(query, (ymd, supermarket_id))
    rows = cur.fetchall()
    result = {}
    for set_name, data_str in rows:
        result[set_name] = set(json.loads(data_str))
    return result
