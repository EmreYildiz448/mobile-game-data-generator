# src/database/bootstrap.py
from __future__ import annotations

from pathlib import Path
import duckdb

from src.settings import runtime as R


def open_or_create_db(db_path: Path) -> duckdb.DuckDBPyConnection:
    """
    Create or open a file-backed DuckDB database and return a connection.
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    # Optional tuning:
    # con.execute("PRAGMA threads=4")
    # con.execute("PRAGMA memory_limit='2GB'")
    return con


def ensure_schema(con: duckdb.DuckDBPyConnection, schema: str) -> None:
    """
    Ensure a schema exists (idempotent).
    """
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def load_csv_dir(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    data_dir: Path,
    mode: str = "replace",
) -> list[str]:
    """
    Load every *.csv in data_dir into <schema>.<filename_stem>.
    mode: 'replace' (CREATE OR REPLACE TABLE) or 'append' (INSERT INTO).
    Returns list of created/affected table names.
    """
    data_dir = Path(data_dir)
    created: list[str] = []

    csvs = sorted(data_dir.glob("*.csv"))
    if not csvs:
        print(f"[bronze] No CSVs found under: {data_dir}")

    for csv_path in csvs:
        table = csv_path.stem
        if mode == "replace":
            ddl = f"""
                CREATE OR REPLACE TABLE {schema}.{table} AS
                SELECT * FROM read_csv_auto($1, header=True)
            """
            con.execute(ddl, [str(csv_path)])
        elif mode == "append":
            # Ensure table exists with correct columns, then append rows
            con.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} AS
                SELECT * FROM read_csv_auto($1, header=True) LIMIT 0
                """,
                [str(csv_path)],
            )
            con.execute(
                f"""
                INSERT INTO {schema}.{table}
                SELECT * FROM read_csv_auto($1, header=True)
                """,
                [str(csv_path)],
            )
        else:
            raise ValueError("mode must be 'replace' or 'append'")

        print(f"[bronze] Loaded: {csv_path.name} -> {schema}.{table}")
        created.append(table)

    return created


def _table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    q = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        LIMIT 1
    """
    return bool(con.execute(q, [schema, table]).fetchone())


def run_bronze_health(con: duckdb.DuckDBPyConnection, schema: str = "bronze") -> None:
    """
    Apply small, idempotent 'schema health' fixes on bronze tables so all
    downstream layers can rely on them.

    Mirrors the original transform_layers.py fixups:
      - sessions.duration_seconds
      - events.event_id (global row_number ordered by event_date, rowid)
      - hosted_ad_interactions.interaction_id (global row_number ordered by interaction_time, rowid)
    """
    if _table_exists(con, schema, "sessions"):
        con.execute(f"""
            ALTER TABLE {schema}.sessions
            ADD COLUMN IF NOT EXISTS duration_seconds BIGINT;
        """)
        con.execute(f"""
            UPDATE {schema}.sessions
            SET duration_seconds = date_diff('second', session_start, session_end)
            WHERE duration_seconds IS NULL;
        """)

    if _table_exists(con, schema, "events"):
        con.execute(f"""
            ALTER TABLE {schema}.events
            ADD COLUMN IF NOT EXISTS event_id BIGINT;
        """)
        con.execute(f"""
            UPDATE {schema}.events AS e
            SET event_id = t.event_id
            FROM (
                SELECT rowid,
                       ROW_NUMBER() OVER (ORDER BY event_date, rowid) AS event_id
                FROM {schema}.events
            ) AS t
            WHERE e.rowid = t.rowid
              AND e.event_id IS NULL;
        """)

    if _table_exists(con, schema, "hosted_ad_interactions"):
        con.execute(f"""
            ALTER TABLE {schema}.hosted_ad_interactions
            ADD COLUMN IF NOT EXISTS interaction_id BIGINT;
        """)
        con.execute(f"""
            UPDATE {schema}.hosted_ad_interactions AS a
            SET interaction_id = t.interaction_id
            FROM (
                SELECT rowid,
                       ROW_NUMBER() OVER (ORDER BY interaction_time, rowid) AS interaction_id
                FROM {schema}.hosted_ad_interactions
            ) AS t
            WHERE a.rowid = t.rowid
              AND a.interaction_id IS NULL;
        """)


def bootstrap_bronze(
    db_path: Path,
    data_dir: Path | None = None,
    schema: str = "bronze",
    mode: str = "replace",
) -> None:
    """
    Orchestrate bronze initialization end-to-end:
      1) open/create DB
      2) ensure schema
      3) load CSVs from data_dir (defaults to R.DATA_INT_DIR)
      4) run bronze health checks
      5) print a quick sample
    """
    if data_dir is None:
        data_dir = R.DATA_INT_DIR

    print(f"[bronze] db_path: {db_path}")
    print(f"[bronze] data_dir: {data_dir}")

    con = open_or_create_db(Path(db_path))
    try:
        ensure_schema(con, schema)

        # Load main data (accounts, events, etc.)
        created = load_csv_dir(con, schema, Path(data_dir), mode=mode)

        # Load prepackaged reference data (e.g., exchange_rate.csv)
        ext_dir = R.DATA_EXT_DIR
        if ext_dir.exists():
            print(f"[bronze] Loading reference data from: {ext_dir}")
            load_csv_dir(con, schema, ext_dir, mode="replace")
        else:
            print(f"[bronze] Warning: external data directory not found: {ext_dir}")

        # Apply bronze-level fixups (session durations, event IDs, etc.)
        run_bronze_health(con, schema=schema)

        if created:
            first = created[0]
            sample = con.sql(f"SELECT * FROM {schema}.{first} LIMIT 5").df()
            print(f"[bronze] Sample from {schema}.{first}:\n{sample}")
        print(f"[bronze] Done. DuckDB at: {db_path}")
    finally:
        con.close()