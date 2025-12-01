# src/database/transform_layers.py
from __future__ import annotations

from pathlib import Path
import re
import sys
import duckdb
import sqlglot as sg

from src.settings import runtime as R

CREATE_TARGET_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+([A-Za-z_][A-Za-z0-9_\.\"$]*)",
    re.IGNORECASE,
)

def _strip_tails(sql_text: str) -> str:
    """
    Minimal cleanup: strip block comments and search_path lines.
    (MV→TABLE, metadata renames, etc., no longer needed.)
    """
    # /* ... */ comments
    s = re.sub(r"/\*[\s\S]*?\*/", "", sql_text, flags=re.MULTILINE)
    # SET search_path ...
    s = re.sub(r"^\s*SET\s+search_path[\s\S]*?;\s*$", "", s,
               flags=re.IGNORECASE | re.MULTILINE)
    return s

def _transpile_duckdb(sql_text: str) -> list[str]:
    """
    Split into executable statements. Using sqlglot keeps us safe if
    there are multiple statements or minor dialect quirks.
    """
    return [s for s in sg.transpile(sql_text, read="duckdb", write="duckdb") if s.strip()]

def _created_targets(sql_text: str) -> list[str]:
    return [m.group(1) for m in CREATE_TARGET_RE.finditer(sql_text)]

def transform_layer(db_path: Path, sql_dir: Path, target_schema: str) -> None:
    """
    Execute all *.sql files under sql_dir into target_schema within db_path.
    Each file runs in its own transaction; fails are rolled back per-file.
    """
    sql_dir = Path(sql_dir)
    if not sql_dir.exists():
        print(f"ERROR: SQL directory not found: {sql_dir}")
        sys.exit(1)

    files = sorted(sql_dir.glob("*.sql"))
    if not files:
        print(f"No SQL files found in {sql_dir}. Nothing to do.")
        return

    print(f"DB: {db_path}")
    print(f"Schema: {target_schema}")
    print(f"SQL dir: {sql_dir} ({len(files)} files)")

    con = duckdb.connect(str(db_path))
    try:
        # Ensure target schema and search path
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
        con.execute(f"SET schema '{target_schema}'")

        for path in files:
            print(f"— applying: {path.name}")
            raw_sql = path.read_text(encoding="utf-8")

            mapping = {
                # Simulation window
                "START_DATE": R.START_DATE.date().isoformat(),
                "END_DATE": R.END_DATE.date().isoformat(),

                # A/B test core dates (from AB_START / AB_END in .env via runtime)
                "AB_START": R.AB_TEST_LAUNCH_DATE.date().isoformat(),
                "AB_END": R.AB_TEST_END_DATE.date().isoformat(),

                # Fully-formed versions used in SQL (control/test)
                "CONTROL_VERSION": R.CONTROL_VERSION,
                "AB_TEST_VERSION": R.AB_TEST_VERSION,
            }
            sql = raw_sql
            for key, value in mapping.items():
                placeholder = "{" + key + "}"
                sql = sql.replace(placeholder, str(value))
                
            # Minimal prepass + transpile (DuckDB→DuckDB split)
            s = _strip_tails(sql)
            try:
                stmts = _transpile_duckdb(s)
            except Exception as e:
                print(f"TRANSPILER FAIL in {path.name}: {e}")
                raise

            if not stmts:
                print(f"(skip) no executable statements in {path.name}")
                continue

            # Run in a per-file transaction
            try:
                con.execute("BEGIN")
                for i, stmt in enumerate(stmts, 1):
                    con.execute(stmt)
                con.execute("COMMIT")
            except Exception as e:
                con.execute("ROLLBACK")
                print(f"ROLLBACK — {path.name} failed at statement #{i}: {e}")
                raise

            # Row count report for created tables
            targets = _created_targets(";\n".join(stmts))
            for t in targets:
                try:
                    cnt = con.sql(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                    print(f"OK -> {t} (rows: {cnt})")
                except Exception as e:
                    print(f"Note: couldn't count rows for {t}: {e}")

        print("All files applied.")
    finally:
        con.close()
