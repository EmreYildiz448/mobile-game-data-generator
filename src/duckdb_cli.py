# src/tools/duckdb_cli.py

import sys
import re
from pathlib import Path
import matplotlib.pyplot as plt

import duckdb
import pandas as pd

from src.settings import runtime as R  # uses R.DUCKDB_PATH


# ---------- Identifier escaping ----------

def escape_identifier(name: str) -> str:
    """
    Safely quote an SQL identifier (optionally schema-qualified) for DuckDB.

    - Supports names like 'events' or 'bronze.events'.
    - Wraps each part in double quotes.
    - Escapes internal double quotes by doubling them.
    """
    parts = name.split(".")
    escaped_parts = []

    for part in parts:
        part = part.strip()
        if not part:
            raise ValueError(f"Invalid identifier: {name}")
        part = part.replace('"', '""')  # escape internal quotes
        escaped_parts.append(f'"{part}"')

    return ".".join(escaped_parts)


# ---------- DataFrame printing helper ----------

MAX_COLWIDTH_DEFAULT = 30  # tweak this if you want wider/narrower columns


def _truncate_df_strings(df: pd.DataFrame, max_colwidth: int) -> pd.DataFrame:
    """Return a copy where all string cells are truncated to max_colwidth with '...'."""
    if max_colwidth is None:
        return df

    df_trunc = df.copy()
    for col in df_trunc.columns:
        # Treat object dtype (mixed/strings) as truncate-able
        if df_trunc[col].dtype == "object":
            df_trunc[col] = (
                df_trunc[col]
                .astype(str)
                .map(
                    lambda s: (s[: max_colwidth - 3] + "...")
                    if len(s) > max_colwidth
                    else s
                )
            )
    return df_trunc


def print_dataframe(
    df: pd.DataFrame,
    max_rows: int | None = None,
    max_colwidth: int = MAX_COLWIDTH_DEFAULT,
) -> None:
    """Pretty-print a pandas DataFrame with hard string truncation + ellipses."""
    if df.empty:
        print("[duckdb_cli] Query returned 0 rows.\n")
        return

    df_trunc = _truncate_df_strings(df, max_colwidth)

    with pd.option_context(
        "display.max_rows",
        max_rows if max_rows is not None else 60,
        "display.max_columns",
        None,
        "display.width",
        None,  # auto-detect terminal width
        "display.colheader_justify",
        "left",
        "display.max_colwidth",
        max_colwidth,
    ):
        print()
        print(df_trunc.to_string(index=False))
        print()


# ---------- Connection helpers ----------

def connect_duckdb():
    """Connect to the DuckDB file defined in runtime.R.DUCKDB_PATH.

    Raises SystemExit if the file does not exist.
    """
    db_path = Path(R.DUCKDB_PATH)

    if not db_path.exists():
        print(f"[duckdb_cli] DuckDB file not found at: {db_path}")
        print("Run the data generator first (e.g. `python -m src.main`).")
        raise SystemExit(1)

    print(f"[duckdb_cli] Connecting to DuckDB at: {db_path}")
    con = duckdb.connect(str(db_path))
    return con


def fetch_tables(con):
    """
    Return:
        tables: list of (schema, name, fully_qualified)
        short_map: dict[table_name -> list[fully_qualified]]
    """
    rows = con.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name
        """
    ).fetchall()

    tables = []
    short_map = {}
    for schema, name in rows:
        fq = f"{schema}.{name}"
        tables.append((schema, name, fq))
        short_map.setdefault(name, []).append(fq)

    return tables, short_map


def print_tables(con):
    """Print available tables grouped by schema, showing fully-qualified names."""
    tables, _ = fetch_tables(con)
    if not tables:
        print("\n[duckdb_cli] No tables found in this database.\n")
        return

    print("\nAvailable tables:")
    current_schema = None
    for schema, name, fq in tables:
        if schema != current_schema:
            current_schema = schema
            print(f"  Schema: {schema}")
        print(f"    - {name} ({fq})")
    print("")


def resolve_table_name(user_input: str, con) -> str | None:
    """
    Resolve a user-provided table name to a fully-qualified name.

    - If input contains '.', assume it's already schema-qualified.
    - If it's a bare table name:
        - If it exists in exactly one schema, return that schema.table.
        - If it exists in multiple schemas, print an error and return None.
        - If it doesn't exist, return the original (so DuckDB error shows).

    Returns:
        fully_qualified_name or None if we want to cancel.
    """
    tables, short_map = fetch_tables(con)

    if "." in user_input:
        return user_input  # assume schema.table; DB will validate

    if user_input not in short_map:
        # Let DuckDB throw a "table does not exist" error
        return user_input

    fq_list = short_map[user_input]
    if len(fq_list) == 1:
        return fq_list[0]

    # Ambiguous
    print(
        f"[duckdb_cli] Table name '{user_input}' exists in multiple schemas:\n"
        + "\n".join(f"  - {fq}" for fq in fq_list)
        + "\nPlease specify schema.table explicitly."
    )
    return None


# ---------- Table-centric actions ----------

def show_table_head(con):
    """Show the first N rows of a chosen table."""
    print_tables(con)
    table = input("Enter table name to preview (or blank to cancel): ").strip()
    if not table:
        print("[duckdb_cli] Cancelled.\n")
        return

    fq_name = resolve_table_name(table, con)
    if fq_name is None:
        print("[duckdb_cli] Cancelled.\n")
        return

    limit_str = input("Number of rows to show [10]: ").strip()
    try:
        limit = int(limit_str) if limit_str else 10
    except ValueError:
        print("[duckdb_cli] Invalid number, using 10.")
        limit = 10

    try:
        ident = escape_identifier(fq_name)
        df = con.execute(f"SELECT * FROM {ident} LIMIT {limit}").df()
        print_dataframe(df, max_rows=limit)
    except Exception as e:
        print(f"[duckdb_cli] Error querying table '{fq_name}': {e}\n")


def show_table_schema(con):
    """Show schema (columns + types) of a chosen table."""
    print_tables(con)
    table = input("Enter table name to describe (or blank to cancel): ").strip()
    if not table:
        print("[duckdb_cli] Cancelled.\n")
        return

    fq_name = resolve_table_name(table, con)
    if fq_name is None:
        print("[duckdb_cli] Cancelled.\n")
        return

    try:
        ident = escape_identifier(fq_name)
        df = con.execute(f"DESCRIBE {ident}").df()
        print_dataframe(df)
    except Exception as e:
        print(f"[duckdb_cli] Error describing table '{fq_name}': {e}\n")


def export_table_as_csv(con):
    """Export a chosen table to a CSV file."""
    print_tables(con)
    table = input("Enter table name to export (or blank to cancel): ").strip()
    if not table:
        print("[duckdb_cli] Cancelled.\n")
        return

    fq_name = resolve_table_name(table, con)
    if fq_name is None:
        print("[duckdb_cli] Cancelled.\n")
        return

    output_path = R.DUCKDB_EXPORT_DIR
    output_path.mkdir(parents=True, exist_ok=True)
    export_csv_path = output_path / f"{fq_name.replace('.', '_')}.csv"

    try:
        ident = escape_identifier(fq_name)
        con.execute(f"COPY {ident} TO '{export_csv_path}' WITH (HEADER, DELIMITER ',')")
        print(f"[duckdb_cli] Table '{fq_name}' exported to '{export_csv_path}'.\n")
    except Exception as e:
        print(f"[duckdb_cli] Error exporting table '{fq_name}': {e}\n")


# ---------- Visualization helper functions ----------


def _query_df(
    con,
    sql: str,
    x_col: str | None = None,
    parse_dates: bool = False,
):
    """Run SQL and return a DataFrame, optionally parsing a datetime x_col."""
    df = con.execute(sql).df()
    if df.empty:
        print("[duckdb_cli] Query returned 0 rows; nothing to plot.\n")
        return None

    if parse_dates and x_col is not None:
        df[x_col] = pd.to_datetime(df[x_col])

    return df


def _get_viz_output_path(filename: str) -> Path:
    """Ensure the viz directory exists and return full output path."""
    viz_dir = Path(R.REPORT_VIZ_DIR)
    viz_dir.mkdir(parents=True, exist_ok=True)
    return viz_dir / filename


def _save_figure(fig, out_path: Path):
    """Save a matplotlib figure and print a standard message."""
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"[duckdb_cli] Saved chart to: {out_path}\n")


def _set_title_and_labels(
    ax,
    title: str,
    x_label: str | None,
    y_label: str | None,
    default_x: str,
    default_y: str,
):
    """Centralized helper for setting title and axis labels."""
    ax.set_title(title)
    ax.set_xlabel(x_label or default_x)
    ax.set_ylabel(y_label or default_y)

def _line_chart_core(
    con,
    sql: str,
    x_col: str,
    series: list[tuple[str | None, str]],
    title: str,
    filename: str,
    x_label: str | None = None,
    y_label: str | None = None,
    show_legend: bool = False,
):
    """
    Internal core for both single-line and multi-line time series charts.

    Parameters:
        con     : duckdb connection
        sql     : SQL query returning at least x_col + all y_cols
        x_col   : name of datetime/date column (X axis)
        series  : list of (label, column_name) for each line
                  label can be None for unlabeled (single-line) plots
        title   : plot title
        filename: output PNG filename under R.REPORT_VIZ_DIR
        x_label : optional custom X label (default = x_col)
        y_label : optional custom Y label (default = 'Value')
        show_legend : whether to show a legend (multi-line charts)
    """
    df = _query_df(con, sql, x_col=x_col, parse_dates=True)
    if df is None:
        return

    out_path = _get_viz_output_path(filename)

    fig, ax = plt.subplots(figsize=(16, 8))

    for label, col in series:
        if col not in df.columns:
            print(
                f"[duckdb_cli] Warning: column '{col}' not found in query result; "
                f"skipping series '{label or col}'."
            )
            continue

        if label is None:
            ax.plot(df[x_col], df[col])
        else:
            ax.plot(df[x_col], df[col], label=label)

    _set_title_and_labels(
        ax,
        title=title,
        x_label=x_label,
        y_label=y_label,
        default_x=x_col,
        default_y=y_label or "Value",
    )

    if show_legend:
        ax.legend()

    fig.autofmt_xdate()
    _save_figure(fig, out_path)


def bar_chart_from_sql(
    con,
    sql: str,
    x_col: str,
    y_col: str,
    title: str,
    filename: str,
    x_label: str | None = None,
    y_label: str | None = None,
    rotate_xticks: bool = True,
):
    """
    Simple bar chart from a SQL query.

    Expects the query to return at least x_col and y_col.
    """
    df = _query_df(con, sql, x_col=None, parse_dates=False)
    if df is None:
        return

    out_path = _get_viz_output_path(filename)

    fig, ax = plt.subplots()
    ax.bar(df[x_col], df[y_col])

    _set_title_and_labels(
        ax,
        title=title,
        x_label=x_label,
        y_label=y_label,
        default_x=x_col,
        default_y=y_col,
    )

    if rotate_xticks:
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    fig.tight_layout()
    _save_figure(fig, out_path)


# ---------- Visualization functions ----------
    

def plot_daily_revenue(con):
    sql = """
        SELECT session_day, total_rev
        FROM silver.session_metrics
        ORDER BY session_day
    """
    _line_chart_core(
        con=con,
        sql=sql,
        x_col="session_day",
        series=[(None, "total_rev")],
        title="Daily Revenue (silver.session_metrics)",
        filename="daily_revenue_timeseries.png",
        x_label="Date",
        y_label="Total Revenue",
        show_legend=False,
    )


def plot_daily_acquisition(con):
    sql = """
        SELECT date AS acq_day, SUM(daily_acquired_users) AS dau
        FROM gold.acquisition_daily
        GROUP BY date
        ORDER BY date
    """

    _line_chart_core(
        con=con,
        sql=sql,
        x_col="acq_day",
        series=[(None, "dau")],  # single line → label = None
        title="Daily Acquisition (gold.acquisition_daily)",
        filename="daily_acquisition_timeseries.png",
        x_label="Date",
        y_label="Daily Acquired Users",
        show_legend=False,
    )


def plot_daily_event_count(con):
    sql = """
        SELECT DATE(event_date) AS event_day, COUNT(*) AS events_count
        FROM bronze.events
        GROUP BY DATE(event_date)
        ORDER BY DATE(event_date)
    """

    _line_chart_core(
        con=con,
        sql=sql,
        x_col="event_day",
        series=[(None, "events_count")],  # single line
        title="Daily Event Count (bronze.events)",
        filename="daily_event_count_timeseries.png",
        x_label="Event Day",
        y_label="Event Count",
        show_legend=False,
    )


def plot_all_offers_daily_revenue(con):
    sql = """
        SELECT
            event_date,
            SUM(CASE WHEN offer_id = 'off_chest_legendary' THEN total_revenue ELSE 0 END) AS off_chest_legendary,
            SUM(CASE WHEN offer_id = 'off_diamond_1500' THEN total_revenue ELSE 0 END) AS off_diamond_1500,
            SUM(CASE WHEN offer_id = 'off_diamond_500' THEN total_revenue ELSE 0 END) AS off_diamond_500,
            SUM(CASE WHEN offer_id = 'off_hero_bundle_battlemage' THEN total_revenue ELSE 0 END) AS off_hero_bundle_battlemage,
            SUM(CASE WHEN offer_id = 'off_hero_bundle_deluxe_battlemage' THEN total_revenue ELSE 0 END) AS off_hero_bundle_deluxe_battlemage,
            SUM(CASE WHEN offer_id = 'off_hero_bundle_deluxe_rogue' THEN total_revenue ELSE 0 END) AS off_hero_bundle_deluxe_rogue,
            SUM(CASE WHEN offer_id = 'off_hero_bundle_deluxe_warrior' THEN total_revenue ELSE 0 END) AS off_hero_bundle_deluxe_warrior,
            SUM(CASE WHEN offer_id = 'off_hero_bundle_rogue' THEN total_revenue ELSE 0 END) AS off_hero_bundle_rogue,
            SUM(CASE WHEN offer_id = 'off_hero_bundle_warrior' THEN total_revenue ELSE 0 END) AS off_hero_bundle_warrior,
            SUM(CASE WHEN offer_id = 'off_item_bundle_epic' THEN total_revenue ELSE 0 END) AS off_item_bundle_epic,
            SUM(CASE WHEN offer_id = 'off_subscription_basic' THEN total_revenue ELSE 0 END) AS off_subscription_basic,
            SUM(CASE WHEN offer_id = 'off_subscription_premium' THEN total_revenue ELSE 0 END) AS off_subscription_premium
        FROM gold.biz_offer_performance
        GROUP BY event_date
        ORDER BY event_date
    """

    _line_chart_core(
        con=con,
        sql=sql,
        x_col="event_date",
        series=[
            ("Legendary Chest",         "off_chest_legendary"),
            ("Diamond 1500",            "off_diamond_1500"),
            ("Diamond 500",             "off_diamond_500"),
            ("Hero Bundle Battlemage",  "off_hero_bundle_battlemage"),
            ("Deluxe Battlemage",       "off_hero_bundle_deluxe_battlemage"),
            ("Deluxe Rogue",            "off_hero_bundle_deluxe_rogue"),
            ("Deluxe Warrior",          "off_hero_bundle_deluxe_warrior"),
            ("Hero Bundle Rogue",       "off_hero_bundle_rogue"),
            ("Hero Bundle Warrior",     "off_hero_bundle_warrior"),
            ("Item Bundle Epic",        "off_item_bundle_epic"),
            ("Sub Basic",               "off_subscription_basic"),
            ("Sub Premium",             "off_subscription_premium"),
        ],
        title="Daily Revenue by Offer (gold.biz_offer_performance)",
        filename="offers_daily_revenue_multiline.png",
        x_label="Date",
        y_label="Total Revenue",
        show_legend=True,
    )


def plot_ad_roas_bar_chart(con):
    bar_chart_from_sql(
        con,
        sql= """SELECT ad_name, roas FROM gold.marketing_ad_metrics ORDER BY roas DESC""",
        x_col="ad_name",
        y_col="roas",
        title="Ad ROAS (gold.marketing_ad_metrics)",
        filename="ad_roas_bar_chart.png",
        x_label="Ad Name",
        y_label="ROAS"
    )

# ---------- Simple SQL safety / injection guard ----------


FORBIDDEN_KEYWORDS = {
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "attach",
    "copy",
    "pragma",
    "call",
    "transaction",
    "truncate",
}


def is_safe_select(sql: str) -> bool:
    """
    Very simple SQL-safety check for the custom query option.

    - Only allows queries whose first non-whitespace token is 'select'.
    - Rejects obvious DDL/DML/admin keywords anywhere in the statement.
    - Rejects semicolons to avoid multiple statements.

    This is intentionally conservative and is for demonstration, not
    bulletproof security.
    """
    raw = sql.strip()
    if not raw:
        return False

    lower = raw.lower()

    # Only allow SELECT as the first keyword
    if not lower.lstrip().startswith("select"):
        return False

    # No multiple statements
    if ";" in lower:
        return False

    # Tokenize into simple words (letters + underscores)
    tokens = set(re.findall(r"[a-z_]+", lower))
    if tokens & FORBIDDEN_KEYWORDS:
        return False

    return True


def run_custom_select(con):
    """Run a custom SELECT query with a basic SQL safety guard."""
    print_tables(con)
    print(
        "Enter a SELECT query to run against this database.\n"
        "- Only SELECT statements are allowed.\n"
        "- Dangerous keywords (DROP, DELETE, etc.) are blocked.\n"
        "- Multiple statements (using ';') are not allowed.\n"
    )
    sql = input("SQL> ").strip()
    if not sql:
        print("[duckdb_cli] Cancelled.\n")
        return

    if not is_safe_select(sql):
        print(
            "[duckdb_cli] Query rejected by safety checks.\n"
            "Only simple SELECT queries without DDL/DML/admin keywords are allowed.\n"
        )
        return

    try:
        df = con.execute(sql).df()
        print_dataframe(df)
    except Exception as e:
        print(f"[duckdb_cli] Error executing query: {e}\n")


# ---------- Menu / entrypoint ----------


def menu_loop(con):
    """Interactive menu loop for exploring the DuckDB file."""
    while True:
        print(
            "\nDuckDB Viewer Menu\n"
            "1) List tables and schemas\n"
            "2) Show first N rows of a table\n"
            "3) Show schema of a table\n"
            "4) Run a custom SELECT query\n"
            "5) Export a table as CSV\n"
            "6) Produce visualizations from tables\n"
            "0) Quit\n"
        )
        choice = input("Select an option: ").strip()

        if choice == "1":
            print_tables(con)
        elif choice == "2":
            show_table_head(con)
        elif choice == "3":
            show_table_schema(con)
        elif choice == "4":
            run_custom_select(con)
        elif choice == "5":
            export_table_as_csv(con)
        elif choice == "6":
            while True:
                print(
                    "\nVisualization Options:\n"
                    "1) Daily Revenue\n"
                    "2) Daily Acquisition\n"
                    "3) Daily Event Count\n"
                    "4) Daily Revenue by All Offers\n"
                    "5) Ad ROAS Bar Chart\n"
                    "0) Return to main menu\n"
                )
                viz_choice = input("Select a visualization to produce: ").strip()

                if viz_choice == "1":
                    plot_daily_revenue(con)
                elif viz_choice == "2":
                    plot_daily_acquisition(con)
                elif viz_choice == "3":
                    plot_daily_event_count(con)
                elif viz_choice == "4":
                    plot_all_offers_daily_revenue(con)
                elif viz_choice == "5":
                    plot_ad_roas_bar_chart(con)
                elif viz_choice == "0":
                    print("[duckdb_cli] Returning to DuckDB viewer menu...\n")
                    break
                else:
                    print("[duckdb_cli] Invalid choice. Please select 0–3.\n")
        elif choice == "0":
            print("[duckdb_cli] Goodbye :)")
            break
        else:
            print("[duckdb_cli] Invalid choice. Please select 0–6.\n")


def duckdb_cli_main():
    """Entry point for both standalone use and being called from main.py."""
    try:
        con = connect_duckdb()
    except SystemExit:
        # Propagate to caller if used as a module,
        # but still exit cleanly in standalone mode.
        raise
    except Exception as e:
        print(f"[duckdb_cli] Unexpected error while connecting: {e}")
        raise SystemExit(1)

    try:
        menu_loop(con)
    finally:
        con.close()


if __name__ == "__main__":
    # Running as: python -m src.tools.duckdb_cli
    try:
        duckdb_cli_main()
    except SystemExit as e:
        sys.exit(e.code)
