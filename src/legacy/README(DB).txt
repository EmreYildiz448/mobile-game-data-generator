Scripts found in this folder were previously used to push the generated data to a designated PostgreSQL server. However, more recent versions dropped this feature and replaced it with a DuckDB local database file. If you would like to push the generated data to a PostgreSQL server, you can use these scripts to do so.

The following modifications are necessary to use legacy scripts and push data to a PostgreSQL server:

1-) Either rewrite the import lines in said scripts to match the folder structure, or adhere to the following structure:
- infra.py should be placed under src.settings
    - - If you decide to place the script elsewhere, also change the import line for DATABASE_URL under _ensure_session() to avoid an error. Currently, DATABASE_URL is imported from src.settings.infra
- orm.py should be placed under src.models
- db_writer.py should be placed under src.io
2-) db_writer.py imports database credentials from infra.py, which is currently a placeholder. Replace the placeholder with your server credentials.
3-) In main.py, uncomment the block of code that starts with "if R.WRITE_TO_DB:"
4-) When executing the program via main.py, include WRITE_TO_DB environment variable as True:
- $env:WRITE_TO_DB = "false" in terminal or WRITE_TO_DB=False in the .env file
