# Mobile Game Data Generator

A Python-based data generation and analytics pipeline for simulating mobile game player behavior and practicing data engineering, analytics, and visualization.
The project uses ProcessPoolExecutor for parallelized event generation and stores all data in a local DuckDB database organized with a medallion architecture.

---

## Features

### 1. Full Mobile-Game Data Simulation Pipeline

Generate realistic mobile-game data including:
- Accounts & acquisition attributes
- Session lifecycle
- Level progression
- Resource sinks & sources
- Ads, IAP purchases, subscriptions
- Monetization behavior
- A/B test assignment & effects
- Retention shaping & churn modeling

All components are reproducible via a single centralized random seed.

### 2. Configurable via .env (with safe fallbacks)

The generator reads configuration from a .env file (optional). All settings have default values, and invalid .env files are ignored automatically.

Users can customize:

- Date ranges
- Simulation sizes
- A/B test configuration
- Archetype multipliers
- Ad probabilities
- Retention parameters
- Output destinations (CSV, DuckDB)

A template .env.example is provided.

### 3. Clean ELT-Style Project Structure

Outputs follow a warehouse-like layout:

- **bronze/** – raw simulated CSV or DuckDB tables
- **silver/** – cleaned & enriched intermediate tables
- **gold/** – final analytics-ready data marts
- **analytics/** – A/B test & ML-related outputs

### 4. Automatic DuckDB Export

The simulation can export all results into:

- tilecrashers.duckdb — a complete analytical database
- Optional CSV outputs for inspection or BI tools
- Optional reports/visuals directory for generated charts

### 5. Built-In DuckDB CLI Explorer (duckdb_cli.py)

A custom command-line tool for exploring your DuckDB file. It supports:

- List all schemas and tables (with fully-qualified names)
- Preview N rows of any table
- View table schemas
- Export tables to CSV
- Run a custom SELECT query

*(Uses safe identifier escaping, pretty-printed DataFrames, and auto-truncation.)*

### 6. Built-In Visualization Framework

Directly from the CLI, users can generate:

- Daily revenue time series
- Daily acquisition time series
- Daily event count
- Daily revenue by all offers (multi-line chart)
- Ad ROAS bar chart
- **Custom SELECT visualizations** *(line or bar)*

Charts are automatically saved to **mobile-game-data-generator/duckdb_exports/report_viz/** *(directory is created automatically if it does not exist)*

Visualization safety includes:

- Type checks (numeric Y, date-like X for time series)
- Simple SQL injection guard for custom queries
- Unified plotting backbone based on _line_chart_core() and bar_chart_from_sql()

### 7. Reproducible Simulation (Centralized Seed Control)

All randomness (Python random, NumPy, Faker, UUID generation) is controlled via a single seed set in main.py. Workers spawned through ProcessPoolExecutor also receive deterministic seeding.

### 8. First-Run Safety & Existing File Protection

The orchestrator detects existing output directories and prompts:

- **Y** --> delete old files and regenerate
- **N** --> abort safely

### 9. Modular, Extensible Codebase

The system cleanly separates:

- Generation logic
- Business rules
- Event simulation
- Data export
- Visualization
- Auxiliary tools (DuckDB CLI)

New components can be added without touching core modules.

---

## Initial folder structure
```
mobile-game-data-generator
|   .gitignore
|   README.md
|   requirements.txt
|   
+---data
|   \---external
|           exchange_rate.csv
|           
+---sql
|   +---analytics
|   |       ab_test_daily_account_counts.sql
|   |       ab_test_daily_revenue.sql
|   |       ab_test_eda.sql
|   |       equipment_contribution_eda.sql
|   |       
|   +---gold
|   |       acquisition_daily.sql
|   |       biz_offer_performance.sql
|   |       cohort_churn.sql
|   |       hosted_ad_metrics.sql
|   |       level_progression.sql
|   |       marketing_ad_metrics.sql
|   |       shop_offer_performance.sql
|   |       
|   \---silver
|           accounts_extended.sql
|           hosted_ads_act_detailed.sql
|           session_metrics.sql
|           
+---src
    |   catalogs.py
    |   event_handler.py
    |   main.py
    |   duckdb_cli.py
    |       
    +---analysis
    |       ab_test.py
    |       ml_models.py
    |          
    +---database
    |       bootstrap.py
    |       transform_layers.py
    |           
    +---generators
    |       accounts.py
    |       ad_events.py
    |       business.py
    |       chest_handler.py
    |       errors.py
    |       gameplay.py
    |       ig_purchases.py
    |           
    +---io
    |       file_writer.py
    |       
    |           
    +---legacy
    |       analytics.py
    |       db_writer.py
    |       infra.py
    |       orm.py
    |       README(DB).txt
    |       
    +---marketing
    |       ad_generator.py
    |       hosted_ads.py
    |           
    +---models
    |           
    \---settings
    |       runtime.py
```
---

## Requirements

* Python **3.12+**  
* Python dependencies listed in requirements.txt

---

## Installation & Setup

1. ### Clone the repository:
```bash
git clone https://github.com/EmreYildiz448/mobile-game-data-generator.git
cd mobile-game-data-generator
```

2. ### Create and activate a virtual environment:
Run this command first (all platforms):
```bash
python -m venv .venv
```

1. *On Windows*

    ```bash
    .venv\Scripts\activate
    ```

2. *On macOS/Linux*

    ```bash
    source .venv/bin/activate
    ```

3. ### Install dependencies:
```bash
pip install -r requirements.txt
```

---

## Usage

This project can be used in two primary ways:
1. Run the full data-generation pipeline
2. Explore an existing DuckDB database interactively

Both entry points are intentionally simple and rely on sensible defaults.

### 1. Run the Full Pipeline

This command executes the entire workflow:
- Data generation
- Optional CSV export
- DuckDB database creation
- SQL transformations (bronze → silver → gold → analytics)
- Optional visualizations
- Optional interactive database exploration prompt

```bash
python -m src.main
```

- If output files already exist, you’ll be prompted to:
  - Delete and regenerate, or
  - Abort safely
- All configuration is loaded from defaults in runtime.py
- If a .env file exists and is valid, its values override defaults
- If .env contains invalid or illogical values, it is ignored and defaults are used

This command is intended for end-to-end execution.

### 2. Explore an Existing DuckDB File Only

If a DuckDB file already exists and you just want to explore or visualize the data:
```bash
python -m src.duckdb_cli
```
This launches an interactive DuckDB viewer that allows you to:
- List schemas and tables
- Preview table rows
- Inspect table schemas
- Run safe SELECT queries
- Export tables to CSV
- Generate built-in or custom visualizations

**This mode does not generate data. It only operates on existing files. If no DuckDB file is found, the CLI will stop and print a warning.**

### Optional: Configuration via Environment Variables

You can customize the simulation by providing a .env file.
1. Copy the provided template:
```bash
cp .env.example .env
```
2. Edit .env (in a text or code editor) and set only the variables you care about
3. Leave others blank to fall back to defaults

Key configurable options include:
- Simulation date range
- Number of accounts, ads, campaigns
- A/B test launch window and effects
- Random seed (for reproducibility)
- Output behavior (CSV vs DuckDB)

Every variable has a default value in runtime.py:
- Invalid or illogical configurations (e.g. end date before start date) cause .env to be ignored
- The program automatically reverts to defaults when this happens
- You can safely experiment without breaking the pipeline.

Example .env values:
```bash
NUM_ACC=10000
SEED=42
WRITE_TO_FILE=False
```
---

## Output
```
+---data              # If WRITE_TO_FILE is True, data will be written here
|   \---interim
|           accounts.csv
|           ads.csv
|           ad_campaign_map.csv
|           campaigns.csv
|           events.csv
|           hosted_ads.csv
|           hosted_ad_interactions.csv
|           sessions.csv
|           
+---duckdb_exports    # duckdb_cli.py writes CSV files to this path
|   \---report_viz    # duckdb_cli.py exports images to this path
|
+---output
|   +---duckdb
|   |       tilecrashers.duckdb
|   |       
|   \---reports
|       +---ab_test
|       |       ab_test_results.csv
|       |       ab_test_results.txt
|       |       
|       \---ml
|           +---individual
|           |   +---binary
|           |   |   \---figures
|           |   |           
|           |   +---ordinal
|           |   |   \---figures
|           |   |           
|           |   +---raw_regression
|           |   |   \---figures
|           |   |           
|           |   \---residual_regression
|           |       \---figures
|           |               
|           \---summary
|                   feature_importance_results.csv
|                   feature_importance_topk.txt
|                   model_metrics.csv
```
---

## Roadmap
- The variables included in the example environment file are supported by validation logic. However, most of the environment variables that manage internal math logic of the simulation (e.g archetype values, retention & monetization factors, etc.) have not been tested. An extensive test to prevent simulation failure due to illogical variable values is planned. For the moment, in-depth variable modification beyond documented options is not recommended.
- An extensive TUI for improved program control is planned.
- Integration of an Ollama AI model as a chatbot to facilitate DuckDB queries is planned.
---

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you'd like to change.

---
## Acknowledgements

This project relies on a number of efficient open-source libraries.

Core data & visualization stack:
- NumPy
- pandas
- matplotlib
- DuckDB

Specialized libraries:
- Faker: Synthetic data generation 
  - https://github.com/joke2k/faker
- sqlglot: SQL parsing & transformation 
  - https://github.com/tobymao/sqlglot
- python-dotenv: Environment configuration
  - https://github.com/theskumar/python-dotenv
- scikit-learn, XGBoost, SHAP: Modeling & explainability
  - https://github.com/scikit-learn/scikit-learn
  - https://github.com/dmlc/xgboost
  - https://github.com/shap/shap

---

## License
This project is licensed under the MIT License - see the LICENSE file for details.

---