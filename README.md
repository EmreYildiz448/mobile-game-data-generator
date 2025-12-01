# Mobile Game Data Generator

A Python-based data generation and analytics pipeline for simulating mobile game player behavior and practicing data engineering, analytics, and visualization.
The project uses ProcessPoolExecutor for parallelized event generation and stores all data in a local DuckDB database organized with a medallion architecture.

---

## Features
- **Logical account and event generation**: Generates realistic data for advertisements, user accounts, game sessions, level progression, purchases, chests, ad interactions, and more. Events follow a clear structure based on event_type and event_subtype.
- **Configurable archetypes and values**: Player behavior, difficulty scaling, A/B testing windows, device distributions, and simulation parameters are fully configurable through environment variables.
- **Parallel event generation**: Uses Python’s ProcessPoolExecutor to generate millions of events efficiently.
- **Lightweight storage**: Data is stored in a DuckDB database, with optional CSV exports for debugging or external BI tools.
- **Built-in SQL transformation scripts**: Converts raw generated events into cleaned & expanded Silver tables and analytical Gold-layer KPIs.
- **Structured database schema**: DuckDB database uses a Medallion (Bronze-Silver-Gold) architecture to simplify analysis.
- **Advanced analytics**: Built-in A/B hypothesis testing and machine learning model suite (Random Forest, XGBoost, SHAP).

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
    |   analytics.py
    |   catalogs.py
    |   event_handler.py
    |   main.py
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
```bash
pip install -r requirements.txt
```
---

## Usage
```bash
python -m src.main
```
---

## Output
```
+---data    
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

---

## Contributing

---

## License

---