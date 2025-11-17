# Mobile Game Data Generator

A Python-based data generation and analytics pipeline for simulating mobile game player behavior and practicing data visualization.
The project uses ProcessPoolExecutor for parallelized event generation and stores all data in a local DuckDB database organized with the Medallion (Bronze–Silver–Gold) architecture.

---

## Features
- **Account and event generation**: Generates consistent data for advertisements, user accounts and game events based on internal & game logic.
- **Dynamic player archetypes and behavior patterns**: Player archetypes are stored as environment variables and used to generate realistic data. Users can alter these variables to simulate different scenarios and generate data accordingly. 
- **Lightweight storage**: