# SG Food Price Tracker

A data pipeline and interactive dashboard for tracking, matching, and comparing grocery prices across Singapore's four major supermarkets: FairPrice, RedMart, Cold Storage, and Sheng Siong.

## Overview

This project collects supermarket price data daily, standardises it into a unified schema, matches comparable products across stores, and exposes the results through a Streamlit dashboard.

The system supports three comparison layers:
- `Packaged goods`: branded products matched across stores using brand, title, size, and variant similarity.
- `Fresh and commodity goods`: meat, seafood, fruits, and vegetables compared by commodity type and pack size.
- `Analytics and ML outputs`: price-tier clustering, anomaly detection, and price prediction tables for downstream analysis.

## Key Features

- Daily supermarket scraping across four stores
- ETL pipeline for transforming raw JSON into a consistent product schema
- Cross-store product matching for packaged goods
- Commodity-based comparison for fresh products
- Derived daily comparison tables showing cheapest store, priciest store, and price spread
- Streamlit dashboard with overview, product comparison, fresh commodity, price tiers, and price prediction pages

## Repository Structure

```text
SG-foodprice-tracker/
|-- dashboard/                 Streamlit app and dashboard pages
|-- pipeline/
|   |-- dags/                 Airflow DAG definitions
|   |-- etl/                  Transform and load steps
|   |-- matching/             Product and commodity matching logic
|   |-- ml/                   Clustering, anomaly detection, prediction
|   |-- pricing/              Daily price comparison table builders
|   |-- schemas/              SQL schema files for Supabase
|-- scrapers/                 Store-specific scraper scripts
|-- data/                     Generated outputs and analysis artifacts
|-- Dockerfile                Minimal Docker setup for the dashboard
|-- docker-compose.yml        Local Docker run configuration
|-- requirements.txt          Main Python dependencies
|-- requirements-airflow.txt  Optional Airflow dependencies
```

## Tech Stack

| Layer | Technology |
| --- | --- |
| Scraping | Python, Requests, Selenium, Playwright |
| Orchestration | Apache Airflow |
| Data storage | Local JSON outputs and Supabase |
| Dashboard | Streamlit, Plotly, pandas |
| Machine learning | scikit-learn, matplotlib, numpy |
| Environment | dotenv, Docker, Docker Compose |

## Data Flow

The project follows an ETLT workflow:

1. `Extract`: scrape raw product data into `data/raw/<store>/<date>/`
2. `Transform`: clean fields and normalize store outputs into a shared schema
3. `Load`: insert products into Supabase
4. `Transform`: build canonical product matches, commodity comparisons, daily pricing tables, and analytics outputs from the loaded data

## Supabase Tables

The scripts in this repository read from and write to Supabase tables such as:

- `products`
- `canonical_products`
- `canonical_product_members`
- `product_match_candidates`
- `canonical_product_daily_prices`
- `canonical_product_daily_recommendations`
- `commodity_price_comparisons`
- `product_clusters`
- `product_price_predictions`
- `price_prediction_metrics`

Schema files are provided in `pipeline/schemas/`.

## Environment Variables

Copy `.env.example` to `.env` and provide valid credentials before running the project.

```env
SUPABASE_URL=https://your-project-id.supabase.co
SUPABASE_KEY=your-supabase-service-role-key
SUPABASE_DB_URL=postgresql://postgres:<your-password>@<your-host>:5432/postgres
```

Notes:
- `.env` is gitignored and should remain private.
- `.env.example` contains placeholders only.

## Local Setup

Recommended Python version: `3.11`

Core project dependencies:

PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Git Bash:

```bash
python -m venv .venv
source .venv/Scripts/activate
cp .env.example .env
pip install -r requirements.txt
```

Additional setup for optional components:

If you want to run the Airflow DAGs locally, install the Airflow dependencies as an additional step:

```powershell
pip install -r requirements-airflow.txt
```

If you want to run the Playwright-based scrapers, install the Chromium browser binary used by Playwright:

```powershell
python -m playwright install chromium
```

## Running The Dashboard

Run locally:

```powershell
streamlit run dashboard/Home.py
```

The dashboard will be available at `http://localhost:8501`.

## Running With Docker

The Docker configuration starts the Streamlit dashboard in a reproducible Python environment.

```powershell
docker compose up --build
```

Before running Docker:
- ensure Docker Desktop is installed
- create a local `.env` file from `.env.example`

## Running Pipeline Scripts

Examples of direct script execution:

```powershell
python scrapers\fairprice_scraper.py
python scrapers\coldstorage_scraper.py
python scrapers\shengsiong_scraper.py
python scrapers\redmart_scraper.py
python pipeline\etl\transform.py 2026-04-04
python pipeline\etl\load.py 2026-04-04
python pipeline\matching\matching.py Beverages 2026-04-04
python pipeline\matching\meat_produce_matching.py "Meat & Seafood" 2026-04-04
python pipeline\matching\vegetable_produce_matching.py 2026-04-04
python pipeline\matching\commodity_matching.py "Fruits & Vegetables"
python pipeline\pricing\build_price_comparison_tables.py Beverages
python pipeline\ml\product_clustering.py
python pipeline\ml\anomaly_detector.py
python pipeline\ml\future_price.py
```

Airflow DAGs for orchestration are available in `pipeline/dags/`.

## Reproducing The Pipeline

One example end-to-end run for a single date is:

```powershell
python scrapers\fairprice_scraper.py
python scrapers\coldstorage_scraper.py
python scrapers\shengsiong_scraper.py
python scrapers\redmart_scraper.py
python pipeline\etl\transform.py 2026-04-04
python pipeline\etl\load.py 2026-04-04
python pipeline\matching\matching.py Beverages 2026-04-04
python pipeline\matching\matching.py Dairy 2026-04-04
python pipeline\matching\matching.py Staples 2026-04-04
python pipeline\matching\matching.py "Snacks & Confectionery" 2026-04-04
python pipeline\matching\matching.py "Bakery & Breakfast" 2026-04-04
python pipeline\matching\meat_produce_matching.py "Meat & Seafood" 2026-04-04
python pipeline\matching\vegetable_produce_matching.py 2026-04-04
python pipeline\matching\commodity_matching.py "Meat & Seafood"
python pipeline\matching\commodity_matching.py "Fruits & Vegetables"
python pipeline\pricing\build_price_comparison_tables.py Beverages
python pipeline\pricing\build_price_comparison_tables.py Dairy
python pipeline\pricing\build_price_comparison_tables.py Staples
python pipeline\pricing\build_price_comparison_tables.py "Snacks & Confectionery"
python pipeline\pricing\build_price_comparison_tables.py "Bakery & Breakfast"
python pipeline\pricing\build_price_comparison_tables.py "Meat & Seafood"
python pipeline\pricing\build_price_comparison_tables.py "Fruits & Vegetables"
```

This sequence assumes:
- raw data for the date has been scraped successfully
- the Supabase schema has already been applied
- the required environment variables are present in `.env`

## Dashboard Pages

- `Home`: project landing page
- `Overview`: store leadership, discounts, category spread, and savings summary
- `Compare Products`: packaged-goods comparison across stores
- `Fresh Commodity`: fresh product and commodity comparison
- `Price Tiers`: clustering-based price segmentation
- `Price Prediction`: comparison of actual prices against predicted prices

## Notes

- The dashboard expects the required Supabase tables to exist and contain data.
- Some scrapers require browser tooling in addition to Python packages.
- Docker support in this repository is focused on running the dashboard and shared application environment.
