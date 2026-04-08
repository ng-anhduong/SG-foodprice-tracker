# Pipeline Design

## Overview

This document describes the full end-to-end data pipeline for the SG Food Price Tracker.

The pipeline follows an **ETLT** pattern:
- **E** — Extract: scrape raw product data from 4 stores
- **T1** — Transform (Clean): normalize raw data into a unified schema
- **L** — Load: push cleaned data into Supabase
- **T2** — Transform again (Match + Enrich): run matching algorithms to enable cross-store price comparison

---

## E — Extract

Scrapers crawl all 4 stores daily and save raw JSON to `data/raw/<store>/<date>/`.

| Store | Scheduled Time (SGT) | Method |
|-------|---------------------|--------|
| FairPrice | 2:00 PM | Playwright (JSON API) |
| RedMart | 2:30 PM | Selenium (headless browser) |
| Cold Storage | 3:00 PM | Playwright (headless browser) |
| Sheng Siong | 3:30 PM | Playwright (infinite scroll) |

Airflow DAGs: `fairprice_scraper`, `redmart_scraper`, `coldstorage_scraper`, `shengsiong_scraper`

Raw output structure:
```
data/raw/
  fairprice/2026-04-07/meat-seafood.json
  shengsiong/2026-04-07/meat-poultry-seafood.json
  redmart/2026-04-07/meat.json
  coldstorage/2026-04-07/meat-seafood.json
  ...
```

---

## T1 — Transform (Clean)

**Script:** `pipeline/etl/transform.py`

**Triggered:** 4:30 PM SGT via `etl_transform_load` Airflow DAG, after all 4 scrapers succeed.

**What it does:**
- Casts price fields from string → float
- Maps each store's category slugs to a unified category name
- Cleans product URLs per store (prepend base URL for Sheng Siong, strip query string for RedMart)
- Drops excluded categories (e.g. convenience food, dried herbs)
- Writes unified schema JSON to `data/processed/<store>/<date>/`

**Unified categories:**
`Beverages`, `Dairy`, `Staples`, `Snacks & Confectionery`, `Bakery & Breakfast`, `Meat & Seafood`, `Fruits & Vegetables`

**Unified schema per product:**
```json
{
  "name": "Kee Song Fresh Chicken - Boneless Breast",
  "brand": "Kee Song",
  "price_sgd": 5.50,
  "original_price_sgd": 6.00,
  "discount_sgd": 0.50,
  "unit": "500g",
  "unified_category": "Meat & Seafood",
  "category_slug": "meat-seafood",
  "store": "fairprice",
  "product_url": "https://www.fairprice.com.sg/product/...",
  "scraped_at": "2026-04-07T14:05:00"
}
```

---

## L — Load

**Script:** `pipeline/etl/load.py`

**Triggered:** Immediately after transform, same DAG (`etl_transform_load`).

**What it does:**
- Reads all processed JSON files for the given date
- Batch upserts into Supabase `products` table via REST API (supabase-py)
- Deduplicates on `(name, store, scraped_at)` — safe to re-run

**Supabase table:** `products`

---

## T2 — Transform Again (Match + Enrich)

**Triggered:** 4:45 PM SGT via `product_matching_pipeline` Airflow DAG, after `etl_transform_load` succeeds.

The second transform layer reads from `products` and runs four matching algorithms that produce cross-store comparable product identities and price comparisons.

---

### Algorithm 1A: Branded Product Matching

**Script:** `pipeline/matching/matching.py` 

**Categories:** Beverages, Dairy, Staples, Snacks & Confectionery, Bakery & Breakfast

**How it works:**
1. Fetches latest products per store per category from Supabase
2. Parses each product: normalizes name, brand, size, variant, packaging
3. Generates candidate pairs across stores using pre-filters (brand, size, token overlap)
4. Scores each pair on 5 dimensions: brand (30%), size (30%), title (25%), variant (10%), packaging (5%)
5. Applies penalties for conflicts (brand mismatch, size mismatch, variant conflict)
6. Keeps only reciprocal best strong matches
7. Groups matches into canonical products using union-find
8. Syncs to Supabase

**Output tables:** `canonical_products`, `canonical_product_members`, `product_match_candidates`

---

### Algorithm 1B: Branded Meat Matching

**Script:** `pipeline/matching/meat_produce_matching.py`

**Category:** Meat & Seafood (packaged/branded portion)

**Same engine as Algorithm 1A, but domain-tuned:**

| Dimension | Algorithm 1A (beverages) | Algorithm 1B (meat) |
|-----------|--------------------------|---------------------|
| STOPWORDS | `tea`, `bottle`, `drink` | `frozen`, `chilled`, `halal`, `boneless`, `reared` |
| VARIANT_GROUPS | `zero sugar`, `lychee`, `oolong` | `boneless/bone-in`, `skinless`, `breast/thigh/wing` |
| BRAND_ALIASES | `coke`, `pokka`, `milo` | `farmfresh`, `seara`, `jean fresh`, `pasar` |
| Brand weight | 30% | 20% |
| Title weight | 25% | 35% |
| Size tolerance | ±12% | ±20% |
| Brand conflict penalty | 0.25 | 0.20 (softer — fresh cuts often have no brand) |
| Variant conflict penalty | 0.20 | 0.25 (stricter — boneless ≠ bone-in) |

**Best for:** packaged branded meat (`Seara Frozen Chicken Breast`, `Le Bao Frozen Pork Belly`)

**Output tables:** `canonical_products`, `canonical_product_members`, `product_match_candidates`

---

### Algorithm 1C: Fresh Produce Matching

**Script:** `pipeline/matching/vegetable_produce_matching.py`

**Category:** Fruits & Vegetables

**Why Algorithm 1A/1B fails for produce:**
- Store-private labels (`Pasar`, `Agro Fresh`, `Yuan Zhen Yuan`) are exclusive to FairPrice — brand score always 0
- Most produce has no pack size on the label — size score always unknown
- Short product names → weak title similarity

**Key innovation — strip private labels before comparing:**
```
"Pasar Australian Broccoli"      → "australian broccoli"
"Agro Fresh Australian Broccoli" → "australian broccoli"
"Australian Broccoli"            → "australian broccoli"
```

**Scoring dimensions (completely different from 1A/1B):**

| Dimension | Weight | Logic |
|-----------|--------|-------|
| Produce type | 40% | `broccoli == broccoli` — if fails, stop immediately |
| Origin | 25% | `australia == australia` — AU broccoli ≠ CN broccoli |
| Qualifier | 20% | `organic`, `baby`, `fuji` — conflicts penalised hard |
| Title | 15% | Fuzzy match on stripped name |

**Penalties:**
- Origin conflict: −0.20 (Australian vs Chinese broccoli = different product)
- Qualifier conflict: −0.25 (Fuji vs Gala apple, organic vs non-organic = different product)

**Private labels stripped:** `pasar`, `agro fresh`, `yuan zhen yuan`, `simply finest`, `chef`, `hydrogreen`, `vegeponics`, `churo` and others

**Output tables:** `canonical_products`, `canonical_product_members`, `product_match_candidates`

---

### Algorithm 2: Commodity Price Comparison

**Script:** `pipeline/matching/commodity_matching.py`

**Categories:** Meat & Seafood + Fruits & Vegetables

**Why this is needed:**
Algorithms 1B and 1C match products by identity. But for fresh commodities (chicken breast, broccoli),
what shoppers really want to know is: *"where is it cheapest to buy this today?"*

**How it works:**

1. Fetch all products for the category from Supabase
2. Extract cut/commodity type using a keyword taxonomy (~70 cuts: chicken, pork, beef, lamb, seafood, fruits, vegetables)
3. Extract weight from product name or unit field (handles `500g`, `1.5kg`, `6 x 200g`)
4. Flag `fresh/chilled` vs `frozen` — these are not mixed
5. **Find most common pack size** across all stores for that cut (within ±20% tolerance)
6. Filter to only products at that pack size
7. Compare actual prices at the same pack size across stores
8. Keep only groups where 2+ stores carry the same cut at a comparable size

**Design decision — why not price per 100g?**

An earlier version compared price per 100g. This was changed because:
- You cannot buy 100g — you buy the whole pack
- Comparing price per 100g across a 300g pack and a 1kg pack is misleading for a shopper
- The correct comparison is: find the pack size that most stores carry, then compare the actual price you pay

**Example:**

| Store | Product | Pack size | Price |
|-------|---------|-----------|-------|
| FairPrice | Kee Song Fresh Chicken Breast | 500g | $5.50 |
| Sheng Siong | Jean Fresh Packet Chicken Breast | 500g | $4.80 ← cheapest |
| RedMart | FarmFresh Chicken Breast Boneless | 500g | $5.20 |
| Cold Storage | (no 500g option) | — | skipped |

Most common weight: **500g** → compare $5.50 vs $4.80 vs $5.20 → spread = $0.70

The `store_prices` JSON still includes `unit_price_per_100g` per store for analytical reference.

**Output table:** `commodity_price_comparisons`

```
cut                    | frozen_flag   | common_weight_g | cheapest_store | cheapest_price_sgd | price_spread_sgd
chicken - breast       | fresh/chilled | 500             | shengsiong     | 4.80               | 0.70
pork - belly           | fresh/chilled | 300             | redmart        | 3.20               | 1.10
veg - broccoli         | fresh/chilled | 400             | fairprice      | 1.90               | 0.80
```

---

### Price Table Refresh

**Script:** `pipeline/pricing/build_price_comparison_tables.py`

**What it does:**
- Joins `canonical_products` + `canonical_product_members` + `products`
- Builds one row per matched product per store per day with price rank, gap from cheapest
- Builds one summary row per canonical product per day with cheapest/priciest store

**Output tables:**
- `canonical_product_daily_prices` — one row per matched product observation
- `canonical_product_daily_recommendations` — one row per canonical product per day with cheapest-store summary

---

## Full Pipeline Flow

```
Stores (FairPrice, RedMart, Cold Storage, Sheng Siong)
  │
  ▼ [E] Scrapers — 2:00–3:30 PM SGT
data/raw/<store>/<date>/
  │
  ▼ [T1] transform.py — 4:30 PM SGT (etl_transform_load DAG)
data/processed/<store>/<date>/
  │
  ▼ [L] load.py — 4:30 PM SGT (etl_transform_load DAG)
Supabase: products
  │
  ▼ [T2] matching scripts — 4:45 PM SGT (product_matching_pipeline DAG)
  │
  ├── matching.py × 5 categories ──────────────────┐
  ├── meat_produce_matching.py ────────────────────▶ canonical_products
  ├── vegetable_produce_matching.py ───────────────▶ canonical_product_members
  │                                                  product_match_candidates
  ├── commodity_matching.py ────────────────────────▶ commodity_price_comparisons
  │
  └── build_price_comparison_tables.py ────────────▶ canonical_product_daily_prices
                                                      canonical_product_daily_recommendations
                                                              │
                                                              ▼
                                                    Dashboard / Recommender
```

---

## File Structure

```
pipeline/
  etl/
    transform.py          # T1: clean raw → unified schema
    load.py               # L: load processed → Supabase products table
  matching/
    matching.py           # Algorithm 1A: branded products (beverages, dairy, etc.)
    meat_produce_matching.py      # Algorithm 1B: branded meat
    vegetable_produce_matching.py # Algorithm 1C: fresh produce
    commodity_matching.py         # Algorithm 2: commodity price comparison
  pricing/
    build_price_comparison_tables.py  # price summary tables
    price_comparison_preview.py       # top-50 preview export for demos
  schemas/
    matching_schema.sql                 # canonical_products, members, candidates
    commodity_matching_schema.sql       # commodity_price_comparisons
    price_comparison_schema.sql         # live views
    price_comparison_tables_schema.sql  # cached price tables
  dags/
    fairprice_dag.py        # scraper DAG
    shengsiong_dag.py       # scraper DAG
    redmart_dag.py          # scraper DAG
    coldstorage_dag.py      # scraper DAG
    etl_dag.py              # T1 + L DAG (waits for all 4 scrapers)
    matching_dag.py         # T2 DAG (waits for etl_dag)
    daily_pipeline_dag.py   # optional: single DAG for full pipeline
```

---

## Supabase Tables

| Table | Written by | Purpose |
|-------|-----------|---------|
| `products` | `load.py` | All scraped products, unified schema |
| `canonical_products` | matching scripts | One row per stable cross-store product identity |
| `canonical_product_members` | matching scripts | Links each product row to its canonical identity |
| `product_match_candidates` | matching scripts | Audit trail of all pairwise comparisons |
| `commodity_price_comparisons` | `commodity_matching.py` | Cheapest store per cut per day at same pack size |
| `canonical_product_daily_prices` | `build_price_comparison_tables.py` | Price per matched product per store per day |
| `canonical_product_daily_recommendations` | `build_price_comparison_tables.py` | Cheapest store per canonical product per day |
