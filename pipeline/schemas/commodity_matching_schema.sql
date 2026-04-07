-- Commodity price comparison table for Meat & Seafood and Fruits & Vegetables.
-- Run this once in the Supabase SQL Editor before running pipeline/matching/commodity_matching.py.

create table if not exists public.commodity_price_comparisons (
    id bigint generated always as identity primary key,
    cut text not null,
    frozen_flag text not null,                  -- 'fresh/chilled' or 'frozen'
    unified_category text not null,             -- 'Meat & Seafood' or 'Fruits & Vegetables'
    stores_seen integer not null,
    cheapest_store text not null,
    cheapest_unit_price_per_100g numeric not null,
    cheapest_product_name text,
    priciest_store text not null,
    priciest_unit_price_per_100g numeric not null,
    price_spread_per_100g numeric not null,
    store_prices jsonb not null default '{}'::jsonb,
    scraped_date date not null,
    refreshed_at timestamptz not null default now(),
    unique (cut, frozen_flag, scraped_date)
);

create index if not exists idx_commodity_category
    on public.commodity_price_comparisons(unified_category);

create index if not exists idx_commodity_cut
    on public.commodity_price_comparisons(cut);

create index if not exists idx_commodity_date
    on public.commodity_price_comparisons(scraped_date);

create index if not exists idx_commodity_cheapest_store
    on public.commodity_price_comparisons(cheapest_store);
