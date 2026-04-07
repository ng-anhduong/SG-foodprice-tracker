-- Cached derived tables for dashboards and recommender features.
-- Run this in Supabase SQL Editor before using pipeline/pricing/build_price_comparison_tables.py.

create table if not exists public.canonical_product_daily_prices (
    id bigint generated always as identity primary key,
    canonical_product_id bigint not null references public.canonical_products(id) on delete cascade,
    canonical_key text not null,
    canonical_name text not null,
    canonical_brand text,
    unified_category text not null,
    size_total_value numeric,
    size_base_unit text,
    size_display text,
    pack_count integer,
    packaging text,
    variant_tokens jsonb not null default '[]'::jsonb,
    product_id bigint not null unique references public.products(id) on delete cascade,
    store text not null,
    store_product_name text not null,
    store_brand text,
    scraped_at timestamptz not null,
    scraped_date_sg date not null,
    price_sgd numeric,
    original_price_sgd numeric,
    discount_sgd numeric,
    unit text,
    product_url text,
    cheapest_price_for_day numeric,
    highest_price_for_day numeric,
    price_rank_for_day integer,
    price_gap_from_cheapest numeric,
    is_cheapest_for_day boolean not null default false,
    matched_store_count_for_day integer not null default 1,
    refreshed_at timestamptz not null default now()
);

create table if not exists public.canonical_product_daily_recommendations (
    id bigint generated always as identity primary key,
    canonical_product_id bigint not null references public.canonical_products(id) on delete cascade,
    canonical_key text not null,
    canonical_name text not null,
    canonical_brand text,
    unified_category text not null,
    size_total_value numeric,
    size_base_unit text,
    size_display text,
    pack_count integer,
    packaging text,
    variant_tokens jsonb not null default '[]'::jsonb,
    scraped_date_sg date not null,
    stores_seen_for_day integer not null,
    cheapest_store text,
    cheapest_price_sgd numeric,
    priciest_store text,
    priciest_price_sgd numeric,
    price_spread_sgd numeric,
    store_prices jsonb not null default '{}'::jsonb,
    refreshed_at timestamptz not null default now(),
    unique (canonical_product_id, scraped_date_sg)
);

create index if not exists idx_canonical_product_daily_prices_category_date
    on public.canonical_product_daily_prices(unified_category, scraped_date_sg);

create index if not exists idx_canonical_product_daily_prices_canonical_date
    on public.canonical_product_daily_prices(canonical_product_id, scraped_date_sg);

create index if not exists idx_canonical_product_daily_prices_store_date
    on public.canonical_product_daily_prices(store, scraped_date_sg);

create index if not exists idx_canonical_product_daily_recs_category_date
    on public.canonical_product_daily_recommendations(unified_category, scraped_date_sg);

create index if not exists idx_canonical_product_daily_recs_spread
    on public.canonical_product_daily_recommendations(price_spread_sgd desc);
