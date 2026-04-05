-- Matching tables for stable cross-store product identities.
-- Run this in Supabase SQL Editor before enabling DB sync from pipeline/matching.py.

create table if not exists public.canonical_products (
    id bigint generated always as identity primary key,
    canonical_key text not null unique,
    canonical_name text not null,
    brand text,
    unified_category text not null,
    size_total_value numeric,
    size_base_unit text,
    size_display text,
    pack_count integer,
    packaging text,
    variant_tokens jsonb not null default '[]'::jsonb,
    member_count integer not null default 0,
    stores_present jsonb not null default '[]'::jsonb,
    first_seen_at timestamptz,
    last_seen_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.canonical_product_members (
    id bigint generated always as identity primary key,
    canonical_product_id bigint not null references public.canonical_products(id) on delete cascade,
    product_id bigint not null unique references public.products(id) on delete cascade,
    store text not null,
    name text not null,
    brand text,
    price_sgd numeric,
    unit text,
    product_url text,
    scraped_at timestamptz,
    run_key text,
    match_source text not null default 'rule_based_v1',
    match_confidence numeric,
    matched_at timestamptz not null default now()
);

create table if not exists public.product_match_candidates (
    id bigint generated always as identity primary key,
    run_key text not null,
    unified_category text not null,
    product_id_a bigint not null references public.products(id) on delete cascade,
    product_id_b bigint not null references public.products(id) on delete cascade,
    store_a text not null,
    store_b text not null,
    name_a text not null,
    name_b text not null,
    brand_a text,
    brand_b text,
    size_a text,
    size_b text,
    variant_a jsonb not null default '[]'::jsonb,
    variant_b jsonb not null default '[]'::jsonb,
    brand_score numeric not null,
    size_score numeric not null,
    title_score numeric not null,
    variant_score numeric not null,
    packaging_score numeric not null,
    match_score numeric not null,
    match_status text not null,
    explanation text,
    created_at timestamptz not null default now(),
    unique (run_key, product_id_a, product_id_b)
);

create index if not exists idx_canonical_products_category
    on public.canonical_products(unified_category);

create index if not exists idx_canonical_products_key
    on public.canonical_products(canonical_key);

create index if not exists idx_canonical_product_members_product_id
    on public.canonical_product_members(product_id);

create index if not exists idx_canonical_product_members_canonical_id
    on public.canonical_product_members(canonical_product_id);

create index if not exists idx_product_match_candidates_run_key
    on public.product_match_candidates(run_key);

create index if not exists idx_product_match_candidates_status
    on public.product_match_candidates(match_status);
