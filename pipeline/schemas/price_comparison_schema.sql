-- Derived price comparison views for canonical matched products.
-- Run this in Supabase SQL Editor after canonical matching tables are populated.
-- These are useful for development and ad hoc querying, but heavier than the
-- downstream cached tables built by pipeline/pricing/build_price_comparison_tables.py.

create index if not exists idx_products_category_store_scraped
    on public.products(unified_category, store, scraped_at);

create index if not exists idx_products_scraped_store
    on public.products(scraped_at, store);

create index if not exists idx_canonical_members_canonical_store
    on public.canonical_product_members(canonical_product_id, store);

create or replace view public.canonical_product_price_observations as
with base as (
    select
        cp.id as canonical_product_id,
        cp.canonical_key,
        cp.canonical_name,
        cp.brand as canonical_brand,
        cp.unified_category,
        cp.size_total_value,
        cp.size_base_unit,
        cp.size_display,
        cp.pack_count,
        cp.packaging,
        cp.variant_tokens,
        cm.id as canonical_member_id,
        cm.product_id,
        p.name as store_product_name,
        p.brand as store_brand,
        p.store,
        p.scraped_at,
        (p.scraped_at at time zone 'Asia/Singapore')::date as scraped_date_sg,
        p.price_sgd,
        p.original_price_sgd,
        p.discount_sgd,
        p.unit,
        p.product_url
    from public.canonical_product_members cm
    join public.canonical_products cp
        on cp.id = cm.canonical_product_id
    join public.products p
        on p.id = cm.product_id
)
select
    base.*,
    min(price_sgd) over (
        partition by canonical_product_id, scraped_date_sg
    ) as cheapest_price_for_day,
    max(price_sgd) over (
        partition by canonical_product_id, scraped_date_sg
    ) as highest_price_for_day,
    rank() over (
        partition by canonical_product_id, scraped_date_sg
        order by price_sgd asc nulls last, store asc
    ) as price_rank_for_day,
    price_sgd - min(price_sgd) over (
        partition by canonical_product_id, scraped_date_sg
    ) as price_gap_from_cheapest,
    case
        when price_sgd = min(price_sgd) over (
            partition by canonical_product_id, scraped_date_sg
        ) then true
        else false
    end as is_cheapest_for_day,
    count(*) over (
        partition by canonical_product_id, scraped_date_sg
    ) as matched_store_count_for_day
from base;


create or replace view public.canonical_product_daily_summary as
with ranked as (
    select
        *,
        row_number() over (
            partition by canonical_product_id, scraped_date_sg
            order by price_sgd asc nulls last, store asc
        ) as cheapest_row_num,
        row_number() over (
            partition by canonical_product_id, scraped_date_sg
            order by price_sgd desc nulls last, store asc
        ) as priciest_row_num
    from public.canonical_product_price_observations
),
price_map as (
    select
        canonical_product_id,
        scraped_date_sg,
        jsonb_object_agg(
            store,
            jsonb_build_object(
                'product_id', product_id,
                'store_product_name', store_product_name,
                'price_sgd', price_sgd,
                'original_price_sgd', original_price_sgd,
                'discount_sgd', discount_sgd,
                'unit', unit,
                'product_url', product_url,
                'price_gap_from_cheapest', price_gap_from_cheapest,
                'is_cheapest_for_day', is_cheapest_for_day
            )
        ) as store_prices
    from ranked
    group by canonical_product_id, scraped_date_sg
)
select
    r.canonical_product_id,
    r.canonical_key,
    r.canonical_name,
    r.canonical_brand,
    r.unified_category,
    r.size_total_value,
    r.size_base_unit,
    r.size_display,
    r.pack_count,
    r.packaging,
    r.variant_tokens,
    r.scraped_date_sg,
    r.matched_store_count_for_day as stores_seen_for_day,
    c.store as cheapest_store,
    c.price_sgd as cheapest_price_sgd,
    p.store as priciest_store,
    p.price_sgd as priciest_price_sgd,
    (p.price_sgd - c.price_sgd) as price_spread_sgd,
    m.store_prices
from (
    select distinct
        canonical_product_id,
        canonical_key,
        canonical_name,
        canonical_brand,
        unified_category,
        size_total_value,
        size_base_unit,
        size_display,
        pack_count,
        packaging,
        variant_tokens,
        scraped_date_sg,
        matched_store_count_for_day
    from ranked
) r
join ranked c
    on c.canonical_product_id = r.canonical_product_id
   and c.scraped_date_sg = r.scraped_date_sg
   and c.cheapest_row_num = 1
join ranked p
    on p.canonical_product_id = r.canonical_product_id
   and p.scraped_date_sg = r.scraped_date_sg
   and p.priciest_row_num = 1
join price_map m
    on m.canonical_product_id = r.canonical_product_id
   and m.scraped_date_sg = r.scraped_date_sg;
