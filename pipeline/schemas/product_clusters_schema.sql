
CREATE TABLE IF NOT EXISTS product_clusters (
    id                  bigserial PRIMARY KEY,
    canonical_product_id bigint NOT NULL UNIQUE,
    price_tier          text NOT NULL,
    mean_price          numeric,
    median_price        numeric,
    min_price           numeric,
    max_price           numeric,
    std_price           numeric,
    price_range         numeric,
    cv                  numeric,
    num_observations    integer,
    num_stores          integer,
    shopping_advice     text,
    refreshed_at        timestamptz DEFAULT now()
);