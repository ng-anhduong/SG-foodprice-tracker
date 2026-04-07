Pipeline structure:

- `pipeline/dags/`
  Airflow orchestration.
- `pipeline/etl/`
  Transform and load steps that populate the `products` table.
- `pipeline/matching/`
  Category-specific matching logic for packaged goods, meat, vegetables, and commodity comparisons.
- `pipeline/pricing/`
  Cached daily price-table refresh and preview/export utilities.
- `pipeline/schemas/`
  Supabase SQL setup files for matching and pricing tables/views.
- `pipeline/docs/`
  Local design notes and algorithm documentation.
