# =============================================================================
# SG Food Price Tracker — Product Clustering (K-Means)
#
# Goal: Segment products into price tiers based on pricing behaviour
# Output: canonical_product_id + price_tier + features → Supabase
# =============================================================================

# =============================================================================
# Libraries
# =============================================================================
import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from supabase import create_client, Client
from dotenv import load_dotenv
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

print(f"Python: {sys.executable}")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "ml")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================================================================
# Step 1: Fetch data from Supabase
# Fetch from canonical_product_daily_prices — one row per product per store per day
# =============================================================================
print("\n[1] Fetching data from Supabase...")

all_data = []
batch_size = 1000
start = 0

while True:
    response = (
        supabase
        .table("canonical_product_daily_prices")
        .select("""
            canonical_product_id,
            canonical_name,
            unified_category,
            store,
            scraped_date_sg,
            price_sgd,
            matched_store_count_for_day
        """)
        .range(start, start + batch_size - 1)
        .execute()
    )

    batch = response.data
    if not batch:
        break

    all_data.extend(batch)
    print(f"  Fetched rows {start} → {start + len(batch) - 1}")

    if len(batch) < batch_size:
        break

    start += batch_size

df_raw = pd.DataFrame(all_data)
print(f"  Total rows fetched: {len(df_raw)}")


# =============================================================================
# Step 2: Clean and engineer features
# =============================================================================
print("\n[2] Cleaning and engineering features...")

df_raw["price_sgd"] = pd.to_numeric(df_raw["price_sgd"], errors="coerce")
df_raw = df_raw.dropna(subset=["canonical_product_id", "canonical_name", "price_sgd"])

# Only include products seen across 2+ stores — single-store products
# cannot be meaningfully compared and skew the clustering
df_raw = df_raw[df_raw["matched_store_count_for_day"] >= 2]

# Aggregate to product level
prod_df = (
    df_raw.groupby(["canonical_product_id"])
    .agg(
        canonical_name=("canonical_name", "first"),
        unified_category=("unified_category", "first"),
        mean_price=("price_sgd", "mean"),
        median_price=("price_sgd", "median"),
        min_price=("price_sgd", "min"),
        max_price=("price_sgd", "max"),
        std_price=("price_sgd", "std"),
        num_observations=("price_sgd", "count"),
        num_stores=("store", "nunique"),
    )
    .reset_index()
)

#After aggregation make final clustered products comparable across stores
prod_df = prod_df[prod_df["num_stores"] >= 2]

# Fill std for single-observation products
prod_df["std_price"] = prod_df["std_price"].fillna(0)

# Price range — absolute spread across all observations
prod_df["price_range"] = prod_df["max_price"] - prod_df["min_price"]

# Coefficient of variation — relative volatility (std / mean)
# High CV = price changes a lot relative to its average
# Low CV = stable pricing
prod_df["cv"] = prod_df["std_price"] / prod_df["mean_price"]
prod_df["cv"] = prod_df["cv"].fillna(0)

print(f"  Products after cleaning: {len(prod_df)}")
print(f"  Categories: {prod_df['unified_category'].nunique()}")
print(prod_df[["mean_price", "price_range", "cv", "num_stores"]].describe().round(3))


# =============================================================================
# Step 3: Feature selection and scaling
# Features chosen:
#   mean_price    — overall price level (budget vs premium)
#   price_range   — spread between cheapest and priciest store
#   cv            — price volatility relative to mean
#   num_stores    — how widely available the product is
# =============================================================================
print("\n[3] Scaling features...")

feature_cols = ["mean_price", "price_range", "cv", "num_stores"]
X = prod_df[feature_cols].copy()

scaler = StandardScaler()
scaled_X = scaler.fit_transform(X)

# =============================================================================
# Step 4: Elbow method + Silhouette score to find optimal k
# =============================================================================
print("\n[4] Finding optimal number of clusters...")

kmin, kmax = 2, 9
wcss = []
silhouette_scores = []

for k in range(kmin, kmax):
    km = KMeans(n_clusters=k, init="k-means++", random_state=42, n_init=10)
    labels = km.fit_predict(scaled_X)
    wcss.append(km.inertia_)
    sil = silhouette_score(scaled_X, labels)
    silhouette_scores.append(sil)
    print(f"  k={k}  WCSS={km.inertia_:.1f}  Silhouette={sil:.4f}")

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

axes[0].plot(range(kmin, kmax), wcss, marker="o", color="#F5821F", linewidth=2)
axes[0].scatter(range(kmin, kmax), wcss, color="#F5821F", s=80, zorder=5)
axes[0].set_title("Elbow Method", fontsize=14, fontweight="bold")
axes[0].set_xlabel("Number of Clusters (k)")
axes[0].set_ylabel("WCSS")
axes[0].set_xticks(range(kmin, kmax))
axes[0].spines["top"].set_visible(False)
axes[0].spines["right"].set_visible(False)

axes[1].plot(range(kmin, kmax), silhouette_scores, marker="o", color="#005BAC", linewidth=2)
axes[1].scatter(range(kmin, kmax), silhouette_scores, color="#005BAC", s=80, zorder=5)
axes[1].set_title("Silhouette Score", fontsize=14, fontweight="bold")
axes[1].set_xlabel("Number of Clusters (k)")
axes[1].set_ylabel("Silhouette Score (higher = better)")
axes[1].set_xticks(range(kmin, kmax))
axes[1].spines["top"].set_visible(False)
axes[1].spines["right"].set_visible(False)

plt.suptitle("Optimal k Selection", fontsize=16, fontweight="bold", y=1.02)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, "elbow_silhouette.png"), dpi=150, bbox_inches="tight")
plt.show()
print("  Saved → data/ml/elbow_silhouette.png")


# =============================================================================
# Step 5: Train final K-Means model
# k=4 gives interpretable tiers: Budget / Lower Mid / Upper Mid / Premium
# Adjust if elbow/silhouette suggest differently
# =============================================================================
print("\n[5] Training final K-Means model...")

K_FINAL = 4
kmeans = KMeans(n_clusters=K_FINAL, init="k-means++", random_state=42, n_init=10)
y_kmeans = kmeans.fit_predict(scaled_X)
prod_df["cluster"] = y_kmeans

final_sil = silhouette_score(scaled_X, y_kmeans)
print(f"  Final silhouette score (k={K_FINAL}): {final_sil:.4f}")


# =============================================================================
# Step 6: Label clusters by mean price (ascending)
# Sort cluster centers by mean_price to get stable labels
# =============================================================================
print("\n[6] Labeling clusters...")

cluster_summary = (
    prod_df.groupby("cluster")[feature_cols]
    .mean()
    .sort_values("mean_price")
)
print(cluster_summary.round(3))

cluster_order = cluster_summary.index.tolist()
tier_labels = {
    cluster_order[0]: "Budget",
    cluster_order[1]: "Lower Mid-range",
    cluster_order[2]: "Upper Mid-range",
    cluster_order[3]: "Premium",
}
prod_df["price_tier"] = prod_df["cluster"].map(tier_labels)

# Sanity check
print("\nCluster validation:")
print(prod_df.groupby("price_tier")[["mean_price", "price_range", "cv", "num_stores"]].mean().round(3))
print("\nProducts per tier:")
print(prod_df["price_tier"].value_counts())


# =============================================================================
# Step 7: Visualise clusters — Mean Price vs Price Range
# Color by tier, not raw cluster number
# =============================================================================
print("\n[7] Visualising clusters...")

TIER_COLORS = {
    "Budget": "#00843D",
    "Lower Mid-range": "#005BAC",
    "Upper Mid-range": "#F5821F",
    "Premium": "#C8102E",
}

unscaled_X = scaler.inverse_transform(scaled_X)
cluster_centers_unscaled = scaler.inverse_transform(kmeans.cluster_centers_)

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Plot 1: Mean Price vs Price Range
for tier, color in TIER_COLORS.items():
    mask = prod_df["price_tier"] == tier
    axes[0].scatter(
        unscaled_X[mask, 0],   # mean_price
        unscaled_X[mask, 1],   # price_range
        s=30, c=color, alpha=0.5, label=tier
    )

for i, center in enumerate(cluster_centers_unscaled):
    axes[0].scatter(
        center[0], center[1],
        s=200, marker="X", color="black", zorder=10
    )

axes[0].set_title("Mean Price vs Price Range", fontsize=13, fontweight="bold")
axes[0].set_xlabel("Mean Price (SGD)")
axes[0].set_ylabel("Price Range (SGD)")
axes[0].legend(title="Price Tier", fontsize=10)
axes[0].spines["top"].set_visible(False)
axes[0].spines["right"].set_visible(False)

# Plot 2: Mean Price vs Coefficient of Variation
for tier, color in TIER_COLORS.items():
    mask = prod_df["price_tier"] == tier
    axes[1].scatter(
        unscaled_X[mask, 0],   # mean_price
        unscaled_X[mask, 2],   # cv
        s=30, c=color, alpha=0.5, label=tier
    )

for i, center in enumerate(cluster_centers_unscaled):
    axes[1].scatter(
        center[0], center[2],
        s=200, marker="X", color="black", zorder=10
    )

axes[1].set_title("Mean Price vs Price Volatility (CV)", fontsize=13, fontweight="bold")
axes[1].set_xlabel("Mean Price (SGD)")
axes[1].set_ylabel("Coefficient of Variation (CV)")
axes[1].legend(title="Price Tier", fontsize=10)
axes[1].spines["top"].set_visible(False)
axes[1].spines["right"].set_visible(False)

plt.suptitle("Product Price Tier Clusters", fontsize=16, fontweight="bold", y=1.02)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, "clusters_visualisation.png"), dpi=150, bbox_inches="tight")
plt.show()
print("  Saved → data/ml/clusters_visualisation.png")


# =============================================================================
# Step 8: Category breakdown per tier
# Shows which categories dominate each tier
# =============================================================================
print("\n[8] Category breakdown per tier...")

cat_tier = (
    prod_df.groupby(["price_tier", "unified_category"])
    .size()
    .reset_index(name="count")
)
cat_pivot = cat_tier.pivot(index="unified_category", columns="price_tier", values="count").fillna(0)
cat_pivot_pct = cat_pivot.div(cat_pivot.sum(axis=1), axis=0) * 100

tier_order = ["Budget", "Lower Mid-range", "Upper Mid-range", "Premium"]
cat_pivot_pct = cat_pivot_pct[[t for t in tier_order if t in cat_pivot_pct.columns]]

fig, ax = plt.subplots(figsize=(12, 6))
bottom = np.zeros(len(cat_pivot_pct))
colors = [TIER_COLORS[t] for t in cat_pivot_pct.columns]

for col, color in zip(cat_pivot_pct.columns, colors):
    vals = cat_pivot_pct[col].values
    bars = ax.bar(cat_pivot_pct.index, vals, bottom=bottom, color=color,
                  label=col, alpha=0.9, edgecolor="white", linewidth=0.5)
    for bar, val, bot in zip(bars, vals, bottom):
        if val >= 8:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bot + val / 2,
                f"{val:.0f}%",
                ha="center", va="center",
                fontsize=9, color="white", fontweight="bold"
            )
    bottom += vals

ax.set_title("Price Tier Distribution by Category", fontsize=14, fontweight="bold")
ax.set_ylabel("Share of Products (%)")
ax.set_xlabel("")
ax.set_ylim(0, 105)
ax.legend(title="Price Tier", bbox_to_anchor=(1.01, 1), loc="upper left")
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
plt.xticks(rotation=25, ha="right")
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, "tier_by_category.png"), dpi=150, bbox_inches="tight")
plt.show()
print("  Saved → data/ml/tier_by_category.png")


# =============================================================================
# Step 9: Shopping recommendation flags
# Tag each product with actionable advice based on tier + CV
# =============================================================================
print("\n[9] Generating shopping recommendation flags...")

CV_VOLATILE_THRESHOLD = 0.15  # CV above this = worth comparing stores

def shopping_advice(row):
    tier = row["price_tier"]
    cv = row["cv"]
    stores = row["num_stores"]

    if stores < 2:
        return "Only available at one store — no comparison possible"

    if tier == "Budget":
        if cv > CV_VOLATILE_THRESHOLD:
            return "Budget product with volatile pricing — compare stores before buying"
        else:
            return "Budget product with stable pricing — consistent across stores"

    elif tier == "Lower Mid-range":
        if cv > CV_VOLATILE_THRESHOLD:
            return "Mid-range product with some price variation — worth checking stores"
        else:
            return "Mid-range product with stable pricing — similar across stores"

    elif tier == "Upper Mid-range":
        if cv > CV_VOLATILE_THRESHOLD:
            return "Higher-priced product with noticeable price differences — compare stores"
        else:
            return "Higher-priced product with stable pricing"

    elif tier == "Premium":
        if cv > CV_VOLATILE_THRESHOLD:
            return "Premium product with significant price variation — always compare stores"
        else:
            return "Premium product with consistent pricing across stores"

    return "No advice available"

prod_df["shopping_advice"] = prod_df.apply(shopping_advice, axis=1)

# Summary of advice distribution
print(prod_df["shopping_advice"].value_counts())

# Sample output
print("\nSample products with advice:")
print(
    prod_df[["canonical_name", "unified_category", "mean_price",
             "price_range", "cv", "price_tier", "shopping_advice"]]
    .sort_values("price_range", ascending=False)
    .head(15)
    .to_string(index=False)
)


# =============================================================================
# Step 10: Sync results to Supabase
# Creates/updates a product_clusters table
# Run pipeline/schemas/product_clusters_schema.sql first if table doesn't exist
# =============================================================================
print("\n[10] Syncing to Supabase...")

output_cols = [
    "canonical_product_id",
    "price_tier",
    "mean_price",
    "median_price",
    "min_price",
    "max_price",
    "std_price",
    "price_range",
    "cv",
    "num_observations",
    "num_stores",
    "shopping_advice",
]

cluster_output = prod_df[output_cols].copy()

# Round floats
for col in ["mean_price", "median_price", "min_price", "max_price",
            "std_price", "price_range", "cv"]:
    cluster_output[col] = cluster_output[col].round(4)

records = cluster_output.to_dict(orient="records")
cluster_output = cluster_output.drop_duplicates(subset=["canonical_product_id"], keep="first")

BATCH_SIZE = 200
for i in range(0, len(records), BATCH_SIZE):
    batch = records[i:i + BATCH_SIZE]
    supabase.table("product_clusters").upsert(
        batch,
        on_conflict="canonical_product_id"
    ).execute()
    print(f"  Upserted rows {i} → {i + len(batch) - 1}")

print(f"\nDone. {len(records)} products synced to Supabase table: product_clusters")


# =============================================================================
# Step 11: Final summary
# =============================================================================
print("\n" + "=" * 60)
print("CLUSTERING SUMMARY")
print("=" * 60)
print(f"Total products clustered:  {len(prod_df):,}")
print(f"Silhouette score (k={K_FINAL}):   {final_sil:.4f}")
print()
print("Tier breakdown:")
for tier in tier_order:
    n = (prod_df["price_tier"] == tier).sum()
    pct = n / len(prod_df) * 100
    print(f"  {tier:<20} {n:>5,} products  ({pct:.1f}%)")

print()
print("Tier characteristics (mean values):")
print(
    prod_df.groupby("price_tier")[["mean_price", "price_range", "cv", "num_stores"]]
    .mean()
    .round(3)
    .to_string()
)
print()
print("Outputs saved:")
print("  data/ml/elbow_silhouette.png")
print("  data/ml/clusters_visualisation.png")
print("  data/ml/tier_by_category.png")
print("  Supabase: product_clusters table")