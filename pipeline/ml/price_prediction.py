# =============================================================================
# SG Food Price Tracker — Price Prediction
# Model: Random Forest Regressor
# Output: feature importance chart + predicted and actual chart
# =============================================================================

# =============================================================================
# Libraries
# =============================================================================
import os
import numpy as np
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def run():
    load_dotenv()

    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")

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

    #Outlier removal
    prod_df = prod_df.reset_index(drop=True)

    def iqr(series, multiplier=3.0):
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        return q3 + multiplier * (q3 - q1)

    mean_cap = iqr(prod_df["mean_price"])
    range_cap = iqr(prod_df["price_range"])
    cv_cap = iqr(prod_df["cv"])

    outliers = (
        (prod_df["mean_price"] > mean_cap) |
        (prod_df["price_range"] > range_cap) |
        (prod_df["cv"] > cv_cap)
    )

    print(f"  No. of removed before training: {outliers.sum()}")
    prod_df = prod_df[~outliers].reset_index(drop=True)
    print(f"  No. of products after outlier removal: {len(prod_df)}")

    # =============================================================================
    # Step 3: Train Random Forest Model
    # =============================================================================

    # Encode cat
    prod_df["category_enc"] = prod_df["unified_category"].astype("category").cat.codes

    FEATURES = ["price_range", "cv", "num_stores", "num_observations", "category_enc"]
    X = prod_df[FEATURES]
    y = prod_df["mean_price"]

    # Splitting for train-test, 80-20
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Train model
    randomF = RandomForestRegressor(n_estimators=100, random_state=42)
    randomF.fit(X_train, y_train)

    # =============================================================================
    # Step 4: Evaluate
    # =============================================================================
    y_pred = randomF.predict(X_test) 
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print(f"MAE (Test):${mae:.3f}")
    print(f"R² (Test): {r2:.4f}")

    # =============================================================================
    # Step 5: Add Feature Importance bar chart
    # =============================================================================
    # Which var does Random Forest relied on most to predict mean price?
    print("\n[5] Plot feature importance bar chart")

    feat = ["Price Range", "CV (Volatility)", "Num Stores", "Num Observations", "Category"]
    sort_importances = randomF.feature_importances_

    # sort features by importance score from small to big
    score  = np.argsort(sort_importances)

    names = []

    for f in score:
        names.append(feat[f])

    importances = sort_importances[score]

    colors = ["#1F77B4"] * len(importances)
    colors[-1] = "orange"

    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.barh(names, importances, color=colors, edgecolor="white")
    ax.set_title("Price Prediction by Feature Importance (Random Forest) ", fontsize=12, fontweight="bold")
    ax.set_xlabel("Importance Score")
    for bar, value in zip(bars, importances):
        ax.text(value + 0.002, bar.get_y() + bar.get_height() / 2,
                f"{value:.3f}", va="center", fontsize=9)
        
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "feature_importance.png"), dpi=150, bbox_inches="tight")

    # =============================================================================
    # Step 6: Predicted vs Actual price scatter
    # =============================================================================
    # How close predicted prices are to the actual prices?
    print("\n[6] Plot predicted vs actual price")

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.scatter(y_test, y_pred, alpha=0.5, s=25, color="#005BAC", label="Products")
    min_v = min(y_test.min(), y_pred.min())
    max_v = max(y_test.max(), y_pred.max())
    limits = [min_v, max_v]
    ax.plot(limits, limits, "r--", linewidth=1.5, label="Prediction")
    ax.set_title(f"Predicted vs Actual Mean Price\nMAE=${mae:.2f}  R²={r2:.3f}",
                fontsize=13, fontweight="bold")
    ax.set_xlabel("Actual Mean Price (SGD)")
    ax.set_ylabel("Predicted Mean Price (SGD)")
    ax.legend(fontsize=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "actual_vs_predicted.png"), dpi=150, bbox_inches="tight")

    # =============================================================================
    # Step 8: Summary
    # =============================================================================
    print("\n" + "=" * 60)
    print("PRICE PREDICTION SUMMARY")
    print("=" * 60)
    print(f"No. of Products for training: {len(X_train)}")
    print(f"No. of Products for testing: {len(X_test)}")
    print(f"MAE (Test): ${mae:.3f}")
    print(f"R²(Test): {r2:.4f}")
    print()

if __name__ == "__main__":
    run()

