# =============================================================================
# SG Food Price Tracker — Price Anomaly Detection
# Goal: Identify product with unusual price
# Output: anomaly flags + anomly scores + charts
# =============================================================================

# =============================================================================
# Libraries
# =============================================================================
import os
import numpy as np
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from sklearn.ensemble import IsolationForest
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

    # =============================================================================
    # Step 3: Run Isolation Forest
    # =============================================================================
    features = ["mean_price", "price_range", "cv", "num_stores"]
    X = prod_df[features].copy()

    isolation = IsolationForest(
        n_estimators=200,
        contamination=0.03,   #note: 3% anomalies
        random_state=42
    )

    prod_df["anomaly_raw"] = isolation.fit_predict(X)
    prod_df["anomaly_score"] = isolation.decision_function(X)

    # Convert to readable labels
    prod_df["anomaly_flag"] = prod_df["anomaly_raw"].map({
        1: "Normal",
        -1: "Anomalous"
    })

    # =============================================================================
    # Step 4: Statistics Summary
    # =============================================================================
    #Print total product + Anomalies flagged + Normal price product + Avg anomaly price range
    print("\nAnomaly flag counts:")
    print(prod_df["anomaly_flag"].value_counts())

    total = len(prod_df)
    anomalyCount = (prod_df["anomaly_flag"] == "Anomalous").sum()
    normalCount  = (prod_df["anomaly_flag"] == "Normal").sum()
    avg_anomaly_range = prod_df[prod_df["anomaly_flag"]=="Anomalous"]["price_range"].mean()
    avg_normal_range = prod_df[prod_df["anomaly_flag"]=="Normal"]["price_range"].mean()

    print(f"Total products: {total}")
    print(f"Anomalies flagged: {anomalyCount} ({anomalyCount/total*100:.1f}%)")
    print(f"Normal products: {normalCount}")
    print(f"Average anomaly price range: ${avg_anomaly_range:.2f}")
    print(f"Average normal price range : ${avg_normal_range:.2f}")

    # =============================================================================
    # Step 5: Show top anomalous products
    # =============================================================================
    print("\n[4a] Top anomalous products")

    top_anomalies = (
        prod_df.loc[prod_df["anomaly_flag"] == "Anomalous", [
            "canonical_product_id",
            "canonical_name",
            "unified_category",
            "mean_price",
            "price_range",
            "cv",
            "num_stores",
            "anomaly_score"
        ]]
        .sort_values("anomaly_score")
        .head(20)
    )

    print(top_anomalies.round(2).to_string(index=False))

    print("\n[4b] Anomaly summary table")

    anomaly_summary = (
        prod_df.groupby("anomaly_flag")
        .agg(
            count=("canonical_product_id", "count"),
            avg_mean_price=("mean_price", "mean"),
            avg_price_range=("price_range", "mean"),
            avg_cv=("cv", "mean"),
            avg_num_stores=("num_stores", "mean")
        )
        .round(2)
    )

    print(anomaly_summary.to_string())

    # =============================================================================
    # Step 6: Bar Chart of Anomalies by Category
    # =============================================================================
    # list of category
    categories = sorted(prod_df["unified_category"].unique())
    anomaly_category = (
        prod_df[prod_df["anomaly_flag"] == "Anomalous"]
        .groupby("unified_category")
        .size()
        .sort_values(ascending=False)
    )

    #Different color for different categories
    color_cat = {
        "Fruits & Vegetables": "red",
        "Staples": "blue",
        "Beverages": "green",
        "Meat & Seafood": "orange",
        "Dairy & Eggs": "purple",
        "Snacks": "pink",
        "Household": "yellow"
    }
    colors_bar = [color_cat.get(cat, "#95a5a6") for cat in anomaly_category.index]

    # No. of anomaly product by category
    print("\nAnomalies by category:")
    print(anomaly_category)

    #Plot Bar Chart
    fig, ax = plt.subplots(figsize=(9, 5))
    anomaly_category.plot(kind="barh",
                        ax=ax,
                        color=colors_bar,
                        edgecolor="white")

    for x, y in enumerate(anomaly_category.values):
        ax.text(y + 0.05, x, str(y), va="center")
    ax.set_title("Anomalies by Category", fontsize=18, fontweight="bold")
    ax.set_xlabel("Number of anomalous products")
    ax.set_ylabel("Category")

    plt.tight_layout()

    # =============================================================================
    # Step 7: Anomalies identification on Scatter
    # =============================================================================
    print("\n[5] Visualising anomalies...")

    normal = prod_df["anomaly_flag"] == "Normal"
    anomaly = prod_df["anomaly_flag"] == "Anomalous"

    plt.figure(figsize=(8, 6))

    plt.scatter(
        prod_df.loc[normal, "mean_price"],
        prod_df.loc[normal, "price_range"],
        s=20,
        alpha=0.4,
        label="Normal"
    )

    plt.scatter(
        prod_df.loc[anomaly, "mean_price"],
        prod_df.loc[anomaly, "price_range"],
        s=45,
        alpha=0.9,
        label="Anomalous"
    )

    plt.title("Anomaly Detection: Mean Price vs Price Range")
    plt.xlabel("Mean Price (SGD)")
    plt.ylabel("Price Range (SGD)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "anomaly_scatter.png"), dpi=150, bbox_inches="tight")

    # =============================================================================
    # Step 8: Saving anomaly results
    # =============================================================================
    print("\n[6] Saving full anomaly results...")

    anomaly_cols = [
        "canonical_product_id",
        "canonical_name",
        "unified_category",
        "mean_price",
        "median_price",
        "min_price",
        "max_price",
        "std_price",
        "price_range",
        "cv",
        "num_observations",
        "num_stores",
        "anomaly_score",
        "anomaly_flag"
    ]

    anomaly_output = prod_df[anomaly_cols].copy()
    anomaly_output.to_csv(os.path.join(OUTPUT_DIR, "anomaly_results.csv"), index=False)

    # =============================================================================
    # Step 9: Final summary
    # =============================================================================
    print("\n" + "=" * 60)
    print("ANOMALY DETECTION SUMMARY")
    print("=" * 60)
    print(f"Total products: {len(prod_df):,}")
    print(prod_df["anomaly_flag"].value_counts().to_string())
    print()

if __name__ == "__main__":
    run()