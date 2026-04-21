# =============================================================================
# SG Food Price Tracker — Price Prediction
# Model: Random Forest Regressor
# Output: predicted and actual chart + residuals chart + prediction results table
# =============================================================================

# =============================================================================
# Libraries
# =============================================================================
import os
import numpy as np
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error

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
    # Convert data types (price - numeric, date - date format)
    df_raw["price_sgd"] = pd.to_numeric(df_raw["price_sgd"], errors="coerce")
    df_raw["scraped_date_sg"] = pd.to_datetime(df_raw["scraped_date_sg"], errors="coerce")
    
    #Remove rows wih missing info
    df_raw = df_raw.dropna(subset=["canonical_product_id", 
                           "canonical_name",
                           "unified_category",
                            "store",
                            "scraped_date_sg",
                            "price_sgd"
                            ])
    
    # Only include products seen across 2+ stores
    df_raw = df_raw[df_raw["matched_store_count_for_day"] >= 2].copy()

    # Sort by product, store, date
    df = df_raw.sort_values(["canonical_product_id", "store", "scraped_date_sg"]).reset_index(drop=True)

    # create date features in dataset
    df["month"] = df["scraped_date_sg"].dt.month
    df["day_week"] = df["scraped_date_sg"].dt.dayofweek
    df["day_month"] = df["scraped_date_sg"].dt.day

    # grouping by same product in same store
    grouped_prices = df.groupby(["canonical_product_id", "store"])["price_sgd"]

    # for each product, take price from previous recorded price
    df["move_price_1"] = grouped_prices.shift(1)

    # mean price of 3 same product in same store from previous record (maybe prev day if there is data)
    df["mean_3"] = (
        df.groupby(["canonical_product_id", "store"])["move_price_1"]
        .transform(lambda x: x.rolling(3, min_periods=1).mean())
    )

    # drop rows without enough historical data
    df = df.dropna(subset=["move_price_1", "mean_3"]).copy()

    #Remove extreme price outliers
    q1 = df["price_sgd"].quantile(0.25)
    q3 = df["price_sgd"].quantile(0.75)
    upper_cap = q3 + 3.0 * (q3 - q1)
    df = df[df["price_sgd"] <= upper_cap].copy()

    print(f"Rows after feature engineering: {len(df)}")
    print(f"No. of products: {df['canonical_product_id'].nunique()}")

    # =============================================================================
    # Step 3: Train random forest model
    # =============================================================================
    # Label encoding for 7 categories & 4 stores
    df["cat_encode"] = df["unified_category"].astype("category").cat.codes
    df["store_encode"] = df["store"].astype("category").cat.codes

    FEATURES = ["cat_encode", 
                "store_encode", 
                "month","day_week",
                "day_month",
                "move_price_1",
                "mean_3"
            ]
    X = df[FEATURES]
    y = df["price_sgd"] #results to be price

    # Splitting for train-test, 80-20
    split_index = int(len(df) * 0.8)

    #first 80% for training
    X_train = X.iloc[:split_index]
    y_train = y.iloc[:split_index]
    #another 20% for testing
    X_test = X.iloc[split_index:]
    y_test = y.iloc[split_index:]

    # Train model with Random Forest Regression
    # collection of decision trees, each tree make prediction, final prediction is avg of all trees
    model = RandomForestRegressor(
        n_estimators=200,   #create 200 trees
        max_depth=12,       #deepness of tree can grow
        min_samples_leaf=2, #at least 2 row of data 
        random_state=42,     #random
    )
    model.fit(X_train, y_train)

    # =============================================================================
    # Step 4: Evaluate
    # =============================================================================
    # Fit model and predict on test set
    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    rmse = mean_squared_error(y_test, y_pred) ** 0.5
    r2 = r2_score(y_test, y_pred)

    print(f"MAE(Test): ${mae:.3f}")
    print(f"RMSE(Test): ${rmse:.3f}")
    print(f"R²(Test): {r2:.4f}")

    # =============================================================================
    # Step 5: Predicted vs Actual price scatter
    # =============================================================================
    print("\n[9] Plot predicted vs actual scatter plot")

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.scatter(y_test, y_pred, alpha=0.5, s=25, label="Products")

    min_v = min(y_test.min(), y_pred.min())
    max_v = max(y_test.max(), y_pred.max())
    limits = [min_v, max_v]

    title = (f"Predicted vs Actual Price\n"
             f"MAE = ${mae:.2f}   RMSE = ${rmse:.2f}   R² = {r2:.3f}"
        )
    ax.set_title(title, fontsize=13, fontweight="bold")
    ax.plot(limits, limits, "r--", linewidth=1.5, label="Perfect Prediction")
    ax.set_xlabel("Actual Price (SGD)")
    ax.set_ylabel("Predicted Price (SGD)")
    ax.legend(fontsize=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "actual_vs_predicted.png"), dpi=150, bbox_inches="tight")

    # =============================================================================
    # Step 6: Residual Graph
    # =============================================================================
    residuals = y_test.values - y_pred
    fig, ax = plt.subplots(figsize=(8, 8))
    ax.scatter(y_pred, residuals, alpha=0.4, s=20, color="#2266AA")
    ax.axhline(0, color="#871616", linewidth=1.5, linestyle="--")
    ax.set_title("Residuals: Predicted Price vs Error", fontsize=13, fontweight="bold")
    ax.set_xlabel("Predicted price (SGD)")
    ax.set_ylabel("Residual")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "residuals.png"), dpi=150, bbox_inches="tight")

    # Create prediction results table
    output = df.iloc[split_index:].copy()

    output = output[[
        "canonical_product_id",
        "canonical_name",
        "unified_category",
        "store",
        "scraped_date_sg",
        "price_sgd"
    ]].copy()

    output["predicted_price"] = y_pred
    output["error"] = output["price_sgd"] - output["predicted_price"]
    output["abs_error"] = output["error"].abs()

    # Display 20 rows
    print("\nProduct Predictions:")
    print(output.head(10).to_string(index=False))

    # =============================================================================
    # Step 7: Summary
    # =============================================================================
    print("\n" + "=" * 60)
    print("PRICE PREDICTION SUMMARY")
    print("=" * 60)
    print(f"No. of rows for training: {len(X_train)}")
    print(f"No. of rows for testing: {len(X_test)}")
    print(f"MAE (Test): ${mae:.3f}")
    print(f"RMSE (Test): ${rmse:.3f}")
    print(f"R² (Test): {r2:.4f}")
    print()

if __name__ == "__main__":
    run()

