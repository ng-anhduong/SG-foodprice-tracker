# =============================================================================
# Importing the libraries
# =============================================================================
from supabase import create_client, Client
from dotenv import load_dotenv
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sys

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

print(sys.executable)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# =============================================================================
# Importing the dataset
# =============================================================================
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
            scraped_date_sg,
            price_sgd
        """)
        .range(start, start + batch_size - 1)
        .execute()
    )

    batch = response.data

    if not batch:
        break

    all_data.extend(batch)
    print(f"Fetched rows {start} to {start + len(batch) - 1}")

    if len(batch) < batch_size:
        break

    start += batch_size

dataFrame = pd.DataFrame(all_data)

# =============================================================================
# Basic cleaning
# =============================================================================
dataFrame["price_sgd"] = pd.to_numeric(dataFrame["price_sgd"], errors="coerce")
dataFrame = dataFrame.dropna(subset=["canonical_product_id", "canonical_name", "price_sgd"])

# group dataset by productId & canonical name
prod_df = (
    dataFrame.groupby(["canonical_product_id", "canonical_name"], as_index=False)
      .agg({
          "price_sgd": ["mean", "median", "min", "max", "std", "count"]
      })
)

# flatten multi-level column names
prod_df.columns = [
    "canonical_product_id",
    "canonical_name",
    "mean_price",
    "median_price",
    "min_price",
    "max_price",
    "std_price",
    "num_observations"
]

# default fill std for products with only 1 observation
prod_df["std_price"] = prod_df["std_price"].fillna(0)

# create price range column
prod_df["price_range"] = prod_df["max_price"] - prod_df["min_price"]


price_fields = ["mean_price", "median_price", "std_price", "price_range"]
X = prod_df[price_fields].dropna()

# =============================================================================
# Feature scaling
# =============================================================================
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
scaled_X = scaler.fit_transform(X)

# =============================================================================
# Use the elbow method to find the optimal number
# =============================================================================
from sklearn.cluster import KMeans

kmin = 1
kmax = 11
wcss = []

for i in range(kmin, kmax):
    kmeans = KMeans(n_clusters=i, init='k-means++', random_state=42, n_init=10)
    kmeans.fit(scaled_X)
    wcss.append(kmeans.inertia_)

plt.figure(figsize=(8,6))
plt.plot(range(kmin, kmax), wcss, marker='o')
plt.scatter(range(kmin, kmax), wcss)
plt.title('The Elbow Method')
plt.xlabel('Number of Clusters')
plt.ylabel('WCSS')
plt.xticks(range(kmin, kmax))
plt.show()

# =============================================================================
# Training the K-Means model on the dataset
# =============================================================================
from sklearn.cluster import KMeans
k_optimized = 4
kmeans = KMeans(n_clusters=k_optimized, init='k-means++', random_state=42, n_init=10)
y_kmeans = kmeans.fit_predict(scaled_X)
prod_df["cluster"] = y_kmeans

# =============================================================================
# Visualizing the clusters
# =============================================================================
# For visualization purpose, X and cluster center will be converted to unscaled X, cluster center.
unscaled_X = scaler.inverse_transform(scaled_X)
cluster_center = scaler.inverse_transform(kmeans.cluster_centers_)

plt.figure(figsize=(8,6))

for j in range(k_optimized):
    color = ['red', 'blue', 'green', 'yellow', 'cyan']
    clusterlabel = "Cluster" + str(j+1)
    plt.scatter(unscaled_X[y_kmeans==j,0],
                unscaled_X[y_kmeans==j,3],
                s=100, c=color[j],
                label=clusterlabel)
    
plt.scatter(cluster_center[:, 0],
            cluster_center[:, 3],
            s=100, marker='X',
            color='Magenta',
            label="centroid")

plt.title('Product Clusters')
plt.xlabel("Mean Price (SGD)")
plt.ylabel("Price Range (SGD)")
plt.legend()
plt.show()

# =============================================================================
# Create Summary for Cluster
# =============================================================================
eachCluster_price = (
    prod_df.groupby("cluster")[[
        "mean_price",
        "median_price",
        "min_price",
        "max_price",
        "std_price",
        "price_range"
    ]]
    .mean()
    .sort_values("mean_price")
)

print(eachCluster_price)
print(prod_df["cluster"].value_counts())

clusterList = eachCluster_price.index.tolist()

map_clusters = {
    clusterList[0]: "Budget",
    clusterList[1]: "Lower Mid-range",
    clusterList[2]: "Upper Mid-range",
    clusterList[3]: "Premium"
}

prod_df["price_tier"] = prod_df["cluster"].map(map_clusters)

print(prod_df[["canonical_name", "mean_price", "price_range", "price_tier"]].head(20))