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
        .table("canonical_product_daily_recommendations")
        .select("""
            canonical_product_id,
            canonical_name,
            scraped_date_sg,
            cheapest_store,
            cheapest_price_sgd,
            priciest_store,
            priciest_price_sgd,
            price_spread_sgd,
            stores_seen_for_day
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

#group dataset by productId & canonical name
prod_df = (
    dataFrame.groupby(["canonical_product_id", "canonical_name"], as_index=False)
      .agg({
          "cheapest_price_sgd": "mean",
          "priciest_price_sgd": "mean",
          "price_spread_sgd": "mean",
          "stores_seen_for_day": "mean"
      })
)

#create estimated avg price column for each product
#  = (cheapest price + priciest price)/2
prod_df["estimated_avg_price"] = (
    prod_df["cheapest_price_sgd"] + prod_df["priciest_price_sgd"]
) / 2

price_fields = ["estimated_avg_price", "cheapest_price_sgd", "priciest_price_sgd"]
X = prod_df[price_fields].dropna()

# =============================================================================
# Feature Scaling
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

plt.plot(range(kmin, kmax), wcss)
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
k_optimized = 3
kmeans = KMeans(n_clusters=k_optimized,init='k-means++',random_state=42, n_init=10)
y_kmeans = kmeans.fit_predict(scaled_X)
prod_df["cluster"] = y_kmeans

# =============================================================================
# Visualizing the Clusters
# =============================================================================
# For visualization purpose, X and cluster center will be converted to unscaled X, cluster center.
unscaled_X = scaler.inverse_transform(scaled_X)
cluster_center = scaler.inverse_transform(kmeans.cluster_centers_)

for j in range(k_optimized):
    color=['red','blue','green','yellow','cyan']
    clusterlabel="Cluster" + str(j+1)
    plt.scatter(unscaled_X[y_kmeans==j,1],
                unscaled_X[y_kmeans==j,2],
                s=100,c=color[j],
                label=clusterlabel)
    
plt.scatter(cluster_center[:, 1],
            cluster_center[:,2],
            s=100,marker='X',
            color='Magenta',
            label="centroid")

plt.title('Product Clusters')
plt.xlabel("Cheapest Price (SGD)")
plt.ylabel("Priciest Price (SGD)")
plt.legend()
plt.show()

# =============================================================================
# Create Summary for Cluster
# =============================================================================

eachCluster_price = (
    prod_df.groupby("cluster")[[
        "estimated_avg_price",
        "cheapest_price_sgd",
        "priciest_price_sgd",
        "price_spread_sgd"
    ]]
    .mean()
    .sort_values("estimated_avg_price")
)

print(eachCluster_price)
print(prod_df["cluster"].value_counts())

clusterList = eachCluster_price.index.tolist()

map_clusters = {
    clusterList[0]: "Budget",
    clusterList[1]: "Mid-range",
    clusterList[2]: "Premium"
}

prod_df["price_tier"] = prod_df["cluster"].map(map_clusters)

print(prod_df[["canonical_name", "estimated_avg_price", "price_tier"]].head(10))