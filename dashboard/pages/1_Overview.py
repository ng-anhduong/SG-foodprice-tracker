import streamlit as st
import pandas as pd
import plotly.express as px
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "../../pipeline/etl"))
from load import get_client

st.set_page_config(page_title="Overview", page_icon="📊", layout="wide")
st.title("📊 Overview")
st.caption("Today's price landscape across all stores.")

STORE_COLORS = {
    "fairprice":   "#F5821F",
    "redmart":     "#E31837",
    "coldstorage": "#0066CC",
    "shengsiong":  "#009B4E",
}

def fetch_all(table, date_col):
    client = get_client()
    all_rows = []
    page = 0
    page_size = 1000
    while True:
        res = (
            client.table(table)
            .select("*")
            .order(date_col, desc=True)
            .range(page * page_size, (page + 1) * page_size - 1)
            .execute()
        )
        if not res.data:
            break
        all_rows.extend(res.data)
        if len(res.data) < page_size:
            break
        page += 1
    return all_rows

@st.cache_data(ttl=300)
def load_data():
    rows = fetch_all("canonical_product_daily_recommendations", "scraped_date_sg")
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    latest = df["scraped_date_sg"].max()
    return df[df["scraped_date_sg"] == latest]

with st.spinner("Loading data..."):
    df = load_data()

if df.empty:
    st.error("No data found. Make sure the pipeline has run.")
    st.stop()

st.markdown(f"**Data as of:** {df['scraped_date_sg'].max()}")

# ── KPI CARDS ─────────────────────────────────────────────────────────────────

st.subheader("At a Glance")

total = len(df)
avg_spread = df["price_spread_sgd"].mean()
multi = df[df["stores_seen_for_day"] >= 2].shape[0]

c1, c2, c3 = st.columns(3)
c1.metric("Total Matched Products", f"{total:,}")
c2.metric("Avg Price Spread", f"${avg_spread:.2f}", help="Avg gap between cheapest and priciest store")
c3.metric("Products Across 2+ Stores", f"{multi:,}")

st.divider()

# ── CHART 1: Which store wins most? ──────────────────────────────────────────

st.subheader("Which store is cheapest most often?")

counts = df["cheapest_store"].value_counts().reset_index()
counts.columns = ["store", "count"]

fig1 = px.bar(
    counts, x="store", y="count",
    color="store",
    color_discrete_map=STORE_COLORS,
    text="count",
    labels={"store": "Store", "count": "Products where cheapest"},
)
fig1.update_traces(textposition="outside")
fig1.update_layout(
    showlegend=False,
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    yaxis=dict(gridcolor="rgba(0,0,0,0.1)"),
    height=400,
)
st.plotly_chart(fig1, use_container_width=True)

st.divider()

# ── CHART 2: Cheapest store share by category ─────────────────────────────────

st.subheader("Cheapest store share by category")

cat_store = (
    df.groupby(["unified_category", "cheapest_store"])
    .size()
    .reset_index(name="count")
)

fig2 = px.bar(
    cat_store,
    x="unified_category", y="count",
    color="cheapest_store",
    color_discrete_map=STORE_COLORS,
    barmode="stack",
    labels={
        "unified_category": "Category",
        "count": "Products",
        "cheapest_store": "Store",
    },
)
fig2.update_layout(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    yaxis=dict(gridcolor="rgba(0,0,0,0.1)"),
    xaxis_tickangle=-30,
    height=450,
)
st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ── CHART 3: Price spread distribution ───────────────────────────────────────

st.subheader("Price spread distribution")
st.caption("How much can you save by choosing the cheapest store?")

fig3 = px.histogram(
    df[df["price_spread_sgd"] > 0],
    x="price_spread_sgd",
    nbins=40,
    labels={"price_spread_sgd": "Price Spread (SGD)"},
    color_discrete_sequence=["#636EFA"],
)
fig3.update_layout(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    yaxis=dict(gridcolor="rgba(0,0,0,0.1)"),
    height=350,
)
st.plotly_chart(fig3, use_container_width=True)

st.divider()

# ── TABLE: Top 10 biggest spreads ─────────────────────────────────────────────

st.subheader("Top 10 biggest price spreads today")
st.caption("Products where switching stores saves you the most.")

top = (
    df[[
        "canonical_name", "unified_category",
        "cheapest_store", "cheapest_price_sgd",
        "priciest_store", "priciest_price_sgd",
        "price_spread_sgd"
    ]]
    .sort_values("price_spread_sgd", ascending=False)
    .head(10)
    .reset_index(drop=True)
)
top.columns = [
    "Product", "Category",
    "Cheapest Store", "Cheapest ($)",
    "Priciest Store", "Priciest ($)",
    "Spread ($)"
]
for col in ["Cheapest ($)", "Priciest ($)", "Spread ($)"]:
    top[col] = top[col].apply(lambda x: f"${x:.2f}")

st.dataframe(top, use_container_width=True, hide_index=True)