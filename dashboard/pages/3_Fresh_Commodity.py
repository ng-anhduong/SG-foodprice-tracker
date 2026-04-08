import streamlit as st
import pandas as pd
import plotly.express as px
import json, sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "../../pipeline/etl"))
from load import get_client

st.set_page_config(page_title="Fresh Commodity", page_icon="🥩", layout="wide")
st.title("🥩 Fresh Commodity Comparison")
st.caption("Compare prices for fresh meat and produce at the same pack size across stores.")

STORE_COLORS = {
    "fairprice":   "#F5821F",
    "redmart":     "#E31837",
    "coldstorage": "#0066CC",
    "shengsiong":  "#009B4E",
}

@st.cache_data(ttl=300)
def load_data():
    client = get_client()
    res = (
        client.table("commodity_price_comparisons")
        .select("*")
        .order("scraped_date", desc=True)
        .limit(2000)
        .execute()
    )
    df = pd.DataFrame(res.data)
    if df.empty:
        return df
    latest = df["scraped_date"].max()
    return df[df["scraped_date"] == latest]

with st.spinner("Loading data..."):
    df = load_data()

if df.empty:
    st.error("No commodity data found.")
    st.stop()

st.markdown(f"**Data as of:** {df['scraped_date'].max()}")

# ── FILTERS ───────────────────────────────────────────────────────────────────

col1, col2, col3 = st.columns(3)

with col1:
    categories = ["All"] + sorted(df["unified_category"].dropna().unique().tolist())
    selected_cat = st.selectbox("Category", categories)

with col2:
    frozen_options = ["All"] + sorted(df["frozen_flag"].dropna().unique().tolist())
    selected_frozen = st.selectbox("Fresh / Frozen", frozen_options)

with col3:
    search_cut = st.text_input("Search cut/type", placeholder="e.g. chicken breast, broccoli")

filtered = df.copy()
if selected_cat != "All":
    filtered = filtered[filtered["unified_category"] == selected_cat]
if selected_frozen != "All":
    filtered = filtered[filtered["frozen_flag"] == selected_frozen]
if search_cut:
    filtered = filtered[filtered["cut"].str.contains(search_cut, case=False, na=False)]

st.markdown(f"**{len(filtered)} cuts found**")

st.divider()

# ── CHART: Cheapest store per cut ─────────────────────────────────────────────

st.subheader("Cheapest store per cut")

if not filtered.empty:
    fig = px.bar(
        filtered.sort_values("cheapest_price_sgd"),
        x="cut", y="cheapest_price_sgd",
        color="cheapest_store",
        color_discrete_map=STORE_COLORS,
        text="cheapest_price_sgd",
        labels={
            "cut": "Cut / Type",
            "cheapest_price_sgd": "Cheapest Price (SGD)",
            "cheapest_store": "Store",
        },
        hover_data=["common_weight_g", "frozen_flag", "cheapest_product_name"],
    )
    fig.update_traces(texttemplate="$%{text:.2f}", textposition="outside")
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        yaxis=dict(gridcolor="rgba(0,0,0,0.1)"),
        xaxis_tickangle=-35,
        height=500,
        legend_title="Cheapest Store",
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── TABLE ─────────────────────────────────────────────────────────────────────

st.subheader("Full comparison table")

table = filtered[[
    "cut", "unified_category", "frozen_flag",
    "common_weight_g",
    "cheapest_store", "cheapest_price_sgd", "cheapest_product_name",
    "priciest_store", "priciest_price_sgd", "priciest_product_name",
    "price_spread_sgd", "stores_seen"
]].sort_values("price_spread_sgd", ascending=False).reset_index(drop=True)

table.columns = [
    "Cut", "Category", "Fresh/Frozen",
    "Pack Size (g)",
    "Cheapest Store", "Cheapest ($)", "Cheapest Product",
    "Priciest Store", "Priciest ($)", "Priciest Product",
    "Spread ($)", "Stores"
]

for col in ["Cheapest ($)", "Priciest ($)", "Spread ($)"]:
    table[col] = table[col].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "-")

st.dataframe(table, use_container_width=True, hide_index=True)