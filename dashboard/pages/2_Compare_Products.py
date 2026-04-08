import streamlit as st
import pandas as pd
import plotly.express as px
import json, sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "../../pipeline/etl"))
from load import get_client

st.set_page_config(page_title="Compare Products", page_icon="🔍", layout="wide")
st.title("🔍 Compare Products")
st.caption("Search for a product and compare its price across stores.")

STORE_COLORS = {
    "fairprice":   "#F5821F",
    "redmart":     "#E31837",
    "coldstorage": "#0066CC",
    "shengsiong":  "#009B4E",
}

@st.cache_data(ttl=300)
def load_recommendations():
    client = get_client()
    res = (
        client.table("canonical_product_daily_recommendations")
        .select("*")
        .order("scraped_date_sg", desc=True)
        .limit(3000)
        .execute()
    )
    df = pd.DataFrame(res.data)
    if df.empty:
        return df
    latest = df["scraped_date_sg"].max()
    return df[df["scraped_date_sg"] == latest]

@st.cache_data(ttl=300)
def load_prices():
    client = get_client()
    res = (
        client.table("canonical_product_daily_prices")
        .select("*")
        .order("scraped_date_sg", desc=True)
        .limit(5000)
        .execute()
    )
    df = pd.DataFrame(res.data)
    if df.empty:
        return df
    latest = df["scraped_date_sg"].max()
    return df[df["scraped_date_sg"] == latest]

with st.spinner("Loading data..."):
    df_rec = load_recommendations()
    df_prices = load_prices()

if df_rec.empty:
    st.error("No data found.")
    st.stop()

# ── FILTERS ───────────────────────────────────────────────────────────────────

col1, col2 = st.columns([1, 2])

with col1:
    categories = ["All"] + sorted(df_rec["unified_category"].dropna().unique().tolist())
    selected_cat = st.selectbox("Filter by category", categories)

with col2:
    search = st.text_input("Search product name", placeholder="e.g. Milo, chicken breast, broccoli")

filtered = df_rec.copy()
if selected_cat != "All":
    filtered = filtered[filtered["unified_category"] == selected_cat]
if search:
    filtered = filtered[filtered["canonical_name"].str.contains(search, case=False, na=False)]

st.markdown(f"**{len(filtered)} products found**")

# ── PRODUCT TABLE ─────────────────────────────────────────────────────────────

display = (
    filtered[[
        "canonical_name", "canonical_brand", "unified_category",
        "size_display", "cheapest_store", "cheapest_price_sgd",
        "priciest_store", "priciest_price_sgd", "price_spread_sgd",
        "stores_seen_for_day"
    ]]
    .sort_values("price_spread_sgd", ascending=False)
    .reset_index(drop=True)
)
display.columns = [
    "Product", "Brand", "Category", "Size",
    "Cheapest Store", "Cheapest ($)",
    "Priciest Store", "Priciest ($)",
    "Spread ($)", "Stores"
]
for col in ["Cheapest ($)", "Priciest ($)", "Spread ($)"]:
    display[col] = display[col].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "-")

st.dataframe(display, use_container_width=True, hide_index=True)

st.divider()

# ── PRODUCT DETAIL ────────────────────────────────────────────────────────────

st.subheader("Product detail")

selected_name = st.selectbox(
    "Select a product to see store breakdown",
    options=filtered["canonical_name"].tolist()
)

if selected_name:
    rec_row = filtered[filtered["canonical_name"] == selected_name].iloc[0]
    price_rows = df_prices[df_prices["canonical_name"] == selected_name]

    c1, c2, c3 = st.columns(3)
    c1.metric("Cheapest Store", rec_row["cheapest_store"], f"${rec_row['cheapest_price_sgd']:.2f}")
    c2.metric("Priciest Store", rec_row["priciest_store"], f"${rec_row['priciest_price_sgd']:.2f}")
    c3.metric("Price Spread", f"${rec_row['price_spread_sgd']:.2f}")

    if not price_rows.empty:
        fig = px.bar(
            price_rows.sort_values("price_sgd"),
            x="store", y="price_sgd",
            color="store",
            color_discrete_map=STORE_COLORS,
            text="price_sgd",
            labels={"store": "Store", "price_sgd": "Price (SGD)"},
            title=f"Price by store — {selected_name}",
        )
        fig.update_traces(texttemplate="$%{text:.2f}", textposition="outside")
        fig.update_layout(
            showlegend=False,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            yaxis=dict(gridcolor="rgba(0,0,0,0.1)"),
            height=400,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Store product names per store
        st.markdown("**Store-specific product names:**")
        name_table = price_rows[["store", "store_product_name", "price_sgd", "product_url"]].copy()
        name_table["price_sgd"] = name_table["price_sgd"].apply(lambda x: f"${x:.2f}")
        name_table.columns = ["Store", "Product Name", "Price", "URL"]
        st.dataframe(name_table, use_container_width=True, hide_index=True)