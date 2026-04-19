import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "../../pipeline/etl"))
from load import get_client

st.set_page_config(page_title="Overview", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500;600&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
h1 { font-family: 'DM Serif Display', serif !important; font-size: 3rem !important;
     letter-spacing: -0.02em; color: #1a1a1a; }
h2 { font-family: 'DM Serif Display', serif !important; font-size: 1.9rem !important;
     color: #1a1a1a; font-weight: 400 !important; letter-spacing: -0.01em; }
h3 { font-size: 0.95rem !important; font-weight: 500 !important;
     letter-spacing: 0.1em; text-transform: uppercase; color: #888 !important; }
h4 { font-family: 'DM Serif Display', serif !important; font-size: 1.3rem !important;
     color: #1a1a1a; font-weight: 400 !important; }
.section-header {
    font-family: 'DM Sans', sans-serif;
    font-size: 1.35rem;
    font-weight: 700;
    color: #1a1a1a;
    letter-spacing: -0.01em;
    margin: 0 0 4px 0;
    padding-bottom: 10px;
    border-bottom: 2px solid #ebe7e0;
}
.section-sub {
    font-size: 0.85rem;
    color: #888;
    margin-top: 2px;
    margin-bottom: 16px;
    font-weight: 400;
}
[data-testid="metric-container"] {
    background: #ffffff; border: 1px solid #ebe7e0;
    border-radius: 10px; padding: 18px 22px !important;
}
[data-testid="metric-container"] label {
    font-size: 0.72rem !important; font-weight: 500;
    letter-spacing: 0.08em; text-transform: uppercase; color: #999;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: 'DM Serif Display', serif !important;
    font-size: 2rem !important; color: #1a1a1a;
}
hr { border-color: #ebe7e0 !important; }
.stDataFrame { border: 1px solid #ebe7e0 !important; border-radius: 8px; }
.insight-box {
    background: #f9f7f4;
    border-left: 3px solid #F5821F;
    border-radius: 6px;
    padding: 14px 18px;
    margin-bottom: 16px;
    font-size: 0.95rem;
    color: #1a1a1a;
    line-height: 1.6;
}
</style>
""", unsafe_allow_html=True)

STORE_COLORS = {
    "fairprice":   "#F5821F",
    "redmart":     "#C8102E",
    "coldstorage": "#005BAC",
    "shengsiong":  "#00843D",
}
STORE_LABELS = {
    "fairprice":   "FairPrice",
    "redmart":     "RedMart",
    "coldstorage": "Cold Storage",
    "shengsiong":  "Sheng Siong",
}

PLOTLY_BASE = dict(
    font=dict(family="DM Sans, sans-serif", size=13, color="#1a1a1a"),
    plot_bgcolor="white",
    paper_bgcolor="white",
    margin=dict(t=30, b=10, l=10, r=10),
)

def apply_base_axes(fig):
    fig.update_xaxes(gridcolor="#f0ede8", linecolor="#e0dbd2", zeroline=False)
    fig.update_yaxes(gridcolor="#f0ede8", linecolor="#e0dbd2", zeroline=False)
    return fig

def _fetch_latest_date(table, date_col):
    client = get_client()
    res = client.table(table).select(date_col).order(date_col, desc=True).limit(1).execute()
    return res.data[0][date_col] if res.data else None

def _fetch_for_date(table, date_col, date_val):
    client = get_client()
    rows, page = [], 0
    while True:
        res = (
            client.table(table).select("*")
            .eq(date_col, date_val)
            .range(page * 1000, (page + 1) * 1000 - 1)
            .execute()
        )
        if not res.data:
            break
        rows.extend(res.data)
        if len(res.data) < 1000:
            break
        page += 1
    return rows

@st.cache_data(ttl=300)
def load_recs_today():
    latest = _fetch_latest_date(
        "canonical_product_daily_recommendations", "scraped_date_sg"
    )
    if not latest:
        return pd.DataFrame()
    return pd.DataFrame(_fetch_for_date(
        "canonical_product_daily_recommendations", "scraped_date_sg", latest
    ))

@st.cache_data(ttl=300)
def load_prices_today():
    client = get_client()
    latest_res = client.table("canonical_product_daily_prices").select("scraped_date_sg").order("scraped_date_sg", desc=True).limit(1).execute()
    if not latest_res.data:
        return pd.DataFrame()
    latest = latest_res.data[0]["scraped_date_sg"]
    rows, page = [], 0
    while True:
        res = (
            client.table("canonical_product_daily_prices")
            .select("store,price_sgd,original_price_sgd,discount_sgd,canonical_product_id,canonical_name,unified_category,scraped_date_sg")
            .eq("scraped_date_sg", latest)
            .range(page * 1000, (page + 1) * 1000 - 1)
            .execute()
        )
        if not res.data:
            break
        rows.extend(res.data)
        if len(res.data) < 1000:
            break
        page += 1
    return pd.DataFrame(rows)

with st.spinner("Loading..."):
    df = load_recs_today()
    df_prices = load_prices_today()

if df.empty:
    st.error("No data. Run the pipeline first.")
    st.stop()

# canonical_product_id is the true identifier — use it for all filtering
# df_multi = products with a real cross-store comparison available today
df_multi = df[df["stores_seen_for_day"] >= 2].copy()
latest_date = df["scraped_date_sg"].max()

# ── HEADER ────────────────────────────────────────────────────────────────────

st.title("Overview")
st.markdown(
    f"<p style='color:#888; font-size:1.1rem; margin-top:-12px'>"
    f"Price landscape across FairPrice, RedMart, Cold Storage and Sheng Siong"
    f"&nbsp;·&nbsp;<strong style='color:#1a1a1a'>As of {latest_date}</strong></p>",
    unsafe_allow_html=True
)
st.divider()

# ── KPI CARDS ─────────────────────────────────────────────────────────────────
# Products Tracked = all canonical products seen today (incl. single-store)
# Products You Can Compare = subset matched across 2+ stores today

c1, c2, c3, c4 = st.columns(4)
c1.metric(
    "Products Tracked",
    f"{len(df):,}",
    help="All canonical products seen today across any store"
)
c2.metric(
    "Products You Can Compare",
    f"{len(df_multi):,}",
    help="Products matched across 2 or more stores — these have a real price comparison"
)
c3.metric(
    "Avg Price Spread",
    f"${df_multi['price_spread_sgd'].mean():.2f}" if not df_multi.empty else "—",
    help="Average saving from choosing the cheapest over priciest store for the same product"
)
c4.metric("Categories", f"{df['unified_category'].nunique()}")

st.divider()


# ── WHERE TO SHOP TODAY ───────────────────────────────────────────────────────

st.markdown("<div class='section-header'>Where to shop today</div>", unsafe_allow_html=True)
st.markdown("<div class='section-sub'>Which store offers the lowest price most often across matched products</div>", unsafe_allow_html=True)

if not df_multi.empty:
    store_counts = df_multi["cheapest_store"].value_counts()
    top_store = store_counts.idxmax()
    top_store_label = STORE_LABELS.get(top_store, top_store)
    top_pct = round(store_counts.iloc[0] / len(df_multi) * 100, 1)

    cat_winners = (
        df_multi.groupby(["unified_category", "cheapest_store"])
        .size().reset_index(name="count")
        .sort_values("count", ascending=False)
        .drop_duplicates("unified_category")
    )
    cat_parts = []
    for _, row in cat_winners.iterrows():
        store_label = STORE_LABELS.get(row["cheapest_store"], row["cheapest_store"])
        cat_total = df_multi[df_multi["unified_category"] == row["unified_category"]].shape[0]
        pct = round(row["count"] / cat_total * 100, 0) if cat_total > 0 else 0
        cat_parts.append(
            f"<b>{store_label}</b> for {row['unified_category']} ({pct:.0f}%)"
        )

    st.markdown(
        f"<div class='insight-box'>"
        f"Overall, <b>{top_store_label}</b> offers the lowest price on "
        f"<b>{top_pct}%</b> of comparable products today."
        f"<br>By category — {' &nbsp;·&nbsp; '.join(cat_parts[:4])}."
        f"</div>",
        unsafe_allow_html=True
    )

# ── CHART 1: Price leadership by store — full width ───────────────────────────

st.markdown("### Price leadership by store")

counts = df_multi["cheapest_store"].value_counts().reset_index()
counts.columns = ["store", "count"]
counts["label"] = counts["store"].map(STORE_LABELS).fillna(counts["store"])
counts["pct"] = (counts["count"] / counts["count"].sum() * 100).round(1)
counts = counts.sort_values("count", ascending=False)

fig1 = go.Figure()
for _, row in counts.iterrows():
    fig1.add_trace(go.Bar(
        x=[row["label"]],
        y=[row["count"]],
        marker_color=STORE_COLORS.get(row["store"], "#aaa"),
        text=f"{row['count']:,}<br>({row['pct']}%)",
        textposition="outside",
        textfont=dict(color="#1a1a1a", size=13, family="DM Sans"),
        name=row["label"],
        hovertemplate=(
            f"<b>{row['label']}</b><br>"
            f"{row['count']:,} products cheapest<extra></extra>"
        ),
    ))

fig1.update_layout(
    **{**PLOTLY_BASE, "margin": dict(t=40, b=20, l=10, r=20)},
    showlegend=False,
    height=300,
    yaxis_title="Number of products where cheapest",
)
apply_base_axes(fig1)
fig1.update_xaxes(gridcolor="rgba(0,0,0,0)", linecolor="rgba(0,0,0,0)")
st.plotly_chart(fig1, use_container_width=True)

# ── CHART 2: Heatmap — category × store ──────────────────────────────────────
# Each cell = number of matched products where that store was cheapest
# Color intensity = dominance in that category

st.markdown("### Best store per category")
st.caption(
    "Number of matched products where each store offered the lowest price today. "
    "Darker = more dominant."
)

heat = (
    df_multi.groupby(["unified_category", "cheapest_store"])
    .size().reset_index(name="count")
)
pivot = heat.pivot(
    index="unified_category",
    columns="cheapest_store",
    values="count"
).fillna(0).astype(int)

# Rename columns to display labels
pivot.columns = [STORE_LABELS.get(c, c) for c in pivot.columns]

# Add row total for sorting
pivot["_total"] = pivot.sum(axis=1)
pivot = pivot.sort_values("_total", ascending=False).drop(columns="_total")

fig2 = px.imshow(
    pivot,
    color_continuous_scale="Oranges",
    aspect="auto",
    text_auto=True,
    labels=dict(x="Store", y="Category", color="Products cheapest"),
)
fig2.update_layout(
    **{**PLOTLY_BASE, "margin": dict(t=10, b=20, l=10, r=20)},
    height=max(260, len(pivot) * 42),
    coloraxis_showscale=False,
)
fig2.update_xaxes(side="bottom", gridcolor="rgba(0,0,0,0)", linecolor="rgba(0,0,0,0)")
fig2.update_yaxes(gridcolor="rgba(0,0,0,0)", linecolor="rgba(0,0,0,0)")
fig2.update_traces(textfont_size=13)
st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ── SAVINGS POTENTIAL SUMMARY ─────────────────────────────────────────────────
st.markdown("<div class='section-header'>Savings Potential Summary</div>", unsafe_allow_html=True)
if not df_multi.empty:
    total = len(df_multi)
    over_1 = (df_multi["price_spread_sgd"] > 1).sum()
    over_2 = (df_multi["price_spread_sgd"] > 2).sum()
    over_5 = (df_multi["price_spread_sgd"] > 5).sum()
    zero = (df_multi["price_spread_sgd"] == 0).sum()

    col_a, col_b, col_c, col_d = st.columns(4)
    col_a.metric(
        "Same price everywhere",
        f"{zero:,}",
        help="Products priced identically across all stores today — no benefit in shopping around"
    )
    col_b.metric(
        "Save over $1",
        f"{over_1:,}",
        help="Products where choosing the cheapest store saves you more than $1"
    )
    col_c.metric(
        "Save over $2",
        f"{over_2:,}",
        help="Products where choosing the cheapest store saves you more than $2"
    )
    col_d.metric(
        "Save over $5",
        f"{over_5:,}",
        help="Products where choosing the cheapest store saves you more than $5"
    )

    pct_worth = round((over_1 / total) * 100, 1) if total > 0 else 0
    st.markdown(
        f"<div class='insight-box'>"
        f"<b>{pct_worth}%</b> of comparable products today have a price difference of more than $1 "
        f"between stores — for these products, it's worth checking which store to buy from. "
        f"The remaining {100 - pct_worth:.1f}% are priced similarly enough that it doesn't matter."
        f"</div>",
        unsafe_allow_html=True
    )

# ── PRICE SPREAD BY CATEGORY — VERTICAL BAR ──────────────────────────────────

st.markdown("<div class='section-header'>How much prices vary by category</div>", unsafe_allow_html=True)
st.markdown("<div class='section-sub'>Median price spread — the typical saving from choosing the cheapest over the most expensive store</div>", unsafe_allow_html=True)

spread_df = df_multi[df_multi["price_spread_sgd"] > 0].copy()

if not spread_df.empty:
    cat_spreads = (
        spread_df.groupby("unified_category")["price_spread_sgd"]
        .agg(
            median_spread="median",
            mean_spread="mean",
            product_count="count",
        )
        .reset_index()
        .sort_values("median_spread", ascending=False)
    )

    highest = cat_spreads.iloc[0]
    lowest = cat_spreads.iloc[-1]

    st.markdown(
        f"<div class='insight-box'>"
        f"<b>{highest['unified_category']}</b> has the most price variation today "
        f"(median spread: <b>${highest['median_spread']:.2f}</b>) — "
        f"always worth comparing stores before buying. "
        f"<b>{lowest['unified_category']}</b> is the most consistent "
        f"(median spread: <b>${lowest['median_spread']:.2f}</b>) — "
        f"prices are similar wherever you shop."
        f"</div>",
        unsafe_allow_html=True
    )

    fig_spread = go.Figure()
    fig_spread.add_trace(go.Bar(
        x=cat_spreads["unified_category"],
        y=cat_spreads["median_spread"],
        marker_color="#F5821F",
        marker_opacity=0.85,
        text=cat_spreads["median_spread"].apply(lambda x: f"${x:.2f}"),
        textposition="outside",
        textfont=dict(color="#1a1a1a", size=13, family="DM Sans"),
        customdata=cat_spreads[["mean_spread", "product_count"]].values,
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Median spread: $%{y:.2f}<br>"
            "Mean spread: $%{customdata[0]:.2f}<br>"
            "Products with price diff: %{customdata[1]:,}<extra></extra>"
        ),
    ))

    fig_spread.update_layout(
        **{**PLOTLY_BASE, "margin": dict(t=40, b=20, l=10, r=20)},
        height=320,
        showlegend=False,
        yaxis_title="Median Price Spread (SGD)",
        yaxis=dict(rangemode="tozero"),
    )
    apply_base_axes(fig_spread)
    fig_spread.update_xaxes(gridcolor="rgba(0,0,0,0)", linecolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_spread, use_container_width=True)

    st.caption(
        "Only products with a measurable price difference across stores are included. "
        "Median is used over mean to avoid skew from outliers. Hover for mean and product count."
    )

st.divider()

# ── STORE DISCOUNT ACTIVITY ───────────────────────────────────────────────────

st.markdown(
    "<div class='section-header'>Store discount activity today</div>",
    unsafe_allow_html=True
)
st.markdown(
    "<div class='section-sub'>"
    "Products currently discounted from their original price — includes all tracked products, not just cross-store matches"
    "</div>",
    unsafe_allow_html=True
)

if not df_prices.empty and "original_price_sgd" in df_prices.columns:
    disc_df = df_prices[
        df_prices["discount_sgd"].notna() & (df_prices["discount_sgd"] > 0)
    ].copy()
    disc_df["discount_pct"] = (
        disc_df["discount_sgd"] / disc_df["original_price_sgd"] * 100
    ).round(1)

    if not disc_df.empty:

        disc_summary = (
            disc_df.groupby("store")
            .agg(
                discounted_products=("canonical_product_id", "nunique"),
                avg_discount=("discount_sgd", "mean"),
                max_discount=("discount_sgd", "max"),
            )
            .reset_index()
            .sort_values("discounted_products", ascending=False)
        )
        disc_summary["store_label"] = disc_summary["store"].map(STORE_LABELS).fillna(
            disc_summary["store"]
        )

        top_disc = disc_summary.iloc[0]
        st.markdown(
            f"<div class='insight-box'>"
            f"<b>{top_disc['store_label']}</b> has the most active promotions today — "
            f"<b>{top_disc['discounted_products']:,}</b> products discounted from their "
            f"original price, with an average discount of "
            f"<b>${top_disc['avg_discount']:.2f}</b>."
            f"</div>",
            unsafe_allow_html=True
        )

        fig_disc = go.Figure()
        for _, row in disc_summary.iterrows():
            fig_disc.add_trace(go.Bar(
                x=[row["store_label"]],
                y=[row["discounted_products"]],
                marker_color=STORE_COLORS.get(row["store"], "#aaa"),
                text=f"{row['discounted_products']:,}<br>avg ${row['avg_discount']:.2f} off",
                textposition="outside",
                textfont=dict(color="#1a1a1a", size=12, family="DM Sans"),
                name=row["store_label"],
                hovertemplate=(
                    f"<b>{row['store_label']}</b><br>"
                    f"Discounted products: {row['discounted_products']:,}<br>"
                    f"Avg discount: ${row['avg_discount']:.2f}<br>"
                    f"Max discount: ${row['max_discount']:.2f}<extra></extra>"
                ),
            ))

        fig_disc.update_layout(
            **{**PLOTLY_BASE, "margin": dict(t=60, b=20, l=10, r=20)},
            showlegend=False,
            height=300,
            yaxis_title="Number of discounted products",
        )
        apply_base_axes(fig_disc)
        fig_disc.update_xaxes(gridcolor="rgba(0,0,0,0)", linecolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_disc, use_container_width=True)

        st.caption(
            "Counts all tracked products where current price is below original/RRP — "
        )

        st.divider()

        st.markdown("<div class='section-header'>Top 10 biggest discounts today</div>", unsafe_allow_html=True)
        st.caption(
            "Ranked by absolute discount (SGD). "
            "Includes all tracked products. "
        )

        top10 = (
            disc_df.sort_values("discount_sgd", ascending=False)
            .drop_duplicates("canonical_product_id")  # one row per product
            .head(10)
            .reset_index(drop=True)
        )

        top10["store_label"] = top10["store"].map(STORE_LABELS).fillna(top10["store"])

        display_top10 = top10[[
            "canonical_name", "unified_category",
            "store_label",
            "original_price_sgd", "price_sgd",
            "discount_sgd", "discount_pct",
        ]].copy()

        display_top10["original_price_sgd"] = display_top10["original_price_sgd"].apply(
            lambda x: f"${x:.2f}"
        )
        display_top10["price_sgd"] = display_top10["price_sgd"].apply(
            lambda x: f"${x:.2f}"
        )
        display_top10["discount_sgd"] = display_top10["discount_sgd"].apply(
            lambda x: f"${x:.2f}"
        )
        display_top10["discount_pct"] = display_top10["discount_pct"].apply(
            lambda x: f"{x:.1f}%"
        )

        display_top10.columns = [
            "Product", "Category",
            "Store",
            "Original Price", "Current Price",
            "You Save", "Discount %",
        ]
        st.dataframe(display_top10, use_container_width=True, hide_index=True)

    else:
        st.info("No discounted products found today.")
else:
    st.info("Price data not available.")

