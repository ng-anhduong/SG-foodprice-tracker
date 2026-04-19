import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import json, sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "../../pipeline/etl"))
from load import get_client

st.set_page_config(page_title="Fresh & Commodity", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500;600&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
h1 { font-family: 'DM Serif Display', serif !important; font-size: 2.2rem !important;
     letter-spacing: -0.02em; color: #1a1a1a; }
h2 { font-family: 'DM Serif Display', serif !important; font-size: 1.4rem !important;
     color: #1a1a1a; font-weight: 400 !important; }
h3 { font-size: 0.72rem !important; font-weight: 600 !important;
     letter-spacing: 0.1em; text-transform: uppercase; color: #888 !important; }
[data-testid="metric-container"] {
    background: #fff; border: 1px solid #ebe7e0;
    border-radius: 10px; padding: 16px 20px !important;
}
[data-testid="metric-container"] label {
    font-size: 0.72rem !important; font-weight: 500;
    letter-spacing: 0.08em; text-transform: uppercase; color: #999;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: 'DM Serif Display', serif !important; font-size: 1.8rem !important;
}
hr { border-color: #ebe7e0 !important; }
</style>
""", unsafe_allow_html=True)

STORE_COLORS = {
    "fairprice": "#F5821F",
    "redmart": "#C8102E",
    "coldstorage": "#005BAC",
    "shengsiong": "#00843D",
}
STORE_LABELS = {
    "fairprice": "FairPrice",
    "redmart": "RedMart",
    "coldstorage": "Cold Storage",
    "shengsiong": "Sheng Siong",
}

PLOTLY_BASE = dict(
    font=dict(family="DM Sans, sans-serif", size=13, color="#1a1a1a"),
    plot_bgcolor="white",
    paper_bgcolor="white",
    margin=dict(t=30, b=10, l=10, r=100),
)

def apply_base_axes(fig):
    fig.update_xaxes(gridcolor="#f0ede8", linecolor="#e0dbd2", zeroline=False)
    fig.update_yaxes(gridcolor="rgba(0,0,0,0)", linecolor="rgba(0,0,0,0)", zeroline=False)
    return fig

def fetch_all(table, date_col):
    client = get_client()
    rows, page = [], 0
    while True:
        res = (
            client.table(table).select("*")
            .order(date_col, desc=True)
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
def load_data():
    df = pd.DataFrame(fetch_all("commodity_price_comparisons", "scraped_date"))
    if df.empty:
        return df
    latest = df["scraped_date"].max()
    return df[df["scraped_date"] == latest]

@st.cache_data(ttl=300)
def load_all_dates():
    return pd.DataFrame(fetch_all("commodity_price_comparisons", "scraped_date"))

def parse_unit_prices(row):
    try:
        sp = row["store_prices"]
        if isinstance(sp, str):
            sp = json.loads(sp)
        return {
            store: v.get("unit_price_per_100g")
            for store, v in sp.items()
            if v.get("unit_price_per_100g")
        }
    except Exception:
        return {}

with st.spinner("Loading..."):
    df = load_data()

if df.empty:
    st.error("No commodity data found.")
    st.stop()

df["unit_prices"] = df.apply(parse_unit_prices, axis=1)
df["cheapest_store_label"] = df["cheapest_store"].map(STORE_LABELS).fillna(df["cheapest_store"])
df["priciest_store_label"] = df["priciest_store"].map(STORE_LABELS).fillna(df["priciest_store"])

st.title("Fresh & Commodity Prices")
st.markdown(
    f"<p style='color:#888; font-size:0.9rem; margin-top:-12px'>"
    f"Prices compared at the same pack size. Groups by cut type — premium variants "
    f"(Wagyu, King Salmon) may appear alongside standard cuts due to keyword-based grouping. "
    f"<strong style='color:#1a1a1a'>{df['scraped_date'].max()}</strong></p>",
    unsafe_allow_html=True,
)
st.divider()

# ── FILTERS ───────────────────────────────────────────────────────────────────

col1, col2, col3 = st.columns(3)
with col1:
    cats = ["All"] + sorted(df["unified_category"].dropna().unique().tolist())
    selected_cat = st.selectbox("Category", cats)
with col2:
    frozen_opts = ["All"] + sorted(df["frozen_flag"].dropna().unique().tolist())
    selected_frozen = st.selectbox("Fresh / Frozen", frozen_opts)
with col3:
    search = st.text_input("Search cut", placeholder="chicken breast, broccoli, salmon…")

filtered = df.copy()
if selected_cat != "All":
    filtered = filtered[filtered["unified_category"] == selected_cat]
if selected_frozen != "All":
    filtered = filtered[filtered["frozen_flag"] == selected_frozen]
if search:
    filtered = filtered[filtered["cut"].str.contains(search, case=False, na=False)]

# ── KPI ROW ───────────────────────────────────────────────────────────────────

c1, c2, c3, c4 = st.columns(4)
c1.metric("Cuts found", f"{len(filtered):,}")
c2.metric(
    "Avg price spread",
    f"${filtered['price_spread_sgd'].mean():.2f}" if not filtered.empty else "—",
)
most_wins = (
    filtered["cheapest_store"].value_counts().idxmax()
    if not filtered.empty and not filtered["cheapest_store"].isna().all()
    else "—"
)
c3.metric("Most competitive store", STORE_LABELS.get(most_wins, most_wins))
c4.metric("Data as of", filtered["scraped_date"].max() if not filtered.empty else "—")

st.divider()

if filtered.empty:
    st.info("No cuts match your filters.")
    st.stop()

# ── UNIT PRICE PER 100G SCATTER ───────────────────────────────────────────────

st.subheader("Unit price per 100g by store")
st.markdown(
    "### Each point is one cut at one store — "
    "computed from the unit_price_per_100g field the commodity algorithm produces"
)

unit_rows = []
for _, row in filtered.iterrows():
    for store, upr in row["unit_prices"].items():
        if upr is not None:
            unit_rows.append({
                "cut": row["cut"],
                "store": store,
                "store_label": STORE_LABELS.get(store, store),
                "unit_price_per_100g": upr,
                "frozen_flag": row["frozen_flag"],
            })

if unit_rows:
    unit_df = pd.DataFrame(unit_rows)

    fig_scatter = px.strip(
        unit_df,
        x="unit_price_per_100g",
        y="cut",
        color="store",
        color_discrete_map=STORE_COLORS,
        hover_data=["store_label", "frozen_flag"],
        labels={
            "unit_price_per_100g": "Price per 100g (SGD)",
            "cut": "",
            "store": "Store",
        },
        stripmode="overlay",
    )
    fig_scatter.update_traces(marker=dict(size=10, opacity=0.8))
    for trace in fig_scatter.data:
        trace.name = STORE_LABELS.get(trace.name, trace.name)
    fig_scatter.update_layout(
        **{**PLOTLY_BASE, "margin": dict(t=30, b=10, l=10, r=20)},
        height=max(380, len(filtered) * 22),
        xaxis_title="Price per 100g (SGD)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title=""),
    )
    fig_scatter.update_xaxes(gridcolor="#f0ede8", linecolor="#e0dbd2", zeroline=False)
    fig_scatter.update_yaxes(gridcolor="rgba(0,0,0,0)", linecolor="rgba(0,0,0,0)", zeroline=False,
                              autorange="reversed")
    st.plotly_chart(fig_scatter, width='stretch')
else:
    st.info("No unit price data available for the current filters.")

st.divider()

# ── CHEAPEST + SPREAD SIDE BY SIDE ────────────────────────────────────────────

col_l, col_r = st.columns(2, gap="large")

with col_l:
    st.subheader("Cheapest store per cut")
    st.markdown("### At the most common pack size")

    chart_df = filtered.sort_values("cheapest_price_sgd", ascending=True).head(30)

    fig2 = go.Figure()
    for _, row in chart_df.iterrows():
        fig2.add_trace(go.Bar(
            y=[row["cut"]],
            x=[row["cheapest_price_sgd"]],
            orientation="h",
            marker_color=STORE_COLORS.get(row["cheapest_store"], "#aaa"),
            text=f"  ${row['cheapest_price_sgd']:.2f}",
            textposition="outside",
            name=STORE_LABELS.get(row["cheapest_store"], row["cheapest_store"]),
            hovertemplate=(
                f"<b>{row['cut']}</b><br>"
                f"Store: {STORE_LABELS.get(row['cheapest_store'], row['cheapest_store'])}<br>"
                f"Price: ${row['cheapest_price_sgd']:.2f} ({row['common_weight_g']:.0f}g)"
                f"<extra></extra>"
            ),
        ))
    fig2.update_layout(
        **PLOTLY_BASE,
        showlegend=False,
        height=max(380, len(chart_df) * 26),
        xaxis_title="Price (SGD)",
    )
    apply_base_axes(fig2)
    fig2.update_yaxes(autorange="reversed")
    st.plotly_chart(fig2, width='stretch')

with col_r:
    st.subheader("Price spread per cut")
    st.markdown("### Savings from switching to the cheapest store")

    spread_df = (
        filtered[filtered["price_spread_sgd"] > 0]
        .sort_values("price_spread_sgd", ascending=True)
        .head(30)
    )

    fig3 = go.Figure()
    for _, row in spread_df.iterrows():
        fig3.add_trace(go.Bar(
            y=[row["cut"]],
            x=[row["price_spread_sgd"]],
            orientation="h",
            marker_color=STORE_COLORS.get(row["cheapest_store"], "#aaa"),
            text=f"  ${row['price_spread_sgd']:.2f}",
            textposition="outside",
            name=STORE_LABELS.get(row["cheapest_store"], row["cheapest_store"]),
            hovertemplate=(
                f"<b>{row['cut']}</b><br>"
                f"Cheapest: {STORE_LABELS.get(row['cheapest_store'], '')} "
                f"${row['cheapest_price_sgd']:.2f}<br>"
                f"Priciest: {STORE_LABELS.get(row['priciest_store'], '')} "
                f"${row['priciest_price_sgd']:.2f}<br>"
                f"Spread: ${row['price_spread_sgd']:.2f}<extra></extra>"
            ),
        ))
    fig3.update_layout(
        **PLOTLY_BASE,
        showlegend=False,
        height=max(380, len(spread_df) * 26),
        xaxis_title="Price Spread (SGD)",
    )
    apply_base_axes(fig3)
    fig3.update_yaxes(autorange="reversed")
    st.plotly_chart(fig3, width='stretch')

st.divider()

# ── FULL TABLE ────────────────────────────────────────────────────────────────

st.subheader("Full comparison table")

table = (
    filtered[[
        "cut", "unified_category", "frozen_flag", "common_weight_g",
        "cheapest_store_label", "cheapest_price_sgd", "cheapest_product_name",
        "priciest_store_label", "priciest_price_sgd", "priciest_product_name",
        "price_spread_sgd", "stores_seen",
    ]]
    .sort_values("price_spread_sgd", ascending=False)
    .reset_index(drop=True)
)
table.columns = [
    "Cut", "Category", "Fresh/Frozen", "Pack (g)",
    "Cheapest Store", "Cheapest", "Cheapest Product",
    "Priciest Store", "Priciest", "Priciest Product",
    "Spread", "Stores",
]
for col in ["Cheapest", "Priciest", "Spread"]:
    table[col] = table[col].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "—")
st.dataframe(table, width='stretch', hide_index=True)

st.divider()

# ── HISTORICAL TRENDS ─────────────────────────────────────────────────────────

st.subheader("Commodity price trends over time")
st.markdown("### Average cheapest price per cut type — daily")

with st.spinner("Loading historical data..."):
    df_all = load_all_dates()

if df_all.empty or df_all["scraped_date"].nunique() <= 1:
    st.info(
        "Only one day of data so far. "
        "Historical trends will appear once the pipeline has run across multiple days."
    )
else:
    cut_options = ["All"] + sorted(df_all["cut"].dropna().unique().tolist())
    selected_cut = st.selectbox("Select cut to track", cut_options, key="hist_cut")

    hist_df = df_all.copy()
    if selected_cut != "All":
        hist_df = hist_df[hist_df["cut"] == selected_cut]

    hist_trend = (
        hist_df.groupby(["scraped_date", "cut", "frozen_flag"])
        .agg(avg_cheapest=("cheapest_price_sgd", "mean"),
             avg_spread=("price_spread_sgd", "mean"))
        .reset_index()
    )

    if not hist_trend.empty:
        col_h1, col_h2 = st.columns(2, gap="large")

        with col_h1:
            st.markdown("#### Avg cheapest price")
            fig_h1 = px.line(
                hist_trend,
                x="scraped_date", y="avg_cheapest",
                color="cut", markers=True,
                labels={"scraped_date": "Date", "avg_cheapest": "Avg Cheapest (SGD)", "cut": "Cut"},
            )
            fig_h1.update_layout(**{**PLOTLY_BASE, "margin": dict(t=30, b=10, l=10, r=10)}, height=300)
            fig_h1.update_xaxes(gridcolor="#f0ede8", linecolor="#e0dbd2", zeroline=False)
            fig_h1.update_yaxes(gridcolor="#f0ede8", linecolor="#e0dbd2", zeroline=False)
            st.plotly_chart(fig_h1, width='stretch')

        with col_h2:
            st.markdown("#### Avg price spread")
            fig_h2 = px.line(
                hist_trend,
                x="scraped_date", y="avg_spread",
                color="cut", markers=True,
                labels={"scraped_date": "Date", "avg_spread": "Avg Spread (SGD)", "cut": "Cut"},
            )
            fig_h2.update_layout(**{**PLOTLY_BASE, "margin": dict(t=30, b=10, l=10, r=10)}, height=300)
            fig_h2.update_xaxes(gridcolor="#f0ede8", linecolor="#e0dbd2", zeroline=False)
            fig_h2.update_yaxes(gridcolor="#f0ede8", linecolor="#e0dbd2", zeroline=False)
            st.plotly_chart(fig_h2, width='stretch')

# ── TOP SAVINGS — FRESH GOODS (COMMODITY) ─────────────────────────────────────

st.subheader("Top savings right now — fresh goods")
st.markdown("### Same cut, same pack size — the most reliable cross-store comparison")

if not df.empty:
    top_com = (
        df[[
            "cut", "unified_category", "frozen_flag", "common_weight_g",
            "cheapest_store", "cheapest_price_sgd",
            "priciest_store", "priciest_price_sgd",
            "cheapest_product_name", "priciest_product_name",
            "price_spread_sgd",
        ]]
        .sort_values("price_spread_sgd", ascending=False)
        .head(8)
        .reset_index(drop=True)
    )
    top_com["cheapest_label"] = top_com["cheapest_store"].map(STORE_LABELS).fillna(
        top_com["cheapest_store"]
    )
    top_com["priciest_label"] = top_com["priciest_store"].map(STORE_LABELS).fillna(
        top_com["priciest_store"]
    )

    best = top_com.iloc[0]
    st.markdown(
        f"<div class='insight-box'>"
        f"Biggest saving today: <b>{best['cut']}</b> "
        f"({best['common_weight_g']:.0f}g, {best['frozen_flag']}) — "
        f"buy at <b>{best['cheapest_label']}</b> (${best['cheapest_price_sgd']:.2f}) "
        f"instead of <b>{best['priciest_label']}</b> "
        f"(${best['priciest_price_sgd']:.2f}) "
        f"and save <b>${best['price_spread_sgd']:.2f}</b>."
        f"</div>",
        unsafe_allow_html=True
    )

    fig_sav = go.Figure()
    for _, row in top_com.sort_values("price_spread_sgd", ascending=True).iterrows():
        bar_label = f"{row['cut']} ({row['common_weight_g']:.0f}g · {row['frozen_flag']})"
        fig_sav.add_trace(go.Bar(
            y=[bar_label],
            x=[row["price_spread_sgd"]],
            orientation="h",
            marker_color=STORE_COLORS.get(row["cheapest_store"], "#aaa"),
            text=f"  Save ${row['price_spread_sgd']:.2f}",
            textposition="outside",
            name=row["cheapest_label"],
            hovertemplate=(
                f"<b>{row['cut']}</b><br>"
                f"Pack: {row['common_weight_g']:.0f}g · {row['frozen_flag']}<br>"
                f"Buy at: {row['cheapest_label']} "
                f"${row['cheapest_price_sgd']:.2f}<br>"
                f"Avoid: {row['priciest_label']} "
                f"${row['priciest_price_sgd']:.2f}<br>"
                f"Save: ${row['price_spread_sgd']:.2f}<extra></extra>"
            ),
        ))

    fig_sav.update_layout(
        **{**PLOTLY_BASE, "margin": dict(t=10, b=20, l=10, r=130)},
        showlegend=False,
        height=300,
        xaxis_title="How much you save (SGD) by choosing the cheapest store",
    )
    apply_base_axes(fig_sav)
    fig_sav.update_yaxes(gridcolor="rgba(0,0,0,0)", linecolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_sav, use_container_width=True)
else:
    st.info("No commodity data available for today.")

st.divider()