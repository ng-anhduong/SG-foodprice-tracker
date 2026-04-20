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
.section-header {
    font-family: 'DM Sans', sans-serif; font-size: 1.2rem; font-weight: 700;
    color: #1a1a1a; letter-spacing: -0.01em; margin: 0 0 4px 0;
    padding-bottom: 10px; border-bottom: 2px solid #ebe7e0;
}
.section-sub { font-size: 0.85rem; color: #888; margin-top: 2px; margin-bottom: 16px; }
.insight-box {
    background: #f9f7f4; border-left: 3px solid #F5821F; border-radius: 6px;
    padding: 14px 18px; margin-bottom: 16px; font-size: 0.95rem;
    color: #1a1a1a; line-height: 1.6;
}
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
    cut_opts = ["All"] + sorted(df["cut"].dropna().unique().tolist())
    selected_cut_filter = st.selectbox("Cut", cut_opts)

filtered = df.copy()
if selected_cat != "All":
    filtered = filtered[filtered["unified_category"] == selected_cat]
if selected_frozen != "All":
    filtered = filtered[filtered["frozen_flag"] == selected_frozen]
if selected_cut_filter != "All":
    filtered = filtered[filtered["cut"] == selected_cut_filter]

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

# ── BAR CHART: CHEAPEST UNIT PRICE PER CUT ────────────────────────────────────

st.markdown("<div class='section-header'>Cheapest unit price per cut</div>", unsafe_allow_html=True)
st.markdown("<div class='section-sub'>Lowest price per 100g available today — bar colour shows which store is cheapest</div>", unsafe_allow_html=True)

unit_rows = []
for _, row in filtered.iterrows():
    for store, upr in row["unit_prices"].items():
        if upr is not None:
            unit_rows.append({
                "cut": row["cut"],
                "store": store,
                "store_label": STORE_LABELS.get(store, store),
                "unit_price_per_100g": float(upr),
                "frozen_flag": row["frozen_flag"],
                "common_weight_g": row.get("common_weight_g"),
            })

if unit_rows:
    unit_df = pd.DataFrame(unit_rows)
    cheapest_unit = (
        unit_df.sort_values("unit_price_per_100g")
        .drop_duplicates("cut")
        .sort_values("unit_price_per_100g", ascending=False)
        .reset_index(drop=True)
    )

    fig_bar = go.Figure()
    for _, row in cheapest_unit.iterrows():
        fig_bar.add_trace(go.Bar(
            y=[row["cut"]],
            x=[row["unit_price_per_100g"]],
            orientation="h",
            marker_color=STORE_COLORS.get(row["store"], "#aaa"),
            text=f"  ${row['unit_price_per_100g']:.2f}",
            textposition="outside",
            textfont=dict(size=12, color="#1a1a1a"),
            name=row["store_label"],
            hovertemplate=(
                f"<b>{row['cut']}</b><br>"
                f"Store: {row['store_label']}<br>"
                f"Price per 100g: ${row['unit_price_per_100g']:.2f}<br>"
                f"Pack: {row['common_weight_g']:.0f}g · {row['frozen_flag']}"
                f"<extra></extra>"
            ),
        ))

    # legend entries per store
    seen = set()
    for trace in fig_bar.data:
        if trace.name in seen:
            trace.showlegend = False
        else:
            seen.add(trace.name)
            trace.showlegend = True

    fig_bar.update_layout(
        **{**PLOTLY_BASE, "margin": dict(t=10, b=10, l=10, r=80)},
        height=max(320, len(cheapest_unit) * 28),
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1, title=""),
        xaxis_title="Price per 100g (SGD)",
    )
    fig_bar.update_xaxes(gridcolor="#f0ede8", linecolor="#e0dbd2", zeroline=False)
    fig_bar.update_yaxes(gridcolor="rgba(0,0,0,0)", linecolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_bar, use_container_width=True)
else:
    st.info("No unit price data available for the current filters.")

st.divider()

# ── TABLE: CHEAPEST / PRICIEST / SPREAD PER 100G ──────────────────────────────

st.markdown("<div class='section-header'>Store comparison table</div>", unsafe_allow_html=True)
st.markdown("<div class='section-sub'>Cheapest vs priciest store per cut — with price spread per 100g</div>", unsafe_allow_html=True)

if unit_rows:
    cheapest_by_cut = (
        unit_df.sort_values("unit_price_per_100g")
        .drop_duplicates("cut")
        .rename(columns={
            "store_label": "cheapest_store_label",
            "unit_price_per_100g": "cheapest_per_100g",
        })[["cut", "cheapest_store_label", "cheapest_per_100g"]]
    )
    priciest_by_cut = (
        unit_df.sort_values("unit_price_per_100g", ascending=False)
        .drop_duplicates("cut")
        .rename(columns={
            "store_label": "priciest_store_label",
            "unit_price_per_100g": "priciest_per_100g",
        })[["cut", "priciest_store_label", "priciest_per_100g"]]
    )
    tbl = cheapest_by_cut.merge(priciest_by_cut, on="cut")
    tbl["spread_per_100g"] = (tbl["priciest_per_100g"] - tbl["cheapest_per_100g"]).round(2)
    tbl = tbl[tbl["spread_per_100g"] > 0].sort_values("spread_per_100g", ascending=False).reset_index(drop=True)

    for col in ["cheapest_per_100g", "priciest_per_100g", "spread_per_100g"]:
        tbl[col] = tbl[col].apply(lambda x: f"${x:.2f}")

    tbl.columns = ["Cut", "Cheapest Store", "Cheapest /100g", "Priciest Store", "Priciest /100g", "Spread /100g"]
    st.dataframe(tbl, use_container_width=True, hide_index=True)
    st.caption("Only cuts available at 2+ stores with a price difference are shown.")
else:
    st.info("No unit price data to compare.")

st.divider()

# ── TOP SAVINGS ───────────────────────────────────────────────────────────────

st.markdown("<div class='section-header'>Top savings right now</div>", unsafe_allow_html=True)
st.markdown("<div class='section-sub'>Biggest price spread across stores — same cut, same pack size</div>", unsafe_allow_html=True)

top_com = (
    filtered[filtered["price_spread_sgd"] > 0]
    .sort_values("price_spread_sgd", ascending=False)
    .head(10)
    .reset_index(drop=True)
)

if not top_com.empty:
    top_com["cheapest_label"] = top_com["cheapest_store"].map(STORE_LABELS).fillna(top_com["cheapest_store"])
    top_com["priciest_label"] = top_com["priciest_store"].map(STORE_LABELS).fillna(top_com["priciest_store"])

    best = top_com.iloc[0]
    st.markdown(
        f"<div class='insight-box'>"
        f"Biggest saving today: <b>{best['cut']}</b> "
        f"({best['common_weight_g']:.0f}g · {best['frozen_flag']}) — "
        f"buy at <b>{best['cheapest_label']}</b> (${best['cheapest_price_sgd']:.2f}) "
        f"instead of <b>{best['priciest_label']}</b> (${best['priciest_price_sgd']:.2f}) "
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
            textfont=dict(size=12, color="#1a1a1a"),
            name=row["cheapest_label"],
            hovertemplate=(
                f"<b>{row['cut']}</b><br>"
                f"Pack: {row['common_weight_g']:.0f}g · {row['frozen_flag']}<br>"
                f"Buy at: {row['cheapest_label']} ${row['cheapest_price_sgd']:.2f}<br>"
                f"Avoid: {row['priciest_label']} ${row['priciest_price_sgd']:.2f}<br>"
                f"Save: ${row['price_spread_sgd']:.2f}<extra></extra>"
            ),
        ))
    fig_sav.update_layout(
        **{**PLOTLY_BASE, "margin": dict(t=10, b=10, l=10, r=100)},
        showlegend=False,
        height=max(320, len(top_com) * 36),
        xaxis_title="Price spread (SGD)",
    )
    fig_sav.update_xaxes(gridcolor="#f0ede8", linecolor="#e0dbd2", zeroline=False)
    fig_sav.update_yaxes(gridcolor="rgba(0,0,0,0)", linecolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_sav, use_container_width=True)
else:
    st.info("No price spread data for the current filters.")

st.divider()

# ── HISTORICAL TRENDS ─────────────────────────────────────────────────────────

st.markdown("<div class='section-header'>Price trends over time</div>", unsafe_allow_html=True)
st.markdown("<div class='section-sub'>Average cheapest price and spread per cut — daily</div>", unsafe_allow_html=True)

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
            st.markdown("<div class='section-sub' style='margin-bottom:8px'>Avg cheapest price</div>", unsafe_allow_html=True)
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
            st.markdown("<div class='section-sub' style='margin-bottom:8px'>Avg price spread</div>", unsafe_allow_html=True)
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

