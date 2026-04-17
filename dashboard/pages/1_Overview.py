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
    margin=dict(t=30, b=10, l=10, r=10),
)

def apply_base_axes(fig):
    fig.update_xaxes(gridcolor="#f0ede8", linecolor="#e0dbd2", zeroline=False)
    fig.update_yaxes(gridcolor="#f0ede8", linecolor="#e0dbd2", zeroline=False)
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
def load_recs_today():
    df = pd.DataFrame(fetch_all("canonical_product_daily_recommendations", "scraped_date_sg"))
    if df.empty:
        return df
    latest = df["scraped_date_sg"].max()
    return df[df["scraped_date_sg"] == latest]

@st.cache_data(ttl=300)
def load_recs_all():
    return pd.DataFrame(fetch_all("canonical_product_daily_recommendations", "scraped_date_sg"))

@st.cache_data(ttl=300)
def load_commodity():
    df = pd.DataFrame(fetch_all("commodity_price_comparisons", "scraped_date"))
    if df.empty:
        return df
    latest = df["scraped_date"].max()
    return df[df["scraped_date"] == latest]

with st.spinner("Loading..."):
    df = load_recs_today()
    df_com = load_commodity()

if df.empty:
    st.error("No data. Run the pipeline first.")
    st.stop()

# Only products seen in 2+ stores — identical to your original working version
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

c1, c2, c3, c4 = st.columns(4)
c1.metric("Products Tracked", f"{len(df):,}")
c2.metric("Cross-Store Comparable", f"{len(df_multi):,}")
c3.metric(
    "Avg Price Spread",
    f"${df_multi['price_spread_sgd'].mean():.2f}" if not df_multi.empty else "—",
    help="On average, you save this much by choosing the cheapest over the priciest store for the same product"
)
c4.metric("Categories", f"{df['unified_category'].nunique()}")

st.divider()

# ── WHERE TO SHOP TODAY ───────────────────────────────────────────────────────

st.subheader("Where to shop today")
st.markdown("### Based on which store offers the lowest price most often across matched products")

if not df_multi.empty:
    store_counts = df_multi["cheapest_store"].value_counts()
    top_store = store_counts.idxmax()
    top_store_label = STORE_LABELS.get(top_store, top_store)
    top_pct = round(store_counts.iloc[0] / len(df_multi) * 100, 1)

    # Per category: which store wins most — accurate calculation
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
        cat_parts.append(f"<b>{store_label}</b> for {row['unified_category']} ({pct:.0f}%)")

    st.markdown(
        f"<div class='insight-box'>"
        f"Overall, <b>{top_store_label}</b> offers the lowest price on <b>{top_pct}%</b> of comparable products today."
        f"<br>By category — {' &nbsp;·&nbsp; '.join(cat_parts[:4])}."
        f"</div>",
        unsafe_allow_html=True
    )

col_l, col_r = st.columns([1, 1], gap="large")

with col_l:
    st.markdown("### Price leadership by store")

    # Exact same logic as your original working version
    counts = df_multi["cheapest_store"].value_counts().reset_index()
    counts.columns = ["store", "count"]
    counts["label"] = counts["store"].map(STORE_LABELS).fillna(counts["store"])
    counts["pct"] = (counts["count"] / counts["count"].sum() * 100).round(1)
    counts = counts.sort_values("count", ascending=True)

    fig1 = go.Figure()
    for _, row in counts.iterrows():
        fig1.add_trace(go.Bar(
            y=[row["label"]],
            x=[row["count"]],
            orientation="h",
            marker_color=STORE_COLORS.get(row["store"], "#aaa"),
            text=f"{row['count']:,}  ({row['pct']}%)",
            textposition="inside",
            insidetextanchor="end",
            textfont=dict(color="white", size=13, family="DM Sans"),
            name=row["label"],
            hovertemplate=f"<b>{row['label']}</b><br>{row['count']:,} products cheapest<extra></extra>",
        ))

    fig1.update_layout(
        **{**PLOTLY_BASE, "margin": dict(t=20, b=20, l=10, r=20)},
        showlegend=False,
        height=240,
        xaxis_title="Number of products where cheapest",
    )
    apply_base_axes(fig1)
    fig1.update_yaxes(gridcolor="rgba(0,0,0,0)", linecolor="rgba(0,0,0,0)")
    st.plotly_chart(fig1, width='stretch')

with col_r:
    st.markdown("### Best store per category")

    heat = (
        df_multi.groupby(["unified_category", "cheapest_store"])
        .size().reset_index(name="count")
    )
    pivot = heat.pivot(
        index="unified_category",
        columns="cheapest_store",
        values="count"
    ).fillna(0)
    pivot.columns = [STORE_LABELS.get(c, c) for c in pivot.columns]

    fig2 = px.imshow(
        pivot,
        color_continuous_scale="Oranges",
        aspect="auto",
        text_auto=True,
        labels=dict(x="Store", y="", color="Products"),
    )
    fig2.update_layout(
        **{**PLOTLY_BASE, "margin": dict(t=20, b=20, l=10, r=20)},
        height=240,
        coloraxis_showscale=False,
    )
    fig2.update_xaxes(side="bottom", gridcolor="rgba(0,0,0,0)", linecolor="rgba(0,0,0,0)")
    fig2.update_yaxes(gridcolor="rgba(0,0,0,0)", linecolor="rgba(0,0,0,0)")
    fig2.update_traces(textfont_size=12)
    st.plotly_chart(fig2, width='stretch')

st.divider()

# ── TOP SAVINGS RIGHT NOW — COMMODITY ─────────────────────────────────────────
# Uses commodity_price_comparisons which compares same cut at same pack size.
# More reliable than canonical product spread which can have matching noise.

st.subheader("Top savings right now")
st.markdown("### Same cut, same pack size — the most reliable cross-store comparison")

if not df_com.empty:
    top_com = (
        df_com[[
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
    top_com["cheapest_label"] = top_com["cheapest_store"].map(STORE_LABELS).fillna(top_com["cheapest_store"])
    top_com["priciest_label"] = top_com["priciest_store"].map(STORE_LABELS).fillna(top_com["priciest_store"])

    if not top_com.empty:
        best = top_com.iloc[0]
        st.markdown(
            f"<div class='insight-box'>"
            f"Biggest saving today: <b>{best['cut']}</b> ({best['common_weight_g']:.0f}g, {best['frozen_flag']}) — "
            f"buy at <b>{best['cheapest_label']}</b> (${best['cheapest_price_sgd']:.2f}) instead of "
            f"<b>{best['priciest_label']}</b> (${best['priciest_price_sgd']:.2f}) "
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
                f"Buy at: {row['cheapest_label']} ${row['cheapest_price_sgd']:.2f}<br>"
                f"Avoid: {row['priciest_label']} ${row['priciest_price_sgd']:.2f}<br>"
                f"Save: ${row['price_spread_sgd']:.2f}<extra></extra>"
            ),
        ))

    fig_sav.update_layout(
        **{**PLOTLY_BASE, "margin": dict(t=40, b=20, l=10, r=130)},
        showlegend=False,
        height=250,
        xaxis_title="How much you save (SGD) by choosing the cheapest store",
    )
    apply_base_axes(fig_sav)
    fig_sav.update_yaxes(gridcolor="rgba(0,0,0,0)", linecolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_sav, width='stretch')
else:
    st.info("No commodity data available for today.")

st.divider()

# ── PRICE SPREAD BY CATEGORY ──────────────────────────────────────────────────

st.subheader("How much prices vary by category")
st.markdown("### Wider spread means more to gain from comparing stores before buying")

spread_df = df_multi[df_multi["price_spread_sgd"] > 0].copy()

if not spread_df.empty:
    cat_spreads = spread_df.groupby("unified_category")["price_spread_sgd"].median().sort_values(ascending=False)
    highest_cat = cat_spreads.index[0]
    highest_val = cat_spreads.iloc[0]
    lowest_cat = cat_spreads.index[-1]
    lowest_val = cat_spreads.iloc[-1]

    st.markdown(
        f"<div class='insight-box'>"
        f"<b>{highest_cat}</b> has the most price variation across stores today "
        f"(median spread: <b>${highest_val:.2f}</b>) — always worth comparing before buying. "
        f"<b>{lowest_cat}</b> is the most consistent "
        f"(median spread: <b>${lowest_val:.2f}</b>) — prices are similar wherever you shop."
        f"</div>",
        unsafe_allow_html=True
    )

    fig3 = go.Figure()
    categories = sorted(spread_df["unified_category"].dropna().unique())
    palette = px.colors.qualitative.Set2

    for i, cat in enumerate(categories):
        cat_data = spread_df[spread_df["unified_category"] == cat]["price_spread_sgd"]
        fig3.add_trace(go.Violin(
            y=cat_data,
            name=cat,
            box_visible=True,
            meanline_visible=True,
            line_color=palette[i % len(palette)],
            fillcolor=palette[i % len(palette)],
            opacity=0.7,
            points="outliers",
            hovertemplate=f"<b>{cat}</b><br>Spread: $%{{y:.2f}}<extra></extra>",
        ))

    fig3.update_layout(
        **{**PLOTLY_BASE, "margin": dict(t=20, b=40, l=60, r=20)},
        height=380,
        showlegend=False,
        yaxis_title="Price Spread (SGD)",
        violingap=0.3,
        violinmode="overlay",
    )
    apply_base_axes(fig3)
    fig3.update_xaxes(gridcolor="rgba(0,0,0,0)", linecolor="#e0dbd2")
    st.plotly_chart(fig3, width='stretch')

st.divider()

# ── HISTORICAL TRENDS ─────────────────────────────────────────────────────────

st.subheader("Trends over time")
st.markdown("### How store price competitiveness has shifted day by day")

try:
    with st.spinner("Loading historical data..."):
        df_hist = load_recs_all()
except Exception:
    st.warning("Could not load historical data — Supabase may be temporarily unavailable. Try refreshing.")
    st.stop()

if df_hist.empty or df_hist["scraped_date_sg"].nunique() <= 1:
    st.info("Only one day of data so far. Trends will appear once the pipeline has run across multiple days.")
else:
    daily_counts = (
        df_hist.groupby("scraped_date_sg")["canonical_product_id"]
        .count().reset_index(name="count")
    )
    median_count = daily_counts["count"].median()
    valid_dates = daily_counts[
        (daily_counts["count"] >= median_count * 0.10) &
        (daily_counts["count"] <= median_count * 3.0)
    ]["scraped_date_sg"].tolist()

    df_hist_clean = df_hist[df_hist["scraped_date_sg"].isin(valid_dates)]

    if df_hist_clean.empty or df_hist_clean["scraped_date_sg"].nunique() <= 1:
        st.info("Not enough complete scrape days yet. Check back after the pipeline has run a few more times.")
    else:
        dates = sorted(df_hist_clean["scraped_date_sg"].unique())

        col_d1, col_d2, col_d3 = st.columns([1, 1, 2])
        with col_d1:
            date_from = st.selectbox("From", options=dates, index=0)
        with col_d2:
            date_to = st.selectbox("To", options=dates, index=len(dates) - 1)
        with col_d3:
            hist_cat = st.selectbox(
                "Category",
                options=["All"] + sorted(
                    df_hist_clean["unified_category"].dropna().unique().tolist()
                ),
                key="hist_cat",
            )

        hist_filtered = df_hist_clean[
            (df_hist_clean["scraped_date_sg"] >= date_from) &
            (df_hist_clean["scraped_date_sg"] <= date_to)
        ]
        if hist_cat != "All":
            hist_filtered = hist_filtered[
                hist_filtered["unified_category"] == hist_cat
            ]

        if not hist_filtered.empty:

            # Historical insight
            store_win_totals = hist_filtered.groupby("cheapest_store").size().sort_values(ascending=False)
            if not store_win_totals.empty:
                most_consistent = STORE_LABELS.get(
                    store_win_totals.index[0], store_win_totals.index[0]
                )
                most_consistent_pct = round(
                    store_win_totals.iloc[0] / store_win_totals.sum() * 100, 1
                )
                insight = (
                    f"Over this period, <b>{most_consistent}</b> was the cheapest store most often "
                    f"— offering the lowest price on <b>{most_consistent_pct}%</b> of product comparisons."
                )
                if len(store_win_totals) > 1:
                    second = STORE_LABELS.get(store_win_totals.index[1], store_win_totals.index[1])
                    second_pct = round(store_win_totals.iloc[1] / store_win_totals.sum() * 100, 1)
                    insight += f" <b>{second}</b> came second at <b>{second_pct}%</b>."
                st.markdown(
                    f"<div class='insight-box'>{insight}</div>",
                    unsafe_allow_html=True
                )

            col_t1, col_t2 = st.columns(2, gap="large")

            with col_t1:
                st.markdown("#### Avg price spread over time")
                st.caption("Higher spread = more savings available by comparing stores that day")

                spread_trend = (
                    hist_filtered[hist_filtered["stores_seen_for_day"] >= 2]
                    .groupby(["scraped_date_sg", "unified_category"])
                    .agg(avg_spread=("price_spread_sgd", "mean"))
                    .reset_index()
                )

                if not spread_trend.empty and spread_trend["scraped_date_sg"].nunique() > 1:
                    fig_t1 = px.line(
                        spread_trend,
                        x="scraped_date_sg",
                        y="avg_spread",
                        color="unified_category",
                        markers=True,
                        labels={
                            "scraped_date_sg": "Date",
                            "avg_spread": "Avg Spread (SGD)",
                            "unified_category": "Category",
                        },
                    )
                    fig_t1.update_traces(
                        line=dict(width=2), marker=dict(size=7), connectgaps=False
                    )
                    fig_t1.update_layout(
                        **{**PLOTLY_BASE, "margin": dict(t=20, b=100, l=60, r=20)},
                        height=360,
                        legend=dict(
                            orientation="h",
                            yanchor="top", y=-0.30,
                            xanchor="left", x=0,
                            title="", font=dict(size=11),
                        ),
                    )
                    apply_base_axes(fig_t1)
                    st.plotly_chart(fig_t1, width='stretch')
                else:
                    st.info("Need at least 2 days to show a trend.")

            with col_t2:
                st.markdown("#### Which store was cheapest most often")
                st.caption("Share of products where each store held the lowest price that day")

                store_wins = (
                    hist_filtered.groupby(["scraped_date_sg", "cheapest_store"])
                    .size().reset_index(name="count")
                )
                daily_totals = store_wins.groupby("scraped_date_sg")["count"].transform("sum")
                store_wins["pct"] = (store_wins["count"] / daily_totals * 100).round(1)
                store_wins["store_label"] = store_wins["cheapest_store"].map(STORE_LABELS).fillna(
                    store_wins["cheapest_store"]
                )

                fig_t2 = px.bar(
                    store_wins,
                    x="scraped_date_sg",
                    y="pct",
                    color="cheapest_store",
                    color_discrete_map=STORE_COLORS,
                    barmode="stack",
                    text=store_wins["pct"].apply(lambda x: f"{x:.0f}%" if x >= 4 else ""),
                    labels={
                        "scraped_date_sg": "Date",
                        "pct": "Share (%)",
                        "cheapest_store": "Store",
                    },
                    custom_data=["store_label", "count"],
                )
                fig_t2.update_traces(
                    textposition="inside",
                    insidetextanchor="middle",
                    textfont=dict(color="white", size=12),
                    hovertemplate=(
                        "<b>%{customdata[0]}</b><br>"
                        "Share: %{y:.1f}%<br>"
                        "Products: %{customdata[1]:,}<extra></extra>"
                    ),
                )
                for trace in fig_t2.data:
                    trace.name = STORE_LABELS.get(trace.name, trace.name)
                fig_t2.update_layout(
                    **{**PLOTLY_BASE, "margin": dict(t=20, b=100, l=60, r=20)},
                    height=440,
                    yaxis_title="Share (%)",
                    yaxis_range=[0, 105],
                    legend=dict(
                        orientation="h",
                        yanchor="top", y=-0.30,
                        xanchor="left", x=0,
                        title="",
                    ),
                )
                apply_base_axes(fig_t2)
                st.plotly_chart(fig_t2, width='stretch')

            st.caption(
                f"Showing {len(dates)} complete scrape day(s). "
                f"Days with fewer than 10% or more than 3× the median product count are excluded."
            )