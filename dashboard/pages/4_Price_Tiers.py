import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "../../pipeline/etl"))
from load import get_client

st.set_page_config(page_title="Price Tiers", layout="wide")

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
.insight-box {
    background: #f9f7f4; border-left: 3px solid #F5821F;
    border-radius: 6px; padding: 14px 18px; margin-bottom: 16px;
    font-size: 0.95rem; color: #1a1a1a; line-height: 1.6;
}
</style>
""", unsafe_allow_html=True)

TIER_COLORS = {
    "Budget":    "#00843D",
    "Mid-range": "#005BAC",
    "Premium":   "#C8102E",
}
TIER_ORDER = ["Budget", "Mid-range", "Premium"]

PLOTLY_BASE = dict(
    font=dict(family="DM Sans, sans-serif", size=13, color="#1a1a1a"),
    plot_bgcolor="white", paper_bgcolor="white",
    margin=dict(t=30, b=10, l=10, r=10),
)

def apply_base_axes(fig):
    fig.update_xaxes(gridcolor="#f0ede8", linecolor="#e0dbd2", zeroline=False)
    fig.update_yaxes(gridcolor="#f0ede8", linecolor="#e0dbd2", zeroline=False)
    return fig

@st.cache_data(ttl=600)
def load_clusters():
    client = get_client()
    rows, page = [], 0
    while True:
        res = (
            client.table("product_clusters")
            .select(
                "canonical_product_id,price_tier,mean_price,median_price,"
                "min_price,max_price,std_price,price_range,cv,"
                "num_observations,num_stores,shopping_advice"
            )
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

@st.cache_data(ttl=300)
def load_product_names():
    client = get_client()
    res = client.table("canonical_product_daily_recommendations") \
        .select("scraped_date_sg").order("scraped_date_sg", desc=True).limit(1).execute()
    if not res.data:
        return pd.DataFrame()
    latest = res.data[0]["scraped_date_sg"]
    rows, page = [], 0
    while True:
        r = (
            client.table("canonical_product_daily_recommendations")
            .select("canonical_product_id,canonical_name,unified_category,canonical_brand")
            .eq("scraped_date_sg", latest)
            .range(page * 1000, (page + 1) * 1000 - 1)
            .execute()
        )
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        page += 1
    return pd.DataFrame(rows)

with st.spinner("Loading tier data..."):
    df_clusters = load_clusters()
    df_names = load_product_names()

if df_clusters.empty:
    st.error("No cluster data found. Run pipeline/ml/product_clustering.py first.")
    st.stop()

# Join names/categories from today's recs
if not df_names.empty:
    df = df_clusters.merge(
        df_names[["canonical_product_id", "canonical_name", "unified_category", "canonical_brand"]]
        .drop_duplicates("canonical_product_id"),
        on="canonical_product_id", how="left",
    )
else:
    df = df_clusters.copy()
    df["canonical_name"] = "—"
    df["unified_category"] = "—"
    df["canonical_brand"] = "—"

# Drop products with no name resolved from recommendations
df = df[df["canonical_name"].notna() & (df["canonical_name"] != "—")]

# Only keep known tiers, sorted
tiers_present = [t for t in TIER_ORDER if t in df["price_tier"].unique()]
df = df[df["price_tier"].isin(tiers_present)]
df["price_tier"] = pd.Categorical(df["price_tier"], categories=tiers_present, ordered=True)

# ── HEADER ────────────────────────────────────────────────────────────────────

st.title("Price Tiers")
st.markdown(
    "<p style='color:#888; font-size:1.1rem; margin-top:-12px'>"
    "K-Means clustering (k=3) segments products into Budget, Mid-range, and Premium "
    "based on mean price and median price across all scraped stores.</p>",
    unsafe_allow_html=True
)
st.divider()

# ── KPI CARDS ─────────────────────────────────────────────────────────────────

c1, c2, c3, c4 = st.columns(4)
c1.metric("Products Clustered", f"{len(df):,}")
for col, tier in zip([c2, c3, c4], tiers_present):
    n = (df["price_tier"] == tier).sum()
    col.metric(tier, f"{n:,}", f"{n/len(df)*100:.1f}% of total")

st.divider()

# ── TIER SUMMARY STATS ────────────────────────────────────────────────────────

st.subheader("Tier characteristics")
st.markdown("### How each tier differs in price level, spread, and volatility")

tier_summary = (
    df.groupby("price_tier", observed=True)
    .agg(
        avg_mean_price=("mean_price", "mean"),
        avg_price_range=("price_range", "mean"),
        avg_cv=("cv", "mean"),
        avg_stores=("num_stores", "mean"),
        products=("canonical_product_id", "count"),
    )
    .reindex(tiers_present)
    .reset_index()
)

best_value = tiers_present[0] if tiers_present else "Budget"
most_volatile_row = tier_summary.loc[tier_summary["avg_cv"].idxmax()]
st.markdown(
    f"<div class='insight-box'>"
    f"<b>{best_value}</b> products average "
    f"<b>${tier_summary.loc[tier_summary['price_tier']==best_value, 'avg_mean_price'].iloc[0]:.2f}</b>. "
    f"<b>{most_volatile_row['price_tier']}</b> products show the most price variation across stores "
    f"(avg CV: <b>{most_volatile_row['avg_cv']:.3f}</b>) — worth comparing before buying."
    f"</div>",
    unsafe_allow_html=True
)

col_a, col_b, col_c = st.columns(3, gap="large")

with col_a:
    st.markdown("#### Avg mean price")
    fig_mp = go.Figure()
    for _, row in tier_summary.iterrows():
        fig_mp.add_trace(go.Bar(
            x=[row["price_tier"]], y=[row["avg_mean_price"]],
            marker_color=TIER_COLORS.get(row["price_tier"], "#aaa"),
            text=f"${row['avg_mean_price']:.2f}", textposition="outside",
            name=row["price_tier"],
            hovertemplate=f"<b>{row['price_tier']}</b><br>${row['avg_mean_price']:.2f}<extra></extra>",
        ))
    fig_mp.update_layout(
        **{**PLOTLY_BASE, "margin": dict(t=10, b=10, l=10, r=10)},
        showlegend=False, height=240, yaxis_title="SGD",
    )
    apply_base_axes(fig_mp)
    st.plotly_chart(fig_mp, width='stretch')

with col_b:
    st.markdown("#### Avg price range")
    st.caption("Max − min across all stores and days")
    fig_pr = go.Figure()
    for _, row in tier_summary.iterrows():
        fig_pr.add_trace(go.Bar(
            x=[row["price_tier"]], y=[row["avg_price_range"]],
            marker_color=TIER_COLORS.get(row["price_tier"], "#aaa"),
            text=f"${row['avg_price_range']:.2f}", textposition="outside",
            name=row["price_tier"],
            hovertemplate=f"<b>{row['price_tier']}</b><br>${row['avg_price_range']:.2f}<extra></extra>",
        ))
    fig_pr.update_layout(
        **{**PLOTLY_BASE, "margin": dict(t=10, b=10, l=10, r=10)},
        showlegend=False, height=240, yaxis_title="SGD",
    )
    apply_base_axes(fig_pr)
    st.plotly_chart(fig_pr, width='stretch')

with col_c:
    st.markdown("#### Price volatility (CV)")
    st.caption("Higher CV = prices vary more relative to the mean — more reason to compare stores")
    fig_cv = go.Figure()
    for _, row in tier_summary.iterrows():
        fig_cv.add_trace(go.Bar(
            x=[row["price_tier"]], y=[row["avg_cv"]],
            marker_color=TIER_COLORS.get(row["price_tier"], "#aaa"),
            text=f"{row['avg_cv']:.3f}", textposition="outside",
            name=row["price_tier"],
            hovertemplate=f"<b>{row['price_tier']}</b><br>CV: {row['avg_cv']:.3f}<extra></extra>",
        ))
    fig_cv.update_layout(
        **{**PLOTLY_BASE, "margin": dict(t=10, b=10, l=10, r=10)},
        showlegend=False, height=240, yaxis_title="Coefficient of Variation",
    )
    apply_base_axes(fig_cv)
    st.plotly_chart(fig_cv, width='stretch')

st.divider()

# ── PRICE DISTRIBUTION BOX PLOT ───────────────────────────────────────────────

st.subheader("Price distribution within each tier")
st.markdown("### Spread of mean prices — see overlap and outliers between tiers")

fig_box = go.Figure()
for tier in tiers_present:
    sub = df[df["price_tier"] == tier]["mean_price"]
    fig_box.add_trace(go.Box(
        x=sub, name=tier, orientation="h",
        marker_color=TIER_COLORS.get(tier, "#aaa"),
        boxmean=True, line_width=1.5,
        hovertemplate=f"<b>{tier}</b><br>Mean price: $%{{x:.2f}}<extra></extra>",
    ))
fig_box.update_layout(
    **{**PLOTLY_BASE, "margin": dict(t=20, b=30, l=10, r=10)},
    height=260, showlegend=False, xaxis_title="Mean Price (SGD)",
)
apply_base_axes(fig_box)
fig_box.update_yaxes(gridcolor="rgba(0,0,0,0)", linecolor="rgba(0,0,0,0)")
st.plotly_chart(fig_box, width='stretch')

st.divider()

# ── CATEGORY BREAKDOWN ────────────────────────────────────────────────────────

st.subheader("Which categories land in each tier")
st.markdown("### Share of products per tier, broken down by category")

if "unified_category" in df.columns and df["unified_category"].notna().any():
    cat_tier = (
        df.dropna(subset=["unified_category"])
        .groupby(["price_tier", "unified_category"], observed=True)
        .size().reset_index(name="count")
    )

    col_tiers = st.columns(len(tiers_present), gap="medium")
    for i, tier in enumerate(tiers_present):
        with col_tiers[i]:
            sub = cat_tier[cat_tier["price_tier"] == tier].sort_values("count", ascending=False).head(8)
            total = sub["count"].sum()
            sub = sub.copy()
            sub["pct"] = (sub["count"] / total * 100).round(1) if total > 0 else 0
            sub = sub.sort_values("pct", ascending=True)

            max_label_len = sub["unified_category"].str.len().max() if not sub.empty else 10
            left_margin = max(80, int(max_label_len * 7))

            fig_t = go.Figure(go.Bar(
                x=sub["pct"], y=sub["unified_category"],
                orientation="h",
                marker_color=TIER_COLORS.get(tier, "#aaa"),
                text=sub["pct"].apply(lambda x: f"{x:.0f}%"),
                textposition="outside",
                hovertemplate="<b>%{y}</b><br>%{x:.1f}%<extra></extra>",
            ))
            fig_t.update_layout(
                **{**PLOTLY_BASE, "margin": dict(t=40, b=10, l=left_margin, r=50)},
                title=dict(text=tier, font=dict(size=14, color=TIER_COLORS.get(tier, "#aaa")), x=0),
                showlegend=False, height=300,
                xaxis=dict(range=[0, sub["pct"].max() * 1.35 if not sub.empty else 100], showticklabels=False),
            )
            fig_t.update_xaxes(gridcolor="rgba(0,0,0,0)")
            fig_t.update_yaxes(gridcolor="rgba(0,0,0,0)", linecolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_t, width='stretch')

st.divider()

# ── SHOPPING ADVICE ───────────────────────────────────────────────────────────

st.subheader("Shopping advice breakdown")
st.markdown("### How many products fall into each advice bucket, by tier")

advice_df = (
    df.groupby(["price_tier", "shopping_advice"], observed=True)
    .size().reset_index(name="count")
    .sort_values(["price_tier", "count"], ascending=[True, False])
)

fig_adv = px.bar(
    advice_df,
    x="count", y="shopping_advice",
    color="price_tier",
    color_discrete_map=TIER_COLORS,
    orientation="h",
    barmode="group",
    labels={"count": "Number of products", "shopping_advice": "", "price_tier": "Tier"},
    custom_data=["price_tier"],
)
fig_adv.update_traces(
    hovertemplate="<b>%{customdata[0]}</b><br>%{y}<br>%{x:,} products<extra></extra>"
)
for trace in fig_adv.data:
    trace.name = trace.name  # already the tier label
fig_adv.update_layout(
    **{**PLOTLY_BASE, "margin": dict(t=20, b=20, l=10, r=20)},
    height=max(280, advice_df["shopping_advice"].nunique() * 55),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title=""),
)
apply_base_axes(fig_adv)
fig_adv.update_yaxes(gridcolor="rgba(0,0,0,0)", linecolor="rgba(0,0,0,0)")
st.plotly_chart(fig_adv, width='stretch')

st.divider()

# ── PRODUCT EXPLORER ──────────────────────────────────────────────────────────

st.subheader("Explore products by tier")

col_f1, col_f2, col_f3 = st.columns([1, 1, 2])
with col_f1:
    sel_tier = st.selectbox("Price tier", ["All"] + tiers_present)
with col_f2:
    cat_opts = ["All"] + sorted(df["unified_category"].dropna().unique().tolist())
    sel_cat = st.selectbox("Category", cat_opts)
with col_f3:
    search = st.text_input("Search product", placeholder="Milo, Greek yogurt…")

explorer = df.copy()
if sel_tier != "All":
    explorer = explorer[explorer["price_tier"] == sel_tier]
if sel_cat != "All":
    explorer = explorer[explorer["unified_category"] == sel_cat]
if search:
    explorer = explorer[explorer["canonical_name"].str.contains(search, case=False, na=False)]

explorer_display = (
    explorer[["canonical_name", "canonical_brand", "unified_category", "price_tier",
               "mean_price", "price_range", "cv", "num_stores", "shopping_advice"]]
    .sort_values("mean_price")
    .reset_index(drop=True)
    .copy()
)
explorer_display["mean_price"] = explorer_display["mean_price"].apply(lambda x: f"${x:.2f}")
explorer_display["price_range"] = explorer_display["price_range"].apply(lambda x: f"${x:.2f}")
explorer_display["cv"] = explorer_display["cv"].apply(lambda x: f"{x:.3f}")
explorer_display.columns = ["Product", "Brand", "Category", "Tier",
                             "Mean Price", "Price Range", "CV", "Stores", "Shopping Advice"]

st.markdown(f"<p style='color:#888; font-size:0.85rem'>{len(explorer_display):,} products</p>", unsafe_allow_html=True)
st.dataframe(explorer_display, width='stretch', hide_index=True)
