import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "../../pipeline/etl"))
from load import get_client

st.set_page_config(page_title="Compare Products", layout="wide")

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
.section-header {
    font-family: 'DM Sans', sans-serif;
    font-size: 1.2rem; font-weight: 700; color: #1a1a1a;
    letter-spacing: -0.01em; margin: 0 0 4px 0;
    padding-bottom: 10px; border-bottom: 2px solid #ebe7e0;
}
.section-sub {
    font-size: 0.85rem; color: #888;
    margin-top: 2px; margin-bottom: 16px; font-weight: 400;
}
.insight-box {
    background: #f9f7f4; border-left: 3px solid #F5821F;
    border-radius: 6px; padding: 14px 18px; margin-bottom: 16px;
    font-size: 0.95rem; color: #1a1a1a; line-height: 1.6;
}
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
.stDataFrame { border: 1px solid #ebe7e0 !important; border-radius: 8px; }
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
    margin=dict(t=40, b=20, l=10, r=20),
)

def apply_base_axes(fig):
    fig.update_xaxes(gridcolor="#f0ede8", linecolor="#e0dbd2", zeroline=False)
    fig.update_yaxes(gridcolor="#f0ede8", linecolor="#e0dbd2", zeroline=False)
    return fig

def _fetch_latest_date(table, date_col):
    client = get_client()
    res = (
        client.table(table).select(date_col)
        .order(date_col, desc=True).limit(1).execute()
    )
    return res.data[0][date_col] if res.data else None

def _fetch_for_date(table, date_col, date_val, columns="*"):
    client = get_client()
    rows, page = [], 0
    while True:
        res = (
            client.table(table).select(columns)
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

FRESH_CATEGORIES = {"Fruits & Vegetables", "Meat & Seafood"}

@st.cache_data(ttl=300)
def load_recs_and_prices():
    """
    Load both tables for the same date, then compute actual unique store
    count per canonical_product_id from df_prices — this is the ground truth.
    Filter both tables to only products with 2+ unique stores in df_prices.
    """
    latest_rec = _fetch_latest_date(
        "canonical_product_daily_recommendations", "scraped_date_sg"
    )
    latest_price = _fetch_latest_date(
        "canonical_product_daily_prices", "scraped_date_sg"
    )
    if not latest_rec or not latest_price:
        return pd.DataFrame(), pd.DataFrame()

    df_rec = pd.DataFrame(_fetch_for_date(
        "canonical_product_daily_recommendations",
        "scraped_date_sg", latest_rec
    ))
    df_prices = pd.DataFrame(_fetch_for_date(
        "canonical_product_daily_prices",
        "scraped_date_sg", latest_price
    ))

    if df_rec.empty or df_prices.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Exclude fresh categories
    df_rec = df_rec[~df_rec["unified_category"].isin(FRESH_CATEGORIES)]
    df_prices = df_prices[~df_prices["unified_category"].isin(FRESH_CATEGORIES)]

    # Compute actual unique store count per canonical_product_id from df_prices
    # This is the ground truth — not stores_seen_for_day from recommendations
    actual_store_counts = (
        df_prices.groupby("canonical_product_id")["store"]
        .nunique()
        .reset_index()
        .rename(columns={"store": "actual_store_count"})
    )

    # Merge actual store count into both dataframes
    df_rec = df_rec.merge(actual_store_counts, on="canonical_product_id", how="left")
    df_prices = df_prices.merge(actual_store_counts, on="canonical_product_id", how="left")

    return df_rec, df_prices

with st.spinner("Loading..."):
    df_rec, df_prices = load_recs_and_prices()

if df_rec.empty:
    st.error("No comparable data found.")
    st.stop()

# ── HEADER ────────────────────────────────────────────────────────────────────

latest_date = df_rec["scraped_date_sg"].max()

st.title("Compare Products")
st.markdown(
    f"<p style='color:#888; font-size:0.9rem; margin-top:-12px'>"
    f"All tracked canonical products. Select a product to see cross-store price comparison (2+ stores required). "
    f"Fruits &amp; Vegetables and Meat &amp; Seafood are on the Fresh &amp; Commodity page."
    f"&nbsp;·&nbsp;<strong style='color:#1a1a1a'>As of {latest_date}</strong></p>",
    unsafe_allow_html=True
)
st.divider()

# ── FILTERS ───────────────────────────────────────────────────────────────────

col1, col2 = st.columns([1, 2])
with col1:
    cats = ["All"] + sorted(df_rec["unified_category"].dropna().unique().tolist())
    selected_cat = st.selectbox("Category", cats)
with col2:
    search = st.text_input(
        "Search product",
        placeholder="Milo, Greek yogurt, soy sauce…"
    )

filtered = df_rec.copy()
if selected_cat != "All":
    filtered = filtered[filtered["unified_category"] == selected_cat]
if search:
    filtered = filtered[
        filtered["canonical_name"].str.contains(search, case=False, na=False)
    ]

st.markdown(
    f"<p style='color:#888; font-size:0.85rem'>"
    f"{len(filtered):,} products found — select one below to compare prices across stores</p>",
    unsafe_allow_html=True,
)

# ── PRODUCT TABLE ─────────────────────────────────────────────────────────────

display = (
    filtered[[
        "canonical_name", "canonical_brand", "unified_category", "size_display",
        "actual_store_count",   # ground truth store count from df_prices
        "cheapest_store", "cheapest_price_sgd",
        "priciest_store", "priciest_price_sgd",
        "price_spread_sgd",
    ]]
    .sort_values("price_spread_sgd", ascending=False)
    .reset_index(drop=True)
    .copy()
)
display["cheapest_store"] = display["cheapest_store"].map(STORE_LABELS).fillna(
    display["cheapest_store"]
)
display["priciest_store"] = display["priciest_store"].map(STORE_LABELS).fillna(
    display["priciest_store"]
)
display["size_display"] = display["size_display"].fillna("—")
for col in ["cheapest_price_sgd", "priciest_price_sgd", "price_spread_sgd"]:
    display[col] = display[col].apply(
        lambda x: f"${x:.2f}" if pd.notna(x) else "—"
    )
display.columns = [
    "Product", "Brand", "Category", "Size", "Stores",
    "Cheapest Store", "Cheapest",
    "Priciest Store", "Priciest", "Spread",
]
st.dataframe(display, use_container_width=True, hide_index=True)
st.divider()

# ── TOP 10 SAVINGS ────────────────────────────────────────────────────────────

st.markdown("<div class='section-header'>Top 10 savings today</div>", unsafe_allow_html=True)
st.markdown("<div class='section-sub'>Biggest price spread across stores — products available at 2+ stores only</div>", unsafe_allow_html=True)

top10_src = (
    filtered[
        (filtered["actual_store_count"] >= 2) &
        (filtered["price_spread_sgd"] > 0)
    ]
    .sort_values("price_spread_sgd", ascending=False)
    .head(10)
    .reset_index(drop=True)
)

if not top10_src.empty:
    top10_src["cheapest_label"] = top10_src["cheapest_store"].map(STORE_LABELS).fillna(top10_src["cheapest_store"])
    top10_src["priciest_label"] = top10_src["priciest_store"].map(STORE_LABELS).fillna(top10_src["priciest_store"])

    best = top10_src.iloc[0]
    size_str = f" · {best['size_display']}" if pd.notna(best.get("size_display")) and str(best.get("size_display")).strip() not in ("", "—") else ""
    st.markdown(
        f"<div class='insight-box'>"
        f"Biggest saving today: <b>{best['canonical_name']}</b>{size_str} — "
        f"buy at <b>{best['cheapest_label']}</b> (${best['cheapest_price_sgd']:.2f}) "
        f"instead of <b>{best['priciest_label']}</b> (${best['priciest_price_sgd']:.2f}) "
        f"and save <b>${best['price_spread_sgd']:.2f}</b>."
        f"</div>",
        unsafe_allow_html=True
    )

    fig_top10 = go.Figure()
    for _, row in top10_src.sort_values("price_spread_sgd", ascending=True).iterrows():
        size_str = f" · {row['size_display']}" if pd.notna(row.get("size_display")) and str(row.get("size_display")).strip() not in ("", "—") else ""
        bar_label = f"{row['canonical_name']}{size_str}"
        fig_top10.add_trace(go.Bar(
            y=[bar_label],
            x=[row["price_spread_sgd"]],
            orientation="h",
            width=0.5,
            marker_color=STORE_COLORS.get(row["cheapest_store"], "#aaa"),
            text=f"  Save ${row['price_spread_sgd']:.2f}",
            textposition="outside",
            textfont=dict(size=12, color="#1a1a1a"),
            name=row["cheapest_label"],
            hovertemplate=(
                f"<b>{row['canonical_name']}</b>{size_str}<br>"
                f"Buy at: {row['cheapest_label']} ${row['cheapest_price_sgd']:.2f}<br>"
                f"Avoid: {row['priciest_label']} ${row['priciest_price_sgd']:.2f}<br>"
                f"Save: ${row['price_spread_sgd']:.2f}<extra></extra>"
            ),
        ))
    max_spread = top10_src["price_spread_sgd"].max() if not top10_src.empty else 1
    fig_top10.update_layout(
        **{**PLOTLY_BASE, "margin": dict(t=10, b=10, l=10, r=10)},
        showlegend=False,
        height=max(320, len(top10_src) * 36),
        xaxis_title="Price spread (SGD)",
        xaxis=dict(range=[0, max_spread * 1.5]),
    )
    fig_top10.update_xaxes(gridcolor="#f0ede8", linecolor="#e0dbd2", zeroline=False)
    fig_top10.update_yaxes(gridcolor="rgba(0,0,0,0)", linecolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_top10, use_container_width=True)
else:
    st.info("No comparable products with a price spread found.")

st.divider()

# ── PRODUCT DETAIL ────────────────────────────────────────────────────────────

st.markdown(
    "<div class='section-header'>Product detail</div>",
    unsafe_allow_html=True
)
st.markdown(
    "<div class='section-sub'>"
    "Select a product below to see a full store-by-store breakdown. "
    "Price comparison is only shown for products available at 2+ stores."
    "</div>",
    unsafe_allow_html=True
)

if filtered.empty:
    st.info("No products match your search.")
    st.stop()

# Dropdown uses canonical_product_id as the true identifier
selection_df = (
    filtered[filtered["actual_store_count"] >= 2]
    .sort_values(["canonical_name", "size_display"])
    .drop_duplicates(subset=["canonical_product_id"])
    .copy()
)
selection_df["product_label"] = selection_df.apply(
    lambda row: (
        f"{row['canonical_name']}  ·  {row['size_display']}"
        if pd.notna(row.get("size_display"))
        and str(row.get("size_display")).strip()
        and str(row.get("size_display")).strip() != "—"
        else row["canonical_name"]
    ),
    axis=1,
)

selected_label = st.selectbox(
    "Select a product",
    options=selection_df["product_label"].tolist(),
)

if selected_label:
    rec = selection_df[
        selection_df["product_label"] == selected_label
    ].iloc[0]

    # canonical_product_id is the true unique identifier for all downstream filtering
    selected_id = int(rec["canonical_product_id"])

    # Filter df_prices by canonical_product_id and same date as recommendation
    price_rows = df_prices[
        (df_prices["canonical_product_id"] == selected_id) &
        (df_prices["scraped_date_sg"] == rec["scraped_date_sg"])
    ].copy()

    # All products in this page have 2+ stores — but verify with actual data
    unique_stores = price_rows["store"].nunique()

    if unique_stores < 2:
        st.info(
            "This product is only available at one store today — "
            "cross-store price comparison requires 2+ stores."
        )
    else:
        price_rows["store_label"] = price_rows["store"].map(STORE_LABELS).fillna(
            price_rows["store"]
        )
        price_rows = (
            price_rows.sort_values("price_sgd")
            .drop_duplicates(subset=["store"], keep="first")
        )
        min_price = price_rows["price_sgd"].min()
        max_price = price_rows["price_sgd"].max()

        cheapest_label = STORE_LABELS.get(rec["cheapest_store"], rec["cheapest_store"])
        priciest_label = STORE_LABELS.get(rec["priciest_store"], rec["priciest_store"])

        # ── KPI row ───────────────────────────────────────────────────────────
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Cheapest store", cheapest_label, f"${rec['cheapest_price_sgd']:.2f}")
        c2.metric("Priciest store", priciest_label, f"${rec['priciest_price_sgd']:.2f}")
        c3.metric("You save", f"${rec['price_spread_sgd']:.2f}", "by choosing cheapest")
        c4.metric("Stores compared", f"{unique_stores}")

        # ── Insight box ───────────────────────────────────────────────────────
        size_str = (
            f" ({rec['size_display']})"
            if pd.notna(rec.get("size_display"))
            and str(rec.get("size_display")).strip()
            and str(rec.get("size_display")).strip() != "—"
            else ""
        )
        st.markdown(
            f"<div class='insight-box'>"
            f"<b>{rec['canonical_name']}</b>{size_str} is cheapest at "
            f"<b>{cheapest_label}</b> (${rec['cheapest_price_sgd']:.2f}) "
            f"and most expensive at <b>{priciest_label}</b> "
            f"(${rec['priciest_price_sgd']:.2f}) today — "
            f"a difference of <b>${rec['price_spread_sgd']:.2f}</b> "
            f"across {unique_stores} stores."
            f"</div>",
            unsafe_allow_html=True
        )

        # ── Vertical bar chart — all matched stores ───────────────────────────
        fig = go.Figure()
        for _, row in price_rows.sort_values("price_sgd").iterrows():
            is_cheapest = row["price_sgd"] == min_price
            is_priciest = row["price_sgd"] == max_price and unique_stores > 1
            fig.add_trace(go.Bar(
                x=[row["store_label"]],
                y=[row["price_sgd"]],
                marker_color=STORE_COLORS.get(row["store"], "#aaa"),
                marker_line_width=3 if is_cheapest else 0,
                marker_line_color="#1a1a1a" if is_cheapest else "rgba(0,0,0,0)",
                text=f"${row['price_sgd']:.2f}",
                textposition="outside",
                textfont=dict(color="#1a1a1a", size=13, family="DM Sans"),
                name=row["store_label"],
                hovertemplate=(
                    f"<b>{row['store_label']}</b><br>"
                    f"Price: ${row['price_sgd']:.2f}<br>"
                    f"{row.get('store_product_name', '')}"
                    f"<extra></extra>"
                ),
            ))

        fig.update_layout(
            **PLOTLY_BASE,
            showlegend=False,
            height=320,
            yaxis_title="Price (SGD)",
            yaxis=dict(rangemode="tozero"),
        )
        apply_base_axes(fig)
        fig.update_xaxes(gridcolor="rgba(0,0,0,0)", linecolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

        # ── Store-level detail table — all matched stores ─────────────────────
        st.markdown("### Store-level detail")
        detail = price_rows[[
            "store_label", "store_product_name", "price_sgd",
            "original_price_sgd", "discount_sgd", "unit", "product_url",
        ]].copy()
        detail["price_sgd"] = detail["price_sgd"].apply(lambda x: f"${x:.2f}")
        detail["original_price_sgd"] = detail["original_price_sgd"].apply(
            lambda x: f"${x:.2f}" if pd.notna(x) and x else "—"
        )
        detail["discount_sgd"] = detail["discount_sgd"].apply(
            lambda x: f"${x:.2f}" if pd.notna(x) and x else "—"
        )
        detail.columns = [
            "Store", "Product Name", "Price",
            "Original", "Discount", "Unit", "URL"
        ]
        st.dataframe(detail, use_container_width=True, hide_index=True)

        # ── Match score breakdown ──────────────────────────────────────────────
        st.markdown("### Why these products were matched")
        st.caption(
            "Algorithm scores per store pair. "
            "Strong matches require score ≥ 0.93 with title similarity ≥ 0.82."
        )

        product_ids = price_rows["product_id"].tolist()
        if product_ids:
            client = get_client()
            cand_res = (
                client.table("product_match_candidates")
                .select(
                    "name_a,name_b,store_a,store_b,"
                    "brand_score,size_score,title_score,variant_score,"
                    "match_score,match_status,explanation"
                )
                .in_("product_id_a", product_ids)
                .eq("match_status", "strong_match")
                .limit(20)
                .execute()
            )
            cands = pd.DataFrame(cand_res.data or [])

            if not cands.empty:
                for _, cand in cands.iterrows():
                    sa = STORE_LABELS.get(cand["store_a"], cand["store_a"])
                    sb = STORE_LABELS.get(cand["store_b"], cand["store_b"])
                    with st.expander(
                        f"{sa} vs {sb} — match score {cand['match_score']:.2f}"
                    ):
                        mc1, mc2, mc3, mc4 = st.columns(4)
                        mc1.metric("Brand", f"{cand['brand_score']:.2f}")
                        mc2.metric("Size", f"{cand['size_score']:.2f}")
                        mc3.metric("Title", f"{cand['title_score']:.2f}")
                        mc4.metric("Variant", f"{cand['variant_score']:.2f}")
                        st.caption(f"*{cand['name_a']}* → *{cand['name_b']}*")
                        if cand.get("explanation"):
                            st.caption(f"Algo notes: {cand['explanation']}")
            else:
                st.caption("No match candidate records found for this product.")

        st.divider()

        # ── Price history ──────────────────────────────────────────────────────
        st.markdown(
            "<div class='section-header'>Price history</div>",
            unsafe_allow_html=True
        )
        st.markdown(
            "<div class='section-sub'>"
            "How this product's price has moved across all matched stores over time"
            "</div>",
            unsafe_allow_html=True
        )

        with st.spinner("Loading price history..."):
            client_h = get_client()
            hist_rows = []
            p2 = 0
            while True:
                res2 = (
                    client_h.table("canonical_product_daily_prices")
                    .select("store,price_sgd,scraped_date_sg")
                    .eq("canonical_product_id", selected_id)
                    .order("scraped_date_sg", desc=False)
                    .range(p2 * 1000, (p2 + 1) * 1000 - 1)
                    .execute()
                )
                if not res2.data:
                    break
                hist_rows.extend(res2.data)
                if len(res2.data) < 1000:
                    break
                p2 += 1

        if hist_rows:
            hist_df = pd.DataFrame(hist_rows)
            hist_df["store_label"] = hist_df["store"].map(STORE_LABELS).fillna(
                hist_df["store"]
            )
            hist_df["price_sgd"] = pd.to_numeric(hist_df["price_sgd"], errors="coerce")

            # Only show stores that appear on 2+ days to reduce noise
            store_day_counts = hist_df.groupby("store")["scraped_date_sg"].nunique()
            valid_stores = store_day_counts[store_day_counts >= 1].index.tolist()
            hist_df = hist_df[hist_df["store"].isin(valid_stores)]

            if hist_df["scraped_date_sg"].nunique() > 1:
                fig_hist = px.line(
                    hist_df,
                    x="scraped_date_sg",
                    y="price_sgd",
                    color="store",
                    color_discrete_map=STORE_COLORS,
                    markers=True,
                    labels={
                        "scraped_date_sg": "Date",
                        "price_sgd": "Price (SGD)",
                        "store": "Store",
                    },
                )
                for trace in fig_hist.data:
                    trace.name = STORE_LABELS.get(trace.name, trace.name)
                fig_hist.update_traces(
                    line=dict(width=2),
                    marker=dict(size=7),
                    connectgaps=False,
                )
                fig_hist.update_layout(
                    **{**PLOTLY_BASE, "margin": dict(t=20, b=20, l=10, r=10)},
                    height=300,
                    legend=dict(
                        orientation="h",
                        yanchor="bottom", y=1.02,
                        xanchor="right", x=1,
                        title="",
                    ),
                )
                apply_base_axes(fig_hist)
                st.plotly_chart(fig_hist, use_container_width=True)
            else:
                st.info(
                    "Only one day of price data so far. "
                    "History will appear after the pipeline runs across multiple days."
                )
        else:
            st.info("No price history found for this product.")