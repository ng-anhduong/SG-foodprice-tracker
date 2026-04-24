import streamlit as st
import pandas as pd
import sys
import os
import plotly.graph_objects as go
sys.path.append(os.path.join(os.path.dirname(__file__), "../../pipeline/etl"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../../pipeline/ml"))
from load import get_client

st.set_page_config(page_title="Price Predictions", layout="wide")

st.title("Price Predictions")
st.markdown(
    "<p style='color:#888; font-size:1.1rem; margin-top:-12px'>"
    "Compare current product prices with model-estimated expected prices to spot better-value purchases across stores."
    "</p>",
    unsafe_allow_html=True
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] { 
    font-family: 'DM Sans', sans-serif; 
}

h1 { 
    font-family: 'DM Serif Display', serif !important; 
    font-size: 3rem !important;
    letter-spacing: -0.02em; 
    color: #1a1a1a; 
}

h2 { 
    font-family: 'DM Serif Display', serif !important; 
    font-size: 1.9rem !important;
    color: #1a1a1a; 
    font-weight: 400 !important; 
    letter-spacing: -0.01em; 
}

h3 { 
    font-size: 0.95rem !important; 
    font-weight: 500 !important;
    letter-spacing: 0.1em; 
    text-transform: uppercase; 
    color: #888 !important; 
}

hr { 
    border-color: #ebe7e0 !important; 
}

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
    margin=dict(t=30, b=20, l=10, r=10),
)

def apply_base_axes(fig):
    fig.update_xaxes(gridcolor="#f0ede8", linecolor="#e0dbd2", zeroline=False)
    fig.update_yaxes(gridcolor="#f0ede8", linecolor="#e0dbd2", zeroline=False)
    return fig

@st.cache_data(ttl=600)
def load_prediction_metrics():
    client = get_client()
    res = (
        client.table("price_prediction_metrics")
        .select("*")
        .order("model_run_date", desc=True)
        .limit(1)
        .execute()
    )
    return pd.DataFrame(res.data)

@st.cache_data(ttl=600)
def load_predictions():
    client = get_client()
    rows, page = [], 0

    while True:
        res = (
            client.table("product_price_predictions")
            .select("*")
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
    results = load_predictions()

    st.markdown(
        """
        <div class='insight-box'>
        This section displays predictions trained on historical pricing data to estimate the expected price of each product.
        Predictions are generated only when the product-store combination has enough recent history.
        </div>
        """,
        unsafe_allow_html=True
    )

    st.divider()

    st.subheader("Product Price Predictions List")
    st.markdown("### Filter by product, category, and store")

    preview = results[[
        "canonical_name",
        "unified_category",
        "store",
        "scraped_date_sg",
        "price_sgd",
        "predicted_price",
    ]].copy()

    preview = preview.rename(columns={
        "canonical_name": "Product",
        "unified_category": "Category",
        "store": "Store",
        "scraped_date_sg": "Date",
        "price_sgd": "Actual Price (SGD)",
        "predicted_price": "Predicted Price (SGD)",
    })
    
    preview["Date"] = pd.to_datetime(preview["Date"])

    preview["Actual Price (SGD)"] = preview["Actual Price (SGD)"].round(2)
    preview["Predicted Price (SGD)"] = preview["Predicted Price (SGD)"].round(2)
    difference = (preview["Actual Price (SGD)"] - preview["Predicted Price (SGD)"]).round(2)
    preview["Price Difference (SGD)"] = difference
    preview["Price Difference (%)"] = ((difference / preview["Predicted Price (SGD)"]) * 100).round(2)
    met_df = load_prediction_metrics()

    if not met_df.empty:
        threshold = float(met_df.iloc[0]["mae"])
    else:
        threshold = 0.3

    def whether_deal(each_row):
        dollar_diff = each_row["Price Difference (SGD)"]
        percent_diff = each_row["Price Difference (%)"]
        if dollar_diff <= -threshold and percent_diff <= -5:
            return "Good deal"
        elif dollar_diff >= threshold and percent_diff >= 5:
            return "Overpriced"
        else:
            return "Normal price"
        
    preview["Deal Status"] = preview.apply(whether_deal, axis=1)

    col_0, col_1, col_2, col_3 = st.columns([1, 2, 1, 1])

    with col_0:
        date_options = sorted(preview["Date"].dropna().dt.date.unique(), reverse=True)
        date_labels = ["All dates"] + [str(d) for d in date_options]
        selected_date = st.selectbox("Date", date_labels)
    with col_1:
        search_product = st.text_input("Search product", placeholder="Milo, Greek yogurt…")

    with col_2:
        category_options = ["All"] + sorted(preview["Category"].dropna().unique().tolist())
        selected_category = st.selectbox("Category", category_options)

    with col_3:
        store_options = ["All"] + sorted(preview["Store"].dropna().unique().tolist())
        selected_store = st.selectbox("Store", store_options)

    filtered_preview = preview.copy()
    if selected_date != "All dates":
        filtered_preview = filtered_preview[
            filtered_preview["Date"].dt.date == pd.to_datetime(selected_date).date()
        ]

    if search_product:
        filtered_preview = filtered_preview[
            filtered_preview["Product"].str.contains(search_product, case=False, na=False)
        ]

    if selected_category != "All":
        filtered_preview = filtered_preview[
            filtered_preview["Category"] == selected_category
        ]

    if selected_store != "All":
        filtered_preview = filtered_preview[
            filtered_preview["Store"] == selected_store
        ]

    #Remove duplicates
    filtered_preview = filtered_preview.drop_duplicates(
        subset=["Product", "Store", "Date"],
        keep="first"
    ).copy()

    # Sort date, product, then store
    filtered_preview = filtered_preview.sort_values(
        ["Date", "Product", "Store"],
        ascending=[False, True, True]
    ).copy()

    filtered_preview = filtered_preview[[
        "Product",
        "Category",
        "Store",
        "Date",
        "Actual Price (SGD)",
        "Predicted Price (SGD)",
        "Price Difference (%)",
        "Deal Status"
    ]].copy()

    #Format date
    filtered_preview["Date"] = filtered_preview["Date"].dt.strftime("%Y-%m-%d")

    # Row count
    st.markdown(
        f"<p style='color:#888; font-size:0.85rem'>{len(filtered_preview):,} rows</p>",
        unsafe_allow_html=True
    )

    good_deals = (filtered_preview["Deal Status"] == "Good deal").sum()
    above_expected = (filtered_preview["Deal Status"] == "Overpriced").sum()
    fair_price = (filtered_preview["Deal Status"] == "Normal price").sum()

    if selected_date == "All dates":
        summary_label = "all available dates"
    else:
        summary_label = selected_date
    
    # SHOW
    st.dataframe(filtered_preview, width="stretch", hide_index=True)

    st.markdown(f"""
        <div class='insight-box'>
        For <b>{summary_label}</b>, <b>{good_deals}</b> are good deals,
        <b>{above_expected}</b> are priced above expectations and
        <b>{fair_price}</b> are close to expected pricing
        </div>
        """, unsafe_allow_html=True)
    st.divider()

    st.markdown("### Distribution of deal status & top savings opportunities")
    
    col_chart1, col_chart2 = st.columns(2, gap="large")
    with col_chart1:
        st.subheader("Product Price Status Overview")

        status_counts = (
            filtered_preview["Deal Status"]
            .value_counts()
            .reindex(["Good deal", "Normal price", "Overpriced"], fill_value=0)
        )

        status_colors = {
            "Good deal": "#00843D",
            "Normal price": "#005BAC",
            "Overpriced": "#C8102E"
        }

        fig_status = go.Figure()

        fig_status.add_trace(go.Bar(
            x=status_counts.index.tolist(),
            y=status_counts.values.tolist(),
            marker_color=[status_colors[s] for s in status_counts.index],
            text=[str(v) for v in status_counts.values],
            textposition="outside",
            textfont=dict(color="#1a1a1a", size=13, family="DM Sans"),
            hovertemplate="<b>%{x}</b><br>Rows: %{y}<extra></extra>",
        ))

        fig_status.update_layout(
            **PLOTLY_BASE,
            showlegend=False,
            height=360,
            yaxis_title="Number of rows",
            yaxis=dict(rangemode="tozero"),
        )

        apply_base_axes(fig_status)
        fig_status.update_xaxes(
        gridcolor="rgba(0,0,0,0)",
        linecolor="rgba(0,0,0,0)",
        tickangle=0
        )

        st.plotly_chart(fig_status, use_container_width=True)

    with col_chart2:
        st.subheader("Top Products with Good Deals")

        top = filtered_preview[
            filtered_preview["Deal Status"] == "Good deal"
        ].copy()

        top = top.sort_values("Price Difference (%)").head(5)

        top["Savings from Expected Price(%)"] = -top["Price Difference (%)"]
        top["Shorten Name"] = top["Product"].str.slice(0, 25) + "..."

        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=top["Shorten Name"],
            y=top["Savings from Expected Price(%)"],
            text=top["Savings from Expected Price(%)"].round(1).astype(str) + "%",
            textposition="outside",
            marker_color="#178E4F",
        ))

        fig.update_layout(
            **PLOTLY_BASE,
            yaxis_title="Savings from Expected Price(%)",
            height=360,
        )

        fig.update_yaxes(rangemode="tozero")

        apply_base_axes(fig)

        fig.update_xaxes(tickangle=0)

        st.plotly_chart(fig, use_container_width=True)


    st.subheader("Good Deals by Store")

    good_deals_store = (
        filtered_preview[filtered_preview["Deal Status"] == "Good deal"]
        .groupby("Store")
        .size()
        .sort_values(ascending=False)
    )

    if len(good_deals_store) > 0:
        stores = good_deals_store.index.tolist()
        labels = [STORE_LABELS.get(s, s) for s in stores]
        values = good_deals_store.values.tolist()
        colors = [STORE_COLORS.get(s, "#999999") for s in stores]
        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=labels,
            y=values,
            marker_color=colors,
            text=[str(v) for v in values],
            textposition="outside",
            textfont=dict(color="#1a1a1a", size=13, family="DM Sans"),
            hovertemplate="<b>%{x}</b><br>Good deals: %{y}<extra></extra>",
        ))

        fig.update_layout(
            **PLOTLY_BASE,
            showlegend=False,
            height=320,
            yaxis_title="Number of good deals",
            yaxis=dict(rangemode="tozero"),
        )

        apply_base_axes(fig)

        fig.update_xaxes(
            gridcolor="rgba(0,0,0,0)",
            linecolor="rgba(0,0,0,0)",
            tickangle=0
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No good deals with current filters")

    #Check trend over time for each product by filtering by product and store
    product_list = sorted(results["canonical_name"].dropna().unique())
    st.divider()

    st.subheader("Price Trend over time")
    st.markdown("### Historical trends of actual vs predicted price")

    col_a, col_b = st.columns([2, 1])
    with col_a:
        trend_product = st.selectbox("Product", product_list, key="trend_prod")
    with col_b:
        trend_store = st.selectbox("Store", ["All stores"] + list(STORE_LABELS.values()), key="trend_store")

    trend_data = results[results["canonical_name"] == trend_product].copy()
    trend_data["scraped_date_sg"] = pd.to_datetime(trend_data["scraped_date_sg"])

    if trend_store != "All stores":
        store_key = {v: k for k, v in STORE_LABELS.items()}[trend_store]
        trend_data = trend_data[trend_data["store"] == store_key]

    trend_data = trend_data.groupby("scraped_date_sg")[["price_sgd","predicted_price"]].mean().reset_index()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trend_data["scraped_date_sg"], y=trend_data["price_sgd"],
        mode="lines+markers", name="Actual price",
        line=dict(color="#F5821F", width=2.5),
        fill="tozeroy", fillcolor="rgba(245,130,31,0.08)"
    ))
    fig.add_trace(go.Scatter(
        x=trend_data["scraped_date_sg"], y=trend_data["predicted_price"],
        mode="lines", name="Predicted price",
        line=dict(color="#888", width=1.5, dash="dash")
    ))
    fig.update_layout(**PLOTLY_BASE, height=320, yaxis_title="Price (SGD)")
    apply_base_axes(fig)
    st.plotly_chart(fig, use_container_width=True)

    # tag of trend
    if len(trend_data) >= 2:
        first_p = trend_data["price_sgd"].iloc[0]
        last_p  = trend_data["price_sgd"].iloc[-1]
        chg_pct = (last_p - first_p) / first_p * 100
        if chg_pct > 2:
            st.warning(f"↑ Price up {chg_pct:.1f}% since first observation — consider buying soon")
        elif chg_pct < -2:
            st.success(f"↓ Price down {abs(chg_pct):.1f}% since first observation — good time to buy")
        else:
            st.info("→ Price has been stable")