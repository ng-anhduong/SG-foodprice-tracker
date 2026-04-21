import streamlit as st
import pandas as pd
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "../../pipeline/etl"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../../pipeline/ml"))

from future_price import run as run_prediction

st.set_page_config(page_title="Price Predictions", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }

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

[data-testid="metric-container"] {
    background: #ffffff;
    border: 1px solid #ebe7e0;
    border-radius: 10px;
    padding: 18px 22px !important;
}

[data-testid="metric-container"] label {
    font-size: 0.72rem !important;
    font-weight: 500;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: #999;
}

[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: 'DM Serif Display', serif !important;
    font-size: 2rem !important;
    color: #1a1a1a;
}

hr { border-color: #ebe7e0 !important; }

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

st.title("Price Predictions")
st.markdown(
    "<p style='color:#888; font-size:1.1rem; margin-top:-12px'>"
    "This section uses regression models to estimate product prices using historical data, "
    "store information, and short-term pricing behaviour.</p>",
    unsafe_allow_html=True
)
st.divider()

output_dir = os.path.join(os.path.dirname(__file__), "../../data/ml")

if st.button("Run Price Prediction Model", type="primary"):
    with st.spinner("Generating predictions..."):
        st.session_state["prediction_results"] = run_prediction()

results = st.session_state.get("prediction_results")

if results:
    st.subheader("Model Performance")
    st.markdown("### Summary metrics from recent prediction run")

    m1, m2, m3 = st.columns(3)
    m1.metric("MAE", f"${results['mae']:.2f}")
    m2.metric("RMSE", f"${results['rmse']:.2f}")
    m3.metric("R²", f"{results['r2']:.3f}")

    st.markdown(
        f"<div class='insight-box'>"
        f"The model has an average absolute error of <b>${results['mae']:.2f}</b> "
        f"and an R² of <b>{results['r2']:.3f}</b>. "
        f"This suggests that recent price history provides useful predictive signal."
        f"</div>",
        unsafe_allow_html=True
    )

    st.divider()

    st.subheader("Prediction visualization")
    col1, col2 = st.columns(2, gap="large")

    with col1:
        st.markdown("#### Predicted vs actual price")
        st.image(os.path.join(output_dir, "actual_vs_predicted.png"), width="stretch")

    with col2:
        st.markdown("#### Residual analysis")
        st.image(os.path.join(output_dir, "residuals.png"), width="stretch")

    st.divider()

    st.subheader("Product Price Predictions List")
    st.markdown("### Filter by product, category, and store")

    preview = results["output"][[
        "canonical_name",
        "unified_category",
        "store",
        "scraped_date_sg",
        "price_sgd",
        "predicted_price",
        "abs_error"
    ]].copy()

    preview = preview.rename(columns={
        "canonical_name": "Product",
        "unified_category": "Category",
        "store": "Store",
        "scraped_date_sg": "Date",
        "price_sgd": "Actual Price (SGD)",
        "predicted_price": "Predicted Price (SGD)",
        "abs_error": "Absolute Error"
    })

    preview["Date"] = pd.to_datetime(preview["Date"])
    preview["Actual Price (SGD)"] = preview["Actual Price (SGD)"].round(2)
    preview["Predicted Price (SGD)"] = preview["Predicted Price (SGD)"].round(2)
    preview["Absolute Error"] = preview["Absolute Error"].round(2)

    col_f1, col_f2, col_f3 = st.columns([2, 1, 1])

    with col_f1:
        search_product = st.text_input("Search product", placeholder="Milo, Greek yogurt…")

    with col_f2:
        category_options = ["All"] + sorted(preview["Category"].dropna().unique().tolist())
        selected_category = st.selectbox("Category", category_options)

    with col_f3:
        store_options = ["All"] + sorted(preview["Store"].dropna().unique().tolist())
        selected_store = st.selectbox("Store", store_options)

    filtered_preview = preview.copy()

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

    filtered_preview = filtered_preview.sort_values(["Product", "Date"]).copy()
    filtered_preview["Date"] = filtered_preview["Date"].dt.strftime("%Y-%m-%d")

    st.markdown(
        f"<p style='color:#888; font-size:0.85rem'>{len(filtered_preview):,} rows</p>",
        unsafe_allow_html=True
    )

    st.dataframe(filtered_preview, width="stretch", hide_index=True)

    st.divider()

    st.subheader("How the model works")
    st.markdown(
        """
        - **Features:** Product category, store, month, day-of-week, day-of-month, previous observed price, and rolling mean of previous prices  
        - **Model:** Random Forest Regressor  
        - **Statistics:** MAE, RMSE, and R²  
        - **Output:** Predicted prices, residual analysis, and prediction results  
        """
    )