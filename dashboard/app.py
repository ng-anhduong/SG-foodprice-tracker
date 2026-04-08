import streamlit as st

st.set_page_config(
    page_title="SG Food Price Tracker",
    page_icon="🛒",
    layout="wide",
)

st.title("🛒 SG Food Price Tracker")
st.markdown("""
Compare food prices across **FairPrice**, **RedMart**, **Cold Storage**, and **Sheng Siong** — updated daily.
""")
st.info("👈 Select a page from the sidebar to get started.")