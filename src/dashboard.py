"""Streamlit dashboard reading Parquet sinks written by stream_app.py."""
import time, glob, os
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Real-Time Recs", layout="wide")
st.title("Real-Time Recommendation Dashboard")
st.caption("Movies / Real-Time Intelligence focus")

placeholder = st.empty()

def load_parquet_dir(path):
    files = glob.glob(os.path.join(path, "*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

while True:
    trending = load_parquet_dir("output/trending")
    recs = load_parquet_dir("output/recs")

    with placeholder.container():
        m1, m2, m3 = st.columns(3)
        m1.metric("Trending windows seen", len(trending))
        m2.metric("Distinct items", trending.item_id.nunique() if not trending.empty else 0)
        m3.metric("Recs batches", len(recs))

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Top trending items")
            if not trending.empty:
                top = (
                    trending.sort_values("trending_score", ascending=False)
                    .drop_duplicates("item_id").head(10)
                )
                st.bar_chart(top.set_index("item_id")["trending_score"])
                st.dataframe(top[["item_id", "avg_rating", "n_ratings", "trending_score"]])
            else:
                st.info("Waiting for trending data...")
        with c2:
            st.subheader("Sample recommendations (Top-5 per user)")
            if not recs.empty:
                st.dataframe(recs.head(20))
            else:
                st.info("Waiting for recommendations...")

        st.subheader("Alerts (avg_rating > 4.5 and n_ratings >= 5)")
        if not trending.empty:
            alerts = trending[(trending.avg_rating > 4.5) & (trending.n_ratings >= 5)]
            st.dataframe(alerts.tail(20))
    time.sleep(5)
