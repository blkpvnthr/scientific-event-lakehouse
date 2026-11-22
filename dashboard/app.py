"""Streamlit analytics dashboard for the Scientific Event Data Lakehouse.

Consumes the actual Gold datasets produced by the pipeline (no hardcoded
numbers). Run with:

    streamlit run dashboard/app.py

If the Gold layer is missing, build it first: ``make pipeline``.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from dashboard import data

st.set_page_config(page_title="Scientific Event Lakehouse", layout="wide")


@st.cache_data
def _load(name: str) -> pd.DataFrame:
    return data.load_dataset(name)


@st.cache_data
def _fact() -> pd.DataFrame:
    return data.load_fact()


def main() -> None:
    st.title("Scientific Event Data Lakehouse — Seismic Analytics")

    summary = data.dataset_summary()
    if summary["total_events"] == 0:
        st.warning("No Gold data found. Build the pipeline first: `make pipeline`.")
        return

    st.caption(summary["note"])
    fact = _fact()

    # --- Sidebar filters ---
    st.sidebar.header("Filters")
    mag_min, mag_max = float(fact["magnitude"].min()), float(fact["magnitude"].max())
    magnitude_range = st.sidebar.slider(
        "Magnitude", mag_min, mag_max, (mag_min, mag_max)
    )
    depth_min, depth_max = float(fact["depth_km"].min()), float(fact["depth_km"].max())
    depth_range = st.sidebar.slider(
        "Depth (km)", depth_min, depth_max, (depth_min, depth_max)
    )
    networks = st.sidebar.multiselect(
        "Reporting network", sorted(fact["network_key"].unique())
    )
    event_types = st.sidebar.multiselect(
        "Event type", sorted(fact["event_type_key"].unique())
    )

    filtered = data.filter_fact(
        fact,
        magnitude_range=magnitude_range,
        depth_range=depth_range,
        networks=networks or None,
        event_types=event_types or None,
    )

    col1, col2, col3 = st.columns(3)
    col1.metric("Total events (catalog)", f"{summary['total_events']:,}")
    col2.metric("Events after filters", f"{len(filtered):,}")
    col3.metric(
        "Max magnitude (filtered)",
        f"{filtered['magnitude'].max():.1f}" if len(filtered) else "-",
    )

    # --- Trends ---
    st.subheader("Annual seismic activity")
    annual = _load("annual_seismic_activity")
    if not annual.empty:
        st.bar_chart(annual.set_index("event_year")["event_count"])

    st.subheader("Monthly seismic activity")
    monthly = _load("monthly_seismic_activity")
    if not monthly.empty:
        monthly = monthly.sort_values(["event_year", "month"])
        monthly["period"] = (
            monthly["event_year"].astype(str)
            + "-"
            + monthly["month"].astype(str).str.zfill(2)
        )
        st.line_chart(monthly.set_index("period")["event_count"])

    # --- Distributions ---
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Magnitude distribution")
        mag = _load("magnitude_distribution")
        if not mag.empty:
            st.bar_chart(mag.set_index("magnitude_bucket")["event_count"])
    with c2:
        st.subheader("Depth distribution")
        depth = _load("depth_distribution")
        if not depth.empty:
            st.bar_chart(depth.set_index("depth_category")["event_count"])

    # --- Geographic ---
    st.subheader("Geographic distribution (filtered)")
    geo = filtered[["latitude", "longitude"]].dropna()
    if not geo.empty:
        st.map(geo.sample(min(len(geo), 5000)))

    # --- Significant events ---
    st.subheader("Significant events (M>=6 or significance>=600)")
    sig = _load("significant_events")
    if not sig.empty:
        st.dataframe(
            sig.sort_values("magnitude", ascending=False).head(50),
            use_container_width=True,
        )

    # --- Source networks ---
    st.subheader("Source network metrics")
    net = _load("source_network_metrics")
    if not net.empty:
        st.dataframe(
            net.sort_values("event_count", ascending=False), use_container_width=True
        )

    # --- Data quality ---
    st.subheader("Data quality trends")
    dq = _load("data_quality_trends")
    if not dq.empty:
        st.dataframe(dq, use_container_width=True)


if __name__ == "__main__":
    main()
