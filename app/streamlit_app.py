"""Open Library dashboard: two views over the published gold tables.

Run from the repo root:
    streamlit run app/streamlit_app.py

Reads data/serving.duckdb (built by pipeline/publish.py) in read-only mode, so it
never blocks dbt or the loader.
"""
from __future__ import annotations

import os
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

SERVING = os.getenv("SERVING_DB_PATH", "data/serving.duckdb")
BUCKET_ORDER = ["1", "2-3", "4-9", "10-24", "25+"]

st.set_page_config(page_title="Open Library trends", layout="wide")


def pretty(slug: str) -> str:
    return slug.replace("-", " ").title()


@st.cache_data(show_spinner=False)
def load(path: str, mtime: float):
    """mtime is part of the cache key, so a fresh publish reloads automatically."""
    con = duckdb.connect(path, read_only=True)
    try:
        decades = con.execute(
            "SELECT * FROM gold_works_by_subject_decade ORDER BY queried_subject, decade"
        ).df()
        editions = con.execute("SELECT * FROM gold_edition_popularity_by_subject").df()
        published = con.execute("SELECT published_at FROM published_at").fetchone()[0]
    finally:
        con.close()
    decades["subject"] = decades["queried_subject"].map(pretty)
    decades["decade_label"] = decades["decade"].astype(str) + "s"
    editions["subject"] = editions["queried_subject"].map(pretty)
    return decades, editions, published


if not Path(SERVING).exists():
    st.error(
        f"No serving database at `{SERVING}`. Run the pipeline first: "
        "`python -m pipeline.flow once`."
    )
    st.stop()

decades, editions, published = load(SERVING, Path(SERVING).stat().st_mtime)

st.title("What does the Open Library catalog look like?")
st.caption(f"Data last published: {pd.Timestamp(published):%Y-%m-%d %H:%M} UTC")

tab1, tab2 = st.tabs(["Growth by decade", "Editions vs. ratings"])

# ---------------------------------------------------------------- view 1
with tab1:
    st.subheader("Which subjects added the most works, and which are growing fastest?")
    st.caption(
        "Counts come from the whole catalog (not a sample), grouped by decade of "
        "first publication. Works first published before 1900 are excluded. "
        "The current decade is partial, so per-year rates are the fair comparison."
    )

    all_subjects = sorted(decades["subject"].unique())
    top5 = (
        decades.groupby("subject")["works_count"].sum().nlargest(5).index.tolist()
    )
    left, right = st.columns([3, 1])
    chosen = left.multiselect("Subjects", all_subjects, default=top5, key="s1")
    metric = right.radio(
        "Measure", ["Works per year", "Works in decade (raw)"], key="m1"
    )
    ycol = "works_per_year" if metric == "Works per year" else "works_count"

    if metric.endswith("(raw)"):
        st.warning("The latest decade is still in progress, so its raw count is understated.")

    view = decades[decades["subject"].isin(chosen)]
    if view.empty:
        st.info("Pick at least one subject.")
    else:
        chart = (
            alt.Chart(view)
            .mark_line(point=True)
            .encode(
                x=alt.X("decade_label:O", title="Decade of first publication"),
                y=alt.Y(f"{ycol}:Q", title=metric),
                color=alt.Color("subject:N", title="Subject"),
                tooltip=[
                    "subject",
                    "decade_label",
                    "works_count",
                    "years_covered",
                    "works_per_year",
                    "growth_pct",
                ],
            )
            .properties(height=380)
        )
        st.altair_chart(chart, use_container_width=True)

    st.markdown("**Fastest growing subjects**")
    options = sorted(decades.loc[decades["decade"] > 1900, "decade"].unique())
    default_idx = options.index(2010) if 2010 in options else len(options) - 1
    decade_pick = st.selectbox(
        "Growth in works per year, compared with the previous decade",
        options,
        index=default_idx,
        format_func=lambda d: f"{d}s vs. {d - 10}s",
    )
    growth = decades[(decades["decade"] == decade_pick)].dropna(subset=["growth_pct"])
    if growth["is_partial_decade"].any():
        st.caption("This decade is still in progress; its rate is based on the years so far.")
    gchart = (
        alt.Chart(growth)
        .mark_bar()
        .encode(
            y=alt.Y("subject:N", sort="-x", title=None),
            x=alt.X("growth_pct:Q", title="Change in works per year (%)"),
            tooltip=["subject", "prev_decade_works_per_year", "works_per_year", "growth_pct"],
        )
        .properties(height=380)
    )
    st.altair_chart(gchart, use_container_width=True)

    with st.expander("Show the data"):
        st.dataframe(view.drop(columns=["queried_subject", "decade_label"]), hide_index=True)

# ---------------------------------------------------------------- view 2
with tab2:
    st.subheader("Do works with more editions get better ratings and more interest?")
    st.caption(
        "Based on up to ~1,500 of the most relevant works per subject, so this describes "
        "prominent works, not the whole catalog. Average ratings only include works with "
        "at least 5 ratings. A work found under several subjects counts in each."
    )

    subs2 = sorted(editions["subject"].unique())
    c1, c2, c3 = st.columns([3, 1, 1])
    chosen2 = c1.multiselect("Subjects", subs2, default=subs2[:4], key="s2")
    min_works = c2.slider(
        "Min. works per cell",
        0, 200, 10,
        help="Hide subject/edition-bucket cells with fewer works than this, "
        "because small groups give unreliable averages.",
    )
    interest = c3.radio("Want-to-read measure", ["Median", "Mean"], key="m2")

    base = editions[editions["subject"].isin(chosen2)]
    ratings = base[base["rated_works_count"] >= min_works].dropna(subset=["avg_rating"])
    wtr_col = "median_want_to_read" if interest == "Median" else "avg_want_to_read"
    wtr = base[base["works_count"] >= min_works]

    if base.empty:
        st.info("Pick at least one subject.")
    else:
        col_a, col_b = st.columns(2)
        xenc = alt.X("edition_bucket:O", sort=BUCKET_ORDER, title="Editions per work")

        col_a.markdown("**Average rating**")
        col_a.altair_chart(
            alt.Chart(ratings)
            .mark_line(point=True)
            .encode(
                x=xenc,
                y=alt.Y("avg_rating:Q", title="Average rating", scale=alt.Scale(zero=False)),
                color=alt.Color("subject:N", title="Subject"),
                tooltip=["subject", "edition_bucket", "avg_rating", "rated_works_count"],
            )
            .properties(height=340),
            use_container_width=True,
        )

        col_b.markdown("**Want-to-read interest**")
        col_b.altair_chart(
            alt.Chart(wtr)
            .mark_line(point=True)
            .encode(
                x=xenc,
                y=alt.Y(f"{wtr_col}:Q", title=f"{interest} want-to-read count"),
                color=alt.Color("subject:N", title="Subject"),
                tooltip=["subject", "edition_bucket", wtr_col, "works_count"],
            )
            .properties(height=340),
            use_container_width=True,
        )

    with st.expander("Show the data"):
        st.dataframe(
            base.drop(columns=["queried_subject", "bucket_order"]), hide_index=True
        )
