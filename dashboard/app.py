from datetime import datetime
import json
import os
from pathlib import Path
import sys

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.extract_openfda import fetch_and_save
from src.quality.data_quality_checks import run_data_quality
from src.scoring.calculate_risk_score import calculate_risk
from src.transformations.bronze_to_silver import bronze_to_silver
from src.transformations.build_gold_tables import build_gold

try:
    import duckdb
except ImportError:
    duckdb = None


st.set_page_config(page_title="Food Recall Risk Intelligence", layout="wide", initial_sidebar_state="collapsed")
px.defaults.template = "plotly_dark"


@st.cache_data
def load_parquet(path):
    candidate = Path(path)
    if not candidate.exists():
        candidate_with_ext = candidate.with_suffix(".parquet")
        if candidate_with_ext.exists():
            candidate = candidate_with_ext
    try:
        return pd.read_parquet(candidate)
    except Exception:
        return pd.DataFrame()


def get_supabase_read_config():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_ANON_KEY") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        return None, None
    return url.rstrip("/"), key


@st.cache_data
def load_supabase_table(table_name, limit=50000):
    base_url, key = get_supabase_read_config()
    if not base_url or not key:
        return pd.DataFrame()

    endpoint = f"{base_url}/rest/v1/{table_name}"
    base_headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Range-Unit": "items",
    }
    params = {"select": "*"}
    rows = []
    page_size = 1000

    try:
        for start in range(0, limit, page_size):
            end = min(start + page_size - 1, limit - 1)
            headers = {**base_headers, "Range": f"{start}-{end}"}
            response = requests.get(endpoint, headers=headers, params=params, timeout=60)
            response.raise_for_status()
            batch = response.json()
            rows.extend(batch)
            if len(batch) < page_size:
                break
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


def load_dataset(table_name, parquet_path):
    df = load_supabase_table(table_name)
    if not df.empty:
        return df
    return load_parquet(parquet_path)


def normalize_dates(df):
    for column in ["recall_initiation_date", "report_date", "termination_date", "latest_recall_date"]:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce")
    return df


def text_search(df, query):
    if not query or df.empty:
        return df

    searchable = [
        "recall_number",
        "recalling_firm",
        "recalling_firm_clean",
        "product_description",
        "reason_for_recall",
        "state",
        "country",
        "classification",
        "recall_reason_category",
        "product_category",
        "risk_tier",
    ]
    columns = [column for column in searchable if column in df.columns]
    if not columns:
        return df

    if duckdb is not None:
        con = duckdb.connect(database=":memory:")
        con.register("recalls", df)
        clauses = " OR ".join([f"lower(cast({column} as varchar)) LIKE ?" for column in columns])
        params = [f"%{query.lower()}%"] * len(columns)
        return con.execute(f"SELECT * FROM recalls WHERE {clauses}", params).df()

    mask = pd.Series(False, index=df.index)
    for column in columns:
        mask = mask | df[column].fillna("").astype(str).str.contains(query, case=False, regex=False)
    return df[mask]


def apply_filters(df, filters):
    filtered = df.copy()

    filtered = text_search(filtered, filters["query"])

    if filters["preset"] == "Open Class I Recalls":
        filtered = filtered[(filtered.get("status", "") == "Ongoing") & (filtered.get("classification", "") == "Class I")]
    elif filters["preset"] == "High-Risk Companies":
        filtered = filtered[filtered.get("risk_tier", "").isin(["Critical", "High"])]
    elif filters["preset"] == "Nationwide Allergen Recalls":
        filtered = filtered[
            (filtered.get("distribution_scope", "") == "Nationwide")
            & (filtered.get("recall_reason_category", "") == "Allergen Issue")
        ]
    elif filters["preset"] == "Recent Bacterial Contamination":
        filtered = filtered[filtered.get("recall_reason_category", "") == "Bacterial Contamination"]
        if "recall_initiation_date" in filtered.columns:
            cutoff = pd.Timestamp(datetime.now().date()) - pd.DateOffset(days=365)
            filtered = filtered[filtered["recall_initiation_date"] >= cutoff]

    if "recall_initiation_date" in filtered.columns and len(filters["date_range"]) == 2:
        start, end = filters["date_range"]
        filtered = filtered[
            (filtered["recall_initiation_date"].dt.date >= start)
            & (filtered["recall_initiation_date"].dt.date <= end)
        ]

    for key, column in [
        ("classification", "classification"),
        ("status", "status"),
        ("reason", "recall_reason_category"),
        ("product_category", "product_category"),
        ("distribution_scope", "distribution_scope"),
        ("state", "state"),
        ("risk_tier", "risk_tier"),
    ]:
        value = filters[key]
        if value != "All" and column in filtered.columns:
            filtered = filtered[filtered[column] == value]

    if "risk_score" in filtered.columns:
        low, high = filters["risk_range"]
        filtered = filtered[(filtered["risk_score"] >= low) & (filtered["risk_score"] <= high)]

    return filtered


def option_list(df, column):
    if df.empty or column not in df.columns:
        return ["All"]
    values = sorted([value for value in df[column].dropna().unique().tolist() if value != ""])
    return ["All"] + values


def show_kpis(df):
    total = len(df)
    critical = len(df[df["risk_tier"] == "Critical"]) if "risk_tier" in df.columns and not df.empty else 0
    ongoing = len(df[df["status"] == "Ongoing"]) if "status" in df.columns and not df.empty else 0
    avg_risk = round(df["risk_score"].mean(), 1) if "risk_score" in df.columns and not df.empty else 0
    latest = df["report_date"].max().strftime("%Y-%m-%d") if "report_date" in df.columns and not df.empty and pd.notna(df["report_date"].max()) else "N/A"

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Recalls", total)
    col2.metric("Critical", critical)
    col3.metric("Ongoing", ongoing)
    col4.metric("Avg Risk", avg_risk)
    col5.metric("Latest Report", latest)


def show_insights(df):
    if df.empty:
        st.info("No records match the current filters.")
        return

    insights = []
    if "recall_reason_category" in df.columns:
        top_reason = df["recall_reason_category"].value_counts().idxmax()
        insights.append(f"Most common reason: {top_reason}")
    if "product_category" in df.columns:
        top_product = df["product_category"].value_counts().idxmax()
        insights.append(f"Top product category: {top_product}")
    if "risk_score" in df.columns and "recalling_firm_clean" in df.columns:
        company = df.groupby("recalling_firm_clean")["risk_score"].mean().sort_values(ascending=False).index[0]
        insights.append(f"Highest average-risk company: {company}")

    st.write(" | ".join(insights))


def themed_chart(fig):
    fig.update_layout(
        paper_bgcolor="#0b0f17",
        plot_bgcolor="#0b0f17",
        font_color="#e5e7eb",
        margin=dict(l=10, r=10, t=30, b=10),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
    )
    fig.update_xaxes(gridcolor="#263244", zerolinecolor="#334155")
    fig.update_yaxes(gridcolor="#263244", zerolinecolor="#334155")
    return fig


def refresh_pipeline(limit, start_date=None, end_date=None):
    bronze_path = fetch_and_save(
        limit=limit,
        page_size=100,
        start_date=start_date.isoformat() if start_date else None,
        end_date=end_date.isoformat() if end_date else None,
    )
    bronze_to_silver(bronze_path, "data/silver/food_recalls")
    run_data_quality("data/silver/food_recalls", "data/gold/data_quality_report")
    calculate_risk("data/silver/food_recalls", "data/gold/food_recall_risk_scores")
    build_gold("data/gold/food_recall_risk_scores", "data/gold")
    return bronze_path


risk_df = normalize_dates(load_dataset("recalls", "data/gold/food_recall_risk_scores"))
company_df = normalize_dates(load_dataset("company_risk", "data/gold/company_risk"))
watchlist_df = normalize_dates(load_dataset("company_risk", "data/gold/company_watchlist"))
reason_df = load_dataset("recall_reason_summary", "data/gold/recall_reason_summary")
monthly_df = load_dataset("monthly_recall_trends", "data/gold/monthly_recall_trends")
geo_df = load_dataset("geographic_recall_summary", "data/gold/geographic_recall_summary")
dq_df = load_parquet("data/gold/data_quality_report")
product_df = load_dataset("product_category_summary", "data/gold/product_category_summary")
tier_df = load_dataset("risk_tier_summary", "data/gold/risk_tier_summary")
open_df = normalize_dates(load_dataset("open_recall_aging", "data/gold/open_recall_aging"))

if "risk_tier" not in risk_df.columns and "risk_score" in risk_df.columns:
    risk_df["risk_tier"] = pd.cut(
        risk_df["risk_score"],
        bins=[-1, 39, 69, 89, float("inf")],
        labels=["Low", "Medium", "High", "Critical"],
    ).astype(str)

if "product_category" not in risk_df.columns:
    risk_df["product_category"] = "Unknown"
if "distribution_scope" not in risk_df.columns:
    risk_df["distribution_scope"] = "Unknown"

st.markdown(
    """
    <style>
    .stApp {
        background: #0b0f17;
        color: #e5e7eb;
    }
    [data-testid="stHeader"],
    [data-testid="stToolbar"],
    #MainMenu,
    footer {
        display: none !important;
    }
    .stApp h1, .stApp h2, .stApp h3, .stApp h4, .stApp h5, .stApp h6,
    .stApp p, .stApp label, .stApp span {
        color: #e5e7eb;
    }
    [data-testid="stSidebar"] {display: none;}
    [data-testid="collapsedControl"] {display: none;}
    .block-container {padding-top: 1.25rem; max-width: 1500px;}
    .app-header {
        border-bottom: 1px solid #263244;
        padding-bottom: 0.85rem;
        margin-bottom: 1rem;
    }
    .app-title {
        font-size: 1.85rem;
        font-weight: 700;
        letter-spacing: 0;
        color: #f8fafc;
        margin: 0;
    }
    .app-subtitle {
        color: #9ca3af;
        font-size: 0.98rem;
        margin-top: 0.2rem;
    }
    div[data-testid="stMetric"] {
        background: #111827;
        border: 1px solid #263244;
        padding: 0.7rem 0.85rem;
        border-radius: 8px;
        box-shadow: 0 1px 2px rgba(0, 0, 0, 0.18);
    }
    div[data-testid="stMetric"] label,
    div[data-testid="stMetric"] [data-testid="stMetricLabel"],
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #f8fafc !important;
    }
    div[data-testid="stMetric"] [data-testid="stMetricLabel"] {
        color: #9ca3af !important;
    }
    div[role="radiogroup"] {
        gap: 0.35rem;
    }
    div[role="radiogroup"] label {
        border: 1px solid #334155;
        border-radius: 8px;
        padding: 0.35rem 0.65rem;
        background: #111827;
        color: #e5e7eb !important;
    }
    div[role="radiogroup"] label p,
    div[role="radiogroup"] label span {
        color: #e5e7eb !important;
    }
    div[data-testid="stSegmentedControl"] button,
    div[data-testid="stSegmentedControl"] button p,
    div[data-testid="stSegmentedControl"] button span {
        background: #111827 !important;
        color: #e5e7eb !important;
        border-color: #334155 !important;
    }
    div[data-testid="stSegmentedControl"] button[aria-pressed="true"],
    div[data-testid="stSegmentedControl"] button[aria-selected="true"] {
        background: #3b1820 !important;
        border-color: #fb7185 !important;
    }
    div[data-testid="stSegmentedControl"] button[aria-pressed="true"] p,
    div[data-testid="stSegmentedControl"] button[aria-selected="true"] p,
    div[data-testid="stSegmentedControl"] button[aria-pressed="true"] span,
    div[data-testid="stSegmentedControl"] button[aria-selected="true"] span {
        color: #fecdd3 !important;
    }
    input,
    textarea,
    [contenteditable="true"] {
        color: #f8fafc !important;
        caret-color: #f8fafc !important;
    }
    input::placeholder,
    textarea::placeholder {
        color: #9ca3af !important;
        opacity: 1 !important;
    }
    div[data-baseweb="input"] > div,
    div[data-baseweb="select"] > div,
    div[data-baseweb="popover"] {
        background: #111827;
        border-color: #334155;
        color: #f8fafc;
    }
    div[data-testid="stExpander"] {
        background: #111827;
        border-color: #263244;
        border-radius: 8px;
    }
    div[data-testid="stDataFrame"] {
        border: 1px solid #263244;
        border-radius: 8px;
    }
    hr {
        border-color: #263244;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="app-header">
      <div class="app-title">Food Recall Risk Intelligence</div>
      <div class="app-subtitle">Monitor recall risk, company patterns, product exposure, and geographic concentration.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

pages = [
    "Executive Overview",
    "Recall Explorer",
    "Company Watchlist",
    "Company Profile",
    "Risk Trends",
    "Geographic Intelligence",
    "Data Quality",
]

try:
    page = st.segmented_control("Navigation", pages, default=pages[0], label_visibility="collapsed")
except AttributeError:
    page = st.radio("Navigation", pages, horizontal=True, label_visibility="collapsed")

search_col, preset_col = st.columns([2.2, 1])
with search_col:
    query = st.text_input("Global Search", placeholder="Search company, product, recall number, reason, state", label_visibility="collapsed")
with preset_col:
    preset = st.selectbox(
        "Saved View",
        ["All Recalls", "Open Class I Recalls", "High-Risk Companies", "Nationwide Allergen Recalls", "Recent Bacterial Contamination"],
        label_visibility="collapsed",
    )

with st.expander("Refresh FDA Data", expanded=False):
    refresh_col1, refresh_col2, refresh_col3, refresh_col4 = st.columns([1, 1, 1, 0.8])
    with refresh_col1:
        refresh_limit = st.number_input("Records", min_value=1, max_value=5000, value=500, step=100)
    with refresh_col2:
        refresh_start = st.date_input("Report Start", value=None)
    with refresh_col3:
        refresh_end = st.date_input("Report End", value=None)
    with refresh_col4:
        st.write("")
        st.write("")
        refresh_clicked = st.button("Run Refresh", type="primary", width="stretch")

    if refresh_clicked:
        try:
            with st.spinner("Fetching openFDA data and rebuilding pipeline outputs..."):
                bronze_path = refresh_pipeline(int(refresh_limit), refresh_start, refresh_end)
                st.cache_data.clear()
            st.success(f"Refresh complete. New bronze file: {bronze_path}")
            with open(bronze_path, "r", encoding="utf-8") as fh:
                refresh_meta = json.load(fh).get("meta", {})
            if refresh_meta.get("warning"):
                st.warning(refresh_meta["warning"])
            st.rerun()
        except Exception as err:
            st.error(f"Refresh failed: {err}")

if not risk_df.empty and "recall_initiation_date" in risk_df.columns and risk_df["recall_initiation_date"].notna().any():
    min_date = risk_df["recall_initiation_date"].min().date()
    max_date = risk_df["recall_initiation_date"].max().date()
else:
    min_date = datetime(2023, 1, 1).date()
    max_date = datetime.now().date()

risk_max = int(max(100, risk_df["risk_score"].max() if "risk_score" in risk_df.columns and not risk_df.empty else 100))

with st.expander("Filters", expanded=False):
    row1_col1, row1_col2, row1_col3, row1_col4 = st.columns(4)
    with row1_col1:
        date_range = st.date_input("Recall Date", value=(min_date, max_date), min_value=min_date, max_value=max_date)
    with row1_col2:
        risk_range = st.slider("Risk Score", 0, risk_max, (0, risk_max))
    with row1_col3:
        classification = st.selectbox("Classification", option_list(risk_df, "classification"))
    with row1_col4:
        status = st.selectbox("Status", option_list(risk_df, "status"))

    row2_col1, row2_col2, row2_col3, row2_col4, row2_col5 = st.columns(5)
    with row2_col1:
        risk_tier = st.selectbox("Risk Tier", option_list(risk_df, "risk_tier"))
    with row2_col2:
        reason = st.selectbox("Reason", option_list(risk_df, "recall_reason_category"))
    with row2_col3:
        product_category = st.selectbox("Product Category", option_list(risk_df, "product_category"))
    with row2_col4:
        distribution_scope = st.selectbox("Distribution Scope", option_list(risk_df, "distribution_scope"))
    with row2_col5:
        state = st.selectbox("State", option_list(risk_df, "state"))

filters = {
    "query": query,
    "preset": preset,
    "date_range": date_range,
    "risk_range": risk_range,
    "classification": classification,
    "status": status,
    "risk_tier": risk_tier,
    "reason": reason,
    "product_category": product_category,
    "distribution_scope": distribution_scope,
    "state": state,
}

filtered_df = apply_filters(risk_df, filters)
show_kpis(filtered_df)
show_insights(filtered_df)
st.divider()

if page == "Executive Overview":
    left, right = st.columns(2)
    with left:
        if not monthly_df.empty:
            st.subheader("Monthly Recall Trend")
            st.plotly_chart(themed_chart(px.line(monthly_df.sort_values("recall_month"), x="recall_month", y="count", markers=True)), width="stretch")
        if not tier_df.empty:
            st.subheader("Risk Tier Mix")
            st.plotly_chart(themed_chart(px.bar(tier_df, x="risk_tier", y="count", color="risk_tier")), width="stretch")
    with right:
        if not reason_df.empty:
            st.subheader("Recall Reasons")
            st.plotly_chart(themed_chart(px.bar(reason_df.sort_values("count"), x="count", y="recall_reason_category", orientation="h")), width="stretch")
        if not product_df.empty:
            st.subheader("Product Categories")
            st.plotly_chart(themed_chart(px.bar(product_df.sort_values("count"), x="count", y="product_category", orientation="h")), width="stretch")

elif page == "Recall Explorer":
    st.subheader("Filtered Recalls")
    cols = [
        "recall_number",
        "recalling_firm_clean",
        "product_category",
        "classification",
        "status",
        "risk_tier",
        "risk_score",
        "recall_reason_category",
        "distribution_scope",
        "state",
        "recall_initiation_date",
        "risk_explanation",
    ]
    cols = [column for column in cols if column in filtered_df.columns]
    st.dataframe(filtered_df[cols].sort_values("risk_score", ascending=False), width="stretch", height=520)

elif page == "Company Watchlist":
    st.subheader("Company Watchlist")
    source = watchlist_df if not watchlist_df.empty else company_df
    if not source.empty:
        sort_cols = [column for column in ["high_risk_recalls", "avg_risk_score"] if column in source.columns]
        source = source.sort_values(sort_cols, ascending=[False] * len(sort_cols)) if sort_cols else source
        st.dataframe(source, width="stretch", height=480)
    if not filtered_df.empty:
        top = filtered_df.groupby("recalling_firm_clean").agg(
            total_recalls=("recall_number", "count"),
            avg_risk_score=("risk_score", "mean"),
            critical_recalls=("risk_tier", lambda s: int((s == "Critical").sum())),
        ).reset_index().sort_values("avg_risk_score", ascending=False).head(20)
        st.plotly_chart(themed_chart(px.bar(top, x="avg_risk_score", y="recalling_firm_clean", orientation="h")), width="stretch")

elif page == "Company Profile":
    companies = sorted(filtered_df["recalling_firm_clean"].dropna().unique().tolist()) if "recalling_firm_clean" in filtered_df.columns else []
    selected_company = st.selectbox("Company", companies)
    if selected_company:
        company_recalls = filtered_df[filtered_df["recalling_firm_clean"] == selected_company]
        show_kpis(company_recalls)
        left, right = st.columns(2)
        with left:
            st.subheader("Reasons")
            counts = company_recalls["recall_reason_category"].value_counts().reset_index()
            counts.columns = ["reason", "count"]
            st.plotly_chart(themed_chart(px.bar(counts, x="count", y="reason", orientation="h")), width="stretch")
        with right:
            st.subheader("Risk Over Time")
            trend = company_recalls.sort_values("recall_initiation_date")
            st.plotly_chart(themed_chart(px.scatter(trend, x="recall_initiation_date", y="risk_score", color="risk_tier")), width="stretch")
        st.dataframe(company_recalls.sort_values("recall_initiation_date", ascending=False), width="stretch", height=360)

elif page == "Risk Trends":
    left, right = st.columns(2)
    with left:
        st.subheader("Risk Distribution")
        st.plotly_chart(themed_chart(px.histogram(filtered_df, x="risk_score", color="risk_tier", nbins=25)), width="stretch")
    with right:
        st.subheader("Severity by Product Category")
        if not filtered_df.empty:
            grouped = filtered_df.groupby(["product_category", "risk_tier"]).size().reset_index(name="count")
            st.plotly_chart(themed_chart(px.bar(grouped, x="product_category", y="count", color="risk_tier")), width="stretch")
    if not open_df.empty:
        st.subheader("Open Recall Aging")
        st.dataframe(open_df.sort_values("recall_duration_days", ascending=False), width="stretch")

elif page == "Geographic Intelligence":
    if not filtered_df.empty and "state" in filtered_df.columns:
        state_summary = filtered_df.groupby("state").agg(
            total_recalls=("recall_number", "count"),
            avg_risk_score=("risk_score", "mean"),
        ).reset_index().sort_values("total_recalls", ascending=False)
        left, right = st.columns(2)
        with left:
            st.subheader("Recall Count by State")
            st.plotly_chart(themed_chart(px.bar(state_summary.head(20), x="state", y="total_recalls")), width="stretch")
        with right:
            st.subheader("Average Risk by State")
            risk_by_state = state_summary.sort_values("avg_risk_score", ascending=False).head(20)
            st.plotly_chart(themed_chart(px.bar(risk_by_state, x="state", y="avg_risk_score")), width="stretch")
        st.subheader("Map")
        st.plotly_chart(
            themed_chart(px.choropleth(
                state_summary,
                locations="state",
                locationmode="USA-states",
                color="avg_risk_score",
                scope="usa",
                hover_data=["total_recalls"],
            )),
            width="stretch",
        )
    elif not geo_df.empty:
        st.dataframe(geo_df, width="stretch")

elif page == "Data Quality":
    st.subheader("Data Quality")
    if not dq_df.empty:
        st.dataframe(dq_df, width="stretch")
    else:
        st.warning("No data quality report available.")
    st.subheader("Loaded Tables")
    st.dataframe(
        pd.DataFrame(
            [
                {"table": "risk_scores", "rows": len(risk_df)},
                {"table": "company_risk", "rows": len(company_df)},
                {"table": "company_watchlist", "rows": len(watchlist_df)},
                {"table": "monthly_recall_trends", "rows": len(monthly_df)},
                {"table": "product_category_summary", "rows": len(product_df)},
                {"table": "risk_tier_summary", "rows": len(tier_df)},
            ]
        ),
        width="stretch",
    )
