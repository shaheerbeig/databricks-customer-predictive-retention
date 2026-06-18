"""SaaS Revenue Intelligence — Streamlit app on
ai_dev_kit_testing.default.gold_mrr_by_plan_industry.

Connects to a Databricks SQL warehouse using the SDK Config and the
DATABRICKS_WAREHOUSE_ID environment variable (sourced from an app resource).
"""
import os
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

# --- page config (must be the first Streamlit call) ----------------------
st.set_page_config(
    page_title="SaaS Revenue Intelligence",
    page_icon="💸",
    layout="wide",
)

CATALOG = "ai_dev_kit_testing"
SCHEMA = "default"
# Plain serving table (snapshot of the gold materialized view). Reading the gold
# MV directly from the app's service principal hits Unity Catalog's MV
# owner-delegation check; a plain managed table is read with the SP's own creds.
TABLE = f"{CATALOG}.{SCHEMA}.app_gold_mrr_by_plan_industry"

PLAN_ORDER = ["Starter", "Growth", "Enterprise"]
PLAN_COLORS = {"Starter": "#94a3b8", "Growth": "#3b82f6", "Enterprise": "#7c3aed"}

# --- styling --------------------------------------------------------------
st.markdown(
    """
    <style>
      [data-testid="stMetric"] {
        background: #ffffff;
        border: 1px solid #e9ecef;
        border-radius: 14px;
        padding: 18px 22px;
        box-shadow: 0 1px 3px rgba(16,24,40,0.06);
      }
      [data-testid="stMetricLabel"] { color: #667085; font-weight: 600; }
      [data-testid="stMetricValue"] { color: #101828; font-size: 1.9rem; }
      .block-container { padding-top: 2.2rem; }
    </style>
    """,
    unsafe_allow_html=True,
)


def usd(x: float) -> str:
    return f"${x:,.0f}"


# --- data access ----------------------------------------------------------
@st.cache_resource
def get_connection():
    cfg = Config()
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}",
        credentials_provider=lambda: cfg.authenticate,
    )


@st.cache_data(ttl=60, show_spinner="Loading revenue data…")
def load_data() -> tuple[pd.DataFrame, datetime]:
    conn = get_connection()
    query = f"""
        SELECT plan_type, industry, active_mrr, total_mrr,
               account_count, churned_account_count
        FROM {TABLE}
    """
    with conn.cursor() as cur:
        cur.execute(query)
        df = cur.fetchall_arrow().to_pandas()

        # data freshness: latest commit on the gold table
        try:
            cur.execute(f"DESCRIBE HISTORY {TABLE} LIMIT 1")
            hist = cur.fetchall_arrow().to_pandas()
            refreshed = pd.to_datetime(hist.iloc[0]["timestamp"]).to_pydatetime()
        except Exception:
            refreshed = datetime.now(timezone.utc)

    # derived columns
    df["active_accounts"] = df["account_count"] - df["churned_account_count"]
    df["churn_rate"] = df["churned_account_count"] / df["account_count"].where(df["account_count"] > 0)
    df["avg_mrr_per_active"] = df["active_mrr"] / df["active_accounts"].where(df["active_accounts"] > 0)
    return df, refreshed


# --- load with friendly "warehouse warming up" handling -------------------
try:
    data, refreshed_at = load_data()
except Exception as exc:  # noqa: BLE001 - surface a friendly message, not a trace
    get_connection.clear()
    load_data.clear()
    st.title("💸 SaaS Revenue Intelligence")
    msg = str(exc).upper()
    if "PERMISSION_DENIED" in msg or "AUTHORIZATION" in msg or "NOT HAVE" in msg:
        st.error(
            "🔒 **Access issue reaching the data.**\n\n"
            "The app's service principal can't read the serving table. "
            "Verify it has `SELECT` on "
            f"`{TABLE}` plus `USE CATALOG` / `USE SCHEMA`.",
            icon="🔒",
        )
    else:
        # A starting/auto-stopped serverless warehouse is the common transient cause.
        st.warning(
            "⏳ **The SQL warehouse is warming up.**\n\n"
            "Serverless warehouses pause when idle and take ~1–3 minutes to resume. "
            "Give it a moment, then reload the data.",
            icon="⏳",
        )
    if st.button("🔄 Retry now", type="primary"):
        st.rerun()
    with st.expander("Technical detail"):
        st.code(f"{type(exc).__name__}: {exc}")
    st.stop()

# --- header ---------------------------------------------------------------
st.title("💸 SaaS Revenue Intelligence")
hdr_l, hdr_r = st.columns([5, 1])
with hdr_l:
    st.caption(
        f"Source: `{TABLE}`  ·  🕒 Data last refreshed: "
        f"**{refreshed_at.strftime('%Y-%m-%d %H:%M UTC')}**"
    )
with hdr_r:
    if st.button("🔄 Refresh data", use_container_width=True):
        load_data.clear()
        get_connection.clear()
        st.rerun()

# --- sidebar filters ------------------------------------------------------
st.sidebar.header("Filters")
all_plans = [p for p in PLAN_ORDER if p in data["plan_type"].unique()] or sorted(
    data["plan_type"].unique()
)
all_industries = sorted(data["industry"].unique())

sel_plans = st.sidebar.multiselect("Plan type", all_plans, default=all_plans)
sel_industries = st.sidebar.multiselect("Industry", all_industries, default=all_industries)

df = data[data["plan_type"].isin(sel_plans) & data["industry"].isin(sel_industries)].copy()

if df.empty:
    st.info("No segments match the current filters. Adjust the sidebar selections.")
    st.stop()

# --- KPI cards ------------------------------------------------------------
total_active_mrr = float(df["active_mrr"].sum())
total_active_accounts = int(df["active_accounts"].sum())
total_accounts = int(df["account_count"].sum())
total_churned = int(df["churned_account_count"].sum())
overall_churn = (total_churned / total_accounts) if total_accounts else 0.0
avg_mrr_active = (total_active_mrr / total_active_accounts) if total_active_accounts else 0.0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Active MRR", usd(total_active_mrr))
c2.metric("Active Accounts", f"{total_active_accounts:,}")
c3.metric("Overall Churn Rate", f"{overall_churn * 100:.1f}%")
c4.metric("Avg MRR / Active Account", usd(avg_mrr_active))

st.divider()

# --- charts: row 1 --------------------------------------------------------
left, right = st.columns(2)

with left:
    st.subheader("Active MRR by industry")
    bar = px.bar(
        df.sort_values("industry"),
        x="industry",
        y="active_mrr",
        color="plan_type",
        barmode="group",
        category_orders={"plan_type": all_plans},
        color_discrete_map=PLAN_COLORS,
        labels={"active_mrr": "Active MRR (USD)", "industry": "Industry", "plan_type": "Plan"},
    )
    bar.update_layout(margin=dict(t=10, b=0, l=0, r=0), legend_title_text="Plan", yaxis_tickprefix="$")
    st.plotly_chart(bar, use_container_width=True)

with right:
    st.subheader("MRR share by plan type")
    by_plan = df.groupby("plan_type", as_index=False)["active_mrr"].sum()
    donut = go.Figure(
        go.Pie(
            labels=by_plan["plan_type"],
            values=by_plan["active_mrr"],
            hole=0.55,
            marker=dict(colors=[PLAN_COLORS.get(p, "#cbd5e1") for p in by_plan["plan_type"]]),
            textinfo="label+percent",
            sort=False,
        )
    )
    donut.update_layout(
        margin=dict(t=10, b=10, l=0, r=0),
        showlegend=True,
        annotations=[
            dict(
                text=f"<b>{usd(total_active_mrr)}</b><br><span style='font-size:0.8em;color:#667085'>Active MRR</span>",
                x=0.5, y=0.5, font_size=16, showarrow=False,
            )
        ],
    )
    st.plotly_chart(donut, use_container_width=True)

# --- charts: row 2 --------------------------------------------------------
left2, right2 = st.columns(2)

with left2:
    st.subheader("Churn rate by plan & industry")
    pivot = df.pivot_table(index="plan_type", columns="industry", values="churn_rate", aggfunc="mean")
    pivot = pivot.reindex([p for p in all_plans if p in pivot.index])
    z = pivot.values
    text = [[("" if pd.isna(v) else f"{v * 100:.1f}%") for v in row] for row in z]
    heat = go.Figure(
        go.Heatmap(
            z=z,
            x=list(pivot.columns),
            y=list(pivot.index),
            colorscale="RdYlGn_r",  # low churn = green, high churn = red
            text=text,
            texttemplate="%{text}",
            colorbar=dict(title="Churn", tickformat=".0%"),
            hovertemplate="Plan: %{y}<br>Industry: %{x}<br>Churn: %{z:.1%}<extra></extra>",
        )
    )
    heat.update_layout(margin=dict(t=10, b=0, l=0, r=0))
    st.plotly_chart(heat, use_container_width=True)

with right2:
    st.subheader("Avg MRR vs churn rate")
    scat = px.scatter(
        df,
        x="churn_rate",
        y="avg_mrr_per_active",
        size="account_count",
        color="plan_type",
        text="industry",
        category_orders={"plan_type": all_plans},
        color_discrete_map=PLAN_COLORS,
        size_max=55,
        labels={
            "churn_rate": "Churn rate",
            "avg_mrr_per_active": "Avg MRR / active account (USD)",
            "plan_type": "Plan",
        },
    )
    scat.update_traces(textposition="top center", textfont_size=10)
    scat.update_layout(
        margin=dict(t=10, b=0, l=0, r=0),
        xaxis_tickformat=".0%",
        yaxis_tickprefix="$",
        legend_title_text="Plan",
    )
    st.plotly_chart(scat, use_container_width=True)

st.divider()

# --- styled segment table -------------------------------------------------
st.subheader("All segments")
table = df[
    [
        "plan_type", "industry", "active_mrr", "total_mrr",
        "account_count", "active_accounts", "churned_account_count",
        "churn_rate", "avg_mrr_per_active",
    ]
].sort_values(["plan_type", "industry"]).rename(
    columns={
        "plan_type": "Plan",
        "industry": "Industry",
        "active_mrr": "Active MRR",
        "total_mrr": "Total MRR",
        "account_count": "Accounts",
        "active_accounts": "Active accounts",
        "churned_account_count": "Churned accounts",
        "churn_rate": "Churn rate",
        "avg_mrr_per_active": "Avg MRR / active",
    }
)

styled = table.style.format(
    {
        "Active MRR": "${:,.0f}",
        "Total MRR": "${:,.0f}",
        "Avg MRR / active": "${:,.0f}",
        "Accounts": "{:,.0f}",
        "Active accounts": "{:,.0f}",
        "Churned accounts": "{:,.0f}",
        "Churn rate": "{:.1%}",
    }
).background_gradient(subset=["Churn rate"], cmap="RdYlGn_r")

st.dataframe(styled, use_container_width=True, hide_index=True)
