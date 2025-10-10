import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px

# =========================
# 1. Load Postgres credentials
# =========================
PG_USER = os.environ.get("AIRFLOW_POSTGRES_USER")
PG_PASSWORD = os.environ.get("AIRFLOW_POSTGRES_PASSWORD")
PG_HOST = "postgres"   # Docker link name
PG_PORT = 5432
PG_DB = "postgres"
PG_SCHEMA = "mart"

# Build connection string
conn_str = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
engine = create_engine(conn_str)

# =========================
# 2. Fetch tables dynamically
# =========================
@st.cache_data(ttl=300)
def get_tables(schema):
    query = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema
        ORDER BY table_name;
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"schema": schema})
        tables = [row[0] for row in result.fetchall()]
    return tables

tables = get_tables(PG_SCHEMA)

# =========================
# 3. Streamlit UI
# =========================
st.set_page_config(page_title="Jaffle Shop Data", layout="wide")
st.title("📊 Jaffle Shop Data Explorer")

# Sidebar: select table
selected_table = st.sidebar.selectbox("Select table to view", tables)

# Fetch selected table
@st.cache_data(ttl=300)
def load_table(table_name):
    query = text(f'SELECT * FROM "{PG_SCHEMA}"."{table_name}" LIMIT 1000;')
    df = pd.read_sql(query, engine)
    return df

if selected_table:
    st.subheader(f"Table: {selected_table}")
    df = load_table(selected_table)
    st.dataframe(df, use_container_width=True)

    # Display basic metrics
    st.markdown("### Metrics")
    st.write(f"Total rows: {len(df)}")
    numeric_cols = df.select_dtypes(include='number').columns.tolist()
    if numeric_cols:
        st.write("Numeric columns summary:")
        st.dataframe(df[numeric_cols].describe())

    # Simple chart examples
    if "customer" in selected_table.lower() and "id" in df.columns:
        st.markdown("### Orders per Customer")
        if "orders" in tables:
            orders_df = pd.read_sql(text(f'SELECT customer_id, COUNT(*) as orders_count FROM "{PG_SCHEMA}"."orders" GROUP BY customer_id;'), engine)
            fig = px.bar(orders_df, x="customer_id", y="orders_count", labels={"customer_id":"Customer ID", "orders_count":"Total Orders"})
            st.plotly_chart(fig, use_container_width=True)
