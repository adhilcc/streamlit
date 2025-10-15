import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px

# =========================
# 1. Load Postgres credentials (from Vault)
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
st.set_page_config(page_title="Data Explorer", layout="wide")
st.title("📊 Data Explorer")

# Sidebar
st.sidebar.header("Navigation")
mode = st.sidebar.radio("Choose view:", ["Browse Tables", "Run SQL Query"])

# =========================
# 4. Browse Tables Mode
# =========================
if mode == "Browse Tables":
    selected_table = st.sidebar.selectbox("Select table to view", tables)

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

        # Simple chart example
        if "customer" in selected_table.lower() and "id" in df.columns:
            st.markdown("### Orders per Customer")
            if "orders" in tables:
                orders_df = pd.read_sql(
                    text(f'SELECT customer_id, COUNT(*) as orders_count FROM "{PG_SCHEMA}"."orders" GROUP BY customer_id;'),
                    engine
                )
                fig = px.bar(
                    orders_df,
                    x="customer_id",
                    y="orders_count",
                    labels={"customer_id": "Customer ID", "orders_count": "Total Orders"}
                )
                st.plotly_chart(fig, use_container_width=True)

# =========================
# 5. SQL Query Tool Mode
# =========================
else:
    st.subheader("🧠 Run Custom SQL Query")

    st.markdown(
        f"""
        **Connected to:** `{PG_DB}` (schema: `{PG_SCHEMA}`)  
        Write your SQL query below and click **Run Query**.
        """
    )

    default_query = f'SELECT * FROM "{PG_SCHEMA}"."{tables[0]}" LIMIT 10;' if tables else "SELECT 1;"
    sql_input = st.text_area("SQL Query", value=default_query, height=150)

    run_query = st.button("▶ Run Query")

    if run_query:
        try:
            with engine.connect() as conn:
                df = pd.read_sql(text(sql_input), conn)
            st.success(f"✅ Query executed successfully! Rows returned: {len(df)}")
            st.dataframe(df, use_container_width=True)

            # Optional chart suggestion if numeric data exists
            numeric_cols = df.select_dtypes(include='number').columns.tolist()
            if numeric_cols:
                st.markdown("### Quick Chart (first two numeric columns)")
                if len(numeric_cols) >= 2:
                    fig = px.scatter(df, x=numeric_cols[0], y=numeric_cols[1], title="Quick Numeric Scatter Plot")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Add more numeric columns in your query for chart visualization.")
        except Exception as e:
            st.error(f"❌ Error executing query: {e}")


#test
