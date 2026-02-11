import streamlit as st
import pandas as pd
import streamlit as st
from snowflake.snowpark import Session

# Create Snowflake session using Streamlit secrets
connection_parameters = {
    "account": st.secrets["snowflake"]["account"],
    "user": st.secrets["snowflake"]["user"],
    "password": st.secrets["snowflake"]["password"],
    "role": st.secrets["snowflake"]["role"],
    "warehouse": st.secrets["snowflake"]["warehouse"],
    "database": st.secrets["snowflake"]["database"],
    "schema": st.secrets["snowflake"]["schema"],
}

session = Session.builder.configs(connection_parameters).create()


st.title("Employee Analytics Dashboard")

# ------------------------------
# Get Available Years
# ------------------------------
years_df = session.sql(
    "SELECT DISTINCT YEAR FROM OVERTIME_ANALYSIS ORDER BY YEAR"
).to_pandas()

selected_year = st.selectbox("Select Year", years_df["YEAR"])

# ------------------------------
# Overtime Analysis
# ------------------------------
df1 = session.sql(f"""
    SELECT DEPARTMENT, TOTAL_OVERTIME
    FROM OVERTIME_ANALYSIS
    WHERE YEAR = {selected_year}
""").to_pandas()

st.subheader("Overtime Analysis")
st.bar_chart(df1.set_index("DEPARTMENT")["TOTAL_OVERTIME"])

# ------------------------------
# Leave Analysis
# ------------------------------
df2 = session.sql(f"""
    SELECT LEAVE_TYPE, TOTAL_LEAVE_DAYS
    FROM LEAVE_ANALYSIS
    WHERE YEAR = {selected_year}
""").to_pandas()

st.subheader("Leave Analysis")
st.bar_chart(df2.set_index("LEAVE_TYPE")["TOTAL_LEAVE_DAYS"])

# ------------------------------
# Project Analysis
# ------------------------------
df3 = session.sql(f"""
    SELECT TOTAL_PROJECTS, TOTAL_REVENUE
    FROM PROJECT_ANALYSIS
    WHERE YEAR = {selected_year}
""").to_pandas()

st.subheader("Project Analysis")
st.dataframe(df3)

# =====================================================
# EMPLOYEE PROJECT STATUS ANALYSIS
# =====================================================

st.divider()
st.header("Employee Project Status Dashboard")

# Get Employee List
emp_df = session.sql("""
    SELECT DISTINCT EMP_ID
    FROM EMP_ANALYTICS_DB.SILVER.PROJECTS_CLEAN
    ORDER BY EMP_ID
""").to_pandas()

selected_emp = st.selectbox("Select Employee ID", emp_df["EMP_ID"])

# Fetch Project Status Counts
project_status_df = session.sql(f"""
    SELECT 
        STATUS,
        COUNT(*) AS TOTAL_PROJECTS
    FROM EMP_ANALYTICS_DB.SILVER.PROJECTS_CLEAN
    WHERE EMP_ID = {selected_emp}
    GROUP BY STATUS
""").to_pandas()

st.subheader(f"Project Status for Employee {selected_emp}")

st.bar_chart(project_status_df.set_index("STATUS")["TOTAL_PROJECTS"])
