#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
etl_reporting.py - Extended Demo for BI & Reporting
Features:
- Reads from SQLite demo source DB (or create demo data)
- Reads additional sources: mitarbeiter.csv, lieferanten.xlsx
- ETL transformations, KPI computation
- Data quality checks
- Writes KPI table to DW (SQLite), and exports CSV, Excel, Parquet
- Generates interactive HTML dashboard (plotly.graph_objects)
- Uses config.json for configuration
"""
import os
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import json

# Load config
CONFIG_PATH = os.environ.get("CONFIG_PATH", "config.json")
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    cfg = json.load(f)

SOURCE_DB_URI = os.environ.get("SOURCE_DB_URI", cfg.get("SOURCE_DB_URI", "sqlite:///source_demo.db"))
DW_DB_URI = os.environ.get("DW_DB_URI", cfg.get("DW_DB_URI", "sqlite:///dw_demo.db"))
MITARBEITER_CSV = cfg.get("MITARBEITER_CSV", "mitarbeiter.csv")
LIEFERANTEN_XLSX = cfg.get("LIEFERANTEN_XLSX", "lieferanten.xlsx")
DASHBOARD_HTML = cfg.get("DASHBOARD_HTML", "outputs/dashboard.html")
CSV_EXPORT = cfg.get("CSV_EXPORT", "outputs/kpi_export.csv")
XLSX_EXPORT = cfg.get("XLSX_EXPORT", "outputs/kpi_export.xlsx")
PARQUET_EXPORT = cfg.get("PARQUET_EXPORT", "outputs/kpi_export.parquet")

# ---------------------------
# Demo data creation (if needed)
# ---------------------------
def create_demo_source_db(engine):
    rng = np.random.default_rng(42)
    n_orders = 500
    start_date = datetime(2024, 1, 1)
    orders = pd.DataFrame({
        "order_id": np.arange(1, n_orders+1),
        "site": rng.choice(["Bremen", "Hamburg", "Rendsburg"], size=n_orders, p=[0.5,0.3,0.2]),
        "created_at": [start_date + timedelta(days=int(x)) for x in rng.integers(0, 600, n_orders)],
        "completed_at": [None]*n_orders,
        "cost": rng.normal(10000, 2000, n_orders).round(2)
    })
    for i in orders.sample(frac=0.8, random_state=1).index:
        lead = int(rng.integers(10, 120))
        orders.at[i, "completed_at"] = (orders.at[i, "created_at"] + timedelta(days=lead)).strftime("%Y-%m-%d")
    orders["created_at"] = orders["created_at"].dt.strftime("%Y-%m-%d")
    production = pd.DataFrame({
        "prod_id": np.arange(1, 301),
        "site": rng.choice(["Bremen", "Hamburg", "Rendsburg"], size=300),
        "start_date": [(start_date + timedelta(days=int(x))).strftime("%Y-%m-%d") for x in rng.integers(0, 600, 300)],
        "percent_complete": rng.integers(0, 101, 300),
        "defects": rng.poisson(0.8, 300)
    })
    employees = pd.DataFrame({
        "emp_id": [1,2,3],
        "name": ["Team A", "Team B", "Team C"],
        "site": ["Bremen", "Hamburg", "Rendsburg"]
    })
    orders.to_sql("orders", engine, if_exists="replace", index=False)
    production.to_sql("production", engine, if_exists="replace", index=False)
    employees.to_sql("employees", engine, if_exists="replace", index=False)
    print("Demoquelle erstellt: orders, production, employees")

# ---------------------------
# Read additional sources
# ---------------------------
def read_additional_sources():
    # mitarbeiter.csv
    try:
        mit = pd.read_csv(MITARBEITER_CSV)
        print(f"MITARBEITER loaded: {MITARBEITER_CSV}")
    except Exception as e:
        mit = pd.DataFrame()
        print("MITARBEITER not loaded:", e)
    # lieferanten.xlsx
    try:
        liefer = pd.read_excel(LIEFERANTEN_XLSX)
        print(f"LIEFERANTEN loaded: {LIEFERANTEN_XLSX}")
    except Exception as e:
        liefer = pd.DataFrame()
        print("LIEFERANTEN not loaded:", e)
    return mit, liefer

# ---------------------------
# Extract
# ---------------------------
def extract_data(engine):
    orders = pd.read_sql("SELECT * FROM orders", con=engine)
    production = pd.read_sql("SELECT * FROM production", con=engine)
    employees = pd.read_sql("SELECT * FROM employees", con=engine)
    return orders, production, employees

# ---------------------------
# Transform / KPI
# ---------------------------
def transform_data(orders, production, mit, liefer):
    orders = orders.copy()
    orders['created_at'] = pd.to_datetime(orders['created_at'])
    orders['completed_at'] = pd.to_datetime(orders['completed_at'], errors='coerce')
    orders['lead_days'] = (orders['completed_at'] - orders['created_at']).dt.days
    orders['is_completed'] = orders['completed_at'].notna().astype(int)
    orders['year_month'] = orders['created_at'].dt.to_period('M').astype(str)
    kpi_orders = orders.groupby(['site', 'year_month']).agg(
        orders_count=('order_id', 'count'),
        completed_count=('is_completed', 'sum'),
        avg_lead_days=('lead_days', 'mean'),
        cost_total=('cost', 'sum')
    ).reset_index()
    kpi_orders['avg_lead_days'] = kpi_orders['avg_lead_days'].fillna(0).round(2)
    production = production.copy()
    production['start_date'] = pd.to_datetime(production['start_date'])
    production['year_month'] = production['start_date'].dt.to_period('M').astype(str)
    kpi_prod = production.groupby(['site', 'year_month']).agg(
        avg_percent_complete=('percent_complete', 'mean'),
        defects_total=('defects', 'sum'),
        production_count=('prod_id', 'count')
    ).reset_index()
    kpi_prod['avg_percent_complete'] = kpi_prod['avg_percent_complete'].round(2)
    kpi = pd.merge(kpi_orders, kpi_prod, how='outer', on=['site', 'year_month']).fillna(0)
    kpi['completion_rate'] = (kpi['completed_count'] / kpi['orders_count']).replace([np.inf, -np.inf], 0).fillna(0).round(3)
    try:
        kpi['year_month_date'] = pd.to_datetime(kpi['year_month'] + '-01')
    except Exception:
        kpi['year_month_date'] = pd.NaT
    kpi['generated_at'] = pd.Timestamp.now()
    if not mit.empty and 'site' in mit.columns:
        emp_counts = mit.groupby('site').size().reset_index(name='employee_count')
        kpi = kpi.merge(emp_counts, on='site', how='left').fillna({'employee_count':0})
    else:
        kpi['employee_count'] = 0
    if not liefer.empty:
        kpi['supplier_count'] = len(liefer)
    else:
        kpi['supplier_count'] = 0
    return kpi

# ---------------------------
# Data Quality Checks
# ---------------------------
def data_quality_checks(orders, production):
    issues = []
    if orders['order_id'].isnull().any():
        issues.append("Nulls in orders.order_id")
    if orders['created_at'].isnull().any():
        issues.append("Nulls in orders.created_at")
    if orders['order_id'].duplicated().any():
        issues.append("Duplicated order_id in orders")
    if (orders['cost'] < 0).any():
        issues.append("Negative cost values found")
    if production['percent_complete'].lt(0).any() or production['percent_complete'].gt(100).any():
        issues.append("percent_complete out of range 0-100")
    return issues

# ---------------------------
# Load/Exports
# ---------------------------
def load_to_dw(engine_dw, kpi_df):
    kpi_df.to_sql("kpis", engine_dw, if_exists="replace", index=False)
    print(f"KPI-Tabelle in DW geschrieben (rows={len(kpi_df)})")

def export_all(kpi_df):
    PathDir = os.path.dirname(CSV_EXPORT)
    if PathDir and not os.path.exists(PathDir):
        os.makedirs(PathDir, exist_ok=True)
    kpi_df.to_csv(CSV_EXPORT, index=False)
    kpi_df.to_excel(XLSX_EXPORT, index=False)
    try:
        kpi_df.to_parquet(PARQUET_EXPORT, index=False)
    except Exception as e:
        print("Parquet export failed (optional):", e)
    print("Exports erzeugt:", CSV_EXPORT, XLSX_EXPORT, PARQUET_EXPORT)

# ---------------------------
# Dashboard (graph_objects)
# ---------------------------
def generate_dashboard(kpi_df, filename=DASHBOARD_HTML):
    try:
        import plotly.graph_objects as go
        import plotly.io as pio
    except Exception as e:
        print("Plotly not available. Skipping dashboard. Error:", e)
        return
    if kpi_df.empty:
        print("No KPI data for dashboard.")
        return
    kpi_df = kpi_df.sort_values(['site','year_month_date'])
    fig = go.Figure()
    for site, df_site in kpi_df.groupby('site'):
        fig.add_trace(go.Scatter(x=df_site['year_month_date'], y=df_site['completion_rate'],
                                 mode='lines+markers', name=str(site)))
    fig.update_layout(title='Completion Rate je Standort (Monat)', xaxis_title='Monat', yaxis_title='Completion Rate')
    fig2 = go.Figure()
    for site, df_site in kpi_df.groupby('site'):
        fig2.add_trace(go.Bar(x=df_site['year_month_date'], y=df_site['orders_count'], name=str(site)))
    fig2.update_layout(title='Bestellanzahl je Monat / Standort', barmode='group')
    html = "<html><head><meta charset='utf-8'></head><body>"
    html += "<h1>BI Dashboard - KPI Export</h1>"
    html += pio.to_html(fig, include_plotlyjs='cdn', full_html=False)
    html += "<hr>"
    html += pio.to_html(fig2, include_plotlyjs=False, full_html=False)
    html += "</body></html>"
    outdir = os.path.dirname(filename)
    if outdir and not os.path.exists(outdir):
        os.makedirs(outdir, exist_ok=True)
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)
    print("Dashboard geschrieben:", filename)

# ---------------------------
# Main
# ---------------------------
def main():
    engine_src = create_engine(SOURCE_DB_URI, echo=False)
    engine_dw = create_engine(DW_DB_URI, echo=False)
    with engine_src.connect() as conn:
        res = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
        tables = [r[0] for r in res.fetchall()]
    if not tables:
        create_demo_source_db(engine_src)
    mit, liefer = read_additional_sources()
    orders, production, employees = extract_data(engine_src)
    dq = data_quality_checks(orders, production)
    if dq:
        print("Data Quality Issues:", dq)
    else:
        print("No DQ issues (basic checks).")
    kpi = transform_data(orders, production, mit, liefer)
    load_to_dw(engine_dw, kpi)
    export_all(kpi)
    generate_dashboard(kpi)
    print("Pipeline finished.")

if __name__ == "__main__":
    main()
