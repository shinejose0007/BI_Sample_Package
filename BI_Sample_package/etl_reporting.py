#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
etl_reporting.py
Demo-ETL / DWH / KPI-Pipeline (fixed: uses plotly.graph_objects to avoid xarray import issue)
- Extraktion aus Quellsystem (SQLite demo)
- Transformation / KPI-Berechnung
- Data Quality Checks
- Laden ins DW (SQLite)
- CSV-Export für BI-Tools (Qlik/Power BI)
- Interaktives HTML-Dashboard (Plotly - graph_objects)
Configure SOURCE_DB_URI and DW_DB_URI via environment vars if needed.
"""

import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ---------------------------
# Konfiguration
# ---------------------------
SOURCE_DB_URI = os.environ.get("SOURCE_DB_URI", "sqlite:///source_demo.db")
DW_DB_URI = os.environ.get("DW_DB_URI", "sqlite:///dw_demo.db")
DASHBOARD_HTML = os.environ.get("DASHBOARD_HTML", "dashboard.html")
CSV_EXPORT = os.environ.get("CSV_EXPORT", "kpi_export.csv")

# ---------------------------
# Demo-Daten erzeugen (nur falls DB leer)
# ---------------------------
def create_demo_source_db(engine):
    rng = np.random.default_rng(42)
    n_orders = 500
    start_date = datetime(2024, 1, 1)

    orders = pd.DataFrame({
        "order_id": np.arange(1, n_orders + 1),
        "site": rng.choice(["Bremen", "Hamburg", "Rendsburg"], size=n_orders, p=[0.5, 0.3, 0.2]),
        "created_at": [start_date + timedelta(days=int(x)) for x in rng.integers(0, 600, n_orders)],
        "completed_at": [None] * n_orders,
        "cost": rng.normal(10000, 2000, n_orders).round(2)
    })
    # set some completed dates
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
        "emp_id": [1, 2, 3],
        "name": ["Team A", "Team B", "Team C"],
        "site": ["Bremen", "Hamburg", "Rendsburg"]
    })

    orders.to_sql("orders", engine, if_exists="replace", index=False)
    production.to_sql("production", engine, if_exists="replace", index=False)
    employees.to_sql("employees", engine, if_exists="replace", index=False)
    print("Demoquelle erstellt: tables orders, production, employees")

# ---------------------------
# Extract
# ---------------------------
def extract_data(engine):
    orders = pd.read_sql("SELECT * FROM orders", con=engine)
    production = pd.read_sql("SELECT * FROM production", con=engine)
    employees = pd.read_sql("SELECT * FROM employees", con=engine)
    return orders, production, employees

# ---------------------------
# Transform
# ---------------------------
def transform_data(orders, production):
    orders = orders.copy()
    orders['created_at'] = pd.to_datetime(orders['created_at'])
    orders['completed_at'] = pd.to_datetime(orders['completed_at'], errors='coerce')
    orders['lead_days'] = (orders['completed_at'] - orders['created_at']).dt.days
    orders['is_completed'] = orders['completed_at'].notna().astype(int)

    # Aggregation per site / month
    orders['year_month'] = orders['created_at'].dt.to_period('M').astype(str)  # e.g. "2024-01"
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

    # Combine
    kpi = pd.merge(kpi_orders, kpi_prod, how='outer', on=['site', 'year_month']).fillna(0)

    # safe completion rate (avoid division by zero)
    kpi['completion_rate'] = (kpi['completed_count'] / kpi['orders_count']).replace([np.inf, -np.inf], 0).fillna(0).round(3)

    # add timestamp and a datetype for plotting
    # create a datetime representing the first day of the month for ordering in plots
    try:
        kpi['year_month_date'] = pd.to_datetime(kpi['year_month'] + '-01')
    except Exception:
        kpi['year_month_date'] = pd.NaT

    kpi['generated_at'] = pd.Timestamp.now()
    return kpi

# ---------------------------
# Data Quality Checks
# ---------------------------
def data_quality_checks(orders, production):
    issues = []
    # Basic null checks
    if orders['order_id'].isnull().any():
        issues.append("Nulls in orders.order_id")
    if orders['created_at'].isnull().any():
        issues.append("Nulls in orders.created_at")
    # Duplicates
    if orders['order_id'].duplicated().any():
        issues.append("Duplicated order_id in orders")
    # Negative costs
    if (orders['cost'] < 0).any():
        issues.append("Negative cost values found")
    # percent range
    if production['percent_complete'].lt(0).any() or production['percent_complete'].gt(100).any():
        issues.append("percent_complete out of range 0-100")
    return issues

# ---------------------------
# Load (DW)
# ---------------------------
def load_to_dw(engine_dw, kpi_df):
    kpi_df.to_sql("kpis", engine_dw, if_exists="replace", index=False)
    print(f"KPI-Tabelle in DW geschrieben (rows={len(kpi_df)})")

# ---------------------------
# Export CSV
# ---------------------------
def export_csv(kpi_df, filename=CSV_EXPORT):
    kpi_df.to_csv(filename, index=False)
    print(f"CSV Export erstellt: {filename}")

# ---------------------------
# Generate Dashboard - using graph_objects (avoids xarray import)
# ---------------------------
def generate_dashboard(kpi_df, filename=DASHBOARD_HTML):
    try:
        import plotly.graph_objects as go
        import plotly.io as pio
    except Exception as e:
        print("Plotly not available or import failed. Skipping dashboard generation.")
        print("Plotly import error:", str(e))
        return

    if kpi_df.empty:
        print("KPI DataFrame empty — skipping dashboard")
        return

    # ensure the data is sorted by site and date
    kpi_df = kpi_df.sort_values(['site', 'year_month_date'])

    # Figure 1: Completion Rate time series per site
    fig = go.Figure()
    for site, df_site in kpi_df.groupby('site'):
        fig.add_trace(go.Scatter(
            x=df_site['year_month_date'],
            y=df_site['completion_rate'],
            mode='lines+markers',
            name=str(site),
            hovertemplate='%{x|%Y-%m}: %{y:.2f}<extra></extra>'
        ))
    fig.update_layout(title='Completion Rate je Standort (Monat)',
                      xaxis_title='Monat',
                      yaxis_title='Completion Rate',
                      hovermode='x unified')

    # Figure 2: Orders count bar chart grouped by site
    fig2 = go.Figure()
    for site, df_site in kpi_df.groupby('site'):
        fig2.add_trace(go.Bar(
            x=df_site['year_month_date'],
            y=df_site['orders_count'],
            name=str(site),
            hovertemplate='%{x|%Y-%m}: %{y}<extra></extra>'
        ))
    fig2.update_layout(title='Bestellanzahl je Monat / Standort',
                       xaxis_title='Monat',
                       yaxis_title='Anzahl Bestellungen',
                       barmode='group')

    # Compose simple HTML with both charts
    html = "<html><head><meta charset='utf-8'></head><body>"
    html += "<h1>BI Dashboard - KPI Export</h1>"
    html += pio.to_html(fig, include_plotlyjs='cdn', full_html=False)
    html += "<hr>"
    html += pio.to_html(fig2, include_plotlyjs=False, full_html=False)
    html += "</body></html>"

    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Dashboard erzeugt: {filename}")

# ---------------------------
# Main pipeline
# ---------------------------
def main():
    engine_src = create_engine(SOURCE_DB_URI, echo=False)
    engine_dw = create_engine(DW_DB_URI, echo=False)

    # If source DB has no tables, create demo data
    with engine_src.connect() as conn:
        res = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
        tables = [r[0] for r in res.fetchall()]
    if not tables:
        create_demo_source_db(engine_src)

    # Extract
    orders, production, employees = extract_data(engine_src)

    # Data Quality checks
    dq_issues = data_quality_checks(orders, production)
    if dq_issues:
        print("Data Quality Issues gefunden:")
        for i in dq_issues:
            print(" -", i)
    else:
        print("Keine Data Quality Issues (Basischecks)")

    # Transform
    kpi = transform_data(orders, production)
    print("KPI-Transformation abgeschlossen. Beispiele:")
    print(kpi.head(3).to_string(index=False))

    # Load to DW
    load_to_dw(engine_dw, kpi)

    # Exports & Dashboard
    export_csv(kpi)
    generate_dashboard(kpi)

    print("ETL Pipeline erfolgreich abgeschlossen.")

if __name__ == "__main__":
    main()
