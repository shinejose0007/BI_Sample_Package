
# Erweiterte BI & Reporting Demo (Lürssen-Anpassung)

Dieses Paket demonstriert eine kleine, aber vollständige BI-Pipeline:
- ETL aus Quellsystemen (SQLite demo)
- Zusätzliche Quellimporte: Mitarbeiter (CSV) und Lieferanten (Excel)
- KPI-Berechnung, Datenqualitätsprüfungen
- Exporte: CSV, Excel, Parquet
- Interaktives Dashboard (HTML)

## Schnellstart
1. Virtuelle Umgebung:
   python -m venv .venv
   source .venv/bin/activate   # Linux/macOS
   .venv\Scripts\activate    # Windows PowerShell
2. Abhängigkeiten:
   pip install -r requirements.txt
3. Ausführen:
   python etl_reporting.py

## Mapping Stellenanforderung -> Demo
- SQL & DWH: uses SQLite and writes KPIs to DW table `kpis`
- ETL/Datenintegration: merges orders + production + mitarbeiter + lieferanten sources
- Datenqualität: basic checks implemented
- KPI-Definition: completion_rate, avg_lead_days, cost_total, defects_total
- Reporting: CSV/Excel/Parquet exports + HTML dashboard suitable for screenshots

## Bewerbungsnutzung
- Fügen Sie `outputs/kpi_export.csv` und `outputs/dashboard.html` als Anhang oder Screenshots zu Ihrer Bewerbung hinzu.
- Verweisen Sie in Anschreiben auf `config.json` und `data_dictionary.md`, um Ihre strukturierte Arbeitsweise zu zeigen.
