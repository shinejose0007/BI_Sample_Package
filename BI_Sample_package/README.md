# BI & Reporting Demo-Paket —  (Demo)

Dieses Paket enthält ein vollständiges, lauffähiges Demo-Projekt, das typische Aufgaben einer Data Analyst (BI & Reporting) Rolle abbildet:
- ETL aus Quellsystemen (Demo-SQLite), Datenbereinigung und -modellierung
- Berechnung von KPIs und Erzeugung einer KPI-Tabelle im Data Warehouse
- Einfache Data Quality Checks
- CSV-Export (kpi_export.csv) für Import in BI-Tools (z. B. Qlik Sense / Power BI)
- Interaktives HTML-Dashboard (dashboard.html) zur schnellen Visualisierung

## Dateien im Paket
- `etl_reporting.py` — Hauptskript (Extraktion, Transformation, Load, Export, Dashboard)
- `requirements.txt` — benötigte Python-Pakete
- `README.md` — diese Datei

## Ziel & Mapping zu Stellenanforderungen (kurz)
Dieses Demo-Paket demonstriert Kernanforderungen der ausgeschriebenen Stelle:
- **SQL & Data Warehouse**: SQL-basierte Extraktion und Speicherung in einem DW-Table (`kpis`).
- **ETL / Datenintegration**: Beispielhafte Zusammenführung von `orders` und `production` mit Transformationen.
- **Datenqualitätssicherung**: Basis-Checks auf Null-Werte, Duplikate, Wertebereiche.
- **KPI-Definition**: Fertigstellungsrate (`completion_rate`), Durchlaufzeiten (`avg_lead_days`), Produktionskennzahlen.
- **Reporting & Dashboards**: CSV-Export (für Qlik) und interaktives HTML-Dashboard (Plotly).
- **Stakeholder-Relevanz**: Die KPIs sind pro Standort und Monat aggregiert — typisch für standortübergreifende Auswertungen.

## Schnellstart (lokal)
1. Python 3.9+ installieren.
2. Virtuelle Umgebung erstellen (empfohlen):
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux/macOS
   .venv\Scripts\activate    # Windows (PowerShell)
   ```
3. Abhängigkeiten installieren:
   ```bash
   pip install -r requirements.txt
   ```
4. Pipeline ausführen (erzeugt DB, CSV und Dashboard):
   ```bash
   python etl_reporting.py
   ```
5. `kpi_export.csv` in Qlik Sense / Power BI importieren oder `dashboard.html` im Browser öffnen.

## Hinweise zur Anpassung an reale Systeme
- Ersetzen Sie `SOURCE_DB_URI` / `DW_DB_URI` durch Produktionsverbindungen (z.B. PostgreSQL / MS SQL).
- In produktiven Umgebungen: Orchestrator (Airflow / Prefect), Logging, Monitoring, Tests, und strukturierte Data Dictionary-Dokumentation ergänzen.
- Für Qlik: CSV oder Parquet als Datenquelle möglich; für große Datenmengen Parquet/SQL-DB bevorzugen.


