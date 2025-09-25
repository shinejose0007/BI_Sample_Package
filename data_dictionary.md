
# Data Dictionary - KPI Felder

- site: Standort der Produktion / Bestellung (z.B. Bremen, Hamburg, Rendsburg)
- year_month: Aggregationsmonat im Format YYYY-MM
- orders_count: Anzahl Bestellungen im Monat
- completed_count: Anzahl abgeschlossener Bestellungen im Monat
- avg_lead_days: Durchschnittliche Durchlaufzeit (Tage) für abgeschlossene Bestellungen
- cost_total: Gesamtkosten der Bestellungen
- avg_percent_complete: Durchschnittlicher Fertigstellungsgrad in der Produktion
- defects_total: Anzahl gemeldeter Defekte
- production_count: Anzahl Produktionslose
- completion_rate: completed_count / orders_count (float 0..1)
- employee_count: Anzahl Mitarbeiter pro Standort (aus mitarbeiter.csv)
- supplier_count: Anzahl Lieferanten (aus lieferanten.xlsx)
- generated_at: Timestamp der KPI-Generierung
