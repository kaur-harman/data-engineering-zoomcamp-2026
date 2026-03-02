# NYC Taxi dlt Pipeline (Workshop 1)

## Overview
This project builds a custom dlt pipeline to extract NYC Yellow Taxi trip data from a paginated REST API and load it into DuckDB.

The data source is a custom API (not scaffolded by dlt), so the REST source and pagination logic were implemented manually.

---

## Data Source

- Base URL: https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api
- Format: Paginated JSON
- Page Size: 1,000 records per page
- Pagination: Stop when an empty page is returned

---

## Architecture

Custom REST API → dlt pipeline → DuckDB

- Data extracted page by page using `requests`
- Loaded using `@dlt.resource`
- Stored in DuckDB under dataset `taxi_data`
- Main table: `taxi_trips`

---

## Setup Instructions

```bash
pip install -r requirements.txt
python taxi_pipeline.py
```

After execution, the following is created:

- `taxi_pipeline.duckdb`
- Dataset: `taxi_data`
- Table: `taxi_trips`