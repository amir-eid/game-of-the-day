# 🏆 Sports Watcher Dashboard: Analytics Pipeline

An automated data pipeline that aggregates daily sports schedules from various sources, ranks them using a weighted "watchability" algorithm, and provides top picks alongside personalized news headlines in an interactive dashboard.

## 🚀 Architecture Update
The project has been migrated from a local DuckDB solution to a scalable cloud infrastructure:
*   **Database:** **Supabase (PostgreSQL)** serves as the central data warehouse.
*   **Orchestration:** **GitHub Actions** handles daily scraping tasks and dbt transformations.
*   **Transformation:** **dbt (data build tool)** manages SQL-based scoring and team mapping.
*   **Dashboard:** **Streamlit Cloud** provides real-time visualization and news integration.
*   **News Scouting:** **SerpApi** (Google News) fetches context-aware headlines for the day's top matchups.

---

## 🛠️ Tech Stack
*   **Python 3.x:** Core logic and web scraping.
*   **SQL / dbt:** Calculation of the `total_watch_score`.
*   **SQLAlchemy:** Connection bridge between Streamlit and the Supabase instance.
*   **Feedparser:** Integration of RSS feeds for the "Scouting Report" tab.

---

## ⚙️ Pipeline Workflow
1.  **Ingest:** Python scripts (triggered via GitHub Actions) load raw data into `public.raw_...` tables in Supabase.
2.  **Transform (dbt):** 
    *   Cleanses team names and timestamps.
    *   Maps team names to IDs in `dim_teams`.
    *   Calculates `league_pts` and `favorite_pts`.
3.  **Serve:** A PostgreSQL view (`v_dashboard_top_picks`) provides the final, scored dataset.
4.  **Visualize:** The Streamlit dashboard allows filtering by league and dynamically generates news reports via SerpApi.

---

## 💻 Setup & Installation

### 1. Environment Variables
Store the following secrets in Streamlit Cloud and your GitHub Repository Secrets:
```text
SUPABASE_DB_URL=postgresql://postgres:[PASSWORD]@db.[REF].supabase.co:5432/postgres
SERPAPI_KEY=your_api_key_here
```

### 2. Database Initialization
The table structure is managed by dbt. Run the following to generate tables and views:
```bash
dbt run
```

### 3. Run Dashboard Locally
```bash
streamlit run streamlit_app.py
```

---

## 📊 Scoring Logic
The core of the dashboard is the `total_watch_score`, which is calculated based on:
*   **League Weight:** NFL (50), NBA (25), World Cup (50), etc.
*   **Personal Favorites:** High-interest teams (e.g., Miami Heat, Valencia, San Diego Padres) receive bonus points.
*   **Match Status:** Extra points for Derbies and Playoff games.
*   **Time Bonus:** Games during Prime Time (CET) are weighted higher.

---

## 🗺️ Roadmap
- [x] Migration from DuckDB to Supabase Cloud.
- [x] Automation via GitHub Actions.
- [ ] Implementation of an ML model to predict "must-watch" matchups based on watch history.

---

## 🛡️ Security
The `.env` file and dbt profiles are excluded via `.gitignore`. The connection to Supabase is encrypted using SSL (`sslmode=require`).