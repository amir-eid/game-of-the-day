# 🏆 Game of the Day: Sports Analytics Pipeline

An automated data pipeline that scrapes daily sports schedules from different sources, ranks games based on a custom "watchability" algorithm, and sends the top picks directly to Discord. Shows top headlines for top games of the day.

## 🚀 Overview
This project is designed to solve the "What should I watch today?" problem for sports fans. It handles everything from raw data extraction to final mobile notifications.

### The Tech Stack
* **Orchestration:** [Prefect](https://www.prefect.io/) (Scheduled runs & task monitoring)
* **Transformation:** [dbt](https://www.getdbt.com/) (SQL-based business logic & scoring)
* **Database:** [DuckDB](https://duckdb.org/) (Fast, local analytical database)
* **Notifications:** **Discord Webhooks** (Mobile alerts)
* **Dashboard:** **Streamlit**
* **Language:** Python 3.x

---

## 🛠️ Pipeline Architecture
1.  **Scrape:** Extracts daily game data (Leagues, Matchups, Times, Odds).
2.  **Load:** Ingests raw JSON/CSV data into a local `sports.duckdb` instance.
3.  **Transform (dbt):**
    * Cleans team names and timestamps.
    * Calculates a `total_watch_score` based on preferred leagues, sports, teams and rivalries.
    * Filters for "Favorites" and "Derby" matches.
4.  **Notify:** Queries the final dbt models and pushes the Top 10 games to a Discord channel.

---

## ⚙️ Setup & Installation

### 1. Environment Variables
To keep sensitive data secure, this project uses a `.env` file. Create one in the root directory:
```text
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Initialize dbt
Ensure your `profiles.yml` is configured to point to `sports.duckdb`, then verify the connection:
```bash
dbt debug
```

---

## 🏃 Running the Pipeline

### Manual Run
To execute the entire flow immediately:
```bash
python run_pipeline.py
```

### Scheduled Serving (Prefect)
To keep the pipeline "listening" for scheduled runs (e.g., every morning at 11:00 AM):
```bash
# This will serve the flow on your local machine
python run_pipeline.py --serve
```

---

## 📊 Database Schema
* `raw_schedule`: The landing zone for scraped data.
* `fct_daily_schedule`: The final analytical table produced by dbt.
* `watched_history`: A persistent table tracking every game recommended over time to avoid duplicates and track performance.

---

## 🛡️ Security Note
The `.env` file, `sports.duckdb`, and `logs/` are explicitly excluded from the repository via `.gitignore` to protect webhook credentials and prevent large binary files from bloating the version history.

## Roadmap

- [ ] PowerBI dashboard to visualize daily recommendations
- [ ] Compare recommendations vs. games actually watched
- [ ] ML model to learn from watch history and improve recommendations over time