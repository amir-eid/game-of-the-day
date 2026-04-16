import os
from dotenv import load_dotenv
from prefect import flow, task
import duckdb
import requests
from datetime import datetime
import pandas as pd
import subprocess
import schedule, time

# ── Leagues to fetch ──────────────────────────────────────────────────────────
LEAGUES = [
    ('basketball', 'nba',                     'NBA'),
    ('football',   'nfl',                     'NFL'),
    ('baseball',   'mlb',                     'MLB'),
    ('baseball',   'world-baseball-classic',  'World Baseball Classic'),
    ('hockey',     'nhl',                     'NHL'),
    ('hockey',     'olympics-mens-ice-hockey','Olympic Ice Hockey'),
    ('basketball', 'mens-college-basketball', 'College Basketball'),
    ('basketball', 'fiba',                    'FIBA World Cup'),
    ('basketball', 'mens-olympics-basketball','Olympic Basketball'),
    ('football',   'college-football',        'College Football'),
    ('soccer',     'esp.1',                   'ESP.1'),
    ('soccer',     'eng.1',                   'ENG.1'),
    ('soccer',     'ger.1',                   'GER.1'),
    ('soccer',     'fra.1',                   'FRA.1'),
    ('soccer',     'ita.1',                   'ITA.1'),
    ('soccer',     'uefa.champions',          'UEFA Champions League'),
    ('soccer',     'uefa.europa',             'UEFA Europa League'),
    ('soccer',     'uefa.europa.conf',        'UEFA Conference League'),
    ('soccer',     'fifa.world',              'FIFA World Cup'),
    ('soccer',     'uefa.euro',               'UEFA European Championship'),
    ('soccer',     'uefa.euroq',              'UEFA European Championship Qualifiers'),
    ('soccer',     'eng.fa',                  'FA Cup'),
    ('soccer',     'esp.copa_del_rey',        'Copa del Rey'),
    ('soccer',     'ned.1',                   'Eredivisie'),
    ('soccer',     'por.1',                   'Portuguese Primeira Liga'),
    ('soccer',     'rus.1',                   'Russian Premier League'),
    ('soccer',     'aut.1',                   'Austrian Bundesliga'),
    ('soccer',     'tur.1',                   'Turkish Süper Lig'),
    ('soccer',     'caf.nations',             'Africa Cup of Nations'),
    ('soccer',     'caf.nations_qual',         'Africa Cup of Nations Qualifiers'),
    ('soccer',     'conmebol.america',        'Copa America'),
    ('soccer',     'uefa.nations',            'UEFA Nations League'),
    ('soccer',     'fifa.olympics',           'Olympic Football Tournament'),
    ('soccer',     'fifa.worldq.uefa',        'FIFA World Cup Qualifiers - UEFA'),
    ('soccer',     'fifa.worldq.caf',         'FIFA World Cup Qualifiers - CAF'),
    ('soccer',     'jpn.1',                   'J1 League'),
    ('soccer',     'conmebol.libertadores',   'Copa Libertadores'),
    ('soccer',     'fifa.friendly',           'International Friendlies'),



]

# ── Average game durations (hours) ───────────────────────────────────────────
DURATION_MAP = {
    'NBA': 2.5, 'NFL': 3.25, 'MLB': 2.5, 'World Baseball Classic': 3, 'NHL': 2.5,
    'College Basketball': 2.5, 'College Football': 3.5, 'FIBA World Cup': 2.5, 'Olympic Ice Hockey': 2.5,
    'ESP.1': 2.0, 'ENG.1': 2.0, 'GER.1': 2.0, 'FRA.1': 2.0, 'ITA.1': 2.0, 'Olympic Basketball': 2.5,
    'UEFA Champions League': 2.0, 'UEFA Europa League': 2.0, 'UEFA Conference League': 2.0,
    'FIFA World Cup': 2.0, 'UEFA European Championship': 2.0, 'UEFA European Championship Qualifiers': 2.0,
    'FA Cup': 2.0, 'Copa del Rey': 2.0, 'Eredivisie': 2.0, 'Portuguese Primeira Liga': 2.0,
    'Russian Premier League': 2.0, 'Austrian Bundesliga': 2.0, 'Turkish Süper Lig': 2.0,
    'Africa Cup of Nations': 2.0, 'Africa Cup of Nations Qualifiers': 2.0, 'Copa America': 2.0,
    'UEFA Nations League': 2.0, 'Olympic Football Tournament': 2.0, 'FIFA World Cup Qualifiers - UEFA': 2.0,
    'FIFA World Cup Qualifiers - CAF': 2.0, 'J1 League': 2.0, 'Copa Libertadores': 2.0, 'International Friendlies': 2.0
}

# ── Score weights ─────────────────────────────────────────────────────────────
#LEAGUE_POINTS = {
 #   'NFL': 20, 'NBA': 18, 'MLB': 16, 'World Baseball Classic': 15, 'ESP.1': 14, 'NHL': 12, 'ENG.1': 10, 'College Basketball': 8,
  #  'College Football': 6, 'GER.1': 4, 'FRA.1': 3, 'ITA.1': 2, 'UEFA Champions League': 2, 'UEFA Europa League': 1.5,
   # 'UEFA Conference League': 1, 'FIFA World Cup': 25, 'UEFA European Championship': 20, 'FIBA World Cup': 5,
    #'UEFA European Championship Qualifiers': 15, 'FA Cup': 1, 'Copa del Rey': 1, 'Eredivisie': 0.5, 'Portuguese Primeira Liga': 0.5,
    #'Russian Premier League': 0.5, 'Austrian Bundesliga': 2, 'Turkish Süper Lig': 0.5, 'Olympic Basketball': 5,
    #'Africa Cup of Nations': 2, 'Africa Cup of Nations Qualifiers': 2, 'Copa America': 0.5, 'Olympic Ice Hockey': 2,
    #'UEFA Nations League': 0.5, 'Olympic Football Tournament': 0.5, 'FIFA World Cup Qualifiers - UEFA': 0.5,
    #'FIFA World Cup Qualifiers - CAF': 0.5, 'J1 League': 0.5, 'Copa Libertadores': 0.5, 'International Friendlies': 0.25
#}
#FAVORITE_BONUS   = 30
#PLAYOFF_BONUS    = 25
#DERBY_BONUS      = 15
#STREAK_BONUS     = 15
#MAX_TABLE_POINTS = 10
#TIME_CUTOFF      = '02:00'
#TOP_N            = 5

# ── Personal preferences ─────────────────────────────────────────────────────
FAVORITE_TEAMS = [
    'Miami Heat', 'Carolina Panthers',
    'Illinois Fighting Illini', 'San Diego Padres', 'Valencia',
    'Austria National Team', 'Egypt National Team', 'Poland National Team'
]

DERBIES = [
    ('Los Angeles Lakers',  'Los Angeles Clippers'),
    ('New York Knicks',     'Brooklyn Nets'),
    ('Real Madrid',         'Barcelona'),
    ('AC Milan',            'Internazionale'),
    ('Bayern Munich',       'Borussia Dortmund'),
    ('Arsenal',             'Tottenham Hotspur'),
    ('Manchester United',   'Manchester City'),
    ('Juventus',            'Inter Milan'),
    ('River Plate',         'Boca Juniors'),
    ('Red Bull Salzburg',   'Rapid Wien'),
    ('Galatasaray',         'Fenerbahçe'),
    ('Paris Saint-Germain', 'Olympique de Marseille'),
    ('Ajax',                'Feyenoord'),
    ('Benfica',             'Porto'),
    ('Galatasaray',         'Besiktas'),
    # Add more derbies here
]

def get_record(competitor):
    """Extract win-loss record from a competitor object."""
    records = competitor.get('records', [])
    return records[0].get('summary', '0-0') if records else 'N/A'


def calculate_end_time(dt, league):
    """Calculate estimated end time based on league duration."""
    hours = DURATION_MAP.get(league, 2.5)
    return (dt + pd.Timedelta(hours=hours)).strftime('%H:%M')


def fetch_league(sport, league_slug, league_name):
    """Fetch today's games for a single league from ESPN API."""
    today = datetime.now().strftime('%Y%m%d')
    url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league_slug}/scoreboard"
    params = {'dates': today}

    if league_slug == 'mens-college-basketball':
        params.update({'groups': 50, 'limit': 350})
    elif league_slug == 'college-football':
        params['groups'] = 80

    try:
        data = requests.get(url, params=params).json()
        games = []

        for event in data.get('events', []):
            comp = event['competitions'][0]
            dt_cet = pd.to_datetime(event.get('date')).tz_convert('Europe/Vienna')

            # Date filter for college leagues (API sometimes returns extra dates)
            if league_slug in ('mens-college-basketball', 'college-football'):
                if dt_cet.strftime('%Y%m%d') != today:
                    continue

            home = next(c for c in comp['competitors'] if c['homeAway'] == 'home')
            away = next(c for c in comp['competitors'] if c['homeAway'] == 'away')

            # Check if game is a playoff/finals game
            notes = comp.get('notes', [])
            is_playoff = any(
                'playoff' in str(n.get('headline', '')).lower() or
                'final' in str(n.get('headline', '')).lower()
                for n in notes
            )

            games.append({
                'Away Team':      away['team']['displayName'],
                'Away Record':    get_record(away),
                'Home Team':      home['team']['displayName'],
                'Home Record':    get_record(home),
                'Date':           dt_cet.strftime('%Y-%m-%d'),
                'Time (CET)':     dt_cet.strftime('%H:%M'),
                'End Time (CET)': calculate_end_time(dt_cet, league_name),
                'League':         league_name,
                'Is Playoff':     is_playoff,
                'Watched':        0,
            })

        return pd.DataFrame(games)

    except Exception as e:
        print(f"Error fetching {league_name}: {e}")
        return pd.DataFrame()


def fetch_all_leagues():
    """Fetch today's games for all configured leagues."""
    dfs = [fetch_league(sport, slug, name) for sport, slug, name in LEAGUES]
    return pd.concat(dfs, ignore_index=True)

@task(retries=3, retry_delay_seconds=60)

def scrape_task():
    print("🛰️  Scraping ESPN for today's games...")
    return fetch_all_leagues()

@task
def save_to_duckdb_task(df):
    print("💾 Saving raw data to DuckDB...")
    con = duckdb.connect('sports.duckdb')
    con.execute("CREATE OR REPLACE TABLE raw_games AS SELECT * FROM df")
    con.close()

@task
def dbt_transform_task():
    print("🚀 Triggering dbt transformation...")

    project_dir = "/app/sports_transform"
    profiles_dir = "/app"
    try:
        result = subprocess.run(
            ["dbt", "run","--profiles-dir", profiles_dir],
            cwd=project_dir, 
            check=True,
            capture_output=True,
            text=True
        )
        print(result.stdout) 
    except subprocess.CalledProcessError as e:
        print(f"❌ dbt failed with return code {e.returncode}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        raise e

load_dotenv()  # Load environment variables from .env file

@task
def update_history_and_display_task():


    print("\n🏆 TOP 10 GAMES FOR TODAY 🏆")
    print("="*60)
    
    con = duckdb.connect('sports.duckdb')
    con.execute("""
        INSERT INTO watched_history (event_date, league, matchup, score, time, watched)
        SELECT 
            CAST(CURRENT_DATE AS DATE),
            "League",
            "Away Team" || ' @ ' || "Home Team",
            total_watch_score,
            "Time (CET)",
            0
        FROM fct_daily_schedule
        ON CONFLICT (event_date, matchup) DO NOTHING
    """)

    # 2. Fetch the Top 10 for the terminal display
    df = con.execute("""
        SELECT 
            CAST(total_watch_score AS INT) as score,
            tags,
            CAST(CURRENT_DATE AS DATE) as date,         
            "League" as league,
            "Away Team" || ' @ ' || "Home Team" as matchup,
            "Time (CET)" as time
        FROM fct_daily_schedule
        ORDER BY total_watch_score DESC
        LIMIT 10
    """).df()
    con.close()
    
    # Print the result to your terminal
    print(df.to_string(index=False))
    print("="*60)

    #send to discord
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")

    if not webhook_url:
        print("No Discord Webhook URL found in .env file!")
        return

    # Format a nice message for your phone
    discord_msg = "🏁 **DAILY TOP PICKS** 🏁\n"
    for _, row in df.head(10).iterrows(): 
        star = "⭐" if "Favorite" in str(row['tags']) else "🔹"
        discord_msg += f"{star} **{row['score']} pts** | {row['league']}\n"
        discord_msg += f"> {row['matchup']} at {row['time']}\n\n"

    try:
        requests.post(webhook_url, json={"content": discord_msg})
        print("📲 Discord notification sent!")
    except Exception as e:
        print(f"❌ Failed to send Discord notification: {e}")

    return df

@task
def notify_dashboard_sync():
    # Wir berühren einfach den Zeitstempel der DB-Datei.
    # Da Streamlit os.path.getmtime prüft, erkennt es die Änderung sofort.
    db_path = 'sports.duckdb'
    if os.path.exists(db_path):
        os.utime(db_path, None)
        print("🔔 Dashboard notified: New dbt data detected.")

@flow(name="Sports Data Pipeline", log_prints=True)
def sports_flow():
    # Execute the tasks in order
    raw_df = scrape_task()
    save_to_duckdb_task(raw_df)
    dbt_transform_task()
    update_history_and_display_task()
    notify_dashboard_sync()

if __name__ == "__main__":
    sports_flow.serve(
        name="daily-11am-sync",
        cron="0 9 * * *",
        tags=["graz-home-server"]
    )

#new test
#if __name__ == "__main__":
    #print("Test Run")  This will run the flow immediately for testing purposes. Uncomment to use.
    #sports_flow()

   # print("Scheduling daily runs at 09:00")
   # schedule.every().day.at("09:00").do(sports_flow)

    #(print("Pipeline scheduled. Waiting for next run..."))
    #while True:
     #   schedule.run_pending()
      #  time.sleep(60)