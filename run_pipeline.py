import os
from dotenv import load_dotenv
from prefect import flow, task
import requests
from datetime import datetime
import pandas as pd
import subprocess
import schedule, time
from sqlalchemy import create_engine, text
import pytz
import sys
import time

# ── Leagues to fetch 
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
    'FIFA World Cup Qualifiers - CAF': 2.0, 'J1 League': 2.0, 'Copa Libertadores': 2.0, 'International Friendlies': 2.0,
    'Sumo': 2.5, 'Boxing': 3, 'EuroLeague': 2.5, 'ABA': 2.5, 'F1': 3, 'UFC': 3
}


def get_record(competitor):
    """Extract win-loss record from a competitor object."""
    records = competitor.get('records', [])
    return records[0].get('summary', '0-0') if records else 'N/A'


def calculate_end_time(dt, league):
    """Calculate estimated end time based on league duration."""
    hours = DURATION_MAP.get(league, 2.5)
    return (dt + pd.Timedelta(hours=hours)).strftime('%H:%M')


def fetch_league(sport, league_slug, league_name, date=None):
    """Fetch today's games for a single league from ESPN API."""
    tz = pytz.timezone('Europe/Vienna')
    today = datetime.now(tz).strftime('%Y%m%d')
    url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league_slug}/scoreboard"
    params = {'dates': today}

    # limit number of games for college leagues to avoid API overload (they often return too many games)
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
            headline = str(notes[0].get('headline', '')).lower() if notes else ""
            is_playoff = any(
                keyword in str(n.get('headline', '')).lower() 
                for n in notes
                for keyword in ['playoff', 'postseason', 'championship', 'final four', 'world series']
            )

            if not is_playoff and league_name in ['NBA', 'NHL']:
                if 'game' in headline:
                    is_playoff = True
            
            if league_name == 'MLB':
                is_playoff = any(k in headline for k in ['world series', 'postseason'])

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

# load environment variables from .env file
load_dotenv()
DB_URL = os.getenv("SUPABASE_DB_URL")
engine = create_engine(DB_URL)

def generic_save_to_supabase(df, table_name):
    """Saves data in Supabase"""
    if df is not None and not df.empty:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"{len(df)} rows saved in {table_name}.")
    else:
        print(f"No data for{table_name}, skip saving.")


@task(name="Save Basketball to Supabase")
def save_basketball_to_supabase(df):
    generic_save_to_supabase(df, "raw_eurobasket")

@task(name="Save Boxing to Supabase")
def save_boxing_to_supabase(df):
    generic_save_to_supabase(df, "raw_boxing")

@task(name="Save NPB to Supabase")
def save_npb_to_supabase(df):
    generic_save_to_supabase(df, "raw_npb")

@task
def save_f1_to_supabase(df):
    generic_save_to_supabase(df, "raw_f1")

@task
def save_ufc_to_supabase(df):
    generic_save_to_supabase(df, "raw_ufc")

@task
def save_to_supabase_task(df):
    generic_save_to_supabase(df, "raw_games")

#fetch other leagues which can not be easily scraped via ESPN API (e.g. NPB, Boxing, Basketball) using SerpApi or other APIs
@task(retries=3)
def fetch_basketball_task():
    """Get EuroLeague and ABA data via SerpApi."""
    print("🏀 Fetching Basketball (EuroLeague & ABA) via SerpApi...")
    api_key = os.getenv("SERPAPI_KEY")  
    
    queries = ["EuroLeague schedule today", "ABA Liga schedule today", "EuroLeague playoffs 2026 today",
               "ABA Liga playoffs 2026 today", "EuroLeague game today score", "ABA Liga game today score"]
    all_baskets = []
    
    for q in queries:
        params = {"engine": "google", "q": q, "api_key": api_key, "hl": "en"}
        try:
            res = requests.get("https://serpapi.com/search", params=params).json()
            games = res.get("sports_results", {}).get("games", [])
            league_name = "EuroLeague" if "EuroLeague" in q else "ABA"
            
            for g in games:
                all_baskets.append({
                    'League': league_name,
                    'Away Team': g['teams'][0].get('name'),
                    'Home Team': g['teams'][1].get('name'),
                    'Away Record': g['teams'][0].get('record', '0-0'),
                    'Home Record': g['teams'][1].get('record', '0-0'),
                    'Date': datetime.now().strftime('%Y-%m-%d'),
                    'Time (CET)': '20:00', # Meist Abendspiele
                    'End Time (CET)': '22:00',
                    'Is Playoff': False
                })
        except Exception as e:
            print(f"Error fetching {q}: {e}")
            
    return pd.DataFrame(all_baskets)

@task(retries=3)
def fetch_boxing_task():
    """Fetches only relevant boxing fights (title bouts or favorites)."""
    print("🥊 Fetching curated Boxing matches via SerpApi...")
    api_key = os.getenv("SERPAPI_KEY")
    
    # Add favorite boxers to filter the results (you can customize this list)
    my_boxers = [
    'Canelo', 'Usyk', 'Fury', 'Joshua', 'Inoue', 'Crawford', 
    'Davis', 'Haney', 'Bivol', 'Garcia', 'Loma', 'Wilder',
    'Beterbiev', 'Bam Rodriguez', 'Shakur Stevenson', 'Nakatani', 
    'Teofimo Lopez', 'Pitbull Cruz', 'Boots Ennis', 'Vergil Ortiz', 
    'Zhilei Zhang', 'Beterbiev', 'Fundora', 'Mbilli'
    ]
    
    params = {
        "engine": "google",
        "q": "major boxing fights today", 
        "api_key": api_key,
        "hl": "en"
    }
    
    try:
        res = requests.get("https://serpapi.com/search", params=params, timeout=15).json()
        all_matches = res.get('sports_results', {}).get('games', [])
        
        filtered_fights = []
        for match in all_matches:
            # Extract names and event notes for filtering
            name_str = " ".join([t.get('name', '') for t in match.get('teams', [])]).lower()
            event_note = match.get('status', '').lower() + match.get('league', '').lower()

            # 1. Is it a fight involving one of my favorite boxers?
            is_favorite = any(boxer.lower() in name_str for boxer in my_boxers)
            
            # 2. Is it a title bout? (Searching for 'Title', 'WBC', 'WBA', 'IBF', 'WBO')
            is_title_bout = any(word in event_note or word in name_str 
                                for word in ['title', 'wbc', 'wba', 'ibf', 'wbo', 'undisputed', 'world'])

            # 3. Only include if it's either a favorite fight or a title bout
            if is_favorite or is_title_bout:
                filtered_fights.append({
                    'League': 'Boxing',
                    'Away Team': match['teams'][0].get('name'),
                    'Home Team': match['teams'][1].get('name'),
                    'Away Record': 'Favorite' if is_favorite else 'Title Fight',
                    'Home Record': match.get('venue', 'Pro Boxing'), 
                    'Date': datetime.now().strftime('%Y-%m-%d'),
                    'Time (CET)': '22:00',
                    'End Time (CET)': '01:00',
                    'Is Playoff': is_title_bout # We use Is Playoff for "Title Fight" marking
                })
        
        df = pd.DataFrame(filtered_fights)
        print(f"✅ {len(df)} found relevant boxing fights.")
        return df
    except Exception as e:
        print(f"❌ Boxing Filter Error: {e}")
        return pd.DataFrame()

def fetch_npb_task():
    """Fetch real NPB matchups via SerpApi."""
    print("🛰️ Fetching live NPB matchups via SerpApi...")
    
    api_key = os.getenv("SERPAPI_KEY")
    params = {
        "engine": "google",
        "q": "NPB schedule today",
        "api_key": api_key,
        "hl": "en",
        "gl": "us"
    }
    
    try:
        response = requests.get("https://serpapi.com/search", params=params, timeout=15)
        data = response.json()
        
        # SerpApi extracts sports results in 'sports_results'
        sports_results = data.get("sports_results", {})
        games_list = sports_results.get("games", [])
        
        # If google does not return a 'games' list  
        # we need to check if there is a 'game_spotlight'
        if not games_list and "game_spotlight" in sports_results:
            games_list = [sports_results["game_spotlight"]]

        bouts = []
        for game in games_list:
            teams = game.get("teams", [])
            if len(teams) >= 2:
                bouts.append({
                    'League': 'NPB',
                    'Away Team': teams[0].get("name"),
                    'Home Team': teams[1].get("name"),
                    'Away Record': teams[0].get("record", "0-0"),
                    'Home Record': teams[1].get("record", "0-0"),
                    'Date': datetime.now().strftime('%Y-%m-%d'),
                    'Time (CET)': '11:00', # standard time for NPB games
                    'End Time (CET)': '14:30',
                    'Is Playoff': False
                })
        
        if not bouts:
            print("No NPB matchups found for today.")
            return pd.DataFrame()
            
        df = pd.DataFrame(bouts)
        print(f"✅ {len(df)} NPB matchups successfully fetched.")
        return df

    except Exception as e:
        print(f"❌ SerpApi Error: {e}")
        return pd.DataFrame()
    
@task(retries=3, retry_delay_seconds=60)
def fetch_f1_task():
    """Fetch today's F1 sessions (FP1-3, Qualy, Sprint, Race) from f1api.dev."""
    print("Fetching F1 schedule from f1api.dev...")

    vienna_tz = pytz.timezone('Europe/Vienna')
    now = datetime.now(vienna_tz)
    today_str = now.strftime('%Y-%m-%d')
    url = "https://f1api.dev/api/current"
 
    # session key in the API response -> human-readable label
    session_types = [
        ('fp1', 'Free Practice 1'),
        ('fp2', 'Free Practice 2'),
        ('fp3', 'Free Practice 3'),
        ('sprintQualy', 'Sprint Qualifying'),
        ('sprintRace', 'Sprint Race'),
        ('qualy', 'Qualifying'),
        ('race', 'Race'),
    ]
 
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()

        f1_events = []
        for race in data.get('races', []):
            schedule = race.get('schedule', {}) or {}
            race_name = race.get('raceName', 'Unknown Grand Prix')
            circuit = (race.get('circuit') or {}).get('circuitName', 'Unknown Circuit')
 
            for key, label in session_types:
                session = schedule.get(key)
                if not session or session.get('date') != today_str:
                    continue
 
                time_str = session.get('time')
                if time_str:
                    # API times are UTC (e.g. "13:30:00Z") -> convert to CET/CEST
                    dt_utc = pd.to_datetime(f"{session['date']}T{time_str}", utc=True)
                    time_cet = dt_utc.tz_convert(vienna_tz).strftime('%H:%M')
                else:
                    time_cet = '00:00'
 
                f1_events.append({
                    'Event_Name': f"{race_name} – {label}",
                    'Circuit': circuit,
                    'Date': today_str,
                    'Time_CET': time_cet,
                    'League': 'F1'
                })
        return pd.DataFrame(f1_events)
    except Exception as e:
        print(f"Error fetching F1 schedule from f1api.dev: {e}")
        return pd.DataFrame()

@task(retries=3, retry_delay_seconds=60)
def fetch_ufc_task():

    vienna_tz = pytz.timezone('Europe/Vienna')
    today_str = datetime.now(vienna_tz).strftime('%Y%m%d')
    today_dash = datetime.now(vienna_tz).strftime('%Y-%m-%d')

    print(f"Fetching UFC events for {today_str}")
    url = f"https://site.api.espn.com/apis/site/v2/sports/mma/ufc/scoreboard?dates={today_str}"


    try:
        response = requests.get(url)
        data = response.json()
        all_fights = []
        
        for event in data.get('events', []):
            for comp in event.get('competitions', []):
                dt_cet = pd.to_datetime(event.get('date')).tz_convert('Europe/Vienna')
                if dt_cet.strftime('%Y%m%d') != today_str:
                    continue
                
                fighters = [c.get('athlete', {}).get('displayName') for c in comp.get('competitors', [])]
                
                if len(fighters) >= 2:
                    all_fights.append({
                        'Fighter_A': fighters[0],
                        'Fighter_B': fighters[1],
                        'Event_Name': event.get('name'),
                        'Date': today_dash,
                        'Time_CET': dt_cet.strftime('%H:%M'),
                        'League': 'UFC'
                    })
        
        return pd.DataFrame(all_fights)
    except Exception as e:
        print(f"Error scraping UFC data {e}")
        return pd.DataFrame()

def fetch_all_leagues():
    """Fetch today's games for all configured leagues."""

    vienna_tz = pytz.timezone('Europe/Vienna')
    today_vienna = datetime.now(vienna_tz).strftime('%Y%m%d')
    dfs = [fetch_league(sport, slug, name, date=today_vienna) for sport, slug, name in LEAGUES]
    return pd.concat(dfs, ignore_index=True)

@task(retries=3, retry_delay_seconds=60)

def scrape_task():
    print("🛰️  Scraping ESPN for today's games...")
    return fetch_all_leagues()

@task
def dbt_transform_task():
    print("🚀 Triggering dbt transformation...")

    try:
        result = subprocess.run(
            ['dbt', 'run', '--project-dir', '/app/sports_transform', '--profiles-dir', '/app/sports_transform'],
            capture_output=True,
            text=True
        )
        print(result.stdout) 
    except subprocess.CalledProcessError as e:
        print(f"dbt failed with return code {e.returncode}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        raise e


@task(name="Auto-Seed All Athletes & Teams")
def auto_seed_teams_task():
    print("Running Full Auto-Seed for Teams and Athletes (with League Context)...")
    seed_sql = text("""
        INSERT INTO public.dim_teams (team_name, league_name)
        SELECT DISTINCT name, league
        FROM (
            -- Usual teams from raw_games (ESPN)
            SELECT "Home Team" AS name, "League" AS league FROM public.raw_games UNION
            SELECT "Away Team", "League" FROM public.raw_games UNION
            
            -- NPB (Japan Baseball)
            SELECT "Home Team", 'NPB' FROM public.raw_npb UNION
            SELECT "Away Team", 'NPB' FROM public.raw_npb UNION
            
            -- Eurobasket
            SELECT "Home Team" AS name, "League" AS league FROM public.raw_eurobasket UNION
            SELECT "Away Team", "League" FROM public.raw_eurobasket UNION
            
            -- Boxing
            SELECT "Home Team", 'Boxing' FROM public.raw_boxing UNION
            SELECT "Away Team", 'Boxing' FROM public.raw_boxing UNION
            
            -- UFC 
            SELECT "Fighter_A", 'UFC' FROM public.raw_ufc UNION
            SELECT "Fighter_B", 'UFC' FROM public.raw_ufc
        ) AS all_competitors
        WHERE name IS NOT NULL 
          AND NOT EXISTS (
            SELECT 1 FROM public.dim_teams dt 
            WHERE dt.team_name = all_competitors.name
              AND dt.league_name = all_competitors.league
        )
        ON CONFLICT (team_name, league_name) DO NOTHING;
    """)
    with engine.begin() as conn:
        conn.execute(seed_sql)

@task
def update_history_and_display_task():
    vienna_tz = pytz.timezone('Europe/Vienna')
    today_vienna = datetime.now(vienna_tz).strftime('%Y-%m-%d')
    
    print("\n🏆 UPDATING HISTORY & NOTIFYING DISCORD 🏆")
    
    with engine.begin() as conn:
        conn.execute(text("""
            WITH unique_source AS (
                -- Added time to DISTINCT ON
                SELECT DISTINCT ON ("Date"::date, "Time (CET)"::time, home_team_id_new, away_team_id_new)
                    "Date"::date as clean_date,
                    "League" as clean_league,
                    "Away Team" || ' @ ' || "Home Team" as clean_matchup,
                    league_id_new,            
                    home_team_id_new, 
                    away_team_id_new,
                    total_watch_score,
                    "Time (CET)"::time as clean_time
                FROM public.fct_daily_schedule
                WHERE home_team_id_new IS NOT NULL 
                  AND away_team_id_new IS NOT NULL
                -- Added time to ORDER BY
                ORDER BY "Date"::date, "Time (CET)"::time, home_team_id_new, away_team_id_new, total_watch_score DESC
            )
            INSERT INTO public.watch_history (
                date, league, matchup, league_id_new, 
                home_team_id_new, away_team_id_new, 
                score, time, watched
            )
            SELECT 
                clean_date, clean_league, clean_matchup, league_id_new,
                home_team_id_new, away_team_id_new, 
                total_watch_score, clean_time, FALSE
            FROM unique_source
            -- Added time to ON CONFLICT
            ON CONFLICT (date, time, home_team_id_new, away_team_id_new)
            DO UPDATE SET
                score = EXCLUDED.score,
                league = EXCLUDED.league,
                matchup = EXCLUDED.matchup,
                league_id_new = EXCLUDED.league_id_new;
        """))

        # Top 10 for logs
        query = f"""
            SELECT 
                score,
                league,
                matchup,
                time
            FROM watch_history
            WHERE date = '{today_vienna}'
            ORDER BY score DESC
            LIMIT 10
        """
        df = pd.read_sql(query, conn)

    print(df.to_string(index=False))
    print("="*60)

    # Discord Notification
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    dashboard_url = "https://game-of-the-day-nqjhazo6peteukrajbpkhw.streamlit.app/"

    if webhook_url:
        discord_msg = (
            "Check the details on your games dashboard:\n"
            f"👉 {dashboard_url}"
        )
        try:
            requests.post(webhook_url, json={"content": discord_msg})
            print("📲 Discord notification sent!")
        except Exception as e:
            print(f"❌ Failed to send Discord notification: {e}")

    return df
    
@flow(name="Sports Data Pipeline", log_prints=True)
def sports_flow():
    #scraping
    raw_df = scrape_task()
    ufc_df = fetch_ufc_task() 
    f1_df = fetch_f1_task()
    npb_df = fetch_npb_task()
    eurobasket_df = fetch_basketball_task()
    boxing_df = fetch_boxing_task()
    #saving  
    save_raw = save_to_supabase_task(raw_df)
    save_ufc = save_ufc_to_supabase(ufc_df)
    save_f1 = save_f1_to_supabase(f1_df)
    save_npb = save_npb_to_supabase(npb_df)
    save_basketball = save_basketball_to_supabase(eurobasket_df)
    save_boxing = save_boxing_to_supabase(boxing_df)


    seed = auto_seed_teams_task(wait_for=[
        save_raw, save_ufc, save_f1, 
        save_npb, save_basketball, save_boxing
    ])  

    print("Pausing 20 seconds to let all raw data and teams settle in Supabase...")
    time.sleep(20)
    #transformation & display
    dbt = dbt_transform_task(wait_for=[seed])
    update_history_and_display_task(wait_for=[dbt])

if __name__ == "__main__":
    # Oneshot mode for local testing: `python run_pipeline.py --oneshot`
    if "--oneshot" in sys.argv:
        print("🚀 Running in One-Shot mode...")
        sports_flow()
    else:
        # Scheduler mode: Run every day at 12:30 CET
        print("⏰ Running in Scheduler mode (Local)...")
        schedule.every().day.at("12:15").do(sports_flow)
        while True:
            schedule.run_pending()
            time.sleep(1)
