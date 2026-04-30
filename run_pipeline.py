import os
from dotenv import load_dotenv
from prefect import flow, task
import duckdb
import requests
from datetime import datetime
import pandas as pd
import subprocess
import schedule, time
from sqlalchemy import create_engine, text
import pytz
import sys

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

@task(name="Save Sumo to Supabase")
def save_sumo_to_supabase(df):
    generic_save_to_supabase(df, "raw_sumo")

@task
def save_f1_to_supabase(df):
    generic_save_to_supabase(df, "raw_f1")

@task
def save_ufc_to_supabase(df):
    generic_save_to_supabase(df, "raw_ufc")

@task
def save_to_supabase_task(df):
    generic_save_to_supabase(df, "raw_games")

#fetch other leagues which can not be easily scraped via ESPN API (e.g. NPB, Sumo, Boxing, Basketball) using SerpApi or other APIs
@task(retries=3)
def fetch_basketball_task():
    """Get EuroLeague and ABA data via SerpApi."""
    print("🏀 Fetching Basketball (EuroLeague & ABA) via SerpApi...")
    api_key = os.getenv("SERPAPI_KEY")  
    
    queries = ["EuroLeague schedule today", "ABA Liga schedule today"]
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
    
    # Hier deine Favoriten eintragen (Teile des Namens reichen)
    my_boxers = [
        'Canelo', 'Usyk', 'Fury', 'Joshua', 'Inoue', 'Crawford', 
        'Davis', 'Haney', 'Bivol', 'Garcia', 'Loma', 'Wilder'
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
    
#def generic_save_to_duckdb(df, table_name):
 #   con = duckdb.connect('sports.duckdb')
  #  if df is None or df.empty:
   #     con.execute(f"""
    #        CREATE TABLE IF NOT EXISTS {table_name} (
     #           "League" VARCHAR,
      #          "Away Team" VARCHAR,
       #         "Home Team" VARCHAR, 
        #        "Away Record" VARCHAR,
         #       "Home Record" VARCHAR, 
          #      Date VARCHAR,
           #     "Time (CET)" VARCHAR,
            #    "End Time (CET)" VARCHAR, 
             #   "Is Playoff" BOOLEAN
            #)
        #""")
    #else:
     #   con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
    #con.close()

#@task(name="Save Basketball to DuckDB")
#def save_basketball_to_duckdb(df):
 #   generic_save_to_duckdb(df, "raw_eurobasket")

#@task(name="Save Boxing to DuckDB")
#def save_boxing_to_duckdb(df):
 #   generic_save_to_duckdb(df, "raw_boxing")

#@task(retries=3, retry_delay_seconds=60)
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

#@task(name="Save NPB to DuckDB")
#def save_npb_to_duckdb(df):
 #   """Save NPB games in DuckDB table 'raw_npb'."""
  #  db_path = 'sports.duckdb'
   # con = duckdb.connect(db_path)
    
    #try:
     #   if df is None or df.empty:
      #      print("No NPB data found. Initialize empty table for dbt...")
       #     con.execute("""
        #        CREATE TABLE IF NOT EXISTS raw_npb (
         #           "League" VARCHAR,
          #          "Away Team" VARCHAR,
           #         "Home Team" VARCHAR,
            #        "Away Record" VARCHAR,
             #       "Home Record" VARCHAR,
              #      "Date" VARCHAR,
               #     "Time (CET)" VARCHAR,
                #    "End Time (CET)" VARCHAR,
                 #   "Is Playoff" BOOLEAN
                #)
            #""")
        #else:
            # Saves real data and overwrites old table
         #   con.execute("CREATE OR REPLACE TABLE raw_npb AS SELECT * FROM df")
          #  count = con.execute("SELECT count(*) FROM raw_npb").fetchone()[0]
           # print(f"Success: {count} NPB-Matchups saved in DuckDB.")
            
    #except Exception as e:
     #   print(f"Error saving NPB games in DuckDB: {e}")
    #finally:
     #   con.close()

@task(retries=2, retry_delay_seconds=60)
def fetch_sumo_task():
    """
    Fetches sumo bouts of the top division (Makuuchi) from Sumo-API.com.
    Automatically calculates the current basho based on month and year.
    """
    print("🎎 Assessing Sumo schedule...")
    now = datetime.now()
    year = now.year
    month = now.month

    # Sumo bashos are only held in odd months: Jan, Mar, May, Jul, Sep, Nov
    if month % 2 == 0:
        print(f"🏮 {now.strftime('%B')} is not a sumo month (Tournaments: Jan, Mar, May, Jul, Sep, Nov)")
        return pd.DataFrame()

    # Construct Basho ID (e.g. 202603 for march 2026)
    basho_id = f"{year}{month:02d}"
    
    try:
        
        basho_info_res = requests.get(f"https://www.sumo-api.com/api/basho/{basho_id}")
        if basho_info_res.status_code != 200:
            return pd.DataFrame()
            
        start_date_str = basho_info_res.json().get('startDate') 
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        
        
        day_diff = (now - start_date).days + 1
        
        if not (1 <= day_diff <= 15):
            print(f"⏳ Basho {basho_id} geplant, aber heute ist nicht Turniertag 1-15 (Tag: {day_diff}).")
            return pd.DataFrame()

        print(f"⭐ Fetching Bouts for Basho {basho_id}, Day {day_diff}...")
        response = requests.get(f"https://www.sumo-api.com/api/basho/{basho_id}/bouts/{day_diff}")
        
        if response.status_code != 200:
            return pd.DataFrame()

        data = response.json()
        bouts = []
        
        for bout in data.get('bouts', []):
            if bout.get('division') == 'Makuuchi':
                bouts.append({
                    'League': 'Sumo',
                    'Away Team': bout.get('eastRikishiName'),
                    'Home Team': bout.get('westRikishiName'),
                    'Away Record': '0-0',
                    'Home Record': '0-0',
                    'Date': now.strftime('%Y-%m-%d'),
                    'Time (CET)': '08:30', # Standard time for Makuuchi Bouts
                    'End Time (CET)': '11:00',
                    'Is Playoff': False
                })
        
        df = pd.DataFrame(bouts)
        print(f"✅ {len(df)} Sumo bouts loaded.")
        return df

    except Exception as e:
        print(f"❌ Sumo-API Error: {e}")
        return pd.DataFrame()
    
@task(retries=3, retry_delay_seconds=60)
def fetch_f1_task():
    """Scrape today's F1 events from the ESPN-API."""
    print("Scraping F1 events...")

    vienna_tz = pytz.timezone('Europe/Vienna')
    now = datetime.now(vienna_tz)
    today_str = now.strftime('%Y-%m-%d')
    api_date = now.strftime('%Y%m%d')
    url = f"http://site.api.espn.com/apis/site/v2/sports/racing/f1/scoreboard?dates={api_date}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        f1_events = []
        
        for event in data.get('events', []):
            dt_cet = pd.to_datetime(event.get('date')).tz_convert('Europe/Vienna')
            event_date = dt_cet.strftime('%Y-%m-%d')

            if event_date != today_str:
                continue
            
            f1_events.append({
                'Event_Name': event.get('name'),
                'Circuit': event.get('venue', {}).get('fullName', 'Unknown Circuit'),
                'Date': dt_cet.strftime('%Y-%m-%d'),
                'Time_CET': dt_cet.strftime('%H:%M'),
                'League': 'F1'
            })
        
        return pd.DataFrame(f1_events)
    except Exception as e:
        print(f"Error Scraping F1: {e}")
        return pd.DataFrame()

#@task(name="Save Sumo to DuckDB")
#def save_sumo_to_duckdb(df):
 #   db_path = 'sports.duckdb'
  #  con = duckdb.connect(db_path)
    
   # if df is None or df.empty:
    #    print("No sumo month. Initialize empty table for dbt...")

     #   con.execute("""
      #      CREATE TABLE IF NOT EXISTS raw_sumo (
       #         "League" VARCHAR,
        #        "Away Team" VARCHAR,
         #       "Home Team" VARCHAR,
          #      "Away Record" VARCHAR,
           #     "Home Record" VARCHAR,
            #    "Date" VARCHAR,
             #   "Time (CET)" VARCHAR,
              #  "End Time (CET)" VARCHAR,
               # "Is Playoff" BOOLEAN
            #)
        #""")
    #else:
     #   con.execute("CREATE OR REPLACE TABLE raw_sumo AS SELECT * FROM df")
      #  print(f"{len(df)} Sumo Bashos saved in DuckDB.")
    
    #con.close()
#@task
#def save_f1_to_duckdb(df):
 #   if df.empty:
  #      print("No F1 events found for today. Skipping DuckDB save.")
   #     return
   # con = duckdb.connect('sports.duckdb')
    #con.execute("CREATE OR REPLACE TABLE raw_f1 AS SELECT * FROM df")
    #con.close()

@task(retries=3, retry_delay_seconds=60)
def fetch_ufc_task():

    vienna_tz = pytz.timezone('Europe/Vienna')
    today_str = datetime.now(vienna_tz).strftime('%Y%m%d')
    today_dash = datetime.now(vienna_tz).strftime('%Y-%m-%d')

    print(f"Fetching UFC events for {today_str}")
    url = f"http://site.api.espn.com/apis/site/v2/sports/mma/ufc/scoreboard?dates={today_str}"


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

#@task
#def save_ufc_to_duckdb(df):
 #   if df.empty:
  #      print("No UFC fights found for today. Skipping DuckDB save.")
   #     return
    #print(f"💾 Save {len(df)} UFC fights in DuckDB...")
    #con = duckdb.connect('sports.duckdb')
    #con.execute("CREATE OR REPLACE TABLE raw_ufc AS SELECT * FROM df")
    #con.close()

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

#@task
#def save_to_duckdb_task(df):
 #   print("💾 Saving raw data to DuckDB...")
  #  con = duckdb.connect('sports.duckdb')
   # con.execute("CREATE OR REPLACE TABLE raw_games AS SELECT * FROM df")
    #con.close()

@task
def dbt_transform_task():
    print("🚀 Triggering dbt transformation...")

    #project_dir = "sports_transform"
    #profiles_dir = ".."
    try:
        result = subprocess.run(
            ['dbt', 'run', '--project-dir', '/app/sports_transform', '--profiles-dir', '/app/sports_transform'],
            #cwd=project_dir, 
            #check=True,
            capture_output=True,
            text=True
        )
        print(result.stdout) 
    except subprocess.CalledProcessError as e:
        print(f"dbt failed with return code {e.returncode}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        raise e

@task
def update_history_and_display_task():
    vienna_tz = pytz.timezone('Europe/Vienna')
    today_vienna = datetime.now(vienna_tz).strftime('%Y-%m-%d')
    
    print("\n🏆 UPDATING HISTORY & NOTIFYING DISCORD 🏆")
    
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO public.watch_history (
                date, 
                league,
                matchup,
                league_id_new, 
                home_team_id_new, 
                away_team_id_new, 
                score, 
                time, 
                watched
            )
            SELECT DISTINCT ON ("Date"::date, home_team_id_new, away_team_id_new)
                "Date"::date as d,
                "League",
                "Away Team" || ' @ ' || "Home Team" as matchup,
                league_id_new,            
                home_team_id_new, 
                away_team_id_new,
                total_watch_score,
                "Time (CET)"::time,
                FALSE
            FROM public.fct_daily_schedule
            ORDER BY d, home_team_id_new, away_team_id_new, total_watch_score DESC
            ON CONFLICT (date, home_team_id_new, away_team_id_new)
            DO UPDATE SET
                league = EXCLUDED.league,
                matchup = EXCLUDED.matchup,
                league_id_new = EXCLUDED.league_id_new,
                home_team_id_new = EXCLUDED.home_team_id_new,
                away_team_id_new = EXCLUDED.away_team_id_new,
                score = EXCLUDED.score;
        """))

        # 2. Wir ziehen die Top 10 trotzdem kurz für die Terminal-Logs (gut zum Debuggen bei GitHub)
        query = f"""
            SELECT 
                total_watch_score::INT as score,
                "League" as league,
                "Away Team" || ' @ ' || "Home Team" as matchup,
                "Time (CET)" as time
            FROM fct_daily_schedule
            WHERE "Date" = '{today_vienna}'
            ORDER BY total_watch_score DESC
            LIMIT 10
        """
        df = pd.read_sql(query, conn)

    # Das erscheint nur in deinen GitHub/Terminal Logs
    print(df.to_string(index=False))
    print("="*60)

    # 3. Neue, schlanke Discord Benachrichtigung
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    dashboard_url = "https://game-of-the-day-nqjhazo6peteukrajbpkhw.streamlit.app/"

    if not webhook_url:
        print("No Discord Webhook URL found!")
        return

    # Nur noch ein kurzer Teaser mit Link
    discord_msg = (
        "🚀 **Der neue Spielplan ist live!**\n\n"
        f"Heute warten **{len(df)} Top-Spiele** auf dich.\n"
        "Schau dir die Details im Dashboard an:\n"
        f"👉 {dashboard_url}"
    )

    try:
        requests.post(webhook_url, json={"content": discord_msg})
        print("📲 Discord notification sent!")
    except Exception as e:
        print(f"❌ Failed to send Discord notification: {e}")

    return df

#@task
#def notify_dashboard_sync():
 #   db_path = 'sports.duckdb'
  #  if os.path.exists(db_path):
   #     os.utime(db_path, None)
    #    print("Dashboard notified: New dbt data detected.")

@flow(name="Sports Data Pipeline", log_prints=True)
def sports_flow():
    #scraping
    raw_df = scrape_task()
    ufc_df = fetch_ufc_task() 
    f1_df = fetch_f1_task()
    sumo_df = fetch_sumo_task()
    npb_df = fetch_npb_task()
    eurobasket_df = fetch_basketball_task()
    boxing_df = fetch_boxing_task()
    #saving  
    save_to_supabase_task(raw_df)
    save_ufc_to_supabase(ufc_df)
    save_f1_to_supabase(f1_df)
    save_sumo_to_supabase(sumo_df)
    save_npb_to_supabase(npb_df)
    save_basketball_to_supabase(eurobasket_df)
    save_boxing_to_supabase(boxing_df)
    #transformation & display
    dbt_transform_task()
    update_history_and_display_task()
    #notify_dashboard_sync()

if __name__ == "__main__":
    # Wenn "oneshot" als Argument übergeben wird, läuft es nur einmal (für GitHub)
    if "--oneshot" in sys.argv:
        print("🚀 Running in One-Shot mode...")
        sports_flow()
    else:
        # Dein bisheriger lokaler Scheduler für das MacBook
        print("⏰ Running in Scheduler mode (Local)...")
        schedule.every().day.at("11:00").do(sports_flow)
        while True:
            schedule.run_pending()
            time.sleep(1)