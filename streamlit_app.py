import streamlit as st
import pandas as pd
import feedparser
from sqlalchemy import create_engine
import os
from datetime import datetime, time as dtime

# congiguration
st.set_page_config(page_title="Sports Watcher Dashboard", layout="wide")
st.title("🏆 Sports Watcher Dashboard")

# load data from supabase
@st.cache_data(ttl=300)
def load_data():
    if "SUPABASE_DB_URL" not in st.secrets:
        st.error("Add SUPABASE_DB_URL in Streamlit Secrets!")
        return pd.DataFrame()

    db_url = st.secrets["SUPABASE_DB_URL"]
    engine = create_engine(db_url)

    try:
        query = "SELECT * FROM v_dashboard_top_picks"
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()


# The dashboard view (v_dashboard_top_picks) only exposes a start "Time" column,
# no end time. We estimate end time using the same average durations (hours) the
# scraper already uses in run_pipeline.py, so this stays consistent with the pipeline.
LEAGUE_DURATION_HOURS = {
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
    'Sumo': 2.5, 'Boxing': 3, 'EuroLeague': 2.5, 'ABA': 2.5, 'F1': 3, 'UFC': 3,
}
DEFAULT_DURATION_HOURS = 2.5


def parse_time_col(series):
    """Parse a HH:MM (or HH:MM:SS) string column into datetime.time, tolerating bad values."""
    parsed = pd.to_datetime(series, format='%H:%M:%S', errors='coerce')
    missing = parsed.isna()
    if missing.any():
        parsed_alt = pd.to_datetime(series[missing], format='%H:%M', errors='coerce')
        parsed.loc[missing] = parsed_alt
    return parsed.dt.time


def game_broadcast_minutes(times, dates, today_date):
    """Continuous minutes anchored to 'today_date'. Games actually dated the next
    calendar day (per the real Date column, not a guessed hour cutoff) get +24h,
    so a 01:10 game on tomorrow's date correctly lines up after a 23:50 game today."""
    out = []
    for t, d in zip(times, dates):
        if pd.isna(t) or pd.isna(d):
            out.append(None)
            continue
        m = t.hour * 60 + t.minute
        if d > today_date:
            m += 24 * 60
        out.append(m)
    return out


def estimate_end_minutes(start_minutes, leagues):
    """Estimate end time as continuous minutes from start + per-league average duration."""
    ends = []
    for m, league in zip(start_minutes, leagues):
        if m is None:
            ends.append(None)
            continue
        hours = LEAGUE_DURATION_HOURS.get(league, DEFAULT_DURATION_HOURS)
        ends.append(m + int(round(hours * 60)))
    return ends


def flag_conflicts(df, start_col, today_date):
    """Add a 'Conflicts With' column listing other matchups that overlap in (estimated) time."""
    starts_t = parse_time_col(df[start_col])
    dates = pd.to_datetime(df['Date']).dt.date
    start_min = game_broadcast_minutes(starts_t, dates, today_date)
    end_min = estimate_end_minutes(start_min, df['League'])
    conflicts = [[] for _ in range(len(df))]

    for i in range(len(df)):
        if start_min[i] is None:
            continue
        for j in range(len(df)):
            if i == j or start_min[j] is None:
                continue
            if start_min[i] < end_min[j] and start_min[j] < end_min[i]:
                away = df.iloc[j].get('Away Team', '')
                home = df.iloc[j].get('Home Team', '')
                label = f"{away} vs {home}" if away or home else str(df.iloc[j].get('matchup', ''))
                conflicts[i].append(label)

    df = df.copy()
    df['Conflicts With'] = ["; ".join(c) if c else "" for c in conflicts]
    return df


try:
    if st.sidebar.button("🔄 Refresh Data Now"):
        load_data.clear()

    df = load_data()
    st.sidebar.caption(f"Data cache refreshes automatically every 5 min. Last loaded: {datetime.now().strftime('%H:%M:%S')}")

    if df.empty:
        st.warning("No data found in database. Is GitHub Action running?")
    else:
        # 1. Sidebar Filters
        st.sidebar.header("Filter")
        liga_options = sorted(df['League'].unique())
        liga_filter = st.sidebar.multiselect("Choose League:", options=liga_options, default=liga_options)
        st.sidebar.info("🔄 **Status:** Live connection with Supabase")

        st.sidebar.subheader("🕐 My Available Window")
        use_time_filter = st.sidebar.checkbox("Filter by time I'm free", value=False)

        window_start, window_end = dtime(0, 0), dtime(23, 59)
        fit_mode = "Any overlap"
        if use_time_filter:
            col_a, col_b = st.sidebar.columns(2)
            with col_a:
                window_start = st.time_input("From", value=dtime(19, 0))
            with col_b:
                window_end = st.time_input("Until", value=dtime(23, 59))
            fit_mode = st.sidebar.radio(
                "Show games that:",
                ["Any overlap with my window", "Fit entirely inside my window"],
                index=0,
            )
            st.sidebar.caption("If 'Until' is earlier than 'From' (e.g. 22:00 → 02:00), it's treated as crossing midnight.")

        df_filtered = df[df['League'].isin(liga_filter)]

        has_time_col = 'Time' in df.columns
        if use_time_filter and not has_time_col:
            st.sidebar.error(
                "Can't filter by time: no 'Time' column in the data. "
                "Actual columns: " + ", ".join(df.columns)
            )
            use_time_filter = False

        if has_time_col:
            st.sidebar.caption("End times aren't in the data yet, so they're estimated from average game length per league.")

        if use_time_filter and not df_filtered.empty:

            df_filtered = df_filtered.copy()
            df_filtered['Date'] = pd.to_datetime(df_filtered['Date']).dt.date
            today_date = df_filtered['Date'].min()

            starts_t = parse_time_col(df_filtered['Time'])
            game_start_min = game_broadcast_minutes(starts_t, df_filtered['Date'], today_date)
            game_end_min = estimate_end_minutes(game_start_min, df_filtered['League'])

            # Window is just clock time (no date attached). Only treat it as crossing
            # midnight if Until is at/before From — no hidden shifting based on the hour itself.
            win_start_min = window_start.hour * 60 + window_start.minute
            win_end_min = window_end.hour * 60 + window_end.minute
            if win_end_min <= win_start_min:
                win_end_min += 24 * 60

            keep = []
            for gs, ge in zip(game_start_min, game_end_min):
                if gs is None:
                    keep.append(False)
                    continue
                if fit_mode == "Fit entirely inside my window":
                    keep.append(gs >= win_start_min and ge <= win_end_min)
                else:
                    keep.append(gs < win_end_min and win_start_min < ge)
            df_filtered = df_filtered[pd.Series(keep, index=df_filtered.index)]

        # 2. Dashboard Tabs
        tab1, tab2 = st.tabs(["🔥 Recommendations", "📰 Sports News"])

        with tab1:
            m1, m2, m3 = st.columns(3)
            m1.metric("Games Today", len(df_filtered))
            max_score = int(df_filtered['total_watch_score'].max()) if not df_filtered.empty else 0
            m2.metric("Highest Score", f"{max_score}")

            if not df_filtered.empty and has_time_col:
                cdf = df_filtered.copy()
                cdf['Date'] = pd.to_datetime(cdf['Date']).dt.date
                conflict_today = cdf['Date'].min()
                df_display = flag_conflicts(cdf, start_col='Time', today_date=conflict_today)
                conflict_count = int((df_display['Conflicts With'] != "").sum())
            else:
                df_display = df_filtered
                conflict_count = 0
            m3.metric("Time Conflicts", conflict_count)


            def highlight_scores(val):
                return 'background-color: #2ecc71; color: black; font-weight: bold' if val >= 50 else ''

            def highlight_conflicts(val):
                return 'background-color: #e74c3c; color: white; font-weight: bold' if val else ''

            st.subheader("Today's Top Picks")
            if use_time_filter:
                st.caption(f"Showing games {fit_mode.lower()} for your window: {window_start.strftime('%H:%M')} – {window_end.strftime('%H:%M')} CET")

            blacklist = ['league_id_new', 'home_team_id_new', 'away_team_id_new', 'id']
            cols_to_show = [c for c in df_display.columns if c not in blacklist]

            styled = df_display[cols_to_show].style.map(highlight_scores, subset=['total_watch_score'])
            if 'Conflicts With' in cols_to_show:
                styled = styled.map(highlight_conflicts, subset=['Conflicts With'])
            styled = styled.format(subset=['total_watch_score'], precision=0)

            st.dataframe(
                styled,
                use_container_width=True,
                hide_index=True
            )

        with tab2:
            st.subheader("📰 Personalized Scouting Report")
            top_games = df_filtered.head(10)

            if not top_games.empty:
                for _, game in top_games.iterrows():
                    away = game.get('Away Team', 'Team A')
                    home = game.get('Home Team', 'Team B')
                    matchup = f"{away} vs {home}"

                    with st.container():
                        st.markdown(f"#### News for: **{matchup}**")
                        query_str = f"{away} {home}".replace(" ", "+")
                        gn_url = f"https://news.google.com/rss/search?q={query_str}&hl=en-US&gl=US&ceid=US:en"

                        try:
                            gn_feed = feedparser.parse(gn_url)
                            if gn_feed.entries:
                                cols = st.columns(2)
                                for i, entry in enumerate(gn_feed.entries[:2]):
                                    with cols[i]:
                                        pub_date = entry.get('published', '')[:16]
                                        st.markdown(f"""
                                        <div style="border: 1px solid #444; padding: 10px; border-radius: 5px; height: 180px; overflow: hidden;">
                                            <p style="font-size: 0.8rem; color: #888;">{entry.source.get('title', 'News')} | {pub_date}</p>
                                            <h6 style="margin-top: 0;">{entry.title}</h6>
                                            <a href="{entry.link}" target="_blank">Read Story</a>
                                        </div>
                                        """, unsafe_allow_html=True)
                            else:
                                st.write("No specific headlines found.")
                        except Exception:
                            st.error(f"Could not load news for {matchup}")
                        st.divider()
            else:
                st.info("No games selected.")

except Exception as e:
    st.error(f"Critical Error: {e}")
