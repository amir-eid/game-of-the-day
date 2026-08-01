import streamlit as st
import pandas as pd
import feedparser
from sqlalchemy import create_engine
import os
from datetime import datetime, time as dtime

# configuration
st.set_page_config(page_title="Sports Watcher Dashboard", layout="wide")
st.title("🏆 Sports Watcher Dashboard")

# load data from supabase
@st.cache_data(ttl=3600)
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


def parse_time_col(series):
    """Parse a HH:MM string column into datetime.time, tolerating bad values."""
    return pd.to_datetime(series, format='%H:%M', errors='coerce').dt.time


def time_ranges_overlap(start_a, end_a, start_b, end_b):
    """True if [start_a, end_a) overlaps [start_b, end_b), handling past-midnight end times."""
    def to_minutes(t):
        return t.hour * 60 + t.minute

    a_start, a_end = to_minutes(start_a), to_minutes(end_a)
    b_start, b_end = to_minutes(start_b), to_minutes(end_b)

    # handle games that roll past midnight (end < start)
    if a_end <= a_start:
        a_end += 24 * 60
    if b_end <= b_start:
        b_end += 24 * 60

    return a_start < b_end and b_start < a_end


def flag_conflicts(df):
    """Add a 'Conflicts With' column listing other matchups that overlap in time."""
    starts = parse_time_col(df['Time (CET)'])
    ends = parse_time_col(df['End Time (CET)'])
    conflicts = [[] for _ in range(len(df))]

    idx = df.index.to_list()
    for i in range(len(idx)):
        if pd.isna(starts.iloc[i]) or pd.isna(ends.iloc[i]):
            continue
        for j in range(len(idx)):
            if i == j:
                continue
            if pd.isna(starts.iloc[j]) or pd.isna(ends.iloc[j]):
                continue
            if time_ranges_overlap(starts.iloc[i], ends.iloc[i], starts.iloc[j], ends.iloc[j]):
                away = df.iloc[j].get('Away Team', '')
                home = df.iloc[j].get('Home Team', '')
                conflicts[i].append(f"{away} vs {home}")

    df = df.copy()
    df['Conflicts With'] = ["; ".join(c) if c else "" for c in conflicts]
    return df


try:
    df = load_data()

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

        has_time_cols = 'Time (CET)' in df.columns and 'End Time (CET)' in df.columns
        if use_time_filter and not has_time_cols:
            st.sidebar.error(
                "Can't filter by time: expected columns 'Time (CET)' / 'End Time (CET)' "
                "aren't in the data. Actual columns: " + ", ".join(df.columns)
            )
            use_time_filter = False

        if use_time_filter and not df_filtered.empty:

            starts = parse_time_col(df_filtered['Time (CET)'])
            ends = parse_time_col(df_filtered['End Time (CET)'])

            keep = []
            for g_start, g_end in zip(starts, ends):
                if pd.isna(g_start) or pd.isna(g_end):
                    keep.append(False)
                    continue
                if fit_mode == "Fit entirely inside my window":
                    def to_min(t):
                        return t.hour * 60 + t.minute
                    ws, we = to_min(window_start), to_min(window_end)
                    gs, ge = to_min(g_start), to_min(g_end)
                    if we <= ws:
                        we += 24 * 60
                    if ge <= gs:
                        ge += 24 * 60
                    keep.append(gs >= ws and ge <= we)
                else:
                    keep.append(time_ranges_overlap(g_start, g_end, window_start, window_end))
            df_filtered = df_filtered[pd.Series(keep, index=df_filtered.index)]

        # 2. Dashboard Tabs
        tab1, tab2 = st.tabs(["🔥 Recommendations", "📰 Sports News"])

        with tab1:
            m1, m2, m3 = st.columns(3)
            m1.metric("Games Today", len(df_filtered))
            max_score = int(df_filtered['total_watch_score'].max()) if not df_filtered.empty else 0
            m2.metric("Highest Score", f"{max_score}")

            if not df_filtered.empty and has_time_cols:
                df_display = flag_conflicts(df_filtered)
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
