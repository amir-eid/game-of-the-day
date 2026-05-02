import streamlit as st
import pandas as pd
import feedparser
from sqlalchemy import create_engine
import os  
from datetime import datetime

# congiguration
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

try:
    df = load_data()

    if df.empty:
        st.warning("No data found in database. Is GitHub Action running?")
    else:
        # 1. Sidebar Filter
        st.sidebar.header("Filter")
        liga_options = sorted(df['League'].unique())
        liga_filter = st.sidebar.multiselect("Choose League:", options=liga_options, default=liga_options)
        st.sidebar.info("🔄 **Status:** Live connection with Supabase")

        df_filtered = df[df['League'].isin(liga_filter)]

        # 2. Dashboard Tabs
        tab1, tab2 = st.tabs(["🔥 Recommendations", "📰 Sports News"])

        with tab1:
            m1, m2 = st.columns(2)
            m1.metric("Games Today", len(df_filtered))
            max_score = int(df_filtered['total_watch_score'].max()) if not df_filtered.empty else 0
            m2.metric("Highest Score", f"{max_score}")

            def highlight_scores(val):
                return 'background-color: #2ecc71; color: black; font-weight: bold' if val >= 50 else ''

            st.subheader("Today's Top Picks")

            blacklist = ['league_id_new', 'home_team_id_new', 'away_team_id_new', 'id']
            cols_to_show = [c for c in df_filtered.columns if c not in blacklist]
            st.dataframe(
                df_filtered[cols_to_show].style.map(highlight_scores, subset=['total_watch_score'])
                .format(subset=['total_watch_score'], precision=0), 
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
                        except:
                            st.error(f"Could not load news for {matchup}")
                        st.divider()
            else:
                st.info("No games selected.")

except Exception as e:
    st.error(f"Critical Error: {e}")