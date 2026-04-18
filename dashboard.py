import streamlit as st
import duckdb
import pandas as pd
import feedparser
import os  
from datetime import datetime

st.set_page_config(page_title="Game of the Day Dashboard", layout="wide")

st.title("🏆 Sports Watcher Dashboard")

@st.cache_data(ttl=3600)
def load_data():
    db_path = 'sports.duckdb'
    #  Return empty df if database does not exist yet
    if not os.path.exists(db_path):
        return pd.DataFrame(), None
        
    last_modified = os.path.getmtime(db_path)
    con = duckdb.connect(db_path, read_only=True)
    try:
        df = con.execute("SELECT * FROM fct_daily_schedule ORDER BY total_watch_score DESC").df()
    except:
        df = pd.DataFrame()
    con.close()
    return df, last_modified

try:
    df, last_update_time = load_data()

    if df.empty:
        st.warning("No data found. Run Prefect pipeline first.")
    else:
        # 1. Sidebar with league filters and last update time
        st.sidebar.header("Filter")
        liga_options = df['League'].unique()
        liga_filter = st.sidebar.multiselect("Choose League:", options=liga_options, default=liga_options)
        
        # Show last update time in sidebar
        if last_update_time:
            readable_time = datetime.fromtimestamp(last_update_time).strftime('%Y-%m-%d %H:%M')
            st.sidebar.info(f"🔄 **Last Update:**\n{readable_time}")

        df_filtered = df[df['League'].isin(liga_filter)]

        # Create Dashboard Tabs
        tab1, tab2, tab3 = st.tabs(["🔥 Recommendations", "📰 Sports News", "📅 League Calendar"])

        with tab1:
            m1, m2 = st.columns(2)
            m1.metric("Games Today", len(df_filtered))
            m2.metric("Highest Score", f"{int(df_filtered['total_watch_score'].max()) if not df_filtered.empty else 0}")

            def highlight_scores(val):
                return 'background-color: #004d00' if val >= 50 else ''

            st.subheader("Today's Top Picks")
            st.dataframe(
                df_filtered.style.map(highlight_scores, subset=['total_watch_score']), 
                width="stretch"
            )

        with tab2:
            st.subheader("📰 Personalized Scouting Report")
    
            # Top 10 games based on watch score
            top_games = df_filtered.head(10)
    
            if not top_games.empty:
            # Loop through the top games and fetch news for each matchup
                for _, game in top_games.iterrows():
                    matchup = f"{game['Away Team']} vs {game['Home Team']}"
                    with st.container():
                        st.markdown(f"#### News for: **{matchup}**")
                
                        # hl=en-US for english news, q=search query
                        query = f"{game['Away Team']} {game['Home Team']}".replace(" ", "+")
                        gn_url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
                
                        try:
                            # Feedparser for parsing the RSS feed from Google News
                            gn_feed = feedparser.parse(gn_url)
                    
                            if gn_feed.entries:
                                # Top 2 headlines per matchup
                                cols = st.columns(2)
                                for i, entry in enumerate(gn_feed.entries[:2]):
                                    with cols[i]:
                                        # Formating the publication date and source for better readability
                                        pub_date = entry.get('published', '')[:16]
                                        st.markdown(f"""
                                        <div style="border: 1px solid #444; padding: 10px; border-radius: 5px; height: 180px;">
                                            <p style="font-size: 0.8rem; color: #888;">{entry.source.get('title', 'News')} | {pub_date}</p>
                                            <h6 style="margin-top: 0;">{entry.title}</h6>
                                            <a href="{entry.link}" target="_blank">Read Story</a>
                                        </div>
                                        """, unsafe_allow_html=True)
                            else:
                                st.write("No specific headlines found for this matchup in the last 24h.")
                        except Exception as e:
                            st.error(f"Could not load news for {matchup}")
                
                        st.divider()
            else:
                st.info("No games selected to generate a news report.")

        with tab3:
            st.subheader("📅 League Season Tracker")
    
            # Manual Calendar
            league_knowledge = {
                "NBA": {"months": [10, 6], "status": "Playoffs", "event": "NBA Finals in June"},
                "EuroLeague": {"months": [10, 5], "status": "Final Stretch", "event": "Final Four in May"},
                "ABA": {"months": [9, 5], "status": "Regular Season", "event": "Playoffs in May"},
                "MLB": {"months": [3, 11], "status": "Early Season", "event": "All-Star Game in July"},
                "NHL": {"months": [10, 6], "status": "Regular Season", "event": "Stanley Cup Playoffs"},
                "NPB": {"months": [3, 10], "status": "Regular Season", "event": "Japan Series in October"},
                "UFC": {"months": [1, 12], "status": "Year-round", "event": "Weekly Fight Nights"},
                "Boxing": {"months": [1, 12], "status": "Year-round", "event": "Major Title Fights"},
                "F1": {"months": [3, 12], "status": "Season Active", "event": "Grand Prix Weekends"},
                "Sumo": {"months": [1, 12], "status": "Odd Months", "event": "Jan, Mar, May, Jul, Sep, Nov"},
                "NFL": {"months": [9, 2], "status": "Offseason", "event": "Training Camp in July"},
                "ENG.1": {"months": [8, 5], "status": "Title Race", "event": "Season Finale in May"},
                "GER.1": {"months": [8, 5], "status": "Final Stretch", "event": "Relegation Battle"},
                "ESP.1": {"months": [8, 5], "status": "Regular Season", "event": "Title Race"},
            }

            current_month = datetime.now().month
            status_data = []

            # 2. Check each league against our knowledge base and today's schedule
            for liga, info in league_knowledge.items():
                start, end = info["months"]
        
                # Handle cases where the season spans across the year-end (e.g., NBA: Oct to June)
                if start <= end:
                    is_active = (start <= current_month <= end)
                else:
                    is_active = (current_month >= start or current_month <= end)
        
                # Check if there are games scheduled for this league today
                has_games_today = liga in df['League'].values
        
                status_data.append({
                    "League": liga,
                    "Status": "✅ Active" if is_active else "❌ Offseason",
                    "Today": "🏀 Scheduled" if has_games_today else "---",
                    "Phase": info["status"] if is_active else "Resting",
                    "Next Highlight": info["event"]
                })

            # 3. Display the status table with conditional formatting
            tracker_df = pd.DataFrame(status_data)
    
            def highlight_today(val):
                return 'color: #00ff00; font-weight: bold' if val == "🏀 Scheduled" else ''

            st.table(tracker_df.style.map(highlight_today, subset=['Today']))

except Exception as e:
    st.error(f"Critical Error: {e}")