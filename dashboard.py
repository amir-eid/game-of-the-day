import streamlit as st
import duckdb
import pandas as pd
import feedparser
import requests
import os  
from datetime import datetime

st.set_page_config(page_title="Game of the Day Dashboard", layout="wide")

st.title("🏆 Sports Watcher Dashboard")

# --- DATA LOADING ---
@st.cache_data(ttl=3600)
def load_data():
    db_path = 'sports.duckdb'
    # Falls die DB noch nicht existiert (erster Run), geben wir ein leeres DF zurück
    if not os.path.exists(db_path):
        return pd.DataFrame(), None
        
    last_modified = os.path.getmtime(db_path)
    con = duckdb.connect(db_path, read_only=True)
    # Sicherstellen, dass die Tabelle existiert
    try:
        df = con.execute("SELECT * FROM fct_daily_schedule ORDER BY total_watch_score DESC").df()
    except:
        df = pd.DataFrame()
    con.close()
    return df, last_modified

try:
    df, last_update_time = load_data()

    if df.empty:
        st.warning("No data found. Please run your Prefect pipeline first.")
    else:
        # 1. Sidebar mit Metadaten
        st.sidebar.header("Filter")
        liga_options = df['League'].unique()
        liga_filter = st.sidebar.multiselect("Choose League:", options=liga_options, default=liga_options)
        
        # Zeitstempel anzeigen
        if last_update_time:
            readable_time = datetime.fromtimestamp(last_update_time).strftime('%Y-%m-%d %H:%M')
            st.sidebar.info(f"🔄 **Last Update:**\n{readable_time}")

        df_filtered = df[df['League'].isin(liga_filter)]

        # --- TABS ERSTELLEN ---
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
    
            # Wir nehmen die Top 3-5 Spiele für die News-Suche
            top_games = df_filtered.head(5)
    
            if not top_games.empty:
            # Wir loopen durch die Top-Spiele, um News pro Matchup anzuzeigen
                for _, game in top_games.iterrows():
                    matchup = f"{game['Away Team']} vs {game['Home Team']}"
                    with st.container():
                        st.markdown(f"#### News for: **{matchup}**")
                
                        # Wir bauen eine gezielte Google News URL für die Teams
                        # hl=en-US für englische News, q=Suchbegriff
                        query = f"{game['Away Team']} {game['Home Team']}".replace(" ", "+")
                        gn_url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
                
                        try:
                            # Wir nutzen feedparser direkt auf der Google News Suche
                            # Das ist viel treffsicherer als allgemeine Feeds
                            gn_feed = feedparser.parse(gn_url)
                    
                            if gn_feed.entries:
                                # Zeige die Top 2 relevantesten Artikel pro Matchup
                                cols = st.columns(2)
                                for i, entry in enumerate(gn_feed.entries[:2]):
                                    with cols[i]:
                                        # Zeitstempel hübsch machen
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
            league_knowledge = {
                "NBA": {"months": [10, 6], "status": "Regular Season", "event": "Playoffs starting April"},
                "MLB": {"months": [3, 11], "status": "Early Season", "event": "All-Star Game in July"},
                "NHL": {"months": [10, 6], "status": "Regular Season", "event": "Stanley Cup Playoffs"},
                "ENG.1": {"months": [8, 5], "status": "Title Race", "event": "Season Finale in May"},
                "GER.1": {"months": [8, 5], "status": "Final Stretch", "event": "Relegation Battle"},
                "ESP.1": {"months": [8, 5], "status": "Regular Season", "event": "Title Race"},
                "ITA.1": {"months": [8, 5], "status": "Regular Season", "event": "Champions League Race"},
                "FRA.1": {"months": [8, 5], "status": "Regular Season", "event": "Title Contenders"},
                "College Basketball": {"months": [11, 4], "status": "Regular Season", "event": "March Madness in March"},
                "College Football": {"months": [8, 1], "status": "Regular Season", "event": "Bowl Games in December"},
                "NFL": {"months": [9, 2], "status": "Regular Season", "event": "Super Bowl in February"}
            }

            current_month = datetime.now().month
            status_data = []
            for liga in liga_filter:
                if liga in league_knowledge:
                    info = league_knowledge[liga]
                    start, end = info["months"]
                    is_active = (start <= current_month <= end) if start <= end else (current_month >= start or current_month <= end)
                    status_data.append({
                        "League": liga,
                        "Status": "✅ Active" if is_active else "❌ Offseason",
                        "Phase": info["status"] if is_active else "Resting",
                        "Next Highlight": info["event"]
                    })

            if status_data:
                st.table(pd.DataFrame(status_data))
            else:
                st.info("Please select leagues in the sidebar.")

except Exception as e:
    st.error(f"Critical Error: {e}")