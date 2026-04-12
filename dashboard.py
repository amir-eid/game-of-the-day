import streamlit as st
import duckdb
import pandas as pd
import feedparser
import requests
import os  # Hinzugefügt für Dateiprüfung
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
                df_filtered.style.applymap(highlight_scores, subset=['total_watch_score']), 
                use_container_width=True
            )

        with tab2:
            st.subheader("📰 Focused News: Top 5 Recommendations")
            top_5_df = df_filtered.head(5)
            if not top_5_df.empty:
                priority_teams = pd.concat([top_5_df['Home Team'], top_5_df['Away Team']]).unique().tolist()
                st.info(f"Searching specific news for: {', '.join(priority_teams)}")

                headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
                sources = {
                    "CBS Sports": "https://www.cbssports.com/rss/external/headlines/",
                    "Yahoo Sports": "https://sports.yahoo.com/rss/",
                    "ESPN": "https://www.espn.com/espn/rss/news",
                    "Sky Sports": "https://www.skysports.com/rss/12040"
                }

                found_articles = []
                for name, url in sources.items():
                    try:
                        r = requests.get(url, headers=headers, timeout=5)
                        feed = feedparser.parse(r.text)
                        for entry in feed.entries:
                            content = (entry.title + " " + entry.get('summary', '')).lower()
                            if any(team.lower() in content for team in priority_teams):
                                found_articles.append((name, entry))
                    except: continue

                if found_articles:
                    for source_name, entry in found_articles[:10]:
                        with st.expander(f"⭐ {entry.title} ({source_name})"):
                            st.write(entry.get('summary', 'No details available.'))
                            st.markdown(f"[Read Article]({entry.link})")
                else:
                    st.write("No specific news found for your top games yet.")
            else:
                st.write("No games selected.")

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