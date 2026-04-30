import streamlit as st
import pandas as pd
import feedparser
from sqlalchemy import create_engine
import os  
from datetime import datetime

# --- KONFIGURATION ---
st.set_page_config(page_title="Sports Watcher Dashboard", layout="wide")
st.title("🏆 Sports Watcher Dashboard")

# --- DATEN LADEN (VON SUPABASE) ---
@st.cache_data(ttl=600) # TTL auf 10 Min reduziert, damit Änderungen schneller sichtbar sind
def load_data():
    if "SUPABASE_DB_URL" not in st.secrets:
        st.error("Bitte SUPABASE_DB_URL in den Streamlit Secrets hinterlegen!")
        return pd.DataFrame()
        
    db_url = st.secrets["SUPABASE_DB_URL"]
    engine = create_engine(db_url)
    
    try:
        # WICHTIG: Wir laden jetzt aus watch_history, da hier unser Fortschritt gespeichert wird
        query = "SELECT * FROM watch_history ORDER BY score DESC"
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Fehler beim Laden der Daten: {e}")
        return pd.DataFrame()

# --- HAUPT-LOGIK ---
try:
    df = load_data()

    if df.empty:
        st.warning("Keine Daten in der Datenbank gefunden. Läuft die GitHub Action?")
    else:
        # 1. Sidebar Filter
        st.sidebar.header("Filter")
        liga_options = sorted(df['league'].unique()) # Kleingeschrieben laut watch_history Schema
        liga_filter = st.sidebar.multiselect("Choose League:", options=liga_options, default=liga_options)
        
        # Filter für "Gesehene Spiele" hinzufügen
        show_watched = st.sidebar.checkbox("Zeige bereits gesehene Spiele", value=True)
        
        st.sidebar.info("🔄 **Status:** Live-Verbindung zu Supabase")

        # Filtering
        df_filtered = df[df['league'].isin(liga_filter)]
        if not show_watched:
            df_filtered = df_filtered[df_filtered['watched'] == False]

        # 2. Dashboard Tabs
        tab1, tab2, tab3 = st.tabs(["🔥 Recommendations", "📰 Sports News", "📅 League Calendar"])

        with tab1:
            m1, m2 = st.columns(2)
            m1.metric("Games Today", len(df_filtered))
            max_score = int(df_filtered['score'].max()) if not df_filtered.empty else 0
            m2.metric("Highest Score", f"{max_score}")

            def highlight_scores(val):
                return 'background-color: #2ecc71; color: black; font-weight: bold' if val >= 50 else ''

            st.subheader("Today's Top Picks")
            
            # --- ID SPALTEN ENTFERNEN ---
            # Wir erstellen eine Kopie für die Anzeige ohne die hässlichen IDs
            cols_to_exclude = ['league_id_new', 'home_team_id_new', 'away_team_id_new', 'id']
            display_df = df_filtered.drop(columns=[c for c in cols_to_exclude if c in df_filtered.columns])
            
            # Spaltennamen für die Anzeige verschönern (optional)
            display_df.columns = [c.replace('_', ' ').title() for c in display_df.columns]

            st.dataframe(
                display_df.style.map(highlight_scores, subset=['Score'])
                .format(subset=['Score'], precision=0), 
                use_container_width=True,
                hide_index=True # Index ausblenden für mehr Platz
            )

        with tab2:
            st.subheader("📰 Personalized Scouting Report")
            top_games = df_filtered.head(10)

            if not top_games.empty:
                for _, game in top_games.iterrows():
                    matchup = game.get('matchup', 'Unknown Matchup')
                    
                    with st.container():
                        st.markdown(f"#### News for: **{matchup}**")
                        # Wir nutzen das matchup-Feld direkt für die Suche
                        query_str = matchup.replace(" @ ", " ").replace(" vs ", " ").replace(" ", "+")
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

        with tab3:
            # (Der Tab3 Code bleibt identisch, nur df['League'] -> df['league'])
            st.subheader("📅 League Season Tracker")
            league_knowledge = {
                "NBA": {"months": [10, 6], "status": "Playoffs", "event": "NBA Finals in June"},
                "EuroLeague": {"months": [10, 5], "status": "Final Stretch", "event": "Final Four in May"},
                "MLB": {"months": [3, 11], "status": "Early Season", "event": "All-Star Game in July"},
                "NHL": {"months": [10, 6], "status": "Regular Season", "event": "Stanley Cup Playoffs"},
                # ... restliche Ligen wie gehabt ...
            }

            current_month = datetime.now().month
            status_data = []

            for liga, info in league_knowledge.items():
                start, end = info["months"]
                is_active = (start <= current_month <= end) if start <= end else (current_month >= start or current_month <= end)
                has_games_today = liga in df['league'].values
                status_data.append({
                    "League": liga,
                    "Status": "✅ Active" if is_active else "❌ Offseason",
                    "Today": "🏀 Scheduled" if has_games_today else "---",
                    "Phase": info["status"] if is_active else "Resting",
                    "Next Highlight": info["event"]
                })

            tracker_df = pd.DataFrame(status_data)
            st.table(tracker_df.style.map(lambda v: 'color: #00ff00; font-weight: bold' if v == "🏀 Scheduled" else '', subset=['Today']))

except Exception as e:
    st.error(f"Critical Error: {e}")