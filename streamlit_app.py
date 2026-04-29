import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os

# Seite konfigurieren
st.set_page_config(page_title="Sports Watch Score", layout="wide")

st.title("🏆 Game of the Day Dashboard")
st.markdown("Finde die besten Spiele für heute und morgen.")

# Verbindung zu Supabase (Postgres)
def get_data():
    # In Streamlit Cloud nutzen wir st.secrets
    db_url = st.secrets["SUPABASE_DB_URL"]
    engine = create_engine(db_url)
    
    query = """
        SELECT 
            total_watch_score as score,
            "League",
            "Away Team" || ' @ ' || "Home Team" as Matchup,
            "Date",
            "Time (CET)",
            tags
        FROM fct_daily_schedule
        ORDER BY total_watch_score DESC
    """
    return pd.read_sql(query, engine)

try:
    df = get_data()

    # --- Sidebar Filter ---
    st.sidebar.header("Filter")
    leagues = st.sidebar.multiselect("Ligen wählen", options=df["League"].unique(), default=df["League"].unique())
    min_score = st.sidebar.slider("Minimum Watch Score", 0, 100, 40)

    # Daten filtern
    filtered_df = df[(df["League"].isin(leagues)) & (df["score"] >= min_score)]

    # --- Hauptanzeige ---
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Top Matchups")
        # Tabelle mit farblicher Markierung (Heatmap-Style)
        st.dataframe(
            filtered_df.style.background_gradient(subset=['score'], cmap='Greens'),
            use_container_width=True,
            hide_index=True
        )

    with col2:
        st.subheader("Stats")
        st.metric("Spiele gefunden", len(filtered_df))
        if not filtered_df.empty:
            st.metric("Höchster Score", f"{filtered_df['score'].max()} pts")

except Exception as e:
    st.error(f"Verbindung fehlgeschlagen: {e}")
