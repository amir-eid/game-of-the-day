# Game of the Day (GOTD)

A personal project that automatically pulls today's games across multiple sports leagues and recommends the top 5 most watchable games based on a custom scoring algorithm.

---

## Motivation

As someone who follows multiple sports across different leagues and time zones, I wanted a simple tool that answers one question every day: **"What's the best game to watch tonight?"**

Instead of manually checking schedules across the NBA, NFL, LaLiga, and others, this project does it automatically — fetching live data, applying personal preferences, and surfacing the top recommendations.

---

## How It Works

1. **Fetches** today's games from the ESPN API across 11 leagues
2. **Scores** each game based on weighted criteria
3. **Returns** a Top 5 list sorted by watchability

### Leagues Covered
NBA, NFL, MLB, NHL, LaLiga, Premier League, Bundesliga, Serie A, Ligue 1, College Basketball, College Football

### Scoring Criteria
| Factor | Points |
|---|---|
| Favorite team playing | 30 |
| Playoff / Finals game | 25 |
| League weight (NFL=20 down to Serie A=2) | 2–20 |
| Winning streak | 15 |
| Derby / Rivalry | 15 |
| Table position (win %) | 0–10 |
| Games after 02:00 CET | excluded |

Favorite team games always appear at the top of the list regardless of score.

---

## Tech Stack

- **Python** – data fetching and scoring logic
- **Pandas** – data manipulation
- **ESPN API** – live game data (free, no auth required)

---

## Project Structure

```
GOTD/
├── GOTD_clean.ipynb   # Main notebook
├── games_YYYYMMDD.csv # Daily game exports
└── README.md
```

---

## Roadmap

- [ ] Add dbt for structured data transformations
- [ ] Containerize with Docker
- [ ] PowerBI dashboard to visualize daily recommendations
- [ ] Compare recommendations vs. games actually watched
- [ ] ML model to learn from watch history and improve recommendations over time
- [ ] News feed for recommended teams
- [ ] Mobile push notifications with daily Top 5

---

## Example Output

```
🏆 TOP 5 SPIELE DES TAGES
==================================================

#1  ⭐ Favorit
   Miami Heat @ Oklahoma City Thunder
   Liga:    NBA
   Zeit:    01:00 – 03:30
   Records: 20-18 vs 32-7
   Score:   56

#2  🔥 Derby
   Real Madrid @ Barcelona
   Liga:    ESP.1
   Zeit:    21:00 – 23:00
   Records: 15-3-1 vs 14-4-1
   Score:   44
```

---

## Notes

This is a personal side project built to learn and apply data engineering concepts. The scoring algorithm reflects my own viewing preferences and is fully configurable.
