with raw_games_source as (
    select
        "League",
        "Away Team",
        "Home Team",
        "Away Record",
        "Home Record",
        "Date",
        "Time (CET)",
        "End Time (CET)",
        "Is Playoff"
    from {{ source('external_data', 'raw_games') }}
),

    raw_ufc_source as (
        select
            'UFC' as "League",
            "Fighter_B" as "Away Team",
            "Fighter_A" as "Home Team",
            '0-0' as "Away Record",
            '0-0' as "Home Record",
            "Date",
            "Time_CET" as "Time (CET)",
            strftime(date_add(CAST("Date" || ' ' || "Time_CET" AS TIMESTAMP), interval 30 minute), '%H:%M') as "End Time (CET)",
            false as "Is Playoff"
        from {{ source('external_data', 'raw_ufc') }}
    ),
base as (
    select * from raw_games_source
    union all
    select * from raw_ufc_source
),
-- This is where we create the "missing" columns based on your preferences
logic as (
    select
        *,
        -- 1. FAVORITE LOGIC
        case 
            when "Home Team" in ('Miami Heat', 'Carolina Panthers', 'Valencia', 'Illinois Fighting Illini', 'San Diego Padres') then 1
            when "Away Team" in ('Miami Heat', 'Carolina Panthers', 'Valencia', 'Illinois Fighting Illini', 'San Diego Padres') then 1
            else 0 
        end as is_favorite,

        -- 2. DERBY LOGIC (Add your top matchups here)
        case 
            when ("Home Team" = 'Real Madrid' and "Away Team" = 'Barcelona') then 1
            when ("Home Team" = 'Barcelona' and "Away Team" = 'Real Madrid') then 1
            when ("Home Team" = 'Manchester City' and "Away Team" = 'Manchester United') then 1
            -- Add more from your Python DERBIES list as needed
            else 0 
        end as is_derby_game,

        -- 3. WIN PERCENTAGE LOGIC (Parsing "10-5" style records)
        -- This looks complex but it just handles the math of Wins / (Wins + Losses)
        coalesce(try_cast(split_part("Home Record", '-', 1) as float) / 
            NULLIF((try_cast(split_part("Home Record", '-', 1) as float) + try_cast(split_part("Home Record", '-', 2) as float)), 0), 0) 
        as home_win_pct,
        
        coalesce(try_cast(split_part("Away Record", '-', 1) as float) / 
            NULLIF((try_cast(split_part("Away Record", '-', 1) as float) + try_cast(split_part("Away Record", '-', 2) as float)), 0), 0) 
        as away_win_pct,

        0 as has_streak -- Placeholder for now unless you scrape streak data
    from base
),

calculated_scores as (
    select
        *,
        -- LEAGUE POINTS
        case 
            when "League" = 'NFL' then 20 when "League" = 'NBA' then 18 when "League" = 'UFC' then 5
            when "League" = 'MLB' then 16 when "League" = 'ESP.1' then 14 when "League" = 'Olympic Ice Hockey' then 2
            when "League" = 'World Baseball Classic' then 15 when "League" = 'NHL' then 12 when "League" = 'ENG.1' then 10
            when "League" = 'College Basketball' then 8 when "League" = 'College Football' then 6
            when "League" = 'GER.1' then 4 when "League" = 'FRA.1' then 3 when "League" = 'FIBA World Cup' then 5
            when "League" = 'ITA.1' then 2 when "League" = 'UEFA Champions League' then 2
            when "League" = 'UEFA Europa League' then 1.5 when "League" = 'UEFA Conference League' then 1
            when "League" = 'FIFA World Cup' then 25 when "League" = 'UEFA European Championship' then 20
            when "League" = 'UEFA European Championship Qualifiers' then 15 when "League" = 'FA Cup' then 1
            when "League" = 'Copa del Rey' then 1 when "League" = 'Eredivisie' then 0.5 when "League" = 'Olympics Basketball' then 5
            when "League" = 'Portuguese Primeira Liga' then 0.5 when "League" = 'Russian Premier League' then 0.5
            when "League" = 'Austrian Bundesliga' then 2 when "League" = 'Turkish Süper Lig' then 0.5
            when "League" = 'Africa Cup of Nations' then 2 when "League" = 'Africa Cup of Nations Qualifiers' then 2
            when "League" = 'Copa America' then 0.5 when "League" = 'UEFA Nations League' then 0.5
            when "League" = 'Olympic Football Tournament' then 0.5 when "League" = 'FIFA World Cup Qualifiers - UEFA' then 0.5
            when "League" = 'FIFA World Cup Qualifiers - CAF' then 0.5 when "League" = 'J1 League' then 0.5 
            when "League" = 'Copa Libertadores' then 0.5 when "League" = 'International Friendlies' then 0.5
            else 0
        end as league_pts,
        
        (is_favorite * 30) as favorite_pts,
        (case when "Is Playoff" = true then 25 else 0 end) as playoff_pts,
        (is_derby_game * 15) as derby_pts,
        (has_streak * 15) as streak_pts,
        -- Calculate record points, defaulting to 0 if we can't get win %
        coalesce(round(((home_win_pct + away_win_pct) / 2) * 10), 0) as record_pts
    from logic
)

select
    "League", "Away Team", "Home Team", "Time (CET)", "End Time (CET)", "Is Playoff",
    (league_pts + favorite_pts + playoff_pts + derby_pts + streak_pts + record_pts) as total_watch_score,
    -- Simple tagging
    case when is_favorite = 1 then '⭐ Favorite ' else '' end || 
    case when is_derby_game = 1 then '🔥 DERBY ' else '' end ||
    case when "Is Playoff" = true then '🏆 Playoff ' else '' end as tags
from calculated_scores
where ("Time (CET)" < '02:00' or "Time (CET)" > '10:00')
order by total_watch_score desc