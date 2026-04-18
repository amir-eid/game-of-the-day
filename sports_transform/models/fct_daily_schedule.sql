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

    raw_f1_source as (
    select 
        'F1' as "League",
        "Event_Name" as "Away Team",
        "Circuit" as "Home Team",
        'N/A' as "Away Record",
        'N/A' as "Home Record",
        "Date",
        "Time_CET" as "Time (CET)",
        strftime(date_add(CAST("Date" || ' ' || "Time_CET" AS TIMESTAMP), interval 2 hour), '%H:%M') as "End Time (CET)",
        false as "Is Playoff"
    from {{ source('external_data', 'raw_f1') }}
),

    raw_sumo_source as (
    select * from {{ source('external_data', 'raw_sumo') }}
),

    raw_npb_source as (
    select * from {{ source('external_data', 'raw_npb') }}
),
    raw_eurobasket_source as (
    select * from {{ source('external_data', 'raw_eurobasket') }}
),
    raw_boxing_source as (
    select * from {{ source('external_data', 'raw_boxing') }}
),
base as (
    select * from raw_games_source
    union all
    select * from raw_ufc_source
    union all
    select * from raw_f1_source
    union all
    select * from raw_sumo_source
    union all
    select * from raw_npb_source
    union all
    select * from raw_eurobasket_source
    union all
    select * from raw_boxing_source
),
-- This is where we create the "missing" columns based on your preferences
logic as (
    select
        *,
        -- 1. FAVORITE LOGIC
        case 
            when "Home Team" in ('Miami Heat', 'Carolina Panthers', 'Valencia', 'Illinois Fighting Illini', 'San Diego Padres',
                                 'Austria National Team', 'Egypt National Team', 'Poland National Team') then 1
            when "Away Team" in ('Miami Heat', 'Carolina Panthers', 'Valencia', 'Illinois Fighting Illini', 'San Diego Padres',
                                 'Austria National Team', 'Egypt National Team', 'Poland National Team') then 1
            else 0 
        end as is_favorite,

        -- 2. DERBY LOGIC (Add your top matchups here)
        case 
            when ("Home Team" = 'Real Madrid' and "Away Team" = 'Barcelona') then 1
            when ("Home Team" = 'Barcelona' and "Away Team" = 'Real Madrid') then 1
            when ("Home Team" = 'Manchester City' and "Away Team" = 'Manchester United') then 1
            when ("Home Team" = 'Manchester United' and "Away Team" = 'Manchester City') then 1
            when ("Home Team" = 'Los Angeles Lakers' and "Away Team" = 'Boston Celtics') then 1
            when ("Home Team" = 'Boston Celtics' and "Away Team" = 'Los Angeles Lakers') then 1
            when ("Home Team" = 'New York Yankees' and "Away Team" = 'Boston Red Sox') then 1
            when ("Home Team" = 'Boston Red Sox' and "Away Team" = 'New York Yankees') then 1
            when ("Home Team" = 'Chicago Bulls' and "Away Team" = 'Detroit Pistons') then 1
            when ("Home Team" = 'Detroit Pistons' and "Away Team" = 'Chicago Bulls') then 1
            when ("Home Team" = 'Liverpool' and "Away Team" = 'Everton') then 1
            when ("Home Team" = 'Everton' and "Away Team" = 'Liverpool') then 1
            when ("Home Team" = 'AC Milan' and "Away Team" = 'Inter Milan') then 1
            when ("Home Team" = 'Inter Milan' and "Away Team" = 'AC Milan') then 1
            when ("Home Team" = 'Juventus' and "Away Team" = 'Torino') then 1
            when ("Home Team" = 'Torino' and "Away Team" = 'Juventus') then 1
            when ("Home Team" = 'Boca Juniors' and "Away Team" = 'River Plate') then 1
            when ("Home Team" = 'River Plate' and "Away Team" = 'Boca Juniors') then 1
            when ("Home Team" = 'Paris Saint-Germain' and "Away Team" = 'Olympique de Marseille') then 1
            when ("Home Team" = 'Olympique de Marseille' and "Away Team" = 'Paris Saint-Germain') then 1
            when ("Home Team" = 'Celtic' and "Away Team" = 'Rangers') then 1
            when ("Home Team" = 'Rangers' and "Away Team" = 'Celtic') then 1
            when ("Home Team" = 'AS Roma' and "Away Team" = 'Lazio') then 1
            when ("Home Team" = 'Lazio' and "Away Team" = 'AS Roma') then 1
            when ("Home Team" = 'Fenerbahçe' and "Away Team" = 'Galatasaray') then 1
            when ("Home Team" = 'Galatasaray' and "Away Team" = 'Fenerbahçe') then 1
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
    from base
),

calculated_scores as (
    select
        *,
        -- LEAGUE POINTS
        case 
            when "League" = 'NFL' then 50 when "League" = 'NBA' then 25 when "League" = 'UFC' then 5 when "League" = 'F1' then 10
            when "League" = 'MLB' then 25 when "League" = 'ESP.1' then 14 when "League" = 'Olympic Ice Hockey' then 2
            when "League" = 'World Baseball Classic' then 30 when "League" = 'NHL' then 12 when "League" = 'ENG.1' then 10
            when "League" = 'College Basketball' then 8 when "League" = 'College Football' then 6 when "League" = 'NPB' then 0.5
            when "League" = 'GER.1' then 4 when "League" = 'FRA.1' then 3 when "League" = 'FIBA World Cup' then 5
            when "League" = 'ITA.1' then 2 when "League" = 'UEFA Champions League' then 2 when "League" = 'Sumo' then 15
            when "League" = 'UEFA Europa League' then 1.5 when "League" = 'UEFA Conference League' then 1
            when "League" = 'FIFA World Cup' then 55 when "League" = 'UEFA European Championship' then 45
            when "League" = 'UEFA European Championship Qualifiers' then 15 when "League" = 'FA Cup' then 1
            when "League" = 'Copa del Rey' then 1 when "League" = 'Eredivisie' then 0.5 when "League" = 'Olympics Basketball' then 5
            when "League" = 'Portuguese Primeira Liga' then 0.5 when "League" = 'Russian Premier League' then 0.5
            when "League" = 'Austrian Bundesliga' then 2 when "League" = 'Turkish Süper Lig' then 0.5
            when "League" = 'Africa Cup of Nations' then 15 when "League" = 'Africa Cup of Nations Qualifiers' then 8
            when "League" = 'Copa America' then 6 when "League" = 'UEFA Nations League' then 1
            when "League" = 'Olympic Football Tournament' then 0.5 when "League" = 'FIFA World Cup Qualifiers - UEFA' then 3
            when "League" = 'FIFA World Cup Qualifiers - CAF' then 3 when "League" = 'J1 League' then 0.5 
            when "League" = 'Copa Libertadores' then 0.5 when "League" = 'International Friendlies' then 0.5
            when "League" = 'EuroLeague' then 0.5 when "League" = 'ABA' then 0.5 when "League" = 'Boxing' then 20
            else 0
        end as league_pts,
        
        case 
            when "Time (CET)" >= '10:00' and "Time (CET)" < '23:00' then 15 
            else 0 
        end as time_pts,

        (is_favorite * 30) as favorite_pts,
        (case when "Is Playoff" = true then 25 else 0 end) as playoff_pts,
        (is_derby_game * 15) as derby_pts,
        -- Calculate record points, defaulting to 0 if we can't get win %
        coalesce(round(((home_win_pct + away_win_pct) / 2) * 10), 0) as record_pts
    from logic
)

select
    "League", "Away Team", "Home Team", "Time (CET)", "End Time (CET)", "Is Playoff",
    (league_pts + favorite_pts + playoff_pts + derby_pts + record_pts + time_pts) as total_watch_score,
    -- Simple tagging
    case when is_favorite = 1 then '⭐ Favorite ' else '' end || 
    case when is_derby_game = 1 then '🔥 DERBY ' else '' end ||
    case when "Is Playoff" = true then '🏆 Playoff ' else '' end as tags
from calculated_scores
where ("Time (CET)" < '02:00' or "Time (CET)" > '10:00')
order by total_watch_score desc