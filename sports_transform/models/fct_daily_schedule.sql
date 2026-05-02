{{ config(
    materialized='table'
) }}

-- 1. Dimensionen definieren
with teams as (
    select team_id, team_name, league_name from {{ source('public', 'dim_teams') }}
),
leagues as (
    select league_id, league_name from {{ source('public', 'dim_leagues') }}
),

raw_games_source as (
    select
        "League", "Away Team", "Home Team", "Away Record", "Home Record",
        "Date"::date, "Time (CET)"::time, "End Time (CET)"::time, "Is Playoff"::boolean
    from {{ source('public', 'raw_games') }}
),

raw_ufc_source as (
    select
        'UFC' as "League", "Fighter_B" as "Away Team", "Fighter_A" as "Home Team",
        '0-0' as "Away Record", '0-0' as "Home Record", 
        "Date"::date, 
        "Time_CET"::time as "Time (CET)",
        ("Time_CET"::time + interval '30 minutes') as "End Time (CET)",
        false as "Is Playoff"
    from {{ source('public', 'raw_ufc') }}
),

raw_f1_source as (
    select 
        'F1' as "League", "Event_Name" as "Away Team", "Circuit" as "Home Team",
        'N/A' as "Away Record", 'N/A' as "Home Record", 
        "Date"::date, 
        "Time_CET"::time as "Time (CET)",
        ("Time_CET"::time + interval '2 hours') as "End Time (CET)",
        false as "Is Playoff"
    from {{ source('public', 'raw_f1') }}
),

-- Die anderen Quellen (Sumo, NPB, etc.) analog einbinden
raw_sumo_source as (
    select "League", "Away Team", "Home Team", "Away Record", "Home Record", 
           "Date"::date, "Time (CET)"::time, "End Time (CET)"::time, "Is Playoff"::boolean 
    from {{ source('public', 'raw_sumo') }}
),

raw_npb_source as (
    select "League", "Away Team", "Home Team", "Away Record", "Home Record", 
           "Date"::date, "Time (CET)"::time, "End Time (CET)"::time, "Is Playoff"::boolean 
    from {{ source('public', 'raw_npb') }}
),

raw_eurobasket_source as (
    select 
        "League", "Away Team", "Home Team", "Away Record", "Home Record", 
        "Date"::date, 
        "Time (CET)"::time, 
        "End Time (CET)"::time, 
        "Is Playoff"::boolean 
    from {{ source('public', 'raw_eurobasket') }}
),

raw_boxing_source as (
    select 
        "League", "Away Team", "Home Team", "Away Record", "Home Record", 
        "Date"::date, 
        "Time (CET)"::time, 
        "End Time (CET)"::time, 
        "Is Playoff"::boolean 
    from {{ source('public', 'raw_boxing') }}
),
-- 3. Union All
base as (
    select * from raw_games_source
    union all select * from raw_ufc_source
    union all select * from raw_f1_source
    union all select * from raw_sumo_source
    union all select * from raw_npb_source
    union all select * from raw_eurobasket_source
    union all select * from raw_boxing_source
),

-- 4. IDs via Join hinzufügen
joined_ids as (
    select
        b.*,
        l.league_id as league_id_new,
        t_home.team_id as home_team_id_new,
        t_away.team_id as away_team_id_new
    from base b
    left join leagues l on b."League" = l.league_name
    left join teams t_home 
        on b."Home Team" = t_home.team_name 
        and b."League" = t_home.league_name
    left join teams t_away 
        on b."Away Team" = t_away.team_name 
        and b."League" = t_away.league_name
),

-- 5. Scoring Logic
logic as (
    select
        *,
        case 
            when "Home Team" in ('Miami Heat', 'Carolina Panthers', 'Valencia', 'Illinois Fighting Illini', 'San Diego Padres', 'Austria National Team', 'Egypt National Team', 'Poland National Team') then 1
            when "Away Team" in ('Miami Heat', 'Carolina Panthers', 'Valencia', 'Illinois Fighting Illini', 'San Diego Padres', 'Austria National Team', 'Egypt National Team', 'Poland National Team') then 1
            else 0 
        end as is_favorite,

        case 
            when ("Home Team" = 'Real Madrid' and "Away Team" = 'Barcelona') or ("Home Team" = 'Barcelona' and "Away Team" = 'Real Madrid') then 1
            -- ... (Restliche Derbies hier)
            else 0 
        end as is_derby_game,

        coalesce(
            case 
                when "Home Record" LIKE '%-%' and "Home Record" NOT LIKE '%N/A%' 
                then (split_part("Home Record", '-', 1)::float) / NULLIF(((split_part("Home Record", '-', 1)::float) + (split_part("Home Record", '-', 2)::float)), 0)
                else 0 
            end, 0) as home_win_pct,
        
        coalesce(
            case 
                when "Away Record" LIKE '%-%' and "Away Record" NOT LIKE '%N/A%' 
                then (split_part("Away Record", '-', 1)::float) / NULLIF(((split_part("Away Record", '-', 1)::float) + (split_part("Away Record", '-', 2)::float)), 0)
                else 0 
            end, 0) as away_win_pct
    from joined_ids
),

calculated_scores as (
    select
        *,
        case 
            when "League" = 'NFL' then 50 when "League" = 'NBA' then 25 when "League" = 'UFC' then 5 when "League" = 'F1' then 10
            when "League" = 'MLB' then 20 when "League" = 'ESP.1' then 15 when "League" = 'Olympic Ice Hockey' then 25 
            when "League" = 'World Baseball Classic' then 40 when "League" = 'NHL' then 12 when "League" = 'ENG.1' then 15
            when "League" = 'College Basketball' then 10 when "League" = 'College Football' then 10 when "League" = 'NPB' then 5
            when "League" = 'GER.1' then 8 when "League" = 'FRA.1' then 7 when "League" = 'FIBA World Cup' then 5 
            when "League" = 'ITA.1' then 6 when "League" = 'UEFA Champions League' then 17 when "League" = 'Sumo' then 20
            when "League" = 'UEFA Europa League' then 5 when "League" = 'UEFA Conference League' then 3 
            when "League" = 'FIFA World Cup' then 50 when "League" = 'UEFA European Championship' then 50
            when "League" = 'UEFA European Championship Qualifiers' then 20 when "League" = 'FA Cup' then 5
            when "League" = 'Copa del Rey' then 3 when "League" = 'Eredivisie' then 3 when "League" = 'Olympics Basketball' then 16
            when "League" = 'Portuguese Primeira Liga' then 3 when "League" = 'Russian Premier League' then 3 
            when "League" = 'Austrian Bundesliga' then 5 when "League" = 'Turkish Süper Lig' then 3 when "League" = 'Africa Cup of Nations' then 30
            when "League" = 'Africa Cup of Nations Qualifiers' then 5 when "League" = 'Copa America' then 10 when "League" = 'UEFA Nations League' then 5
            when "League" = 'Olympic Football Tournament' then 3 when "League" = 'FIFA World Cup Qualifiers - UEFA' then 10
            when "League" = 'FIFA World Cup Qualifiers - CAF' then 8 when "League" = 'J1 League' then 1 when "League" = 'Copa Libertadores' then 2
            when "League" = 'International Friendlies' then 4 when "League" = 'EuroLeague' then 2 when "League" = 'ABA' then 1 when "League" = 'Boxing' then 10

            else 0
        end as league_pts,
        
        case when "Time (CET)" >= '10:00' and "Time (CET)" < '23:00' then 15 else 0 end as time_pts,
        (is_favorite * 30) as favorite_pts,
        (case when "Is Playoff" = true then 25 else 0 end) as playoff_pts,
        (is_derby_game * 15) as derby_pts,
        coalesce(round(((home_win_pct + away_win_pct) / 2) * 10), 0) as record_pts
    from logic
)

-- 6. Finales SELECT mit dem korrekten Zeitfilter
select
    league_id_new,
    home_team_id_new,
    away_team_id_new,
    "League", "Away Team", "Home Team", "Date", "Time (CET)", "End Time (CET)", "Is Playoff",
    (league_pts + favorite_pts + playoff_pts + derby_pts + record_pts + time_pts) as total_watch_score,
    case when is_favorite = 1 then '⭐ Favorite ' else '' end || 
    case when is_derby_game = 1 then '🔥 DERBY ' else '' end ||
    case when "Is Playoff" = true then '🏆 Playoff ' else '' end as tags
from calculated_scores
where 
    -- Datums-Filter (Heute und Morgen früh)
    (
        "Date" = (CURRENT_TIMESTAMP AT TIME ZONE 'EUROPE/VIENNA')::date
        or
        (
            "Date" = ((CURRENT_TIMESTAMP AT TIME ZONE 'EUROPE/VIENNA') + interval '1 day')::date 
            and "Time (CET)" < '05:00'
        )
    )
    -- Zeit-Filter (Keine Spiele zwischen 02:00 und 10:00 morgens)
    and ("Time (CET)" < '02:00' or "Time (CET)" > '10:00')
order by total_watch_score desc