{{ config(
    materialized='table'
) }}

-- define dimensions
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

-- add other sources with similar structure, ensuring to cast date and time fields appropriately and to set "Is Playoff" to false if not available
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

-- 4. add IDs via Join
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
            when ("Home Team" = 'Manchester United' and "Away Team" = 'Liverpool') or ("Home Team" = 'Liverpool' and "Away Team" = 'Manchester United') then 1
            when ("Home Team" = 'Bayern Munich' and "Away Team" = 'Borussia Dortmund') or ("Home Team" = 'Borussia Dortmund' and "Away Team" = 'Bayern Munich') then 1
            when ("Home Team" = 'AC Milan' and "Away Team" = 'Inter Milan') or ("Home Team" = 'Inter Milan' and "Away Team" = 'AC Milan') then 1
            when ("Home Team" = 'Juventus' and "Away Team" = 'Torino') or ("Home Team" = 'Torino' and "Away Team" = 'Juventus') then 1
            when ("Home Team" = 'Paris Saint-Germain' and "Away Team" = 'Olympique de Marseille') or ("Home Team" = 'Olympique de Marseille' and "Away Team" = 'Paris Saint-Germain') then 1
            when ("Home Team" = 'New York Yankees' and "Away Team" = 'Boston Red Sox') or ("Home Team" = 'Boston Red Sox' and "Away Team" = 'New York Yankees') then 1
            when ("Home Team" = 'Miami Heat' and "Away Team" = 'Boston Celtics') or ("Home Team" = 'Boston Celtics' and "Away Team" = 'Miami Heat') then 1
            when ("Home Team" = 'Los Angeles Dodgers' and "Away Team" = 'San Diego Padres') or ("Home Team" = 'San Diego Padres' and "Away Team" = 'Los Angeles Dodgers') then 1
            when ("Home Team" = 'Arsenal' and "Away Team" = 'Tottenham Hotspur') or ("Home Team" = 'Tottenham Hotspur' and "Away Team" = 'Arsenal') then 1
            when ("Home Team" = 'Manchester City' and "Away Team" = 'Manchester United') or ("Home Team" = 'Manchester United' and "Away Team" = 'Manchester City') then 1
            when ("Home Team" = 'Chelsea' and "Away Team" = 'Arsenal') or ("Home Team" = 'Arsenal' and "Away Team" = 'Chelsea') then 1
            when ("Home Team" = 'Borussia Dortmund' and "Away Team" = 'Schalke 04') or ("Home Team" = 'Schalke 04' and "Away Team" = 'Borussia Dortmund') then 1
            when ("Home Team" = 'Real Madrid' and "Away Team" = 'Atletico Madrid') or ("Home Team" = 'Atletico Madrid' and "Away Team" = 'Real Madrid') then 1
            when ("Home Team" = 'Liverpool' and "Away Team" = 'Everton') or ("Home Team" = 'Everton' and "Away Team" = 'Liverpool') then 1
            when ("Home Team" = 'Barcelona' and "Away Team" = 'Real Madrid') or ("Home Team" = 'Real Madrid' and "Away Team" = 'Barcelona') then 1
            when ("Home Team" = 'Partizan' and "Away Team" = 'Crvena zvezda') or ("Home Team" = 'Crvena zvezda' and "Away Team" = 'Partizan') then 1
            when ("Home Team" = 'Fenerbahce' and "Away Team" = 'Galatasaray') or ("Home Team" = 'Galatasaray' and "Away Team" = 'Fenerbahce') then 1
            when ("Home Team" = 'Fenerbahce' and "Away Team" = 'Besiktas') or ("Home Team" = 'Besiktas' and "Away Team" = 'Fenerbahce') then 1
            when ("Home Team" = 'Galatasaray' and "Away Team" = 'Besiktas') or ("Home Team" = 'Besiktas' and "Away Team" = 'Galatasaray') then 1
            when ("Home Team" = 'Borussia Dortmund' and "Away Team" = 'Bayern Munich') or ("Home Team" = 'Bayern Munich' and "Away Team" = 'Borussia Dortmund') then 1
            when ("Home Team" = 'Boca Juniors' and "Away Team" = 'River Plate') or ("Home Team" = 'River Plate' and "Away Team" = 'Boca Juniors') then 1
            when ("Home Team" = 'Celtic' and "Away Team" = 'Rangers') or ("Home Team" = 'Rangers' and "Away Team" = 'Celtic') then 1
            when ("Home Team" = 'St. Pauli' and "Away Team" = 'Hamburg SV') or ("Home Team" = 'Hamburg SV' and "Away Team" = 'St. Pauli') then 1
            when ("Home Team" = 'AS Roma' and "Away Team" = 'Lazio') or ("Home Team" = 'Lazio' and "Away Team" = 'AS Roma') then 1
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

-- 6. Final SELECT 
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
    -- Date-Filter (Today and tomorrow morning)
    (
        "Date" = (CURRENT_TIMESTAMP AT TIME ZONE 'EUROPE/VIENNA')::date
        or
        (
            "Date" = ((CURRENT_TIMESTAMP AT TIME ZONE 'EUROPE/VIENNA') + interval '1 day')::date 
            and "Time (CET)" < '05:00'
        )
    )
    -- no games between 2:00 and 10:00 am
    and ("Time (CET)" < '02:00' or "Time (CET)" > '10:00')
order by total_watch_score desc