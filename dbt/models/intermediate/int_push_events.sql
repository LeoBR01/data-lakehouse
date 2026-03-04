-- intermediate/int_push_events.sql
-- Extrai e enriquece eventos de Push com detalhes dos commits

WITH push_events AS (
    SELECT *
    FROM {{ ref('stg_github_events') }}
    WHERE event_type = 'PushEvent'
),

enriched AS (
    SELECT
        event_id,
        actor_login,
        repo_name,
        org_login,
        created_at,
        event_date,
        event_hour,

        -- Extrai owner e repo separados
        SPLIT_PART(repo_name, '/', 1)   AS repo_owner,
        SPLIT_PART(repo_name, '/', 2)   AS repo_short_name

    FROM push_events
)

SELECT * FROM enriched