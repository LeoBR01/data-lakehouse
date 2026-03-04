-- marts/mart_repo_stats.sql
-- Tabela final com ranking de repositorios mais ativos

WITH repo_activity AS (
    SELECT * FROM {{ ref('int_repo_activity') }}
),

final AS (
    SELECT
        repo_name,
        event_date,
        total_events,
        push_count,
        pr_count,
        issues_count,
        stars_count,
        forks_count,
        unique_contributors,

        -- Metricas derivadas
        ROUND(push_count * 100.0 / NULLIF(total_events, 0), 1)  AS push_pct,
        RANK() OVER (
            PARTITION BY event_date
            ORDER BY total_events DESC
        )                                                         AS daily_rank

    FROM repo_activity
)

SELECT * FROM final