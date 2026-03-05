WITH events AS (
    SELECT *
    FROM {{ ref('stg_github_events') }}
    WHERE actor_login NOT LIKE '%bot%'
),

aggregated AS (
    SELECT
        repo_name,
        event_date,
        COUNT(*)                                                    AS total_events,
        COUNT(*) FILTER (WHERE event_type = 'PushEvent')            AS push_count,
        COUNT(*) FILTER (WHERE event_type = 'PullRequestEvent')     AS pr_count,
        COUNT(*) FILTER (WHERE event_type = 'IssuesEvent')          AS issues_count,
        COUNT(*) FILTER (WHERE event_type = 'WatchEvent')           AS stars_count,
        COUNT(*) FILTER (WHERE event_type = 'ForkEvent')            AS forks_count,
        COUNT(DISTINCT actor_login)                                 AS unique_contributors
    FROM events
    GROUP BY repo_name, event_date
),

filtered AS (
    SELECT *
    FROM aggregated
    WHERE total_events < 500
)

SELECT * FROM filtered




