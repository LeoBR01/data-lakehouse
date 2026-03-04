WITH source AS (
    SELECT * FROM raw.github_events
),

deduplicated AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY created_at
    ) = 1
),

renamed AS (
    SELECT
        id                                          AS event_id,
        type                                        AS event_type,
        actor->>'login'                             AS actor_login,
        (actor->>'id')::BIGINT                      AS actor_id,
        repo->>'name'                               AS repo_name,
        (repo->>'id')::BIGINT                       AS repo_id,
        org->>'login'                               AS org_login,
        created_at,
        DATE_TRUNC('hour', created_at)              AS event_hour,
        created_at::DATE                            AS event_date,
        public
    FROM deduplicated
)

SELECT * FROM renamed