-- Geographic concentration by 10-degree grid cell (CTE + ranking window).
WITH cells AS (
    SELECT
        concat(
            'lat', cast(floor(latitude / 10) * 10 AS int),
            '_lon', cast(floor(longitude / 10) * 10 AS int)
        )                          AS region_key,
        count(*)                   AS event_count,
        round(avg(magnitude), 3)   AS avg_magnitude
    FROM events_current
    GROUP BY 1
)
SELECT
    region_key,
    event_count,
    avg_magnitude,
    rank() OVER (ORDER BY event_count DESC) AS concentration_rank
FROM cells
ORDER BY concentration_rank
LIMIT 20;
