-- Rolling 30-day seismic activity (range window over daily counts).
WITH daily AS (
    SELECT
        to_date(event_time)        AS event_date,
        count(*)                   AS event_count
    FROM events_current
    GROUP BY to_date(event_time)
)
SELECT
    event_date,
    event_count,
    sum(event_count) OVER (
        ORDER BY event_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )                              AS rolling_30d_count,
    round(avg(event_count) OVER (
        ORDER BY event_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ), 2)                          AS rolling_30d_avg
FROM daily
ORDER BY event_date;
