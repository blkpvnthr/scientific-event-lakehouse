-- Year-over-year change in event counts (lag window function).
WITH annual AS (
    SELECT event_year, count(*) AS event_count
    FROM events_current
    GROUP BY event_year
)
SELECT
    event_year,
    event_count,
    lag(event_count) OVER (ORDER BY event_year) AS prev_year_count,
    round(
        (event_count - lag(event_count) OVER (ORDER BY event_year))
        / lag(event_count) OVER (ORDER BY event_year) * 100, 2
    )                              AS yoy_change_pct
FROM annual
ORDER BY event_year;
