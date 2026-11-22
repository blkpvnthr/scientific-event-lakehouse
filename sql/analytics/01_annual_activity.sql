-- Annual earthquake activity (partitioned aggregation).
SELECT
    event_year,
    count(*)                       AS event_count,
    round(avg(magnitude), 3)       AS avg_magnitude,
    max(magnitude)                 AS max_magnitude,
    round(avg(depth_km), 3)        AS avg_depth_km,
    sum(tsunami)                   AS tsunami_count
FROM events_current
GROUP BY event_year
ORDER BY event_year;
