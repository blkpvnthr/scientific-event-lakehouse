-- Monthly earthquake counts.
SELECT
    event_year,
    month(event_time)              AS month,
    count(*)                       AS event_count,
    round(avg(magnitude), 3)       AS avg_magnitude
FROM events_current
GROUP BY event_year, month(event_time)
ORDER BY event_year, month;
