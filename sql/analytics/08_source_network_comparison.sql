-- Source network comparison with share of total (window over all rows).
SELECT
    network,
    count(*)                       AS event_count,
    round(avg(magnitude), 3)       AS avg_magnitude,
    round(max(magnitude), 1)       AS max_magnitude,
    round(count(*) / sum(count(*)) OVER () * 100, 3) AS share_pct
FROM events_current
GROUP BY network
ORDER BY event_count DESC;
