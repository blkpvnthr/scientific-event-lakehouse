-- Depth and magnitude relationship (conditional aggregation by depth category).
SELECT
    CASE
        WHEN depth_km < 0   THEN 'above_datum'
        WHEN depth_km < 70  THEN 'shallow'
        WHEN depth_km < 300 THEN 'intermediate'
        ELSE 'deep'
    END                            AS depth_category,
    count(*)                       AS event_count,
    round(avg(magnitude), 3)       AS avg_magnitude,
    max(magnitude)                 AS max_magnitude,
    round(avg(depth_km), 2)        AS avg_depth_km
FROM events_current
GROUP BY 1
ORDER BY event_count DESC;
