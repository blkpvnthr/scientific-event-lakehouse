-- High-magnitude earthquake identification (M >= 6.0).
SELECT
    event_id,
    event_time,
    magnitude,
    magnitude_type,
    place_normalized,
    latitude,
    longitude,
    depth_km,
    significance
FROM events_current
WHERE magnitude >= 6.0
ORDER BY magnitude DESC, event_time;
