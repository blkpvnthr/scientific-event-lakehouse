-- Missing optional scientific attributes (conditional aggregation of nulls).
SELECT
    count(*)                                                     AS total_events,
    sum(CASE WHEN magnitude_type IS NULL THEN 1 ELSE 0 END)     AS missing_magnitude_type,
    sum(CASE WHEN felt IS NULL THEN 1 ELSE 0 END)               AS missing_felt,
    sum(CASE WHEN cdi IS NULL THEN 1 ELSE 0 END)                AS missing_cdi,
    sum(CASE WHEN mmi IS NULL THEN 1 ELSE 0 END)                AS missing_mmi,
    sum(CASE WHEN station_count IS NULL THEN 1 ELSE 0 END)      AS missing_station_count,
    sum(CASE WHEN azimuthal_gap IS NULL THEN 1 ELSE 0 END)      AS missing_azimuthal_gap
FROM events_current;
