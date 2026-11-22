-- Observed source-revision frequency (how many revisions per event were seen).
WITH per_event AS (
    SELECT event_id, count(*) AS revision_count
    FROM event_revisions
    GROUP BY event_id
)
SELECT
    revision_count,
    count(*)                       AS events_with_this_many_revisions
FROM per_event
GROUP BY revision_count
ORDER BY revision_count;
