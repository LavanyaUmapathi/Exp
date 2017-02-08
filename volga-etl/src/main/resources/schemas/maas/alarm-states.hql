USE maas;
CREATE OR REPLACE VIEW alarm_states AS
SELECT
    m.event_id,
    m.alarm_id,
    cast(m.ts as timestamp) as start_ts,
    m.state,
    n.event_id as next_event_id,
    cast(n.ts as timestamp) as next_ts,
    n.state as next_state,
    (n.ts-m.ts)/60000 as duration_min
FROM
    notifications m  INNER JOIN
    notifications n
ON (m.alarm_id = n.alarm_id)
WHERE m.alarm_id is not null
AND m.ts < n.ts
AND m.state='critical'
AND (n.state='warning' or n.state='ok')
AND split(m.event_id,':')[4]=split(n.event_id,':')[4];