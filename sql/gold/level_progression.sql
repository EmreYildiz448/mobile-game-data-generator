CREATE TABLE gold.level_progression AS
 WITH level_attempts AS (
         SELECT events.account_id,
            (events.event_metadata ->> 'level_id'::text) AS level_id,
            count(*) FILTER (WHERE ((events.event_subtype)::text = 'level_start'::text)) AS attempt_count,
            count(*) FILTER (WHERE ((events.event_subtype)::text = 'level_fail'::text)) AS fail_count,
            count(*) FILTER (WHERE ((events.event_subtype)::text = 'level_success'::text)) AS success_count
           FROM bronze.events
          WHERE (((events.event_type)::text = 'progression'::text) AND ((events.event_metadata ->> 'level_id'::text) !~~ '%TUTORIAL%'::text))
          GROUP BY events.account_id, (events.event_metadata ->> 'level_id'::text)
        ), level_data AS (
         SELECT level_attempts.level_id,
            sum(level_attempts.attempt_count) AS attempt_count,
            sum(level_attempts.success_count) AS success_count,
            sum(level_attempts.fail_count) AS fail_count,
            (sum(level_attempts.fail_count) - sum(level_attempts.fail_count) FILTER (WHERE (level_attempts.success_count > 0))) AS leavers,
            sum(level_attempts.fail_count) FILTER (WHERE (level_attempts.success_count > 0)) AS resolved_fail_count,
            round((sum(level_attempts.fail_count) FILTER (WHERE (level_attempts.success_count > 0)) / (count(level_attempts.account_id))::numeric), 2) AS avg_fail
           FROM level_attempts
          GROUP BY level_attempts.level_id
        )
 SELECT level_id,
    attempt_count,
    success_count,
    fail_count,
    resolved_fail_count,
    leavers,
    (attempt_count - (success_count + fail_count)) AS error_count,
    round(((success_count / attempt_count) * (100)::numeric), 2) AS completion_rate,
    round(((fail_count / attempt_count) * (100)::numeric), 2) AS fail_pct,
    round(((attempt_count - (success_count + fail_count)) / attempt_count), 3) AS error_pct,
    round(((resolved_fail_count / attempt_count) * (100)::numeric), 2) AS resolved_fail_pct,
    avg_fail
   FROM level_data l
  ORDER BY level_id
  ;