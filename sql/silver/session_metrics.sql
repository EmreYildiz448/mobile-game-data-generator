CREATE TABLE silver.session_metrics AS
WITH session_metrics_base AS (
  SELECT
    DATE(sessions.session_start) AS session_day,
    CASE strftime('%w', DATE(sessions.session_start))
      WHEN '0' THEN 'Sun'
      WHEN '1' THEN 'Mon'
      WHEN '2' THEN 'Tue'
      WHEN '3' THEN 'Wed'
      WHEN '4' THEN 'Thu'
      WHEN '5' THEN 'Fri'
      WHEN '6' THEN 'Sat'
    END AS day_of_week,
    AVG(EXTRACT(hour FROM sessions.session_start)) AS avg_hour_of_day,
    COUNT(*) AS total_sessions,
    COUNT(DISTINCT sessions.account_id) AS unique_players,
    ROUND(COUNT(*)::NUMERIC / COUNT(DISTINCT sessions.account_id)::NUMERIC, 2) AS avg_session_per_player,
    ROUND(AVG(sessions.duration_seconds), 2) AS avg_session_duration,
    quantile(sessions.duration_seconds::DOUBLE, 0.10)::NUMERIC AS p10_ses_dur,
    quantile(sessions.duration_seconds::DOUBLE, 0.25)          AS p25_ses_dur,
    median(sessions.duration_seconds)                           AS mdn_ses_dur,
    quantile(sessions.duration_seconds::DOUBLE, 0.75)          AS p75_ses_dur,
    quantile(sessions.duration_seconds::DOUBLE, 0.90)::NUMERIC AS p90_ses_dur,
    mode() WITHIN GROUP (ORDER BY sessions.device_model) AS most_common_device,
    mode() WITHIN GROUP (ORDER BY sessions.end_reason)  AS most_common_end_reason,
    SUM(CASE WHEN sessions.platform = 'iOS'     THEN 1 ELSE 0 END) AS ios_count,
    SUM(CASE WHEN sessions.platform = 'Android' THEN 1 ELSE 0 END) AS android_count,
    COUNT(DISTINCT CASE WHEN sessions.platform = 'iOS'     THEN sessions.account_id END) AS ios_unique_accounts,
    COUNT(DISTINCT CASE WHEN sessions.platform = 'Android' THEN sessions.account_id END) AS android_unique_accounts
  FROM bronze.sessions
  GROUP BY
    DATE(sessions.session_start),
    CASE strftime('%w', DATE(sessions.session_start))
      WHEN '0' THEN 'Sun' WHEN '1' THEN 'Mon' WHEN '2' THEN 'Tue'
      WHEN '3' THEN 'Wed' WHEN '4' THEN 'Thu' WHEN '5' THEN 'Fri'
      WHEN '6' THEN 'Sat'
    END
), business_usd_converted AS (
         SELECT ev.account_id,
            ev.event_date,
            (ev.event_metadata ->> 'currency_name'::text) AS currency_name,
            (ev.event_metadata ->> 'cost_amount'::text) AS rev_amount,
            COALESCE(er.usd_exchange_rate, (1)::numeric) AS "coalesce",
            round((((ev.event_metadata ->> 'cost_amount'::text))::numeric / COALESCE(er.usd_exchange_rate, (1)::numeric)), 2) AS converted_rev
           FROM (bronze.events ev
             LEFT JOIN bronze.exchange_rate er ON (((ev.event_metadata ->> 'currency_name'::text) = er.currency)))
          WHERE ((ev.event_type)::text = 'business'::text)
        ), purchase_rev_per_day AS (
         SELECT date(business_usd_converted.event_date) AS session_day,
            sum(business_usd_converted.converted_rev) AS total_purchase_rev,
            count(DISTINCT business_usd_converted.account_id) AS purchasing_unique_accounts,
            count(*) AS total_purchase_events
           FROM business_usd_converted
          GROUP BY (date(business_usd_converted.event_date))
        ), ad_rev_per_day AS (
         SELECT date(hosted_ad_interactions.interaction_time) AS session_day,
            round(sum(hosted_ad_interactions.revenue), 3) AS total_ad_rev
           FROM bronze.hosted_ad_interactions
          GROUP BY (date(hosted_ad_interactions.interaction_time))
        ), session_action_counts AS (
         SELECT date(s_1.session_start) AS session_day,
            count(DISTINCT
                CASE
                    WHEN ((e.event_type)::text = 'business'::text) THEN s_1.session_id
                    ELSE NULL::integer
                END) AS sessions_with_purchase,
            count(DISTINCT
                CASE
                    WHEN (((e.event_type)::text = 'ad'::text) AND ((e.event_subtype)::text = ANY ((ARRAY['ad_shown'::character varying, 'reward_ad_shown'::character varying])::text[]))) THEN s_1.session_id
                    ELSE NULL::integer
                END) AS sessions_with_ads,
            count(DISTINCT
                CASE
                    WHEN (((e.event_type)::text = 'ad'::text) AND ((e.event_subtype)::text = ANY ((ARRAY['ad_shown'::character varying, 'reward_ad_shown'::character varying])::text[]))) THEN e.account_id
                    ELSE NULL
                END) AS adview_unique_accounts,
            count(
                CASE
                    WHEN (((e.event_type)::text = 'ad'::text) AND ((e.event_subtype)::text = ANY ((ARRAY['ad_shown'::character varying, 'reward_ad_shown'::character varying])::text[]))) THEN e.account_id
                    ELSE NULL
                END) AS total_ad_events
           FROM (bronze.sessions s_1
             LEFT JOIN bronze.events e ON ((s_1.session_id = e.session_id)))
          GROUP BY (date(s_1.session_start))
        ), first_last_session AS (
         SELECT sessions.account_id,
            min(date(sessions.session_start)) AS first_session_day,
            max(date(sessions.session_start)) AS last_session_day
           FROM bronze.sessions
          GROUP BY sessions.account_id
        ), session_day_accounts AS (
         SELECT date(sessions.session_start) AS session_day,
            sessions.account_id
           FROM bronze.sessions
        ), tagged_accounts AS (
         SELECT s_1.session_day,
            s_1.account_id,
                CASE
                    WHEN (s_1.session_day = f.first_session_day) THEN 1
                    ELSE 0
                END AS is_new,
                CASE
                    WHEN (s_1.session_day = f.last_session_day) THEN 1
                    ELSE 0
                END AS is_churned
           FROM (session_day_accounts s_1
             JOIN first_last_session f ON ((s_1.account_id = f.account_id)))
        ), account_tags_per_day AS (
         SELECT tagged_accounts.session_day,
            count(DISTINCT tagged_accounts.account_id) FILTER (WHERE (tagged_accounts.is_new = 1)) AS new_accounts,
            count(DISTINCT tagged_accounts.account_id) FILTER (WHERE (tagged_accounts.is_churned = 1)) AS churned_accounts,
            count(DISTINCT tagged_accounts.account_id) FILTER (WHERE ((tagged_accounts.is_new = 1) AND (tagged_accounts.is_churned = 1))) AS stopover_accounts,
            (((count(DISTINCT tagged_accounts.account_id) - count(DISTINCT tagged_accounts.account_id) FILTER (WHERE (tagged_accounts.is_new = 1))) - count(DISTINCT tagged_accounts.account_id) FILTER (WHERE (tagged_accounts.is_churned = 1))) + count(DISTINCT tagged_accounts.account_id) FILTER (WHERE ((tagged_accounts.is_new = 1) AND (tagged_accounts.is_churned = 1)))) AS retained_accounts
           FROM tagged_accounts
          GROUP BY tagged_accounts.session_day
        ), session_hours AS (
         SELECT date(sessions.session_start) AS session_day,
            (EXTRACT(hour FROM sessions.session_start))::integer AS hour_of_day
           FROM bronze.sessions
        ), hourly_counts AS (
         SELECT session_hours.session_day,
            session_hours.hour_of_day,
            count(*) AS session_count
           FROM session_hours
          GROUP BY session_hours.session_day, session_hours.hour_of_day
        ), peak_hours AS (
         SELECT DISTINCT ON (hourly_counts.session_day) hourly_counts.session_day,
            hourly_counts.hour_of_day AS peak_hour_of_day
           FROM hourly_counts
          ORDER BY hourly_counts.session_day, hourly_counts.session_count DESC
        )
 SELECT s.session_day,
    s.day_of_week,
    s.unique_players,
    apd.new_accounts,
    apd.churned_accounts,
    apd.stopover_accounts,
    apd.retained_accounts,
    s.total_sessions,
    pr.purchasing_unique_accounts,
    sc.sessions_with_purchase,
    pr.total_purchase_events,
    sc.adview_unique_accounts,
    sc.sessions_with_ads,
    sc.total_ad_events,
    round((((pr.purchasing_unique_accounts)::numeric / (s.unique_players)::numeric) * (100)::numeric), 2) AS pct_accounts_with_purchase,
    round((((sc.adview_unique_accounts)::numeric / (s.unique_players)::numeric) * (100)::numeric), 2) AS pct_accounts_with_ad_view,
    round((((sc.sessions_with_purchase)::numeric / (s.total_sessions)::numeric) * (100)::numeric), 2) AS pct_sessions_with_purchase,
    round((((sc.sessions_with_ads)::numeric / (s.total_sessions)::numeric) * (100)::numeric), 2) AS pct_sessions_with_ad_view,
    s.avg_session_per_player,
    s.avg_session_duration,
    round(s.avg_hour_of_day, 2) AS avg_hour_of_day,
    ph.peak_hour_of_day,
    s.p10_ses_dur,
    s.p25_ses_dur,
    s.mdn_ses_dur,
    s.p75_ses_dur,
    s.p90_ses_dur,
    s.ios_count,
    s.android_count,
    s.ios_unique_accounts,
    s.android_unique_accounts,
    round((((s.ios_unique_accounts)::numeric / (s.unique_players)::numeric) * (100)::numeric), 2) AS pct_ios_users,
    round((((s.android_unique_accounts)::numeric / (s.unique_players)::numeric) * (100)::numeric), 2) AS pct_android_users,
    s.most_common_device,
    s.most_common_end_reason,
    pr.total_purchase_rev,
    ar.total_ad_rev,
    (pr.total_purchase_rev + ar.total_ad_rev) AS total_rev,
    round((pr.total_purchase_rev / (s.unique_players)::numeric), 2) AS arpu_purchase,
    round((ar.total_ad_rev / (s.unique_players)::numeric), 3) AS arpu_ads,
    round(((pr.total_purchase_rev + ar.total_ad_rev) / (s.unique_players)::numeric), 2) AS arpu_total,
    round((pr.total_purchase_rev / (pr.purchasing_unique_accounts)::numeric), 2) AS arppu,
    round((pr.total_purchase_rev / (s.total_sessions)::numeric), 2) AS avg_purc_rev_per_ses,
    round((ar.total_ad_rev / (s.total_sessions)::numeric), 3) AS avg_ad_rev_per_ses,
    round(((pr.total_purchase_rev + ar.total_ad_rev) / (s.total_sessions)::numeric), 2) AS avg_total_rev_per_ses
   FROM (((((session_metrics_base s
     LEFT JOIN purchase_rev_per_day pr ON ((s.session_day = pr.session_day)))
     LEFT JOIN ad_rev_per_day ar ON ((s.session_day = ar.session_day)))
     LEFT JOIN session_action_counts sc ON ((s.session_day = sc.session_day)))
     LEFT JOIN account_tags_per_day apd ON ((s.session_day = apd.session_day)))
     LEFT JOIN peak_hours ph ON ((s.session_day = ph.session_day)))
  ORDER BY s.session_day
  ;