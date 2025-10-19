CREATE TABLE silver.accounts_extended AS
 WITH session_percentiles AS (
        SELECT
          quantile(user_sessions.session_count::DOUBLE, 0.33) AS p33_sessions,
          quantile(user_sessions.session_count::DOUBLE, 0.66) AS p66_sessions,
          quantile(user_sessions.avg_duration::DOUBLE, 0.33) AS p33_duration,
          quantile(user_sessions.avg_duration::DOUBLE, 0.66) AS p66_duration
        FROM (
          SELECT
            sessions.account_id,
            COUNT(*) AS session_count,
            ROUND(AVG(sessions.duration_seconds)) AS avg_duration
          FROM bronze.sessions
          GROUP BY sessions.account_id
        ) AS user_sessions
), weekly_aggregated_sessions AS (
         SELECT sessions.account_id,
            date_trunc('week'::text, sessions.session_start) AS week_start_date,
            count(*) AS session_count_weekly,
            sum(sessions.duration_seconds) AS total_session_duration_weekly,
            round(avg(sessions.duration_seconds)) AS avg_session_duration_weekly,
            row_number() OVER (PARTITION BY sessions.account_id ORDER BY (date_trunc('week'::text, sessions.session_start)) DESC) AS row_num
           FROM bronze.sessions
          GROUP BY sessions.account_id, (date_trunc('week'::text, sessions.session_start))
        ), latest_week AS (
         SELECT ws.account_id,
            ws.week_start_date,
            ws.session_count_weekly,
            ws.total_session_duration_weekly,
            ws.avg_session_duration_weekly,
                CASE
                    WHEN (((ws.session_count_weekly)::double precision <= session_percentiles.p33_sessions) AND ((ws.avg_session_duration_weekly)::double precision <= session_percentiles.p33_duration)) THEN 'casual'::text
                    WHEN ((((ws.session_count_weekly)::double precision > session_percentiles.p33_sessions) AND ((ws.session_count_weekly)::double precision <= session_percentiles.p66_sessions)) OR (((ws.avg_session_duration_weekly)::double precision > session_percentiles.p33_duration) AND ((ws.avg_session_duration_weekly)::double precision <= session_percentiles.p66_duration))) THEN 'core'::text
                    WHEN (((ws.session_count_weekly)::double precision > session_percentiles.p66_sessions) OR ((ws.avg_session_duration_weekly)::double precision > session_percentiles.p66_duration)) THEN 'hardcore'::text
                    ELSE 'unknown'::text
                END AS engagement_segment,
                CASE
                    WHEN (('2025-03-31 00:00:00+03'::timestamp with time zone - ws.week_start_date) >= '28 days'::interval) THEN 'churned'::text
                    WHEN (('2025-03-31 00:00:00+03'::timestamp with time zone - ws.week_start_date) >= '14 days'::interval) THEN 'dormant'::text
                    ELSE 'active'::text
                END AS churn_segment
           FROM (weekly_aggregated_sessions ws
             CROSS JOIN session_percentiles)
          WHERE (ws.row_num = 1)
        ), progression_agg AS (
         SELECT level_data.account_id,
            max(level_data.last_level) AS last_completed_level,
                CASE
                    WHEN (max(level_data.last_level) = 100) THEN 'completed'::text
                    WHEN (max(level_data.last_level) > 80) THEN 'endgame'::text
                    WHEN (max(level_data.last_level) > 50) THEN 'late_stage'::text
                    WHEN (max(level_data.last_level) > 20) THEN 'mid_stage'::text
                    ELSE 'starter'::text
                END AS completion_segment
           FROM ( SELECT events.account_id,
                    (split_part((events.event_metadata ->> 'level_id'::text), '_'::text, 2))::integer AS last_level
                   FROM bronze.events
                  WHERE ((events.event_subtype)::text = 'level_success'::text)) level_data
          GROUP BY level_data.account_id
        ), aggregated_sessions AS (
         SELECT sessions.account_id,
            count(*) AS session_count,
            sum(sessions.duration_seconds) AS total_duration,
            count(*) FILTER (WHERE ((sessions.end_reason)::text <> 'player_exit'::text)) AS error_count
           FROM bronze.sessions
          GROUP BY sessions.account_id
        ), initial_account_data AS (
         SELECT DISTINCT ON (sessions.account_id) sessions.account_id,
            sessions.region AS initial_country,
            sessions.platform AS initial_platform,
            sessions.device_model AS initial_device,
            sessions.os_version AS initial_os,
            sessions.app_version AS initial_app_version,
            sessions.session_start AS initial_session
           FROM bronze.sessions
          ORDER BY sessions.account_id, sessions.session_start
        ), current_account_data AS (
         SELECT DISTINCT ON (sessions.account_id) sessions.account_id,
            sessions.region AS current_country,
            sessions.platform AS current_platform,
            sessions.device_model AS current_device,
            sessions.os_version AS current_os,
            sessions.app_version AS current_app_version,
            sessions.session_start AS latest_session
           FROM bronze.sessions
          ORDER BY sessions.account_id, sessions.session_start DESC
        ), ext_ad_revenue AS (
         SELECT hosted_ad_interactions.account_id,
            sum(hosted_ad_interactions.revenue) AS ad_rev
           FROM bronze.hosted_ad_interactions
          GROUP BY hosted_ad_interactions.account_id
        ), business_usd_converted AS (
         SELECT ev.account_id,
            ev.event_date,
            (ev.event_metadata ->> 'offer_id'::text) AS offer_name,
            (ev.event_metadata ->> 'currency_name'::text) AS currency_name,
            (ev.event_metadata ->> 'cost_amount'::text) AS rev_amount,
            COALESCE(er.usd_exchange_rate, (1)::numeric) AS "coalesce",
            round((((ev.event_metadata ->> 'cost_amount'::text))::numeric / COALESCE(er.usd_exchange_rate, (1)::numeric)), 2) AS converted_rev
           FROM (bronze.events ev
             LEFT JOIN bronze.exchange_rate er ON (((ev.event_metadata ->> 'currency_name'::text) = er.currency)))
          WHERE ((ev.event_type)::text = 'business'::text)
        ), purchase_metrics_base AS (
         SELECT business_usd_converted.account_id,
            min(date(business_usd_converted.event_date)) AS first_purchase_date,
            max(date(business_usd_converted.event_date)) AS last_purchase_date,
            count(*) AS number_of_purchases,
            COALESCE(mode() WITHIN GROUP (ORDER BY business_usd_converted.offer_name), 'none'::text) AS most_purchased_item,
            sum(business_usd_converted.converted_rev) AS total_purchase,
            count(*) FILTER (WHERE (business_usd_converted.offer_name ~~ '%subscription%'::text)) AS subscription_purchases,
            max(date(business_usd_converted.event_date)) FILTER (WHERE (business_usd_converted.offer_name ~~ '%subscription%'::text)) AS last_sub_date
           FROM business_usd_converted
          GROUP BY business_usd_converted.account_id
        ), resource_unnested AS (
         SELECT e.event_id,
            e.session_id,
            e.account_id,
            e.event_type,
            e.event_subtype,
            e.event_date,
                CASE
                    WHEN (m.reason ~~ 'LEVEL_%'::text) THEN 'level_progression'::text
                    WHEN (m.reason ~~ 'TUTORIAL_%'::text) THEN 'tutorial_progression'::text
                    ELSE m.reason
                END AS reason,
            m.item_id,
            m.item_amount,
            m.item_category
           FROM bronze.events e
            CROSS JOIN LATERAL (
              SELECT
                json_extract_string(e.event_metadata, '$.reason')        AS reason,
                json_extract_string(e.event_metadata, '$.item_id')       AS item_id,
                CAST(json_extract(e.event_metadata, '$.item_amount') AS INTEGER) AS item_amount,
                json_extract_string(e.event_metadata, '$.item_category') AS item_category
            ) AS m
          WHERE (e.event_type)::text = 'resource'::text
        ), gold_source_json AS (
  SELECT account_id,
         json_group_object(reason, total_gold) AS gold_source_distribution
  FROM (
    SELECT account_id,
           reason,
           SUM(item_amount) AS total_gold
    FROM resource_unnested
    WHERE event_subtype LIKE 'source%'
      AND item_id LIKE '%gold'
    GROUP BY account_id, reason
    HAVING SUM(item_amount) > 0
    ORDER BY account_id, SUM(item_amount) DESC
  )
  GROUP BY account_id
),
gold_sink_json AS (
  SELECT account_id,
         json_group_object(reason, total_gold) AS gold_sink_distribution
  FROM (
    SELECT account_id,
           reason,
           SUM(item_amount) AS total_gold
    FROM resource_unnested
    WHERE event_subtype LIKE 'sink%'
      AND item_id LIKE '%gold'
    GROUP BY account_id, reason
    HAVING SUM(item_amount) > 0
    ORDER BY account_id, SUM(item_amount) DESC
  )
  GROUP BY account_id
),
gold_json AS (
  SELECT COALESCE(gs.account_id, gk.account_id) AS account_id,
         gs.gold_source_distribution,
         gk.gold_sink_distribution
  FROM gold_source_json gs
  FULL OUTER JOIN gold_sink_json gk USING (account_id)
),

-- DIAMOND distributions (split source/sink, then stitch)
diamond_source_json AS (
  SELECT account_id,
         json_group_object(reason, total_diamond) AS diamond_source_distribution
  FROM (
    SELECT account_id,
           reason,
           SUM(item_amount) AS total_diamond
    FROM resource_unnested
    WHERE event_subtype LIKE 'source%'
      AND item_id LIKE '%diamond'
    GROUP BY account_id, reason
    HAVING SUM(item_amount) > 0
    ORDER BY account_id, SUM(item_amount) DESC
  )
  GROUP BY account_id
),
diamond_sink_json AS (
  SELECT account_id,
         json_group_object(reason, total_diamond) AS diamond_sink_distribution
  FROM (
    SELECT account_id,
           reason,
           SUM(item_amount) AS total_diamond
    FROM resource_unnested
    WHERE event_subtype LIKE 'sink%'
      AND item_id LIKE '%diamond'
    GROUP BY account_id, reason
    HAVING SUM(item_amount) > 0
    ORDER BY account_id, SUM(item_amount) DESC
  )
  GROUP BY account_id
),
diamond_json AS (
  SELECT COALESCE(ds.account_id, dk.account_id) AS account_id,
         ds.diamond_source_distribution,
         dk.diamond_sink_distribution
  FROM diamond_source_json ds
  FULL OUTER JOIN diamond_sink_json dk USING (account_id)
), source_aggregation AS (
         SELECT resource_unnested.account_id,
            count(resource_unnested.event_id) FILTER (WHERE (resource_unnested.item_id ~~ '%gold'::text)) AS gold_source_event_count,
            count(DISTINCT resource_unnested.session_id) FILTER (WHERE (resource_unnested.item_id ~~ '%gold'::text)) AS gold_source_session_count,
            sum(resource_unnested.item_amount) FILTER (WHERE (resource_unnested.item_id ~~ '%gold'::text)) AS total_gold_earned,
            mode() WITHIN GROUP (ORDER BY resource_unnested.reason) FILTER (WHERE (resource_unnested.item_id ~~ '%gold'::text)) AS top_gold_earn_reason,
            count(resource_unnested.event_id) FILTER (WHERE (resource_unnested.item_id ~~ '%diamond'::text)) AS diamond_source_event_count,
            count(DISTINCT resource_unnested.session_id) FILTER (WHERE (resource_unnested.item_id ~~ '%diamond'::text)) AS diamond_source_session_count,
            sum(resource_unnested.item_amount) FILTER (WHERE (resource_unnested.item_id ~~ '%diamond'::text)) AS total_diamond_earned,
            mode() WITHIN GROUP (ORDER BY resource_unnested.reason) FILTER (WHERE (resource_unnested.item_id ~~ '%diamond'::text)) AS top_diamond_earn_reason
           FROM resource_unnested
          WHERE ((resource_unnested.event_subtype)::text ~~ 'source%'::text)
          GROUP BY resource_unnested.account_id
        ), sink_aggregation AS (
         SELECT resource_unnested.account_id,
            count(resource_unnested.event_id) FILTER (WHERE (resource_unnested.item_id ~~ '%gold'::text)) AS gold_spend_event_count,
            count(DISTINCT resource_unnested.session_id) FILTER (WHERE (resource_unnested.item_id ~~ '%gold'::text)) AS gold_spend_session_count,
            COALESCE(sum(resource_unnested.item_amount) FILTER (WHERE (resource_unnested.item_id ~~ '%gold'::text)), (0)::bigint) AS total_gold_spent,
            mode() WITHIN GROUP (ORDER BY resource_unnested.reason) FILTER (WHERE (resource_unnested.item_id ~~ '%gold'::text)) AS top_gold_spend_reason,
            count(resource_unnested.event_id) FILTER (WHERE (resource_unnested.item_id ~~ '%diamond'::text)) AS diamond_spend_event_count,
            count(DISTINCT resource_unnested.session_id) FILTER (WHERE (resource_unnested.item_id ~~ '%diamond'::text)) AS diamond_spend_session_count,
            COALESCE(sum(resource_unnested.item_amount) FILTER (WHERE (resource_unnested.item_id ~~ '%diamond'::text)), (0)::bigint) AS total_diamond_spent,
            mode() WITHIN GROUP (ORDER BY resource_unnested.reason) FILTER (WHERE (resource_unnested.item_id ~~ '%diamond'::text)) AS top_diamond_spend_reason
           FROM resource_unnested
          WHERE ((resource_unnested.event_subtype)::text ~~ 'sink%'::text)
          GROUP BY resource_unnested.account_id
        ), segmentation_data AS (
         SELECT l.account_id,
            l.week_start_date,
            l.session_count_weekly,
            l.total_session_duration_weekly,
            l.avg_session_duration_weekly,
            l.engagement_segment,
            l.churn_segment,
            p_1.completion_segment,
            p_1.last_completed_level
           FROM (latest_week l
             LEFT JOIN progression_agg p_1 ON ((l.account_id = p_1.account_id)))
        ), retention_7d_check AS (
         SELECT a_1.account_id,
            true AS retained_within_7_days
           FROM (bronze.accounts a_1
             JOIN bronze.sessions s_1 ON ((a_1.account_id = s_1.account_id)))
          WHERE ((date(s_1.session_start) > a_1.signup_date) AND (date(s_1.session_start) <= (a_1.signup_date + '7 days'::interval)))
          GROUP BY a_1.account_id
        ), retention_30d_check AS (
         SELECT a_1.account_id,
            true AS retained_within_30_days
           FROM (bronze.accounts a_1
             JOIN bronze.sessions s_1 ON ((a_1.account_id = s_1.account_id)))
          WHERE ((date(s_1.session_start) > (a_1.signup_date + '7 days'::interval)) AND (date(s_1.session_start) <= (a_1.signup_date + '30 days'::interval)))
          GROUP BY a_1.account_id
        ), monetization_7d_check AS (
         SELECT a_1.account_id,
            true AS monetized_within_7_days
           FROM (bronze.accounts a_1
             JOIN bronze.events e ON ((a_1.account_id = e.account_id)))
          WHERE (((e.event_type)::text = 'business'::text) AND (date(e.event_date) >= a_1.signup_date) AND (date(e.event_date) <= (a_1.signup_date + '7 days'::interval)))
          GROUP BY a_1.account_id
        )
 SELECT agg.account_id,
    agg.session_count,
    agg.total_duration,
    agg.error_count,
    c.current_country,
    c.current_platform,
    c.current_device,
    c.current_os,
    c.current_app_version,
    c.latest_session,
    COALESCE(r7.retained_within_7_days, false) AS retained_within_7_days,
    COALESCE(r30.retained_within_30_days, false) AS retained_within_30_days,
    a.username,
    a.email,
    a.email_is_anonymized,
    a.signup_date,
    a.referral_source,
    a.creation_method,
    COALESCE((a.acquisition_metadata ->> 'search_query'::text), 'N/A'::text) AS search_query,
    COALESCE((a.acquisition_metadata ->> 'ad_name'::text), 'N/A'::text) AS ad_name,
    COALESCE((a.acquisition_metadata ->> 'campaign_name'::text), 'N/A'::text) AS campaign_name,
    COALESCE((a.acquisition_metadata ->> 'search_ad_keyword'::text), 'N/A'::text) AS search_ad_keyword,
    COALESCE((a.acquisition_metadata ->> 'referral_code'::text), 'N/A'::text) AS referral_code,
    COALESCE((a.acquisition_metadata ->> 'referral_account_id'::text), 'N/A'::text) AS referral_account_id,
    COALESCE(adrev.ad_rev, (0)::numeric) AS ad_rev,
    p.first_purchase_date,
    p.last_purchase_date,
    COALESCE(m7.monetized_within_7_days, false) AS monetized_within_7_days,
    ('2025-04-01'::date - p.last_purchase_date) AS purchase_recency_days,
    p.number_of_purchases,
    p.most_purchased_item,
    p.total_purchase,
    COALESCE(round((p.total_purchase / (NULLIF(p.number_of_purchases, 0))::numeric), 2), (0)::numeric) AS avg_purchase_value,
    s.last_completed_level,
        CASE
            WHEN (a.referral_source IS NULL) THEN 'direct'::text
            WHEN ((a.referral_source)::text = ANY ((ARRAY['ad_fb'::character varying, 'ad_gdn'::character varying, 'ad_ig'::character varying, 'ad_in_app'::character varying, 'ad_tk'::character varying, 'ad_x'::character varying, 'ad_yt'::character varying])::text[])) THEN 'paid_social'::text
            WHEN ((a.referral_source)::text = 'ad_search_engine'::text) THEN 'paid_search'::text
            WHEN ((a.referral_source)::text = 'friend_referral'::text) THEN 'referral'::text
            WHEN ((a.referral_source)::text = 'organic_search'::text) THEN 'organic'::text
            ELSE 'unknown'::text
        END AS acquisition_segment,
        CASE
            WHEN ((p.total_purchase < (1)::numeric) OR (p.total_purchase IS NULL)) THEN 'minnow'::text
            WHEN ((p.total_purchase >= (1)::numeric) AND (p.total_purchase < (100)::numeric)) THEN 'dolphin'::text
            WHEN ((p.total_purchase >= (100)::numeric) AND (p.total_purchase < (500)::numeric)) THEN 'whale'::text
            ELSE 'kraken'::text
        END AS monetization_segment,
    s.engagement_segment,
    s.churn_segment,
    s.completion_segment,
        CASE
            WHEN ((p.last_sub_date + 30) >= '2025-04-01'::date) THEN 'active'::text
            WHEN ((p.last_sub_date + 30) < '2025-04-01'::date) THEN 'inactive'::text
            ELSE 'never'::text
        END AS subscription_status,
    p.subscription_purchases,
    (COALESCE(p.total_purchase, (0)::numeric) + COALESCE(adrev.ad_rev, (0)::numeric)) AS ltv,
    sr.gold_source_event_count,
    sr.gold_source_session_count,
    sr.total_gold_earned,
    sr.top_gold_earn_reason,
    sr.total_diamond_earned,
    sr.top_diamond_earn_reason,
    COALESCE(sk.total_gold_spent, (0)::bigint) AS total_gold_spent,
    COALESCE(sk.top_gold_spend_reason, 'None'::text) AS top_gold_spend_reason,
    COALESCE(sk.total_diamond_spent, (0)::bigint) AS total_diamond_spent,
    COALESCE(sk.top_diamond_spend_reason, 'None'::text) AS top_diamond_spend_reason,
    COALESCE((sr.total_gold_earned - sk.total_gold_spent), sr.total_gold_earned) AS current_gold,
    COALESCE((sr.total_diamond_earned - sk.total_diamond_spent), sr.total_diamond_earned) AS current_diamond,
    COALESCE(g.gold_source_distribution, '{}'::jsonb) AS gold_source_distribution,
    COALESCE(g.gold_sink_distribution, '{}'::jsonb) AS gold_sink_distribution,
    COALESCE(d.diamond_source_distribution, '{}'::jsonb) AS diamond_source_distribution,
    COALESCE(d.diamond_sink_distribution, '{}'::jsonb) AS diamond_sink_distribution
   FROM ((((((((((((aggregated_sessions agg
     LEFT JOIN current_account_data c ON ((agg.account_id = c.account_id)))
     LEFT JOIN bronze.accounts a ON ((agg.account_id = a.account_id)))
     LEFT JOIN ext_ad_revenue adrev ON ((agg.account_id = adrev.account_id)))
     LEFT JOIN purchase_metrics_base p ON ((agg.account_id = p.account_id)))
     LEFT JOIN source_aggregation sr ON ((agg.account_id = sr.account_id)))
     LEFT JOIN sink_aggregation sk ON ((agg.account_id = sk.account_id)))
     LEFT JOIN gold_json g ON ((agg.account_id = g.account_id)))
     LEFT JOIN diamond_json d ON ((agg.account_id = d.account_id)))
     LEFT JOIN segmentation_data s ON ((agg.account_id = s.account_id)))
     LEFT JOIN retention_7d_check r7 ON ((agg.account_id = r7.account_id)))
     LEFT JOIN retention_30d_check r30 ON ((agg.account_id = r30.account_id)))
     LEFT JOIN monetization_7d_check m7 ON ((agg.account_id = m7.account_id)))
  ;