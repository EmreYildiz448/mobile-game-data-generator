CREATE TABLE analytics.ab_test_eda AS
 WITH base_sessions AS (
         SELECT sessions.session_id,
            sessions.account_id,
                CASE
                    WHEN ((sessions.app_version)::text = '{AB_TEST_VERSION}'::text) THEN 'test'::text
                    WHEN ((sessions.app_version)::text = '{CONTROL_VERSION}'::text) THEN 'control'::text
                    ELSE NULL::text
                END AS group_type,
            (sessions.session_start)::date AS session_date
           FROM bronze.sessions
          WHERE (sessions.session_start >= '{AB_START}'::date)
        ), base_events AS (
         SELECT e.session_id,
            e.event_type,
            (e.event_metadata ->> 'offer_id'::text) AS offer_id,
            ((e.event_metadata ->> 'cost_amount'::text))::numeric AS cost_amount,
            ((e.event_metadata ->> 'exchange_rate'::text))::numeric AS exchange_rate,
            round((((e.event_metadata ->> 'cost_amount'::text))::numeric / NULLIF(((e.event_metadata ->> 'exchange_rate'::text))::numeric, (0)::numeric)), 2) AS cost_usd
           FROM bronze.events e
          WHERE ((e.event_type)::text = 'business'::text)
        ), joined_data AS (
         SELECT s.group_type,
            s.account_id,
            s.session_id,
            e.offer_id,
            e.cost_usd
           FROM (base_sessions s
             LEFT JOIN base_events e ON ((s.session_id = e.session_id)))
          WHERE (s.group_type IS NOT NULL)
        ), metrics_cte AS (
         SELECT joined_data.account_id,
            joined_data.group_type,
            count(DISTINCT joined_data.session_id) AS session_count,
                CASE
                    WHEN ((sum(joined_data.cost_usd) IS NOT NULL) OR (sum(joined_data.cost_usd) <> (0)::numeric)) THEN true
                    ELSE false
                END AS converted,
            count(joined_data.offer_id) AS purchase_count,
            sum(joined_data.cost_usd) AS total_revenue,
            round((sum(joined_data.cost_usd) / (count(DISTINCT joined_data.session_id))::numeric), 2) AS avg_rev_per_ses,
            round((sum(joined_data.cost_usd) / (count(joined_data.offer_id))::numeric), 2) AS avg_rev_per_purc
           FROM joined_data
          GROUP BY joined_data.account_id, joined_data.group_type
        )
 SELECT mc.account_id,
    mc.group_type,
    ae.acquisition_segment,
    ae.current_country,
    mc.session_count,
    mc.converted,
    mc.purchase_count,
    COALESCE(mc.total_revenue, (0)::numeric) AS total_revenue,
    COALESCE(mc.avg_rev_per_ses, (0)::numeric) AS avg_rev_per_ses,
    COALESCE(mc.avg_rev_per_purc, (0)::numeric) AS avg_rev_per_purc
   FROM (metrics_cte mc
     LEFT JOIN silver.accounts_extended ae ON ((mc.account_id = ae.account_id)));