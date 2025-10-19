CREATE TABLE gold.biz_offer_performance AS
 WITH business_unnested AS (
         SELECT e.event_id,
            e.account_id,
            e.session_id,
            date(e.event_date) AS event_date,
            m.offer_id,
            m.cost_type,
            m.reward_id,
            m.currency_name,
            COALESCE(er.usd_exchange_rate, (1)::numeric) AS "coalesce",
            m.cost_amount AS preconverted_cost_amount,
            round((m.cost_amount / COALESCE(er.usd_exchange_rate, (1)::numeric)), 2) AS cost_amount,
            m.reward_category
           FROM bronze.events e
            CROSS JOIN LATERAL (
              SELECT
                json_extract_string(e.event_metadata, '$.offer_id')       AS offer_id,
                json_extract(e.event_metadata, '$.reward_id')             AS reward_id,     -- array → JSON
                json_extract_string(e.event_metadata, '$.cost_type')      AS cost_type,
                json_extract_string(e.event_metadata, '$.currency_name')  AS currency_name,
                CAST(json_extract(e.event_metadata, '$.cost_amount') AS NUMERIC) AS cost_amount,
                json_extract(e.event_metadata, '$.reward_category')       AS reward_category -- array → JSON
            ) AS m
            LEFT JOIN bronze.exchange_rate er
              ON m.currency_name = er.currency
            WHERE (e.event_type)::text = 'business'::text
        ), business_daily_grouped AS (
         SELECT business_unnested.offer_id,
            business_unnested.event_date,
            business_unnested.reward_category,
            count(*) AS purchase_count,
            count(DISTINCT business_unnested.account_id) AS daily_unique_buyers,
            count(DISTINCT business_unnested.session_id) AS daily_purchase_sessions,
            sum(business_unnested.cost_amount) AS total_revenue,
            round((sum(business_unnested.cost_amount) / (count(DISTINCT business_unnested.session_id))::numeric), 2) AS avg_revenue_per_purchasing_sessions,
            round((sum(business_unnested.cost_amount) / (NULLIF(count(DISTINCT business_unnested.account_id), 0))::numeric), 2) AS avg_revenue_per_buyer
           FROM business_unnested
          GROUP BY business_unnested.offer_id, business_unnested.event_date, business_unnested.reward_category
        ), business_weekly_grouped AS (
         SELECT business_unnested.offer_id,
            (date_trunc('week'::text, (business_unnested.event_date)::timestamp with time zone))::date AS week_start_date,
            business_unnested.reward_category,
            count(DISTINCT business_unnested.account_id) AS weekly_unique_buyers,
            count(DISTINCT business_unnested.session_id) AS weekly_purchase_sessions
           FROM business_unnested
          GROUP BY business_unnested.offer_id, (date_trunc('week'::text, (business_unnested.event_date)::timestamp with time zone)), business_unnested.reward_category
        ), business_monthly_grouped AS (
         SELECT business_unnested.offer_id,
            (date_trunc('month'::text, (business_unnested.event_date)::timestamp with time zone))::date AS month_start_date,
            business_unnested.reward_category,
            count(DISTINCT business_unnested.account_id) AS monthly_unique_buyers,
            count(DISTINCT business_unnested.session_id) AS monthly_purchase_sessions
           FROM business_unnested
          GROUP BY business_unnested.offer_id, (date_trunc('month'::text, (business_unnested.event_date)::timestamp with time zone)), business_unnested.reward_category
        ), business_total_grouped AS (
         SELECT business_unnested.offer_id,
            business_unnested.reward_category,
            count(DISTINCT business_unnested.account_id) AS total_unique_buyers,
            count(DISTINCT business_unnested.session_id) AS total_purchase_sessions
           FROM business_unnested
          GROUP BY business_unnested.offer_id, business_unnested.reward_category
        )
 SELECT bg.offer_id,
    bg.event_date,
    bg.reward_category,
    bg.purchase_count,
    bg.daily_purchase_sessions,
    bg.daily_unique_buyers,
    COALESCE(bw.weekly_unique_buyers, (0)::bigint) AS weekly_unique_buyers,
    COALESCE(bw.weekly_purchase_sessions, (0)::bigint) AS weekly_purchase_sessions,
    COALESCE(bm.monthly_unique_buyers, (0)::bigint) AS monthly_unique_buyers,
    COALESCE(bm.monthly_purchase_sessions, (0)::bigint) AS monthly_purchase_sessions,
    COALESCE(bt.total_unique_buyers, (0)::bigint) AS total_unique_buyers,
    COALESCE(bt.total_purchase_sessions, (0)::bigint) AS total_purchase_sessions,
    bg.total_revenue,
    bg.avg_revenue_per_purchasing_sessions,
    bg.avg_revenue_per_buyer
   FROM (((business_daily_grouped bg
     LEFT JOIN business_weekly_grouped bw ON (((bg.offer_id = bw.offer_id) AND (bg.reward_category = bw.reward_category) AND (date_trunc('week'::text, (bg.event_date)::timestamp with time zone) = bw.week_start_date))))
     LEFT JOIN business_monthly_grouped bm ON (((bg.offer_id = bm.offer_id) AND (bg.reward_category = bm.reward_category) AND (date_trunc('month'::text, (bg.event_date)::timestamp with time zone) = bm.month_start_date))))
     LEFT JOIN business_total_grouped bt ON (((bg.offer_id = bt.offer_id) AND (bg.reward_category = bt.reward_category))))
  ORDER BY bg.offer_id, bg.event_date, bg.reward_category
  ;