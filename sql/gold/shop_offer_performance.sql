CREATE TABLE gold.shop_offer_performance AS
 WITH ingame_unnested AS (
         SELECT e.event_id,
            e.account_id,
            e.session_id,
            e.event_subtype,
            date(e.event_date) AS event_date,
            m.reason AS offer_id,
            split_part(m.item_id, '_'::text, 2) AS currency_type,
            m.item_amount,
            m.item_category
           FROM bronze.events e
            CROSS JOIN LATERAL (
              SELECT
                json_extract_string(e.event_metadata, '$.reason')        AS reason,
                json_extract_string(e.event_metadata, '$.item_id')       AS item_id,
                CAST(json_extract(e.event_metadata, '$.item_amount') AS NUMERIC) AS item_amount,
                json_extract_string(e.event_metadata, '$.item_category') AS item_category
            ) AS m
          WHERE (e.event_subtype)::text = 'sink_item'::text
            AND (e.event_metadata ->> 'item_category') = 'currency'::text
          ORDER BY e.event_id
        ), ingame_daily_grouped AS (
         SELECT ingame_unnested.offer_id,
            ingame_unnested.currency_type,
            ingame_unnested.event_date,
            count(*) AS daily_purchase_count,
            count(DISTINCT ingame_unnested.session_id) AS daily_shop_sessions,
            count(DISTINCT ingame_unnested.account_id) AS daily_unique_shoppers,
            sum(ingame_unnested.item_amount) AS total_currency_spent,
            round((sum(ingame_unnested.item_amount) / (count(DISTINCT ingame_unnested.session_id))::numeric), 2) AS avg_sink_per_session,
            round((sum(ingame_unnested.item_amount) / (count(DISTINCT ingame_unnested.account_id))::numeric), 2) AS avg_sink_per_shopper
           FROM ingame_unnested
          GROUP BY ingame_unnested.offer_id, ingame_unnested.currency_type, ingame_unnested.event_date
        ), ingame_weekly_grouped AS (
         SELECT ingame_unnested.offer_id,
            ingame_unnested.currency_type,
            date_trunc('week'::text, (ingame_unnested.event_date)::timestamp with time zone) AS week_start_date,
            count(DISTINCT ingame_unnested.session_id) AS weekly_shop_sessions,
            count(DISTINCT ingame_unnested.account_id) AS weekly_unique_shoppers
           FROM ingame_unnested
          GROUP BY ingame_unnested.offer_id, ingame_unnested.currency_type, (date_trunc('week'::text, (ingame_unnested.event_date)::timestamp with time zone))
        ), ingame_monthly_grouped AS (
         SELECT ingame_unnested.offer_id,
            ingame_unnested.currency_type,
            date_trunc('month'::text, (ingame_unnested.event_date)::timestamp with time zone) AS month_start_date,
            count(DISTINCT ingame_unnested.session_id) AS monthly_shop_sessions,
            count(DISTINCT ingame_unnested.account_id) AS monthly_unique_shoppers
           FROM ingame_unnested
          GROUP BY ingame_unnested.offer_id, ingame_unnested.currency_type, (date_trunc('month'::text, (ingame_unnested.event_date)::timestamp with time zone))
        ), ingame_total_grouped AS (
         SELECT ingame_unnested.offer_id,
            ingame_unnested.currency_type,
            count(DISTINCT ingame_unnested.session_id) AS total_shop_sessions,
            count(DISTINCT ingame_unnested.account_id) AS total_unique_shoppers
           FROM ingame_unnested
          GROUP BY ingame_unnested.offer_id, ingame_unnested.currency_type
        )
 SELECT ig.offer_id,
    ig.currency_type,
    ig.event_date,
    ig.daily_purchase_count,
    ig.daily_shop_sessions,
    ig.daily_unique_shoppers,
    ig.total_currency_spent,
    ig.avg_sink_per_session,
    ig.avg_sink_per_shopper,
    iw.weekly_shop_sessions,
    iw.weekly_unique_shoppers,
    im.monthly_shop_sessions,
    im.monthly_unique_shoppers,
    it.total_shop_sessions,
    it.total_unique_shoppers
   FROM (((ingame_daily_grouped ig
     LEFT JOIN ingame_weekly_grouped iw ON (((ig.offer_id = iw.offer_id) AND (ig.currency_type = iw.currency_type) AND (date_trunc('week'::text, (ig.event_date)::timestamp with time zone) = iw.week_start_date))))
     LEFT JOIN ingame_monthly_grouped im ON (((ig.offer_id = im.offer_id) AND (ig.currency_type = im.currency_type) AND (date_trunc('month'::text, (ig.event_date)::timestamp with time zone) = im.month_start_date))))
     LEFT JOIN ingame_total_grouped it ON (((ig.offer_id = it.offer_id) AND (ig.currency_type = it.currency_type))))
  ORDER BY ig.offer_id, ig.event_date
  ;