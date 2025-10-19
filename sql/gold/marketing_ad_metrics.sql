CREATE TABLE gold.marketing_ad_metrics AS
 WITH ad_metrics AS (
         SELECT a.ad_id,
            c.campaign_name,
            a.ad_name,
            c.campaign_type,
            a.impression_count,
            a.click_count,
            a.install_count,
            a.action_count,
            a.cost_per_interaction,
            c.budget,
                CASE
                    WHEN (c.campaign_type = 'CPM') THEN (((a.impression_count / 1000))::numeric * a.cost_per_interaction)
                    WHEN (c.campaign_type = 'CPC') THEN ((a.click_count)::numeric * a.cost_per_interaction)
                    WHEN (c.campaign_type = 'CPI') THEN ((a.install_count)::numeric * a.cost_per_interaction)
                    WHEN (c.campaign_type = 'CPA') THEN ((a.action_count)::numeric * a.cost_per_interaction)
                    ELSE NULL::numeric
                END AS spend,
            (((a.click_count)::numeric / (a.impression_count)::numeric) * (100)::numeric) AS ctr,
            (((a.install_count)::numeric / (a.click_count)::numeric) * (100)::numeric) AS cvr,
            (((a.install_count)::numeric / (a.impression_count)::numeric) * (100)::numeric) AS ir
           FROM ((bronze.ads a
             LEFT JOIN bronze.ad_campaign_map m ON ((a.ad_id = m.ad_id)))
             LEFT JOIN bronze.campaigns c ON ((m.campaign_id = c.campaign_id)))
        ), action_completion AS (
         SELECT (a.acquisition_metadata ->> 'ad_name'::text) AS ad_name,
            count(*) AS user_count,
            sum(
                CASE
                    WHEN (a.email_is_anonymized = false) THEN 1
                    ELSE NULL::integer
                END) AS conversion_success_count,
            round(
                CASE
                    WHEN (count(*) > 0) THEN (((sum(
                    CASE
                        WHEN (a.email_is_anonymized = false) THEN 1
                        ELSE NULL::integer
                    END))::numeric / (count(*))::numeric) * (100)::numeric)
                    ELSE NULL::numeric
                END, 2) AS acr
           FROM (bronze.accounts a
             JOIN ad_metrics am_1 ON (((a.acquisition_metadata ->> 'ad_name'::text) = (am_1.ad_name)::text)))
          WHERE (am_1.campaign_type = 'CPA')
          GROUP BY (a.acquisition_metadata ->> 'ad_name'::text)
        ), signup_count AS (
         SELECT (accounts.acquisition_metadata ->> 'ad_name'::text) AS ad_name,
            count(accounts.account_id) AS signup_count
           FROM bronze.accounts
          WHERE ((accounts.acquisition_metadata ->> 'ad_name'::text) IS NOT NULL)
          GROUP BY (accounts.acquisition_metadata ->> 'ad_name'::text)
        ), user_spend AS (
         SELECT (a.acquisition_metadata ->> 'ad_name'::text) AS ad_name,
            sum((((e.event_metadata ->> 'cost_amount'::text))::numeric / COALESCE(er.usd_exchange_rate, (1)::numeric))) AS revenue
           FROM ((bronze.accounts a
             LEFT JOIN bronze.events e ON ((a.account_id = e.account_id)))
             LEFT JOIN bronze.exchange_rate er ON (((e.event_metadata ->> 'currency_name'::text) = er.currency)))
          WHERE (((e.event_type)::text = 'business'::text) AND ((a.acquisition_metadata ->> 'ad_name'::text) IS NOT NULL))
          GROUP BY (a.acquisition_metadata ->> 'ad_name'::text)
        )
 SELECT am.campaign_name,
    am.ad_name,
    am.campaign_type,
    am.impression_count,
    am.click_count,
    am.install_count,
    sc.signup_count,
    am.action_count,
    am.spend,
    round(us.revenue, 2) AS revenue,
    round(am.ctr, 2) AS ctr,
    round(am.cvr, 2) AS cvr,
    round(am.ir, 2) AS ir,
    round((am.spend / (am.install_count)::numeric), 2) AS ecpi,
    round(((am.spend / (am.impression_count)::numeric) * (1000)::numeric), 2) AS ecpm,
    round((am.spend / (sc.signup_count)::numeric), 2) AS cpa,
    round((us.revenue / am.spend), 2) AS roas,
        CASE
            WHEN (am.campaign_type = 'CPA') THEN ac.acr
            ELSE NULL::numeric
        END AS acr
   FROM (((ad_metrics am
     LEFT JOIN action_completion ac ON (((am.ad_name)::text = ac.ad_name)))
     LEFT JOIN signup_count sc ON (((am.ad_name)::text = sc.ad_name)))
     LEFT JOIN user_spend us ON (((am.ad_name)::text = us.ad_name)))
  ;