CREATE TABLE gold.hosted_ad_metrics AS
 WITH agg_ext_ad_action AS (
         SELECT hosted_ad_interactions.ad_id,
            hosted_ad_interactions.interaction_type,
            count(*) AS interaction_count,
            sum(
                CASE
                    WHEN ((hosted_ad_interactions.interaction_type)::text = 'click'::text) THEN 1
                    ELSE 0
                END) AS click_count,
            sum(
                CASE
                    WHEN ((hosted_ad_interactions.interaction_type)::text = 'install'::text) THEN 1
                    ELSE 0
                END) AS install_count,
            sum(
                CASE
                    WHEN ((hosted_ad_interactions.interaction_type)::text = 'action'::text) THEN 1
                    ELSE 0
                END) AS action_count
           FROM bronze.hosted_ad_interactions
          GROUP BY hosted_ad_interactions.ad_id, hosted_ad_interactions.interaction_type
        ), events_ad_view_data AS (
         SELECT (events.event_metadata ->> 'ad_id'::text) AS ad_id,
            count(*) AS view_count
           FROM bronze.events
          WHERE ((events.event_subtype)::text = ANY ((ARRAY['ad_shown'::character varying, 'reward_ad_shown'::character varying])::text[]))
          GROUP BY (events.event_metadata ->> 'ad_id'::text)
        ), revenue_addition AS (
         SELECT hac.ad_id,
            hac.ad_network,
            hac.pricing_model,
            hac.value_per_action,
            hac.start_date,
            hac.end_date,
            hac.is_active,
            hac.advertised_product,
            hac.ad_length,
            hac.rewarded,
            aac.interaction_count,
            eac.view_count,
            aac.click_count,
            aac.install_count,
            aac.action_count,
            round(
                CASE hac.pricing_model
                    WHEN 'CPM' THEN ((hac.value_per_action / (1000)::numeric) * (aac.interaction_count)::numeric)
                    ELSE (hac.value_per_action * (aac.interaction_count)::numeric)
                END, 2) AS total_revenue,
            NULLIF(round((((aac.interaction_count)::numeric / (eac.view_count)::numeric) * (100)::numeric), 2), (100)::numeric) AS engagement_rate
           FROM ((bronze.hosted_ads hac
             LEFT JOIN agg_ext_ad_action aac ON (((hac.ad_id)::text = (aac.ad_id)::text)))
             LEFT JOIN events_ad_view_data eac ON (((hac.ad_id)::text = eac.ad_id)))
          ORDER BY hac.ad_id
        )
 SELECT ad_id,
    ad_network,
    pricing_model,
    value_per_action,
    start_date,
    end_date,
    is_active,
    advertised_product,
    ad_length,
    rewarded,
    interaction_count,
    view_count,
    click_count,
    install_count,
    action_count,
    total_revenue,
    engagement_rate,
    round(((total_revenue / (view_count)::numeric) * (1000)::numeric), 2) AS ecpm
   FROM revenue_addition
  ;