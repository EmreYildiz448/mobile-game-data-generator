CREATE TABLE silver.hosted_ads_act_detailed AS
 SELECT i.interaction_id,
    i.ad_id,
    i.interaction_time,
    i.interaction_type,
    i.revenue,
    i.platform,
    i.region,
    i.device_model,
    i.account_id,
    c.ad_network,
    c.pricing_model,
    c.value_per_action,
    c.start_date,
    c.end_date,
    c.is_active,
    c.advertised_product,
    c.ad_length,
    c.rewarded
   FROM (bronze.hosted_ad_interactions i
     LEFT JOIN bronze.hosted_ads c ON (((i.ad_id)::text = (c.ad_id)::text)))
  ;