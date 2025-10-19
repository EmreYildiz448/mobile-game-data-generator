CREATE TABLE gold.acquisition_daily AS
 SELECT date(signup_date) AS date,
    referral_source,
    (acquisition_metadata ->> 'campaign_name'::text) AS campaign_name,
    (acquisition_metadata ->> 'ad_name'::text) AS ad_name,
    (acquisition_metadata ->> 'search_ad_keyword'::text) AS search_ad_keyword,
    count(*) AS daily_acquired_users
   FROM bronze.accounts
  GROUP BY (date(signup_date)), referral_source, (acquisition_metadata ->> 'campaign_name'::text), (acquisition_metadata ->> 'ad_name'::text), (acquisition_metadata ->> 'search_ad_keyword'::text)
  ORDER BY (date(signup_date)), referral_source, (acquisition_metadata ->> 'campaign_name'::text), (acquisition_metadata ->> 'ad_name'::text), (acquisition_metadata ->> 'search_ad_keyword'::text)
  ;