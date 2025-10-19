CREATE TABLE gold.cohort_churt AS
 WITH latest_session_day AS (
         SELECT max(date(sessions.session_start)) AS max_day
           FROM bronze.sessions
        ), cohort_base AS (
         SELECT date(accounts_extended.signup_date) AS signup_day,
            accounts_extended.account_id,
            accounts_extended.referral_source,
            accounts_extended.campaign_name,
            accounts_extended.ad_name,
            accounts_extended.search_ad_keyword,
            accounts_extended.acquisition_segment,
            accounts_extended.engagement_segment,
            accounts_extended.monetization_segment,
            accounts_extended.completion_segment,
            accounts_extended.subscription_status
           FROM silver.accounts_extended
        ), date_offsets AS (
         SELECT unnest(ARRAY[0, 1, 7, 14, 21, 28, 35, 42, 49, 56, 63, 70, 77, 85]) AS day_offset
        ), sessions_with_offsets AS (
         SELECT cb.signup_day,
            cb.referral_source,
            cb.campaign_name,
            cb.ad_name,
            cb.search_ad_keyword,
            cb.acquisition_segment,
            cb.engagement_segment,
            cb.monetization_segment,
            cb.completion_segment,
            cb.subscription_status,
            o.day_offset,
                CASE
                    WHEN ((cb.signup_day + o.day_offset) <= lsd.max_day) THEN count(DISTINCT s.account_id)
                    ELSE NULL::bigint
                END AS returning_users
           FROM (((cohort_base cb
             CROSS JOIN date_offsets o)
             JOIN latest_session_day lsd ON (true))
             LEFT JOIN bronze.sessions s ON (((cb.account_id = s.account_id) AND (date(s.session_start) = (cb.signup_day + o.day_offset)))))
          GROUP BY cb.signup_day, cb.referral_source, cb.campaign_name, cb.ad_name, cb.search_ad_keyword, cb.acquisition_segment, cb.engagement_segment, cb.monetization_segment, cb.completion_segment, cb.subscription_status, o.day_offset, lsd.max_day
        ), cohort_aggregated AS (
         SELECT sessions_with_offsets.signup_day,
            sessions_with_offsets.referral_source,
            sessions_with_offsets.campaign_name,
            sessions_with_offsets.ad_name,
            sessions_with_offsets.search_ad_keyword,
            sessions_with_offsets.acquisition_segment,
            sessions_with_offsets.engagement_segment,
            sessions_with_offsets.monetization_segment,
            sessions_with_offsets.completion_segment,
            sessions_with_offsets.subscription_status,
            sum(sessions_with_offsets.returning_users) FILTER (WHERE (sessions_with_offsets.day_offset = 0)) AS day_0,
            sum(sessions_with_offsets.returning_users) FILTER (WHERE (sessions_with_offsets.day_offset = 1)) AS day_1_return,
            sum(sessions_with_offsets.returning_users) FILTER (WHERE (sessions_with_offsets.day_offset = 7)) AS day_7_return,
            sum(sessions_with_offsets.returning_users) FILTER (WHERE (sessions_with_offsets.day_offset = 14)) AS day_14_return,
            sum(sessions_with_offsets.returning_users) FILTER (WHERE (sessions_with_offsets.day_offset = 21)) AS day_21_return,
            sum(sessions_with_offsets.returning_users) FILTER (WHERE (sessions_with_offsets.day_offset = 28)) AS day_28_return,
            sum(sessions_with_offsets.returning_users) FILTER (WHERE (sessions_with_offsets.day_offset = 35)) AS day_35_return,
            sum(sessions_with_offsets.returning_users) FILTER (WHERE (sessions_with_offsets.day_offset = 42)) AS day_42_return,
            sum(sessions_with_offsets.returning_users) FILTER (WHERE (sessions_with_offsets.day_offset = 49)) AS day_49_return,
            sum(sessions_with_offsets.returning_users) FILTER (WHERE (sessions_with_offsets.day_offset = 56)) AS day_56_return,
            sum(sessions_with_offsets.returning_users) FILTER (WHERE (sessions_with_offsets.day_offset = 63)) AS day_63_return,
            sum(sessions_with_offsets.returning_users) FILTER (WHERE (sessions_with_offsets.day_offset = 70)) AS day_70_return,
            sum(sessions_with_offsets.returning_users) FILTER (WHERE (sessions_with_offsets.day_offset = 77)) AS day_77_return,
            sum(sessions_with_offsets.returning_users) FILTER (WHERE (sessions_with_offsets.day_offset = 85)) AS day_85_return
           FROM sessions_with_offsets
          GROUP BY sessions_with_offsets.signup_day, sessions_with_offsets.referral_source, sessions_with_offsets.campaign_name, sessions_with_offsets.ad_name, sessions_with_offsets.search_ad_keyword, sessions_with_offsets.acquisition_segment, sessions_with_offsets.engagement_segment, sessions_with_offsets.monetization_segment, sessions_with_offsets.completion_segment, sessions_with_offsets.subscription_status
        )
 SELECT signup_day,
    referral_source,
    campaign_name,
    ad_name,
    search_ad_keyword,
    acquisition_segment,
    engagement_segment,
    monetization_segment,
    completion_segment,
    subscription_status,
    day_0,
    day_1_return,
    day_7_return,
    day_14_return,
    day_21_return,
    day_28_return,
    day_35_return,
    day_42_return,
    day_49_return,
    day_56_return,
    day_63_return,
    day_70_return,
    day_77_return,
    day_85_return,
    (day_0 - day_1_return) AS day_1_churn,
    (day_0 - day_7_return) AS day_7_churn,
    (day_0 - day_14_return) AS day_14_churn,
    (day_0 - day_21_return) AS day_21_churn,
    (day_0 - day_28_return) AS day_28_churn,
    (day_0 - day_35_return) AS day_35_churn,
    (day_0 - day_42_return) AS day_42_churn,
    (day_0 - day_49_return) AS day_49_churn,
    (day_0 - day_56_return) AS day_56_churn,
    (day_0 - day_63_return) AS day_63_churn,
    (day_0 - day_70_return) AS day_70_churn,
    (day_0 - day_77_return) AS day_77_churn,
    (day_0 - day_85_return) AS day_85_churn,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round(((day_1_return * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS retention_rate_day1,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round(((day_7_return * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS retention_rate_day7,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round(((day_14_return * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS retention_rate_day14,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round(((day_21_return * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS retention_rate_day21,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round(((day_28_return * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS retention_rate_day28,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round(((day_35_return * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS retention_rate_day35,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round(((day_42_return * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS retention_rate_day42,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round(((day_49_return * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS retention_rate_day49,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round(((day_56_return * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS retention_rate_day56,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round(((day_63_return * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS retention_rate_day63,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round(((day_70_return * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS retention_rate_day70,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round(((day_77_return * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS retention_rate_day77,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round(((day_85_return * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS retention_rate_day85,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round((((day_0 - day_1_return) * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS churn_rate_day1,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round((((day_0 - day_7_return) * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS churn_rate_day7,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round((((day_0 - day_14_return) * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS churn_rate_day14,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round((((day_0 - day_21_return) * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS churn_rate_day21,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round((((day_0 - day_28_return) * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS churn_rate_day28,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round((((day_0 - day_35_return) * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS churn_rate_day35,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round((((day_0 - day_42_return) * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS churn_rate_day42,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round((((day_0 - day_49_return) * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS churn_rate_day49,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round((((day_0 - day_56_return) * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS churn_rate_day56,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round((((day_0 - day_63_return) * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS churn_rate_day63,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round((((day_0 - day_70_return) * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS churn_rate_day70,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round((((day_0 - day_77_return) * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS churn_rate_day77,
        CASE
            WHEN (day_0 > (0)::numeric) THEN round((((day_0 - day_85_return) * 100.0) / day_0), 2)
            ELSE NULL::numeric
        END AS churn_rate_day85
   FROM cohort_aggregated
  ORDER BY signup_day, referral_source, campaign_name, ad_name
  ;