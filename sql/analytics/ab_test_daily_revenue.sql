CREATE TABLE analytics.ab_test_daily_revenue AS
	WITH base_sessions AS (
		SELECT
			session_id,
			account_id,
			session_start,
			CASE 
				WHEN app_version = '1.0.0.b' THEN 'test'
				WHEN app_version = '1.0.0.a' THEN 'control'
				ELSE NULL
			END AS group_type,
			session_start::DATE AS session_date
		FROM bronze.sessions
		WHERE session_start >= DATE '2025-03-03'
	),
	base_events AS (
		SELECT
			e.session_id,
			e.event_type,
			(e.event_metadata->>'offer_id') AS offer_id,
			(e.event_metadata->>'cost_amount')::NUMERIC AS cost_amount,
			(e.event_metadata->>'exchange_rate')::NUMERIC AS exchange_rate,
			ROUND((e.event_metadata->>'cost_amount')::NUMERIC / NULLIF((e.event_metadata->>'exchange_rate')::NUMERIC, 0), 2) AS cost_usd
		FROM bronze.events e
		WHERE e.event_type = 'business'
	)
	SELECT
		s.group_type,
		s.account_id,
		s.session_id,
		a.acquisition_segment,
		DATE(s.session_start) AS session_date,
		e.offer_id,
		e.cost_usd,
		SUM(e.cost_usd) OVER (PARTITION BY s.group_type ORDER BY DATE(s.session_start)) AS cumulative_revenue
	FROM base_sessions s
	LEFT JOIN base_events e ON s.session_id = e.session_id
	LEFT JOIN silver.accounts_extended a ON s.account_id = a.account_id
	WHERE s.group_type IS NOT NULL
	AND cost_usd IS NOT NULL
	ORDER BY session_start, group_type, acquisition_segment