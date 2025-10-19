CREATE TABLE analytics.ab_test_daily_accounts
	SELECT
		DATE(session_start) AS session_date,
		COUNT(*) FILTER (WHERE app_version = '1.0.0.a') AS control_session_count,
		COUNT(DISTINCT account_id) FILTER (WHERE app_version = '1.0.0.a') AS control_unique_accounts,
		COUNT(*) FILTER (WHERE app_version = '1.0.0.b') AS test_session_count,
		COUNT(DISTINCT account_id) FILTER (WHERE app_version = '1.0.0.b') AS test_unique_accounts
	FROM bronze.sessions
	WHERE session_start >= '2025-03-02'
	GROUP BY 1 ORDER BY 1