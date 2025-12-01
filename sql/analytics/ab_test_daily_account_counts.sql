CREATE TABLE analytics.ab_test_daily_accounts
	SELECT
		DATE(session_start) AS session_date,
		COUNT(*) FILTER (WHERE app_version = '{CONTROL_VERSION}') AS control_session_count,
		COUNT(DISTINCT account_id) FILTER (WHERE app_version = '{CONTROL_VERSION}') AS control_unique_accounts,
		COUNT(*) FILTER (WHERE app_version = '{AB_TEST_VERSION}') AS test_session_count,
		COUNT(DISTINCT account_id) FILTER (WHERE app_version = '{AB_TEST_VERSION}') AS test_unique_accounts
	FROM bronze.sessions
	WHERE session_start >= '{AB_START}'
	GROUP BY 1 ORDER BY 1