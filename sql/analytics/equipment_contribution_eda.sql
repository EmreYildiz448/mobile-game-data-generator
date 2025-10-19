CREATE TABLE analytics.equipment_contribution_eda AS
	WITH events_with_next AS (
		SELECT
			account_id,
			session_id,
			event_id,
			event_type,
			event_subtype,
			event_date,
			event_metadata->>'level_id' AS level_id,
			event_metadata->>'equipped_hero' AS equipped_hero,
			event_metadata->>'equipped_skin' AS equipped_skin,
			event_metadata->>'equipped_weapon' AS equipped_weapon,
			event_metadata->>'equipped_armor' AS equipped_armor,
			event_metadata->>'equipped_held_item' AS equipped_held_item,
			LEAD(event_subtype) OVER (
				PARTITION BY account_id, session_id
				ORDER BY event_date, event_id
			) AS next_event_subtype,
			LEAD(event_metadata) OVER (
				PARTITION BY account_id, session_id
				ORDER BY event_date, event_id
			) AS next_event_metadata		
		FROM bronze.events
	),
	level_start_equipments AS (
		SELECT 
			*,
			next_event_metadata ->> 'total_score' AS total_score,
			next_event_metadata ->> 'stars_gained' AS stars_gained
		FROM events_with_next
		WHERE event_subtype = 'level_start'
	)
	SELECT 
		event_date,
		level_id,
		equipped_hero,
		equipped_skin,
		equipped_weapon,
		equipped_armor,
		equipped_held_item,
		CASE next_event_subtype
			WHEN 'level_success' THEN TRUE
			WHEN 'level_fail' THEN FALSE
			ELSE NULL
		END AS is_attempt_successful,
		total_score,
		COALESCE(stars_gained, '0') AS stars_gained
	FROM level_start_equipments
	ORDER BY event_date;