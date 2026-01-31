-- Purpose: Identify high-intent users for 'Win-Back' campaign
-- Target: Sync to Braze via Reverse ETL logic

WITH base_users AS (
    SELECT 
        user_id,
        email,
        LOWER(TRIM(city)) AS clean_city, -- Data Hygiene/Standardization
        MAX(last_action_timestamp) AS last_seen
    FROM `compare_club.raw_data.user_events`
    GROUP BY 1, 2, 3
),
inactive_high_value AS (
    SELECT u.*, t.total_lifetime_value
    FROM base_users u
    JOIN `compare_club.raw_data.transactions` t ON u.user_id = t.user_id
    WHERE u.last_seen < CURRENT_TIMESTAMP() - INTERVAL 7 DAY
    AND t.total_lifetime_value > 500 -- Business Logic: Targeting High-LTV users
)
SELECT 
    user_id,
    email,
    clean_city,
    total_lifetime_value,
    'high_value_retention_segment' AS braze_segment_tag,
    CURRENT_TIMESTAMP() AS last_sync_at
FROM inactive_high_value;
