SELECT 
	id,
	subscription_type,
	monthly_revenue,
	join_date,
	last_payment_date,
	cancel_date,
	country,
	age,
	gender,
	device,
	plan_duration,
	active_profiles,
	household_profile_ind,
	movies_watched,
	series_watched
FROM subscriptions
WHERE created_date >= DATETIME('now', '-1 days')  and modified_date is NULL
UNION
SELECT 
	id,
	subscription_type,
	monthly_revenue,
	join_date,
	last_payment_date,
	cancel_date,
	country,
	age,
	gender,
	device,
	plan_duration,
	active_profiles,
	household_profile_ind,
	movies_watched,
	series_watched
FROM subscriptions
WHERE modified_date >= DATETIME('now', '-1 days');


