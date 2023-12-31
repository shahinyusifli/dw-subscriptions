MERGE INTO gold.fct_sales gfs
USING (
    SELECT *
    FROM silver.subscriptions
    WHERE is_valid = True
) ss
ON 
    gfs.account_id = ss.id AND (
    gfs.subscription_sk <> gold.map_to_subscription(ss.subscription_type, ss.plan_duration) or
    gfs.subscription_sk = gold.map_to_subscription(ss.subscription_type, ss.plan_duration) or
    gfs.device_sk = gold.map_to_device(ss.device) or
    gfs.last_payment_date = ss.last_payment_date or
    gfs.cancel_date = ss.cancel_date or
    gfs.active_profiles = ss.active_profiles or
    gfs.household_profile_ind = ss.household_profile_ind or
    gfs.movies_watched = ss.movies_watched or
    gfs.series_watched = ss.series_watched or
    gfs.monthly_revenue = ss.monthly_revenue
    )

WHEN MATCHED THEN
    UPDATE SET
        subscription_sk = gold.map_to_subscription(ss.subscription_type, ss.plan_duration),
        device_sk = gold.map_to_device(ss.device),
        last_payment_date = ss.last_payment_date,
        cancel_date = ss.cancel_date,
        active_profiles = ss.active_profiles,
        household_profile_ind = ss.household_profile_ind,
        movies_watched = ss.movies_watched,
        series_watched = ss.series_watched,
        monthly_revenue = ss.monthly_revenue,
        modified_date = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        account_id, subscription_sk, last_payment_date, device_sk,
        active_profiles, household_profile_ind, movies_watched,
        series_watched, monthly_revenue, cancel_date
    )
    VALUES (
        ss.id, (
            SELECT gold.map_to_subscription(ss.subscription_type, ss.plan_duration)
        ),
        ss.last_payment_date, gold.map_to_device(ss.device),
        ss.active_profiles, ss.household_profile_ind, ss.movies_watched,
        ss.series_watched, ss.monthly_revenue, ss.cancel_date
    );
