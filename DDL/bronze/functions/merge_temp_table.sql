CREATE OR REPLACE FUNCTION merge_temp_table_into_subscriptions()
RETURNS VOID AS
$$
BEGIN
    EXECUTE '
        MERGE INTO bronze.subscriptions  
        USING bronze.temp_table_113 tt ON 	
                cast(subscriptions.id as int) = tt.id
            WHEN MATCHED THEN 
                UPDATE SET 
                    subscription_type = tt.subscription_type,
                    monthly_revenue = tt.monthly_revenue,
                    join_date = tt.join_date,
                    last_payment_date = tt.last_payment_date,
                    cancel_date = tt.cancel_date,
                    country = tt.country,
                    age = tt.age,
                    gender = tt.gender,
                    device = tt.device,
                    plan_duration = tt.plan_duration,
                    active_profiles = tt.active_profiles,
                    household_profile_ind = tt.household_profile_ind,
                    movies_watched = tt.movies_watched,
                    series_watched = tt.series_watched,
                    modified_date = current_timestamp
            WHEN NOT MATCHED THEN 
                INSERT (
                    id, subscription_type, monthly_revenue, join_date, last_payment_date,
                    cancel_date, country, age, gender, device, plan_duration, active_profiles,
                    household_profile_ind, movies_watched, series_watched
                ) VALUES (
                    tt.id, tt.subscription_type, tt.monthly_revenue, tt.join_date, tt.last_payment_date,
                    tt.cancel_date, tt.country, tt.age, tt.gender, tt.device, tt.plan_duration, tt.active_profiles,
                    tt.household_profile_ind, tt.movies_watched, tt.series_watched
                );
        ';

    EXECUTE 'DROP TABLE bronze.temp_table_bronze;';
END;
$$
LANGUAGE plpgsql;
