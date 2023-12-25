CREATE OR REPLACE FUNCTION handle_subscription_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' then
        INSERT INTO silver.subscriptions (
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
        ) VALUES (
            CAST(NEW.id AS INTEGER),
            NEW.subscription_type,
            CAST(NEW.monthly_revenue AS INTEGER),
            TO_DATE(NEW.join_date::TEXT, 'YYYY-MM-DD'),  
            TO_DATE(NEW.last_payment_date::TEXT, 'YYYY-MM-DD'),  
            TO_DATE(NEW.cancel_date::TEXT, 'YYYY-MM-DD'),  
            NEW.country,
            CAST(NEW.age AS INTEGER),
            NEW.gender,
            NEW.device,
            CAST(replace(NEW.plan_duration, 'month', '') AS INTEGER),
            CAST(NEW.active_profiles AS INTEGER),
            CAST(NEW.household_profile_ind AS INTEGER),
            CAST(NEW.movies_watched AS INTEGER),
            CAST(NEW.series_watched AS INTEGER)
        );
    ELSIF TG_OP = 'UPDATE' THEN
        UPDATE silver.subscriptions
        SET 
            valid_to = current_timestamp,
            is_valid = FALSE
        WHERE 
            id = CAST(OLD.id AS INTEGER) AND 
            is_valid;
        INSERT INTO silver.subscriptions (
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
        ) VALUES (
            CAST(NEW.id AS INTEGER),
            NEW.subscription_type,
            CAST(NEW.monthly_revenue AS INTEGER),
            TO_DATE(NEW.join_date::TEXT, 'YYYY-MM-DD'),  
            TO_DATE(NEW.last_payment_date::TEXT, 'YYYY-MM-DD'),  
            TO_DATE(NEW.cancel_date::TEXT, 'YYYY-MM-DD'), 
            NEW.country,
            CAST(NEW.age AS INTEGER),
            NEW.gender,
            NEW.device,
            CAST(replace(NEW.plan_duration, 'month', '') AS INTEGER),
            CAST(NEW.active_profiles AS INTEGER),
            CAST(NEW.household_profile_ind AS INTEGER),
            CAST(NEW.movies_watched AS INTEGER),
            CAST(NEW.series_watched AS INTEGER)
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE or replace TRIGGER subscription_changes_trigger
AFTER INSERT OR UPDATE ON bronze.subscriptions
FOR EACH ROW EXECUTE FUNCTION handle_subscription_changes();

