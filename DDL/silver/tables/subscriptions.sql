CREATE SCHEMA silver;

CREATE TABLE silver.subscriptions (
	sk serial primary key,
    id INT not null,
    subscription_type VARCHAR(20),
    monthly_revenue INT,
    join_date DATE,
    last_payment_date DATE,
    cancel_date DATE,
    country VARCHAR(256),
    age INT,
    gender VARCHAR(20),
    device VARCHAR(20),
    plan_duration INT,
    active_profiles INT,
    household_profile_ind INT,
    movies_watched INT,
    series_watched INT,
    valid_from TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP WITH TIME ZONE DEFAULT '9999-01-01',
    is_valid BOOLEAN default TRUE
);

