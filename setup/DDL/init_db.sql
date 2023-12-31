CREATE TABLE subscriptions (
    id serial4 PRIMARY KEY,
    subscription_type VARCHAR(256),
    monthly_revenue VARCHAR(256),
    join_date VARCHAR(256),
    last_payment_date VARCHAR(256),
    cancel_date VARCHAR(256),
    country VARCHAR(256),
    age VARCHAR(256),
    gender VARCHAR(256),
    device VARCHAR(256),
    plan_duration VARCHAR(256),
    active_profiles VARCHAR(256),
    household_profile_ind VARCHAR(256),
    movies_watched VARCHAR(256),
    series_watched VARCHAR(256),
    created_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP DEFAULT NULL
);
