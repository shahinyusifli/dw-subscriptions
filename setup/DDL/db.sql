create database subscription;

CREATE TABLE subscriptions (
    id INTEGER PRIMARY KEY,
    subscription_type TEXT,
    monthly_revenue INTEGER,
    join_date TEXT,
    last_payment_date TEXT,
    cancel_date TEXT,
    country TEXT,
    age INTEGER,
    gender TEXT,
    device TEXT,
    plan_duration TEXT,
    active_profiles INTEGER,
    household_profile_ind INTEGER,
    movies_watched INTEGER,
    series_watched INTEGER,
    created_date TIMESTAMP DEFAULT NOW(),
    modified_date TIMESTAMP DEFAULT NULL,
);

