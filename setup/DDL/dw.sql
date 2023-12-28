-- ================================================BRONZE====================================================
CREATE SCHEMA bronze;

CREATE TABLE bronze.subscriptions (
    id VARCHAR(256),
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
    inserted_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP DEFAULT NULL
);

-- Create an index on the inserted_date
CREATE INDEX idx_inserted_date ON bronze.subscriptions (inserted_date);

-- Cluster the table based on the index
CLUSTER bronze.subscriptions USING idx_inserted_date;


CREATE OR REPLACE FUNCTION merge_temp_table_into_subscriptions()
RETURNS VOID AS
$$
BEGIN
    EXECUTE '
        MERGE INTO bronze.subscriptions  
        USING bronze.temp_table_bronze tt ON 	
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

-- ============================================SILVER=========================================================

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

-- ===========================================GOLD================================================

CREATE SCHEMA gold;

CREATE TABLE gold.dim_account (
	id serial4,
	join_date date NULL,
	age int4 NULL,
	gender varchar(20) NULL,
	country varchar(50) NULL,
	inserted_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP DEFAULT null,
	CONSTRAINT user_dimension_pkey PRIMARY KEY (id)
);

CREATE table gold.dim_calendar (
	id date NOT NULL,
	"day" int4 NULL,
	"month" int4 NULL,
	month_name varchar(20) NULL,
	"year" int4 NULL,
	quarter int4 NULL,
	weekday int4 NULL,
	day_of_week_name varchar(20) NULL,
	is_weekend bool NULL,
	CONSTRAINT date_dimension_pkey PRIMARY KEY (id)
);

CREATE TABLE gold.dim_device (
	sk smallserial,
	device varchar(50) NULL,
	CONSTRAINT device_dimension_pkey PRIMARY KEY (sk)
);

CREATE TABLE gold.dim_device (
	sk smallserial,
	device varchar(50) NULL,
	CONSTRAINT device_dimension_pkey PRIMARY KEY (sk)
);

CREATE TABLE gold.fct_sales (
	sk serial4,
	account_id int4,
	subscription_sk int4,
	device_sk int4,
	last_payment_date date,
	cancel_date date,
	active_profiles int4,
	household_profile_ind int4,
	movies_watched int4,
	series_watched int4,
	monthly_revenue int4,
	inserted_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP DEFAULT null,
	CONSTRAINT sales_fact_pkey PRIMARY KEY (sk)
);

ALTER TABLE gold.fct_sales ADD CONSTRAINT fk_date_last_patment FOREIGN KEY (last_payment_date) REFERENCES gold.dim_calendar(id);
ALTER TABLE gold.fct_sales ADD CONSTRAINT fk_date_cancel_date FOREIGN KEY (cancel_date) REFERENCES gold.dim_calendar(id);
ALTER TABLE gold.fct_sales ADD CONSTRAINT fk_device FOREIGN KEY (device_sk) REFERENCES gold.dim_device(sk);
ALTER TABLE gold.fct_sales ADD CONSTRAINT fk_subscription FOREIGN KEY (subscription_sk) REFERENCES gold.dim_subscription(sk);
ALTER TABLE gold.fct_sales ADD CONSTRAINT fk_user FOREIGN KEY (account_id) REFERENCES gold.dim_account(id);
