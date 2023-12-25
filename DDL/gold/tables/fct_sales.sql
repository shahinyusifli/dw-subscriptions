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