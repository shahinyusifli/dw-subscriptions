CREATE TABLE gold.dim_subscription (
	sk serial4,
	subscription_type varchar(255) NULL,
	plan_duration int4 null,
	CONSTRAINT subscription_dimension_pkey PRIMARY KEY (sk)
);