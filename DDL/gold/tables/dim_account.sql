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