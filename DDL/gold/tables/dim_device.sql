CREATE TABLE gold.dim_device (
	sk smallserial,
	device varchar(50) NULL,
	CONSTRAINT device_dimension_pkey PRIMARY KEY (sk)
);