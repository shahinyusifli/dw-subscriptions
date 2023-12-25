CREATE TABLE gold.dim_device (
	sk serial4,
	device varchar(50) NULL,
	CONSTRAINT device_dimension_pkey PRIMARY KEY (sk)
);