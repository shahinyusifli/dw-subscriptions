CREATE OR REPLACE FUNCTION gold.map_to_device(device_name text)
RETURNS integer AS $$
DECLARE
    device_sk integer;
BEGIN
    SELECT sk INTO device_sk
    FROM gold.dim_device
    WHERE device = device_name;

    RETURN device_sk;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;