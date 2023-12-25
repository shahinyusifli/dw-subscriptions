CREATE OR REPLACE FUNCTION gold.map_to_subscription(
    p_subscription_type varchar(255),
    p_plan_duration int
) RETURNS integer AS
$$
DECLARE
    subscription_sk integer;
BEGIN
    SELECT sk INTO subscription_sk
    FROM gold.dim_subscription
    WHERE
        subscription_type = p_subscription_type
        AND plan_duration = p_plan_duration;

    RETURN subscription_sk;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;
