BEGIN;

WITH extracted_subscriptions AS (
        SELECT DISTINCT subscription_type,
                        plan_duration
        FROM silver.subscriptions
        order by subscription_type
)


INSERT INTO gold.dim_subscription (subscription_type, plan_duration)
SELECT es.subscription_type, es.plan_duration
FROM extracted_subscriptions es
LEFT JOIN  gold.dim_subscription sd
    ON es.subscription_type = sd.subscription_type
    AND es.plan_duration = sd.plan_duration
WHERE sd.subscription_type IS NULL; 


CLUSTER gold.dim_subscription;

COMMIT;