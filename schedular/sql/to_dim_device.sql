INSERT INTO gold.dim_device (device)
SELECT DISTINCT device
FROM silver.subscriptions
WHERE device NOT IN (SELECT DISTINCT device FROM gold.dim_device);