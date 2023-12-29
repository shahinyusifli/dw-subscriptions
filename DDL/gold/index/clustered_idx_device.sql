-- Create an index on the surrogate key
CREATE INDEX cls_idx_sk_device ON gold.dim_device (sk);

-- Cluster the table based on the index
CLUSTER gold.dim_device USING cls_idx_sk_device;