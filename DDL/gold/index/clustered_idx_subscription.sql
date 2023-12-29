-- Create an index on the surrogate key
CREATE INDEX cls_idx_sk_subs ON gold.dim_subscription (sk);

-- Cluster the table based on the index
CLUSTER gold.dim_subscription USING cls_idx_sk_subs;