-- Create an index on the business key
CREATE INDEX cls_idx_id_account ON gold.dim_account (sk);

-- Cluster the table based on the index
CLUSTER gold.dim_account USING cls_idx_id_account;