-- Create an index on the inserted_date
CREATE INDEX idx_inserted_date ON bronze.subscriptions (inserted_date);

-- Cluster the table based on the index
CLUSTER bronze.subscriptions USING idx_inserted_date;