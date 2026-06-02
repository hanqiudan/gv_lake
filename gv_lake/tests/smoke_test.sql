-- smoke_test.sql
-- Basic smoke test for gv_lake extension

-- 1. Install prerequisites and extension
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS gv_lake;

-- 2. Verify extension is installed
\dx gv_lake

-- 3. Create namespace
SELECT * FROM gv_lake.create_namespace('sales', '{"owner":"team_a"}'::jsonb);

-- 4. List namespaces
SELECT * FROM gv_lake.list_namespaces();

-- 5. Create table
SELECT * FROM gv_lake.create_table(
    'sales',
    'orders',
    '{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":true,"type":"long"},{"id":2,"name":"amount","required":false,"type":"double"}]}'::jsonb,
    NULL,
    '{"format-version":"2"}'::jsonb,
    's3://demo-bucket/sales/orders'
);

-- 6. Load table
SELECT namespace, table_name, table_uuid, metadata_location FROM gv_lake.load_table('sales', 'orders');

-- 7. List tables
SELECT * FROM gv_lake.list_tables('sales');

-- 8. Check gv_lake_tables view
SELECT * FROM gv_lake_tables;

-- 9. Check gv_lake_namespace_properties view
SELECT * FROM gv_lake_namespace_properties;

-- 10. Commit table (success case)
DO $$
DECLARE
    v_current_location TEXT;
BEGIN
    SELECT metadata_location INTO v_current_location
    FROM gv_lake.tables
    WHERE namespace = 'sales' AND table_name = 'orders';

    -- Commit success
    PERFORM gv_lake.commit_table(
        'sales',
        'orders',
        v_current_location,
        's3://demo-bucket/sales/orders/metadata/00001-new.metadata.json',
        '{"format-version":2,"current-snapshot-id":1001}'::jsonb
    );
    RAISE NOTICE 'Commit success: metadata_location updated';
END $$;

-- Verify commit result
SELECT namespace, table_name, metadata_location, previous_metadata_location FROM gv_lake.tables WHERE namespace = 'sales' AND table_name = 'orders';

-- 11. Commit table (conflict case) - expect error
DO $$
BEGIN
    PERFORM gv_lake.commit_table(
        'sales',
        'orders',
        's3://demo-bucket/sales/orders/metadata/00000-old.metadata.json',
        's3://demo-bucket/sales/orders/metadata/00002-conflict.metadata.json'
    );
EXCEPTION WHEN SQLSTATE '40001' THEN
    RAISE NOTICE 'Commit conflict detected correctly (serialization_failure)';
END $$;

-- 12. Rename table
SELECT * FROM gv_lake.rename_table('sales', 'orders', 'sales', 'orders_renamed');

-- Verify rename
SELECT namespace, table_name FROM gv_lake.tables WHERE namespace = 'sales';

-- 13. Drop table without purge
SELECT * FROM gv_lake.drop_table('sales', 'orders_renamed', false);

-- Verify table dropped
SELECT count(*) AS remaining_tables FROM gv_lake.tables WHERE namespace = 'sales';

-- 14. Create another table to test purge
SELECT * FROM gv_lake.create_table(
    'sales',
    'test_purge',
    '{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":true,"type":"long"}]}'::jsonb
);

-- 15. Drop table with purge
SELECT * FROM gv_lake.drop_table('sales', 'test_purge', true);

-- Verify purge_queue entry
SELECT * FROM gv_lake.purge_queue;

-- 16. Drop namespace
SELECT * FROM gv_lake.drop_namespace('sales');

-- 17. Verify cleanup
SELECT * FROM gv_lake.list_namespaces();
SELECT count(*) AS total_tables FROM gv_lake.tables;
SELECT count(*) AS purge_queue_count FROM gv_lake.purge_queue;

-- Smoke test complete
SELECT 'Smoke test completed successfully!' AS result;
