-- test_all_c.sql
-- Full C function verification for gv_lake extension

-- 1. Check functions are C implementation
SELECT proname, prosrc FROM pg_proc
WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'gv_lake')
ORDER BY proname;

-- 2. Test create_namespace (C)
SELECT * FROM gv_lake.create_namespace('c_test', '{"owner":"c_impl"}'::jsonb);

-- 3. Test list_namespaces (C)
SELECT * FROM gv_lake.list_namespaces();

-- 4. Test create_table (C)
SELECT namespace, table_name, metadata_location FROM gv_lake.create_table(
    'c_test',
    'c_table',
    '{"type":"struct","fields":[{"id":1,"name":"id","type":"long"}]}'::jsonb
);

-- 5. Test list_tables (C)
SELECT * FROM gv_lake.list_tables('c_test');

-- 6. Test load_table (C)
SELECT namespace, table_name, metadata_location FROM gv_lake.load_table('c_test', 'c_table');

-- 7. Test register_table (C)
SELECT namespace, table_name FROM gv_lake.register_table(
    'c_test',
    'registered_table',
    's3://external/metadata.json',
    '{"location":"s3://external","properties":{"owner":"external"}}'::jsonb
);

-- 8. Verify register result
SELECT namespace, table_name, metadata_location FROM gv_lake.tables
WHERE namespace = 'c_test' AND table_name = 'registered_table';

-- 9. Test rename_table (C)
SELECT * FROM gv_lake.rename_table('c_test', 'c_table', 'c_test', 'renamed_table');

-- 10. Verify rename
SELECT table_name FROM gv_lake.tables WHERE namespace = 'c_test';

-- 11. Get metadata_location for commit test
SELECT metadata_location FROM gv_lake.tables
WHERE namespace = 'c_test' AND table_name = 'renamed_table';

-- 12. Test commit_table (C) - success
DO $$
DECLARE
    v_loc TEXT;
BEGIN
    SELECT metadata_location INTO v_loc FROM gv_lake.tables
    WHERE namespace = 'c_test' AND table_name = 'renamed_table';

    PERFORM gv_lake.commit_table(
        'c_test', 'renamed_table',
        v_loc,
        's3://demo-bucket/c_test/renamed_table/metadata/v2.metadata.json'
    );
    RAISE NOTICE 'commit_table (C) succeeded';
END $$;

-- 13. Verify commit
SELECT metadata_location, previous_metadata_location FROM gv_lake.tables
WHERE namespace = 'c_test' AND table_name = 'renamed_table';

-- 14. Test commit_table (C) - conflict
DO $$
BEGIN
    PERFORM gv_lake.commit_table(
        'c_test', 'renamed_table',
        's3://old/metadata.json',
        's3://new/metadata.json'
    );
EXCEPTION WHEN SQLSTATE '40001' THEN
    RAISE NOTICE 'commit_table (C) conflict detected correctly';
END $$;

-- 15. Test drop_table with purge (C)
SELECT * FROM gv_lake.drop_table('c_test', 'registered_table', true);

-- 16. Verify purge_queue
SELECT namespace, table_name FROM gv_lake.purge_queue;

-- 17. Test drop_table without purge (C)
SELECT * FROM gv_lake.drop_table('c_test', 'renamed_table', false);

-- 18. Test create and unregister (C)
SELECT namespace, table_name FROM gv_lake.create_table(
    'c_test', 'to_unregister',
    '{"type":"struct","fields":[{"id":1,"name":"id","type":"long"}]}'::jsonb
);
SELECT * FROM gv_lake.unregister_table('c_test', 'to_unregister');

-- 19. Test drop_namespace (C)
SELECT * FROM gv_lake.drop_namespace('c_test');

-- 20. Test alter_table (SQL wrapper)
SELECT * FROM gv_lake.create_namespace('alter_test', '{"owner":"test"}'::jsonb);
SELECT namespace, table_name FROM gv_lake.create_table(
    'alter_test', 'alter_table',
    '{"type":"struct","fields":[{"id":1,"name":"id","type":"long"}]}'::jsonb
);

DO $$
DECLARE
    v_loc TEXT;
BEGIN
    SELECT metadata_location INTO v_loc FROM gv_lake.tables
    WHERE namespace = 'alter_test' AND table_name = 'alter_table';

    PERFORM gv_lake.alter_table(
        'alter_test', 'alter_table',
        v_loc,
        's3://demo-bucket/alter_test/alter_table/metadata/v2.metadata.json',
        '{"owner":"updated"}'::jsonb
    );
    RAISE NOTICE 'alter_table (SQL wrapper) succeeded';
END $$;

-- Cleanup
SELECT * FROM gv_lake.drop_table('alter_test', 'alter_table', false);
SELECT * FROM gv_lake.drop_namespace('alter_test');

-- 21. Verify all C function symbols
SELECT proname, prosrc FROM pg_proc
WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'gv_lake')
AND prosrc NOT IN ('', '-')
ORDER BY proname;

-- 22. Final check
SELECT 'All C functions test completed!' AS result;
