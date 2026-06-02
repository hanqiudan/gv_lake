-- gv_lake--1.0.sql
-- GV Lake PostgreSQL Extension - Full C Implementation

-- ============================================
-- Schema
-- ============================================
CREATE SCHEMA IF NOT EXISTS gv_lake;

-- ============================================
-- Namespace table
-- ============================================
CREATE TABLE gv_lake.namespaces (
    namespace       TEXT PRIMARY KEY,
    properties      JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- ============================================
-- Table metadata table
-- ============================================
CREATE TABLE gv_lake.tables (
    id                          BIGSERIAL PRIMARY KEY,
    catalog_name                TEXT NOT NULL DEFAULT 'default',
    namespace                   TEXT NOT NULL,
    table_name                  TEXT NOT NULL,
    table_uuid                  UUID NOT NULL DEFAULT gen_random_uuid(),
    metadata_location           TEXT NOT NULL,
    previous_metadata_location  TEXT,
    table_location              TEXT,
    iceberg_type                VARCHAR(5) NOT NULL DEFAULT 'TABLE',
    properties                  JSONB DEFAULT '{}'::jsonb,
    metadata_json               JSONB DEFAULT '{}'::jsonb,
    created_at                  TIMESTAMPTZ DEFAULT now(),
    updated_at                  TIMESTAMPTZ DEFAULT now(),

    UNIQUE (catalog_name, namespace, table_name),
    UNIQUE (table_uuid),
    FOREIGN KEY (namespace)
        REFERENCES gv_lake.namespaces(namespace)
        ON DELETE CASCADE
);

-- ============================================
-- Purge queue table
-- ============================================
CREATE TABLE gv_lake.purge_queue (
    id                  BIGSERIAL PRIMARY KEY,
    namespace           TEXT NOT NULL,
    table_name          TEXT NOT NULL,
    metadata_location   TEXT NOT NULL,
    enqueued_at         TIMESTAMPTZ DEFAULT now(),
    processed_at        TIMESTAMPTZ
);

-- ============================================
-- gv_lake_tables view (JDBC Catalog compatible)
-- ============================================
CREATE OR REPLACE VIEW gv_lake_tables AS
SELECT
    catalog_name,
    namespace AS table_namespace,
    table_name,
    metadata_location,
    previous_metadata_location,
    iceberg_type
FROM gv_lake.tables;

-- ============================================
-- gv_lake_namespace_properties view
-- ============================================
CREATE OR REPLACE VIEW gv_lake_namespace_properties AS
SELECT
    'default'::TEXT AS catalog_name,
    namespace,
    key AS property_key,
    value AS property_value
FROM gv_lake.namespaces,
     LATERAL jsonb_each_text(properties) AS props(key, value);

-- ============================================
-- C Function: create_namespace
-- ============================================
CREATE FUNCTION gv_lake.create_namespace(
    p_namespace TEXT,
    p_properties JSONB DEFAULT '{}'::jsonb
)
RETURNS TABLE (
    namespace TEXT,
    properties JSONB
)
AS 'gv_lake', 'gv_lake_create_namespace'
LANGUAGE C;

-- ============================================
-- C Function: drop_namespace
-- ============================================
CREATE FUNCTION gv_lake.drop_namespace(
    p_namespace TEXT
)
RETURNS BOOLEAN
AS 'gv_lake', 'gv_lake_drop_namespace'
LANGUAGE C STRICT;

-- ============================================
-- C Function: list_namespaces
-- ============================================
CREATE FUNCTION gv_lake.list_namespaces()
RETURNS TABLE (
    namespace TEXT,
    properties JSONB
)
AS 'gv_lake', 'gv_lake_list_namespaces'
LANGUAGE C;

-- ============================================
-- C Function: create_table
-- ============================================
CREATE FUNCTION gv_lake.create_table(
    p_namespace TEXT,
    p_table_name TEXT,
    p_schema_json JSONB,
    p_partition_spec JSONB DEFAULT NULL,
    p_properties JSONB DEFAULT '{}'::jsonb,
    p_location TEXT DEFAULT NULL
)
RETURNS TABLE (
    namespace TEXT,
    table_name TEXT,
    table_uuid UUID,
    metadata_location TEXT,
    metadata_json JSONB
)
AS 'gv_lake', 'gv_lake_create_table'
LANGUAGE C;

-- ============================================
-- C Function: register_table
-- ============================================
CREATE FUNCTION gv_lake.register_table(
    p_namespace TEXT,
    p_table_name TEXT,
    p_metadata_location TEXT,
    p_metadata_json JSONB DEFAULT '{}'::jsonb
)
RETURNS TABLE (
    namespace TEXT,
    table_name TEXT,
    table_uuid UUID,
    metadata_location TEXT,
    metadata_json JSONB
)
AS 'gv_lake', 'gv_lake_register_table'
LANGUAGE C;

-- ============================================
-- C Function: load_table
-- ============================================
CREATE FUNCTION gv_lake.load_table(
    p_namespace TEXT,
    p_table_name TEXT
)
RETURNS TABLE (
    namespace TEXT,
    table_name TEXT,
    table_uuid UUID,
    metadata_location TEXT,
    metadata_json JSONB
)
AS 'gv_lake', 'gv_lake_load_table'
LANGUAGE C STRICT;

-- ============================================
-- C Function: list_tables
-- ============================================
CREATE FUNCTION gv_lake.list_tables(
    p_namespace TEXT
)
RETURNS TABLE (
    namespace TEXT,
    table_name TEXT,
    table_uuid UUID,
    metadata_location TEXT
)
AS 'gv_lake', 'gv_lake_list_tables'
LANGUAGE C STRICT;

-- ============================================
-- C Function: commit_table (core CAS)
-- ============================================
CREATE FUNCTION gv_lake.commit_table(
    p_namespace TEXT,
    p_table_name TEXT,
    p_expected_metadata_location TEXT,
    p_new_metadata_location TEXT,
    p_new_metadata_json JSONB DEFAULT NULL
)
RETURNS TABLE (
    namespace TEXT,
    table_name TEXT,
    table_uuid UUID,
    metadata_location TEXT,
    metadata_json JSONB
)
AS 'gv_lake', 'gv_lake_commit_table'
LANGUAGE C STRICT;

-- ============================================
-- SQL Wrapper: alter_table
-- ============================================
CREATE OR REPLACE FUNCTION gv_lake.alter_table(
    p_namespace TEXT,
    p_table_name TEXT,
    p_expected_metadata_location TEXT,
    p_new_metadata_location TEXT,
    p_updates_json JSONB DEFAULT '{}'::jsonb,
    p_new_metadata_json JSONB DEFAULT NULL
)
RETURNS TABLE (
    namespace TEXT,
    table_name TEXT,
    table_uuid UUID,
    metadata_location TEXT,
    metadata_json JSONB
)
LANGUAGE sql
AS $$
    SELECT * FROM gv_lake.commit_table(
        p_namespace,
        p_table_name,
        p_expected_metadata_location,
        p_new_metadata_location,
        COALESCE(p_new_metadata_json, p_updates_json)
    );
$$;

-- ============================================
-- C Function: drop_table
-- ============================================
CREATE FUNCTION gv_lake.drop_table(
    p_namespace TEXT,
    p_table_name TEXT,
    p_purge BOOLEAN DEFAULT false
)
RETURNS BOOLEAN
AS 'gv_lake', 'gv_lake_drop_table'
LANGUAGE C;

-- ============================================
-- C Function: unregister_table
-- ============================================
CREATE FUNCTION gv_lake.unregister_table(
    p_namespace TEXT,
    p_table_name TEXT
)
RETURNS BOOLEAN
AS 'gv_lake', 'gv_lake_unregister_table'
LANGUAGE C STRICT;

-- ============================================
-- C Function: rename_table
-- ============================================
CREATE FUNCTION gv_lake.rename_table(
    p_old_namespace TEXT,
    p_old_table_name TEXT,
    p_new_namespace TEXT,
    p_new_table_name TEXT
)
RETURNS BOOLEAN
AS 'gv_lake', 'gv_lake_rename_table'
LANGUAGE C STRICT;
