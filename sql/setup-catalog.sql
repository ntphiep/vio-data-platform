-- One-time setup, run in a SQL warehouse by the catalog owner. Schemas and volumes are created by the bundle.
-- A catalog on Default Storage cannot be created through the catalogs REST API in this workspace
-- ("Metastore storage root URL does not exist"); CREATE CATALOG in SQL uses Default Storage.
CREATE CATALOG IF NOT EXISTS vio_travel
  COMMENT 'Vio Travel data platform: Travel Compositor API, Lakeflow pipeline vio_travel_lakehouse, Business Central';
-- Delta maintenance (OPTIMIZE, VACUUM, statistics, liquid clustering keys of CLUSTER BY AUTO) for every managed
-- table in the catalog.
ALTER CATALOG vio_travel ENABLE PREDICTIVE OPTIMIZATION;

-- The pipeline and the jobs run as the service principal vio-travel-pipeline (application ID below). It writes
-- every layer, so it gets write privileges on the catalog, not ownership of it.
GRANT USE CATALOG, USE SCHEMA, CREATE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW, CREATE FUNCTION, SELECT,
      MODIFY, READ VOLUME, WRITE VOLUME, EXECUTE
  ON CATALOG vio_travel TO `82205f93-7db0-4ca5-9d78-63bd916ac9db`;

-- People who run the platform (group vio_operators) read every layer and write ops (local/push_to_bc.py writes
-- ops.bc_sync). Personal columns stay masked for them unless they are also in vio_pii_readers.
GRANT USE CATALOG, USE SCHEMA, SELECT, READ VOLUME, EXECUTE ON CATALOG vio_travel TO `vio_operators`;
GRANT MODIFY ON SCHEMA vio_travel.ops TO `vio_operators`;

-- The masking function must exist before the first pipeline update (the silver and gold schemas use it);
-- the job vio_travel_governance replaces it with the same definition.
CREATE SCHEMA IF NOT EXISTS vio_travel.governance COMMENT 'Masking functions and governance helpers';
CREATE OR REPLACE FUNCTION vio_travel.governance.mask_pii(v STRING)
  RETURNS STRING
  RETURN CASE
    WHEN v IS NULL THEN NULL
    WHEN is_account_group_member('vio_pii_readers') OR is_member('vio_pii_readers') THEN v
    WHEN v LIKE '%@%' THEN regexp_replace(v, '^[^@]+', '***')
    ELSE concat(repeat('*', greatest(length(v) - 3, 0)), right(v, 3)) END;
GRANT EXECUTE ON FUNCTION vio_travel.governance.mask_pii TO `82205f93-7db0-4ca5-9d78-63bd916ac9db`;

-- Mapping tables maintained by Vio Finance; the gold model reads ref.umbrella_agency on its first update.
CREATE TABLE IF NOT EXISTS vio_travel.ref.umbrella_agency (agency_id STRING, note STRING);
CREATE TABLE IF NOT EXISTS vio_travel.ref.customer_map (
  agency_reference_key STRING, bc_customer_no STRING, display_name STRING, approved_by STRING, note STRING);
CREATE TABLE IF NOT EXISTS vio_travel.ref.vendor_map (
  source_kind STRING, source_key STRING, bc_vendor_no STRING, display_name STRING, approved_by STRING, note STRING);
