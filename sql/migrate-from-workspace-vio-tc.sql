-- One-time move of the lab state from schema workspace.vio_tc (old layout, one schema with prefixes) into
-- catalog vio_travel. Landing files are copied separately (databricks fs cp), and bronze, silver and gold are
-- rebuilt from them by the job. Only run-history, mapping and sync tables are carried over.
CREATE TABLE IF NOT EXISTS vio_travel.ops.api_calls AS SELECT * FROM workspace.vio_tc.ops_api_calls;
CREATE TABLE IF NOT EXISTS vio_travel.ops.ingest_runs AS
  SELECT run_id, started_at, finished_at, entity, window_from, window_to, pages, records,
         CAST(NULL AS INT) AS distinct_keys, CAST(NULL AS INT) AS expected, bytes, status, error
  FROM workspace.vio_tc.ops_ingest_runs;
CREATE TABLE IF NOT EXISTS vio_travel.ops.watermark AS
  SELECT entity, watermark, updated_at, CAST(NULL AS STRING) AS run_id FROM workspace.vio_tc.ops_watermark;
CREATE TABLE IF NOT EXISTS vio_travel.ops.dq_results AS
  SELECT CAST(NULL AS STRING) AS run_id, checked_at, check_name,
         CASE WHEN blocking THEN 'block' ELSE 'warn' END AS severity, failed_rows, sample
  FROM workspace.vio_tc.ops_dq_results;
CREATE TABLE IF NOT EXISTS vio_travel.ops.bc_sync AS SELECT * FROM workspace.vio_tc.ops_bc_sync;
CREATE TABLE IF NOT EXISTS vio_travel.ref.umbrella_agency AS SELECT * FROM workspace.vio_tc.ref_umbrella_agency;
CREATE TABLE IF NOT EXISTS vio_travel.ref.customer_map AS SELECT * FROM workspace.vio_tc.ref_customer_map;
CREATE TABLE IF NOT EXISTS vio_travel.ref.vendor_map AS SELECT * FROM workspace.vio_tc.ref_vendor_map;
