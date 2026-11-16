-- One-time move of catalog vio_travel from the job tasks bronze_silver and star_schema (removed) to the pipeline
-- vio_travel_lakehouse. A pipeline cannot take over a table it did not create, so the tables it now owns are
-- dropped first; the pipeline rebuilds them from every file in the volume raw.landing, which still holds every
-- page landed since the first run (retention 90 days). ops, ref and business_central are kept.
-- Run after `databricks bundle deploy` and before the first run of the pipeline.

DROP TABLE IF EXISTS vio_travel.bronze.tc_bookings;
DROP TABLE IF EXISTS vio_travel.bronze.tc_agencies;
DROP TABLE IF EXISTS vio_travel.bronze.tc_suppliers;
DROP TABLE IF EXISTS vio_travel.silver.bookings;
DROP TABLE IF EXISTS vio_travel.silver.booking_services;
DROP TABLE IF EXISTS vio_travel.silver.agencies;
DROP TABLE IF EXISTS vio_travel.silver.suppliers;
DROP TABLE IF EXISTS vio_travel.gold.dim_date;
DROP TABLE IF EXISTS vio_travel.gold.dim_agency;
DROP TABLE IF EXISTS vio_travel.gold.dim_supplier;
DROP TABLE IF EXISTS vio_travel.gold.dim_service_type;
DROP TABLE IF EXISTS vio_travel.gold.fact_booking;
DROP TABLE IF EXISTS vio_travel.gold.fact_booking_service;
DROP TABLE IF EXISTS vio_travel.gold.obt_booking_service;
-- progress table of the removed task bronze_silver; its build_state rows stay as history
DROP TABLE IF EXISTS vio_travel.ops.silver_progress;

-- The jobs now run as the service principal vio-travel-pipeline. bc_tables replaces the business_central tables
-- (CREATE OR REPLACE), which needs ownership; ops.bc_sync stays with the user who runs local/push_to_bc.py.
ALTER TABLE vio_travel.business_central.customers OWNER TO `82205f93-7db0-4ca5-9d78-63bd916ac9db`;
ALTER TABLE vio_travel.business_central.vendors OWNER TO `82205f93-7db0-4ca5-9d78-63bd916ac9db`;
ALTER TABLE vio_travel.business_central.sales_invoices OWNER TO `82205f93-7db0-4ca5-9d78-63bd916ac9db`;
ALTER TABLE vio_travel.business_central.sales_lines OWNER TO `82205f93-7db0-4ca5-9d78-63bd916ac9db`;
ALTER TABLE vio_travel.business_central.purchase_invoices OWNER TO `82205f93-7db0-4ca5-9d78-63bd916ac9db`;
ALTER TABLE vio_travel.business_central.purchase_lines OWNER TO `82205f93-7db0-4ca5-9d78-63bd916ac9db`;
