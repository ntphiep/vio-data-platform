-- One-time setup, run in a SQL warehouse by the catalog owner. Schemas and volumes are created by the bundle.
-- A catalog on Default Storage cannot be created through the catalogs REST API in this workspace
-- ("Metastore storage root URL does not exist"); CREATE CATALOG in SQL uses Default Storage.
CREATE CATALOG IF NOT EXISTS vio_travel
  COMMENT 'Vio Travel: Travel Compositor API to Business Central pipeline (job vio_travel_pipeline)';
-- Delta maintenance (OPTIMIZE, VACUUM, statistics) for every managed table in the catalog.
ALTER CATALOG vio_travel ENABLE PREDICTIVE OPTIMIZATION;
