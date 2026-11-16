-- Lab seed for the mapping tables created by src/bronze_silver.py.
-- These rows are NOT approved by Vio Travel Finance. They only let the lab push a few real bookings.
-- In production, Finance owns ref.customer_map and ref.vendor_map (YVS spec API_Sales_T01 row 18,
-- API_Purchases_T01 row 19: an unmapped customer or supplier is held, never sent).
-- Run once per statement through the SQL warehouse; MERGE keeps it idempotent.

MERGE INTO vio_travel.ref.umbrella_agency t
USING (SELECT '130032' AS agency_id, 'Vio Travel Limited: real customer is agencyBookingReference' AS note) s
ON t.agency_id = s.agency_id WHEN NOT MATCHED THEN INSERT *;

MERGE INTO vio_travel.ref.customer_map t
USING (SELECT * FROM VALUES
  ('exoticca usd',         'CEXOTICCA',  'Exoticca',             'LAB SEED, not approved by Vio Finance', 'currency suffix dropped'),
  ('exoticca eur',         'CEXOTICCA',  'Exoticca',             'LAB SEED, not approved by Vio Finance', 'currency suffix dropped'),
  ('exoticca aud',         'CEXOTICCA',  'Exoticca',             'LAB SEED, not approved by Vio Finance', 'currency suffix dropped'),
  ('secret escapes',       'CSECRETESC', 'Secret Escapes',       'LAB SEED, not approved by Vio Finance', NULL),
  ('kimkim',               'CKIMKIM',    'kimkim',               'LAB SEED, not approved by Vio Finance', NULL),
  ('first class holidays', 'AG286039',   'First Class Holidays', 'LAB SEED, not approved by Vio Finance', 'same name as agency 286039'),
  ('eturia',               'AG160506',   'Eturia SRL',           'LAB SEED, not approved by Vio Finance', 'same name as agency 160506')
  AS v(agency_reference_key, bc_customer_no, display_name, approved_by, note)) s
ON t.agency_reference_key = s.agency_reference_key WHEN NOT MATCHED THEN INSERT *;

MERGE INTO vio_travel.ref.vendor_map t
USING (SELECT * FROM VALUES
  ('MANUAL_NAME', 'shingo travel., llc', 'MVSHINGO', 'Shingo Travel., LLC', 'LAB SEED, not approved by Vio Finance', NULL),
  ('MANUAL_NAME', 'airme',               'MVAIRME',  'AIRME',               'LAB SEED, not approved by Vio Finance', NULL),
  ('MANUAL_NAME', 'his',                 'MVHIS',    'HIS',                 'LAB SEED, not approved by Vio Finance', NULL)
  AS v(source_kind, source_key, bc_vendor_no, display_name, approved_by, note)) s
ON t.source_kind = s.source_kind AND t.source_key = s.source_key WHEN NOT MATCHED THEN INSERT *;
