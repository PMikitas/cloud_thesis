CREATE TABLE IF NOT EXISTS THESIS_EXPERIMENTS_DB.PERFORMANCE_TRACKING.OPERATIONAL_EVENTS (
  event_id STRING,
  outbox_id NUMBER,
  occurred_at TIMESTAMP_NTZ,
  entity_name STRING,
  table_name STRING,
  entity_id STRING,
  operation STRING,
  payload STRING
);
