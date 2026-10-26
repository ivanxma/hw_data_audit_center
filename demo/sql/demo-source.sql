-- Illustrative sequence from the demo video. Not executed by the recording.
-- Use only in a disposable demo schema after reviewing each step.
CREATE DATABASE demo_sales;
CREATE TABLE demo_sales.orders (
  id BIGINT PRIMARY KEY,
  status VARCHAR(20) NOT NULL DEFAULT 'new',
  amount DECIMAL(10,2) NOT NULL DEFAULT 0.00
);

-- Register this existing table, prepare the data and schema streams,
-- initialize a NEW flow with End, and verify readiness before these DML steps.
-- The earlier CREATE TABLE is represented by the initial schema baseline,
-- not retrospectively captured as a historical binlog event.
INSERT INTO demo_sales.orders (id, status, amount)
VALUES (1042, 'new', 80.00);

UPDATE demo_sales.orders
SET status = 'paid', amount = 99.95
WHERE id = 1042;

DELETE FROM demo_sales.orders WHERE id = 1042;

-- Inspect schema evidence in the separate target audit_schema_changes table.
ALTER TABLE demo_sales.orders
  ADD COLUMN region VARCHAR(16) DEFAULT 'EMEA',
  MODIFY amount DECIMAL(14,2) NOT NULL DEFAULT 0.00;
