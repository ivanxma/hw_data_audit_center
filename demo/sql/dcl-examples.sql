-- DCL/account-management examples only. Not executed by the recording.
-- This application's row-CDC flow does NOT implement a security audit collector.
-- A separately configured MySQL Enterprise Audit source and collector are required.
-- The demo account is locked and limited to localhost.
CREATE USER 'audit_demo_reader'@'localhost' ACCOUNT LOCK;
GRANT SELECT ON demo_sales.* TO 'audit_demo_reader'@'localhost';
REVOKE SELECT ON demo_sales.* FROM 'audit_demo_reader'@'localhost';
SHOW GRANTS FOR 'audit_demo_reader'@'localhost';

-- Explicit cleanup of this disposable demo account only.
DROP USER 'audit_demo_reader'@'localhost';
