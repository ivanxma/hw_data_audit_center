CREATE DATABASE IF NOT EXISTS audit_control;
USE audit_control;
CREATE TABLE IF NOT EXISTS connections (
 id BIGINT AUTO_INCREMENT PRIMARY KEY,
 name VARCHAR(100) NOT NULL UNIQUE,
 role ENUM('source','target') NOT NULL,
 host VARCHAR(255) NOT NULL,
 port INT NOT NULL DEFAULT 3306,
 username VARCHAR(100) NOT NULL,
 database_name VARCHAR(64) NOT NULL,
 secret_ocid VARCHAR(255) NOT NULL,
 server_identity VARCHAR(255) NOT NULL,
 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS queue_connections (
 id BIGINT AUTO_INCREMENT PRIMARY KEY,
 name VARCHAR(100) NOT NULL UNIQUE,
 kind ENUM('oci_streaming') NOT NULL,
 resource_ocid VARCHAR(255) NOT NULL,
 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS flows (
 id BIGINT AUTO_INCREMENT PRIMARY KEY,
 name VARCHAR(100) NOT NULL UNIQUE,
 source_id BIGINT NOT NULL,
 target_id BIGINT NOT NULL,
 source_schema VARCHAR(64) NOT NULL,
    source_table VARCHAR(64) NOT NULL,
    source_exclusions JSON,
    target_grouping VARCHAR(16) NOT NULL DEFAULT 'flow',
 target_schema VARCHAR(64) NOT NULL,
 target_table VARCHAR(64) NOT NULL,
 topic VARCHAR(249) NOT NULL UNIQUE,
 transport ENUM('streaming') NOT NULL,
 connector_name VARCHAR(150) NOT NULL UNIQUE,
 queue_connection_id BIGINT,
 stream_bindings JSON,
 capture_start JSON,
 status VARCHAR(30) NOT NULL DEFAULT 'draft',
 processor_ocid VARCHAR(255),
 connector_ocid VARCHAR(255),
 last_action VARCHAR(100),
 updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 FOREIGN KEY (source_id) REFERENCES connections(id),
 FOREIGN KEY (target_id) REFERENCES connections(id),
 CONSTRAINT queue_connection_ref FOREIGN KEY (queue_connection_id) REFERENCES queue_connections(id),
 UNIQUE KEY source_registration(source_id, source_schema, source_table)
);
CREATE TABLE IF NOT EXISTS operations (
 id BIGINT AUTO_INCREMENT PRIMARY KEY,
 flow_id BIGINT,
 action VARCHAR(40) NOT NULL,
 outcome VARCHAR(30) NOT NULL,
 actor VARCHAR(100) NOT NULL,
 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS provisioned_resources (
 name VARCHAR(100) PRIMARY KEY,
 kind VARCHAR(40) NOT NULL,
 ocid VARCHAR(255) NOT NULL,
 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS config_revisions (
 id BIGINT AUTO_INCREMENT PRIMARY KEY,
 actor VARCHAR(100) NOT NULL,
 config_digest CHAR(64) NOT NULL,
 config_document JSON NOT NULL,
 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS resource_requests (
 request_token CHAR(32) PRIMARY KEY,
 kind VARCHAR(40) NOT NULL,
 name VARCHAR(150) NOT NULL,
 spec_digest CHAR(64) NOT NULL,
 resource_ocid VARCHAR(255),
 status VARCHAR(30) NOT NULL,
 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 UNIQUE KEY resource_name(kind,name)
);
CREATE TABLE IF NOT EXISTS managed_topics (
 queue_connection_id BIGINT NOT NULL,
 name VARCHAR(249) NOT NULL,
 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY(queue_connection_id,name)
);
