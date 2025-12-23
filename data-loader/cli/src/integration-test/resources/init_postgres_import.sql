-- ============================================================================
-- PostgreSQL Database Initialization Script for Import Tests
-- ============================================================================
-- This script is used by import test classes (ImportCommandCsvPostgreSQLIT, etc.).
-- It includes:
--   - Full database schema (tables, metadata, namespaces)
--   - NO test data in employee table (clean slate for import tests)
--   - Coordinator tables for consensus-commit transaction manager
--
-- Usage: Used by import tests that need empty tables to import into
-- Related: init_postgres.sql (similar structure but includes employee data)
--
-- NOTE: Keep this file in sync with init_postgres.sql structure.
--       Only difference should be the presence/absence of employee table data.
-- ============================================================================

-- Create schemas (PostgreSQL uses schemas, not databases for namespaces)
CREATE SCHEMA IF NOT EXISTS scalardb;
SET search_path TO scalardb;

-- Coordinator table for consensus commit transactions (scalardb namespace)
DROP TABLE IF EXISTS "coordinator";
CREATE TABLE "coordinator" (
  "tx_id" VARCHAR(128) NOT NULL,
  "tx_state" INT NOT NULL,
  "tx_created_at" BIGINT NOT NULL,
  PRIMARY KEY ("tx_id")
);

DROP TABLE IF EXISTS "metadata";
CREATE TABLE "metadata" (
  "full_table_name" VARCHAR(128) NOT NULL,
  "column_name" VARCHAR(128) NOT NULL,
  "data_type" VARCHAR(20) NOT NULL,
  "key_type" VARCHAR(20) DEFAULT NULL,
  "clustering_order" VARCHAR(10) DEFAULT NULL,
  "indexed" BOOLEAN NOT NULL,
  "ordinal_position" INT NOT NULL,
  PRIMARY KEY ("full_table_name","column_name")
);

INSERT INTO "metadata" VALUES ('test.employee','email','TEXT',NULL,NULL,false,3),('test.employee','id','INT','PARTITION',NULL,false,1),('test.employee','name','TEXT',NULL,NULL,false,2),('test.employee_trn','before_email','TEXT',NULL,NULL,false,15),('test.employee_trn','before_name','TEXT',NULL,NULL,false,14),('test.employee_trn','before_tx_committed_at','BIGINT',NULL,NULL,false,13),('test.employee_trn','before_tx_id','TEXT',NULL,NULL,false,9),('test.employee_trn','before_tx_prepared_at','BIGINT',NULL,NULL,false,12),('test.employee_trn','before_tx_state','INT',NULL,NULL,false,10),('test.employee_trn','before_tx_version','INT',NULL,NULL,false,11),('test.employee_trn','email','TEXT',NULL,NULL,false,3),('test.employee_trn','id','INT','PARTITION',NULL,false,1),('test.employee_trn','name','TEXT',NULL,NULL,false,2),('test.employee_trn','tx_committed_at','BIGINT',NULL,NULL,false,8),('test.employee_trn','tx_id','TEXT',NULL,NULL,false,4),('test.employee_trn','tx_prepared_at','BIGINT',NULL,NULL,false,7),('test.employee_trn','tx_state','INT',NULL,NULL,false,5),('test.employee_trn','tx_version','INT',NULL,NULL,false,6), ('coordinator.state','tx_created_at','BIGINT',NULL,NULL,false,3),('coordinator.state','tx_id','TEXT','PARTITION',NULL,false,1),('coordinator.state','tx_state','INT',NULL,NULL,false,2), ('test.all_columns','before_col10','TIMESTAMP',NULL,NULL,false,26),('test.all_columns','before_col11','TIMESTAMPTZ',NULL,NULL,false,24),('test.all_columns','before_col4','FLOAT',NULL,NULL,false,28),('test.all_columns','before_col5','DOUBLE',NULL,NULL,false,29),('test.all_columns','before_col6','TEXT',NULL,NULL,false,25),('test.all_columns','before_col7','BLOB',NULL,NULL,false,27),('test.all_columns','before_col8','DATE',NULL,NULL,false,22),('test.all_columns','before_col9','TIME',NULL,NULL,false,23),('test.all_columns','before_tx_committed_at','BIGINT',NULL,NULL,false,21),('test.all_columns','before_tx_id','TEXT',NULL,NULL,false,17),('test.all_columns','before_tx_prepared_at','BIGINT',NULL,NULL,false,20),('test.all_columns','before_tx_state','INT',NULL,NULL,false,18),('test.all_columns','before_tx_version','INT',NULL,NULL,false,19),('test.all_columns','col1','BIGINT','PARTITION',NULL,false,1),('test.all_columns','col10','TIMESTAMP',NULL,NULL,false,10),('test.all_columns','col11','TIMESTAMPTZ',NULL,NULL,false,11),('test.all_columns','col2','INT','CLUSTERING','ASC',false,2),('test.all_columns','col3','BOOLEAN','CLUSTERING','ASC',false,3),('test.all_columns','col4','FLOAT',NULL,NULL,false,4),('test.all_columns','col5','DOUBLE',NULL,NULL,false,5),('test.all_columns','col6','TEXT',NULL,NULL,false,6),('test.all_columns','col7','BLOB',NULL,NULL,false,7),('test.all_columns','col8','DATE',NULL,NULL,false,8),('test.all_columns','col9','TIME',NULL,NULL,false,9),('test.all_columns','tx_committed_at','BIGINT',NULL,NULL,false,16),('test.all_columns','tx_id','TEXT',NULL,NULL,false,12),('test.all_columns','tx_prepared_at','BIGINT',NULL,NULL,false,15),('test.all_columns','tx_state','INT',NULL,NULL,false,13),('test.all_columns','tx_version','INT',NULL,NULL,false,14),('test.emp_department','department','TEXT',NULL,NULL,false,3),('test.emp_department','emp_id','INT','CLUSTERING','ASC',false,2),('test.emp_department','id','INT','PARTITION',NULL,false,1);

DROP TABLE IF EXISTS "namespaces";
CREATE TABLE "namespaces" (
  "namespace_name" VARCHAR(128) NOT NULL,
  PRIMARY KEY ("namespace_name")
);

INSERT INTO "namespaces" VALUES ('coordinator'),('test');

-- Create test namespace schema
CREATE SCHEMA IF NOT EXISTS test;
SET search_path TO test;

CREATE TABLE "emp_department" (
  "id" INT NOT NULL,
  "emp_id" INT NOT NULL,
  "department" TEXT,
  PRIMARY KEY ("id","emp_id")
);

CREATE TABLE "employee" (
  "id" INT NOT NULL,
  "name" TEXT,
  "email" TEXT,
  PRIMARY KEY ("id")
);

-- NOTE: This file does NOT include test data in employee table.
--       init_postgres.sql has the same table structure WITH 25 test records.
--       This allows import tests to start with a clean slate.

CREATE TABLE "employee_trn" (
"id" INT NOT NULL,
"name" TEXT,
"email" TEXT,
"tx_id" TEXT,
"tx_state" INT DEFAULT NULL,
"tx_version" INT DEFAULT NULL,
"tx_prepared_at" BIGINT DEFAULT NULL,
"tx_committed_at" BIGINT DEFAULT NULL,
"before_tx_id" TEXT,
"before_tx_state" INT DEFAULT NULL,
"before_tx_version" INT DEFAULT NULL,
"before_tx_prepared_at" BIGINT DEFAULT NULL,
"before_tx_committed_at" BIGINT DEFAULT NULL,
"before_name" TEXT,
"before_email" TEXT,
PRIMARY KEY ("id")
);

CREATE TABLE "all_columns" (
  "col1" BIGINT NOT NULL,
  "col2" INT NOT NULL,
  "col3" BOOLEAN NOT NULL,
  "col4" DOUBLE PRECISION DEFAULT NULL,
  "col5" DOUBLE PRECISION DEFAULT NULL,
  "col6" TEXT,
  "col7" BYTEA,
  "col8" DATE DEFAULT NULL,
  "col9" TIME DEFAULT NULL,
  "col10" TIMESTAMP DEFAULT NULL,
  "col11" TIMESTAMP WITH TIME ZONE DEFAULT NULL,
  "tx_id" TEXT,
  "tx_state" INT DEFAULT NULL,
  "tx_version" INT DEFAULT NULL,
  "tx_prepared_at" BIGINT DEFAULT NULL,
  "tx_committed_at" BIGINT DEFAULT NULL,
  "before_tx_id" TEXT,
  "before_tx_state" INT DEFAULT NULL,
  "before_tx_version" INT DEFAULT NULL,
  "before_tx_prepared_at" BIGINT DEFAULT NULL,
  "before_tx_committed_at" BIGINT DEFAULT NULL,
  "before_col8" DATE DEFAULT NULL,
  "before_col9" TIME DEFAULT NULL,
  "before_col11" TIMESTAMP WITH TIME ZONE DEFAULT NULL,
  "before_col6" TEXT,
  "before_col10" TIMESTAMP DEFAULT NULL,
  "before_col7" BYTEA,
  "before_col4" DOUBLE PRECISION DEFAULT NULL,
  "before_col5" DOUBLE PRECISION DEFAULT NULL,
  PRIMARY KEY ("col1","col2","col3")
);

-- Create coordinator namespace schema
CREATE SCHEMA IF NOT EXISTS coordinator;
SET search_path TO coordinator;
CREATE TABLE "state" (
  "tx_id" VARCHAR(128) NOT NULL,
  "tx_state" INT DEFAULT NULL,
  "tx_created_at" BIGINT DEFAULT NULL,
  PRIMARY KEY ("tx_id")
);

