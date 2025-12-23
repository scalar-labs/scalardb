-- ============================================================================
-- PostgreSQL Database Initialization Script for Export Tests
-- ============================================================================
-- This script is used by BasePostgreSQLIntegrationTest for export tests.
-- It includes:
--   - Full database schema (tables, metadata, namespaces)
--   - Test data in employee table (25 records) for export operations
--   - Coordinator tables for consensus-commit transaction manager
--
-- Usage: Used by export tests that need data to export
-- Related: init_postgres_import.sql (similar structure but no employee data)
-- ============================================================================

-- Create schemas (PostgreSQL uses schemas, not databases for namespaces)
CREATE SCHEMA IF NOT EXISTS scalardb;
SET search_path TO scalardb;

-- Coordinator table for consensus commit transactions
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

INSERT INTO "metadata" VALUES ('test.employee','email','TEXT',NULL,NULL,false,3),('test.employee','id','INT','PARTITION',NULL,false,1),('test.employee','name','TEXT',NULL,NULL,false,2),('test.emp_department','department','TEXT',NULL,NULL,false,3),('test.emp_department','emp_id','INT','CLUSTERING','ASC',false,2),('test.emp_department','id','INT','PARTITION',NULL,false,1),('test.employee_trn','before_email','TEXT',NULL,NULL,false,15),('test.employee_trn','before_name','TEXT',NULL,NULL,false,14),('test.employee_trn','before_tx_committed_at','BIGINT',NULL,NULL,false,13),('test.employee_trn','before_tx_id','TEXT',NULL,NULL,false,9),('test.employee_trn','before_tx_prepared_at','BIGINT',NULL,NULL,false,12),('test.employee_trn','before_tx_state','INT',NULL,NULL,false,10),('test.employee_trn','before_tx_version','INT',NULL,NULL,false,11),('test.employee_trn','email','TEXT',NULL,NULL,false,3),('test.employee_trn','id','INT','PARTITION',NULL,false,1),('test.employee_trn','name','TEXT',NULL,NULL,false,2),('test.employee_trn','tx_committed_at','BIGINT',NULL,NULL,false,8),('test.employee_trn','tx_id','TEXT',NULL,NULL,false,4),('test.employee_trn','tx_prepared_at','BIGINT',NULL,NULL,false,7),('test.employee_trn','tx_state','INT',NULL,NULL,false,5),('test.employee_trn','tx_version','INT',NULL,NULL,false,6), ('test.all_columns','before_col10','TIMESTAMP',NULL,NULL,false,26),('test.all_columns','before_col11','TIMESTAMPTZ',NULL,NULL,false,24),('test.all_columns','before_col4','FLOAT',NULL,NULL,false,28),('test.all_columns','before_col5','DOUBLE',NULL,NULL,false,29),('test.all_columns','before_col6','TEXT',NULL,NULL,false,25),('test.all_columns','before_col7','BLOB',NULL,NULL,false,27),('test.all_columns','before_col8','DATE',NULL,NULL,false,22),('test.all_columns','before_col9','TIME',NULL,NULL,false,23),('test.all_columns','before_tx_committed_at','BIGINT',NULL,NULL,false,21),('test.all_columns','before_tx_id','TEXT',NULL,NULL,false,17),('test.all_columns','before_tx_prepared_at','BIGINT',NULL,NULL,false,20),('test.all_columns','before_tx_state','INT',NULL,NULL,false,18),('test.all_columns','before_tx_version','INT',NULL,NULL,false,19),('test.all_columns','col1','BIGINT','PARTITION',NULL,false,1),('test.all_columns','col10','TIMESTAMP',NULL,NULL,false,10),('test.all_columns','col11','TIMESTAMPTZ',NULL,NULL,false,11),('test.all_columns','col2','INT','CLUSTERING','ASC',false,2),('test.all_columns','col3','BOOLEAN','CLUSTERING','ASC',false,3),('test.all_columns','col4','FLOAT',NULL,NULL,false,4),('test.all_columns','col5','DOUBLE',NULL,NULL,false,5),('test.all_columns','col6','TEXT',NULL,NULL,false,6),('test.all_columns','col7','BLOB',NULL,NULL,false,7),('test.all_columns','col8','DATE',NULL,NULL,false,8),('test.all_columns','col9','TIME',NULL,NULL,false,9),('test.all_columns','tx_committed_at','BIGINT',NULL,NULL,false,16),('test.all_columns','tx_id','TEXT',NULL,NULL,false,12),('test.all_columns','tx_prepared_at','BIGINT',NULL,NULL,false,15),('test.all_columns','tx_state','INT',NULL,NULL,false,13),('test.all_columns','tx_version','INT',NULL,NULL,false,14),('coordinator.state','tx_id','TEXT','PARTITION',NULL,false,1),('coordinator.state','tx_state','INT',NULL,NULL,false,2),('coordinator.state','tx_created_at','BIGINT',NULL,NULL,false,3);

DROP TABLE IF EXISTS "namespaces";
CREATE TABLE "namespaces" (
  "namespace_name" VARCHAR(128) NOT NULL,
  PRIMARY KEY ("namespace_name")
);

INSERT INTO "namespaces" VALUES ('coordinator'),('test');

-- Create test namespace schema
CREATE SCHEMA IF NOT EXISTS test;
SET search_path TO test;

CREATE TABLE "employee" (
  "id" INT NOT NULL,
  "name" TEXT,
  "email" TEXT,
  PRIMARY KEY ("id")
);

-- NOTE: This file includes test data for export tests.
--       init_postgres_import.sql has the same table structure but NO data.
INSERT INTO "employee" VALUES (0,'emp0','emp0@example.com'),(1,'emp1','emp1@example.com'),(2,'emp2','emp2@example.com'),(3,'emp3','emp3@example.com'),(4,'emp4','emp4@example.com'),(5,'emp5','emp5@example.com'),(6,'emp6','emp6@example.com'),(7,'emp7','emp7@example.com'),(8,'emp8','emp8@example.com'),(9,'emp9','emp9@example.com'),(10,'emp10','emp10@example.com'),(11,'emp11','emp11@example.com'),(12,'emp12','emp12@example.com'),(13,'emp13','emp13@example.com'),(14,'emp14','emp14@example.com'),(15,'emp15','emp15@example.com'),(16,'emp16','emp16@example.com'),(17,'emp17','emp17@example.com'),(18,'emp18','emp18@example.com'),(19,'emp19','emp19@example.com'),(20,'emp20','emp20@example.com'),(21,'emp21','emp21@example.com'),(22,'emp22','emp22@example.com'),(23,'emp23','emp23@example.com'),(24,'emp24','emp24@example.com');

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

INSERT INTO "employee_trn" VALUES  (1,'sample111n','test@11111.com','adc7139e-c86b-4dc1-bab8-6cedd1ef053e',3,2,1732686695522,1732686695696,'183fa126-bb16-4b0e-8470-94bcd034917f',3,1,1732622694074,1732622694143,'sample111n','test@11111.com'),(10,'sample333n','test@3333.com','adc7139e-c86b-4dc1-bab8-6cedd1ef053e',3,2,1732686695522,1732686695696,'183fa126-bb16-4b0e-8470-94bcd034917f',3,1,1732622694074,1732622694143,'sample333n','test@3333.com'),(100,'sample444n','test@4444.com','adc7139e-c86b-4dc1-bab8-6cedd1ef053e',3,2,1732686695522,1732686695696,'183fa126-bb16-4b0e-8470-94bcd034917f',3,1,1732622694074,1732622694143,'sample444n','test@4444.com'),(1000,'sample555n','test@5555.com','adc7139e-c86b-4dc1-bab8-6cedd1ef053e',3,2,1732686695522,1732686695696,'ce9d10de-3266-435f-a06d-959ad9866bd9',3,1,1732622694070,1732622694137,'sample555n','test@5555.com'),(10000,'sample666n','test@6666.com','adc7139e-c86b-4dc1-bab8-6cedd1ef053e',3,2,1732686695522,1732686695696,'ce9d10de-3266-435f-a06d-959ad9866bd9',3,1,1732622694070,1732622694137,'sample666n','test@6666.com');

CREATE TABLE "emp_department" (
  "id" INT NOT NULL,
  "emp_id" INT NOT NULL,
  "department" TEXT,
  PRIMARY KEY ("id","emp_id")
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

INSERT INTO "all_columns" VALUES (1,1,true,1.4e-45,5e-324,'VALUE!!s',E'\\x626C6F6220746573742076616C7565','2000-01-01','01:01:01','2000-01-01 01:01:00','1970-01-21 03:20:41.740+00','93a076fb-ad33-4acd-b66a-29ced406eab8',3,1,1741080038401,1741080038693,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),(2,2,true,1.4e-45,5e-324,'VALUE!!s',E'\\x626C6F6220746573742076616C7565','2000-01-01','01:01:01','2000-01-01 01:01:00','1970-01-21 03:20:41.740+00','93a076fb-ad33-4acd-b66a-29ced406eab8',3,1,1741080038401,1741080038693,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),(3,3,true,1.4e-45,5e-324,'VALUE!!s',E'\\x626C6F6220746573742076616C7565','2000-01-01','01:01:01','2000-01-01 01:01:00','1970-01-21 03:20:41.740+00','93a076fb-ad33-4acd-b66a-29ced406eab8',3,1,1741080038401,1741080038693,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),(4,4,true,1.4e-45,5e-324,'VALUE!!s',E'\\x626C6F6220746573742076616C7565','2000-01-01','01:01:01','2000-01-01 01:01:00','1970-01-21 03:20:41.740+00','93a076fb-ad33-4acd-b66a-29ced406eab8',3,1,1741080038401,1741080038693,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),(5,5,true,1.4e-45,5e-324,'VALUE!!s',E'\\x626C6F6220746573742076616C7565','2000-01-01','01:01:01','2000-01-01 01:01:00','1970-01-21 03:20:41.740+00','93a076fb-ad33-4acd-b66a-29ced406eab8',3,1,1741080038401,1741080038693,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);

-- Create coordinator namespace schema
CREATE SCHEMA IF NOT EXISTS coordinator;
SET search_path TO coordinator;
CREATE TABLE "state" (
                         "tx_id" VARCHAR(128) NOT NULL,
                         "tx_state" INT DEFAULT NULL,
                         "tx_created_at" BIGINT DEFAULT NULL,
                         PRIMARY KEY ("tx_id")
);
