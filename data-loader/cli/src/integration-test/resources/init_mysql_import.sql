CREATE DATABASE IF NOT EXISTS scalardb;
USE scalardb;

-- Coordinator table for consensus commit transactions (scalardb namespace)
DROP TABLE IF EXISTS `coordinator`;
CREATE TABLE `coordinator` (
  `tx_id` varchar(128) NOT NULL,
  `tx_state` int NOT NULL,
  `tx_created_at` bigint NOT NULL,
  PRIMARY KEY (`tx_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `metadata`;
CREATE TABLE `metadata` (
  `full_table_name` varchar(128) NOT NULL,
  `column_name` varchar(128) NOT NULL,
  `data_type` varchar(20) NOT NULL,
  `key_type` varchar(20) DEFAULT NULL,
  `clustering_order` varchar(10) DEFAULT NULL,
  `indexed` tinyint(1) NOT NULL,
  `ordinal_position` int NOT NULL,
  PRIMARY KEY (`full_table_name`,`column_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
LOCK TABLES `metadata` WRITE;
INSERT INTO `metadata` VALUES ('test.employee','email','TEXT',NULL,NULL,0,3),('test.employee','id','INT','PARTITION',NULL,0,1),('test.employee','name','TEXT',NULL,NULL,0,2),('test.employee_trn','before_email','TEXT',NULL,NULL,0,15),('test.employee_trn','before_name','TEXT',NULL,NULL,0,14),('test.employee_trn','before_tx_committed_at','BIGINT',NULL,NULL,0,13),('test.employee_trn','before_tx_id','TEXT',NULL,NULL,0,9),('test.employee_trn','before_tx_prepared_at','BIGINT',NULL,NULL,0,12),('test.employee_trn','before_tx_state','INT',NULL,NULL,0,10),('test.employee_trn','before_tx_version','INT',NULL,NULL,0,11),('test.employee_trn','email','TEXT',NULL,NULL,0,3),('test.employee_trn','id','INT','PARTITION',NULL,0,1),('test.employee_trn','name','TEXT',NULL,NULL,0,2),('test.employee_trn','tx_committed_at','BIGINT',NULL,NULL,0,8),('test.employee_trn','tx_id','TEXT',NULL,NULL,0,4),('test.employee_trn','tx_prepared_at','BIGINT',NULL,NULL,0,7),('test.employee_trn','tx_state','INT',NULL,NULL,0,5),('test.employee_trn','tx_version','INT',NULL,NULL,0,6), ('coordinator.state','tx_created_at','BIGINT',NULL,NULL,0,3),('coordinator.state','tx_id','TEXT','PARTITION',NULL,0,1),('coordinator.state','tx_state','INT',NULL,NULL,0,2), ('test.all_columns','before_col10','TIMESTAMP',NULL,NULL,0,26),('test.all_columns','before_col11','TIMESTAMPTZ',NULL,NULL,0,24),('test.all_columns','before_col4','FLOAT',NULL,NULL,0,28),('test.all_columns','before_col5','DOUBLE',NULL,NULL,0,29),('test.all_columns','before_col6','TEXT',NULL,NULL,0,25),('test.all_columns','before_col7','BLOB',NULL,NULL,0,27),('test.all_columns','before_col8','DATE',NULL,NULL,0,22),('test.all_columns','before_col9','TIME',NULL,NULL,0,23),('test.all_columns','before_tx_committed_at','BIGINT',NULL,NULL,0,21),('test.all_columns','before_tx_id','TEXT',NULL,NULL,0,17),('test.all_columns','before_tx_prepared_at','BIGINT',NULL,NULL,0,20),('test.all_columns','before_tx_state','INT',NULL,NULL,0,18),('test.all_columns','before_tx_version','INT',NULL,NULL,0,19),('test.all_columns','col1','BIGINT','PARTITION',NULL,0,1),('test.all_columns','col10','TIMESTAMP',NULL,NULL,0,10),('test.all_columns','col11','TIMESTAMPTZ',NULL,NULL,0,11),('test.all_columns','col2','INT','CLUSTERING','ASC',0,2),('test.all_columns','col3','BOOLEAN','CLUSTERING','ASC',0,3),('test.all_columns','col4','FLOAT',NULL,NULL,0,4),('test.all_columns','col5','DOUBLE',NULL,NULL,0,5),('test.all_columns','col6','TEXT',NULL,NULL,0,6),('test.all_columns','col7','BLOB',NULL,NULL,0,7),('test.all_columns','col8','DATE',NULL,NULL,0,8),('test.all_columns','col9','TIME',NULL,NULL,0,9),('test.all_columns','tx_committed_at','BIGINT',NULL,NULL,0,16),('test.all_columns','tx_id','TEXT',NULL,NULL,0,12),('test.all_columns','tx_prepared_at','BIGINT',NULL,NULL,0,15),('test.all_columns','tx_state','INT',NULL,NULL,0,13),('test.all_columns','tx_version','INT',NULL,NULL,0,14),('test.emp_department','department','TEXT',NULL,NULL,0,3),('test.emp_department','emp_id','INT','CLUSTERING','ASC',0,2),('test.emp_department','id','INT','PARTITION',NULL,0,1);
UNLOCK TABLES;

DROP TABLE IF EXISTS `namespaces`;
CREATE TABLE `namespaces` (
  `namespace_name` varchar(128) NOT NULL,
  PRIMARY KEY (`namespace_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

LOCK TABLES `namespaces` WRITE;
INSERT INTO `namespaces` VALUES ('coordinator'),('test');
UNLOCK TABLES;



CREATE DATABASE IF NOT EXISTS test;
USE test;

CREATE TABLE `emp_department` (
  `id` int NOT NULL,
  `emp_id` int NOT NULL,
  `department` longtext,
  PRIMARY KEY (`id`,`emp_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

USE test;
CREATE TABLE `employee` (
  `id` int NOT NULL,
  `name` longtext,
  `email` longtext,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

USE test;
CREATE TABLE `employee_trn` (
`id` int NOT NULL,
`name` longtext,
`email` longtext,
`tx_id` longtext,
`tx_state` int DEFAULT NULL,
`tx_version` int DEFAULT NULL,
`tx_prepared_at` bigint DEFAULT NULL,
`tx_committed_at` bigint DEFAULT NULL,
`before_tx_id` longtext,
`before_tx_state` int DEFAULT NULL,
`before_tx_version` int DEFAULT NULL,
`before_tx_prepared_at` bigint DEFAULT NULL,
`before_tx_committed_at` bigint DEFAULT NULL,
`before_name` longtext,
`before_email` longtext,
PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

USE test;
CREATE TABLE `all_columns` (
  `col1` bigint NOT NULL,
  `col2` int NOT NULL,
  `col3` tinyint(1) NOT NULL,
  `col4` double DEFAULT NULL,
  `col5` double DEFAULT NULL,
  `col6` longtext,
  `col7` longblob,
  `col8` date DEFAULT NULL,
  `col9` time(6) DEFAULT NULL,
  `col10` datetime(3) DEFAULT NULL,
  `col11` datetime(3) DEFAULT NULL,
  `tx_id` longtext,
  `tx_state` int DEFAULT NULL,
  `tx_version` int DEFAULT NULL,
  `tx_prepared_at` bigint DEFAULT NULL,
  `tx_committed_at` bigint DEFAULT NULL,
  `before_tx_id` longtext,
  `before_tx_state` int DEFAULT NULL,
  `before_tx_version` int DEFAULT NULL,
  `before_tx_prepared_at` bigint DEFAULT NULL,
  `before_tx_committed_at` bigint DEFAULT NULL,
  `before_col8` date DEFAULT NULL,
  `before_col9` time(6) DEFAULT NULL,
  `before_col11` datetime(3) DEFAULT NULL,
  `before_col6` longtext,
  `before_col10` datetime(3) DEFAULT NULL,
  `before_col7` longblob,
  `before_col4` double DEFAULT NULL,
  `before_col5` double DEFAULT NULL,
  PRIMARY KEY (`col1`,`col2`,`col3`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE DATABASE IF NOT EXISTS coordinator;
USE coordinator;
CREATE TABLE `state` (
  `tx_id` varchar(128) NOT NULL,
  `tx_state` int DEFAULT NULL,
  `tx_created_at` bigint DEFAULT NULL,
  PRIMARY KEY (`tx_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

