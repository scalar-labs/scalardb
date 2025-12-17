CREATE DATABASE scalardb;
\c scalardb;

DROP TABLE IF EXISTS metadata;
CREATE TABLE metadata (
  full_table_name VARCHAR(128) NOT NULL,
  column_name VARCHAR(128) NOT NULL,
  data_type VARCHAR(20) NOT NULL,
  key_type VARCHAR(20),
  clustering_order VARCHAR(10),
  indexed BOOLEAN NOT NULL,
  ordinal_position INT NOT NULL,
  PRIMARY KEY (full_table_name, column_name)
);


INSERT INTO metadata VALUES
  ('test.employee','email','TEXT',NULL,NULL,false,3),
  ('test.employee','id','INT','PARTITION',NULL,false,1),
  ('test.employee','name','TEXT',NULL,NULL,false,2),
  ('testt.employee_trn','before_email','TEXT',NULL,NULL,false,15),
  ('testt.employee_trn','before_name','TEXT',NULL,NULL,false,14),
  ('testt.employee_trn','before_tx_committed_at','BIGINT',NULL,NULL,false,13),
  ('testt.employee_trn','before_tx_id','TEXT',NULL,NULL,false,9),
  ('testt.employee_trn','before_tx_prepared_at','BIGINT',NULL,NULL,false,12),
  ('testt.employee_trn','before_tx_state','INT',NULL,NULL,false,10),
  ('testt.employee_trn','before_tx_version','INT',NULL,NULL,false,11),
  ('testt.employee_trn','email','TEXT',NULL,NULL,false,3),
  ('testt.employee_trn','id','INT','PARTITION',NULL,false,1),
  ('testt.employee_trn','name','TEXT',NULL,NULL,false,2),
  ('testt.employee_trn','tx_committed_at','BIGINT',NULL,NULL,false,8),
  ('testt.employee_trn','tx_id','TEXT',NULL,NULL,false,4),
  ('testt.employee_trn','tx_prepared_at','BIGINT',NULL,NULL,false,7),
  ('testt.employee_trn','tx_state','INT',NULL,NULL,false,5),
  ('testt.employee_trn','tx_version','INT',NULL,NULL,false,6);


DROP TABLE IF EXISTS `namespaces`;
CREATE TABLE `namespaces` (
  `namespace_name` varchar(128) NOT NULL,
  PRIMARY KEY (`namespace_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `namespaces` VALUES ('test');


CREATE DATABASE test;
\c test;

CREATE TABLE employee (
  id INT NOT NULL,
  name TEXT,
  email TEXT,
  PRIMARY KEY (id)
);

INSERT INTO employee VALUES
  (0,'emp0','emp0@example.com'),
  (1,'emp1','emp1@example.com'),
  (2,'emp2','emp2@example.com'),
  (3,'emp3','emp3@example.com'),
  (4,'emp4','emp4@example.com'),
  (5,'emp5','emp5@example.com'),
  (6,'emp6','emp6@example.com'),
  (7,'emp7','emp7@example.com'),
  (8,'emp8','emp8@example.com'),
  (9,'emp9','emp9@example.com'),
  (10,'emp10','emp10@example.com'),
  (11,'emp11','emp11@example.com'),
  (12,'emp12','emp12@example.com'),
  (13,'emp13','emp13@example.com'),
  (14,'emp14','emp14@example.com'),
  (15,'emp15','emp15@example.com'),
  (16,'emp16','emp16@example.com'),
  (17,'emp17','emp17@example.com'),
  (18,'emp18','emp18@example.com'),
  (19,'emp19','emp19@example.com'),
  (20,'emp20','emp20@example.com'),
  (21,'emp21','emp21@example.com'),
  (22,'emp22','emp22@example.com'),
  (23,'emp23','emp23@example.com'),
  (24,'emp24','emp24@example.com');

CREATE TABLE employee_trn (
  id INT NOT NULL,
  name TEXT,
  email TEXT,
  tx_id TEXT,
  tx_state INT,
  tx_version INT,
  tx_prepared_at BIGINT,
  tx_committed_at BIGINT,
  before_tx_id TEXT,
  before_tx_state INT,
  before_tx_version INT,
  before_tx_prepared_at BIGINT,
  before_tx_committed_at BIGINT,
  before_name TEXT,
  before_email TEXT,
  PRIMARY KEY (id)
);

INSERT INTO employee_trn VALUES
  (1,'sample111n','test@11111.com','adc7139e-c86b-4dc1-bab8-6cedd1ef053e',3,2,1732686695522,1732686695696,'183fa126-bb16-4b0e-8470-94bcd034917f',3,1,1732622694074,1732622694143,'sample111n','test@11111.com'),
  (10,'sample333n','test@3333.com','adc7139e-c86b-4dc1-bab8-6cedd1ef053e',3,2,1732686695522,1732686695696,'183fa126-bb16-4b0e-8470-94bcd034917f',3,1,1732622694074,1732622694143,'sample333n','test@3333.com'),
  (100,'sample444n','test@4444.com','adc7139e-c86b-4dc1-bab8-6cedd1ef053e',3,2,1732686695522,1732686695696,'183fa126-bb16-4b0e-8470-94bcd034917f',3,1,1732622694074,1732622694143,'sample444n','test@4444.com'),
  (1000,'sample555n','test@5555.com','adc7139e-c86b-4dc1-bab8-6cedd1ef053e',3,2,1732686695522,1732686695696,'ce9d10de-3266-435f-a06d-959ad9866bd9',3,1,1732622694070,1732622694137,'sample555n','test@5555.com'),
  (10000,'sample666n','test@6666.com','adc7139e-c86b-4dc1-bab8-6cedd1ef053e',3,2,1732686695522,1732686695696,'ce9d10de-3266-435f-a06d-959ad9866bd9',3,1,1732622694070,1732622694137,'sample666n','test@6666.com');
