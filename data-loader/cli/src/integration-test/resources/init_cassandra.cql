CREATE KEYSPACE scalardb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE scalardb;

CREATE TABLE metadata (
  full_table_name TEXT,
  column_name TEXT,
  data_type TEXT,
  key_type TEXT,
  clustering_order TEXT,
  indexed BOOLEAN,
  ordinal_position INT,
  PRIMARY KEY (full_table_name, column_name)
);

INSERT INTO metadata (full_table_name, column_name, data_type, key_type, clustering_order, indexed, ordinal_position) VALUES
('test.employee', 'email', 'TEXT', null, null, false, 3),
('test.employee', 'id', 'INT', 'PARTITION', null, false, 1),
('test.employee', 'name', 'TEXT', null, null, false, 2),
('testt.employee_trn', 'before_email', 'TEXT', null, null, false, 15),
('testt.employee_trn', 'before_name', 'TEXT', null, null, false, 14),
('testt.employee_trn', 'before_tx_committed_at', 'BIGINT', null, null, false, 13),
('testt.employee_trn', 'before_tx_id', 'TEXT', null, null, false, 9),
('testt.employee_trn', 'before_tx_prepared_at', 'BIGINT', null, null, false, 12),
('testt.employee_trn', 'before_tx_state', 'INT', null, null, false, 10),
('testt.employee_trn', 'before_tx_version', 'INT', null, null, false, 11),
('testt.employee_trn', 'email', 'TEXT', null, null, false, 3),
('testt.employee_trn', 'id', 'INT', 'PARTITION', null, false, 1),
('testt.employee_trn', 'name', 'TEXT', null, null, false, 2),
('testt.employee_trn', 'tx_committed_at', 'BIGINT', null, null, false, 8),
('testt.employee_trn', 'tx_id', 'TEXT', null, null, false, 4),
('testt.employee_trn', 'tx_prepared_at', 'BIGINT', null, null, false, 7),
('testt.employee_trn', 'tx_state', 'INT', null, null, false, 5),
('testt.employee_trn', 'tx_version', 'INT', null, null, false, 6);


CREATE TABLE namespaces (
namespace_name TEXT,
PRIMARY KEY (namespace_name)
);

INSERT INTO  namespaces (namespace_name) VALUES ('test'),('testt');



CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE test;

CREATE TABLE employee (
  id INT,
  name TEXT,
  email TEXT,
  PRIMARY KEY (id)
);

INSERT INTO employee (id, name, email) VALUES
(0, 'emp0', 'emp0@example.com'),
(1, 'emp1', 'emp1@example.com'),
(2, 'emp2', 'emp2@example.com'),
(3, 'emp3', 'emp3@example.com'),
(4, 'emp4', 'emp4@example.com'),
(5, 'emp5', 'emp5@example.com'),
(6, 'emp6', 'emp6@example.com'),
(7, 'emp7', 'emp7@example.com'),
(8, 'emp8', 'emp8@example.com'),
(9, 'emp9', 'emp9@example.com'),
(10, 'emp10', 'emp10@example.com'),
(11, 'emp11', 'emp11@example.com'),
(12, 'emp12', 'emp12@example.com'),
(13, 'emp13', 'emp13@example.com'),
(14, 'emp14', 'emp14@example.com'),
(15, 'emp15', 'emp15@example.com'),
(16, 'emp16', 'emp16@example.com'),
(17, 'emp17', 'emp17@example.com'),
(18, 'emp18', 'emp18@example.com'),
(19, 'emp19', 'emp19@example.com'),
(20, 'emp20', 'emp20@example.com'),
(21, 'emp21', 'emp21@example.com'),
(22, 'emp22', 'emp22@example.com'),
(23, 'emp23', 'emp23@example.com'),
(24, 'emp24', 'emp24@example.com');

CREATE TABLE employee_trn (
  id INT,
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

INSERT INTO employee_trn (id, name, email, tx_id, tx_state, tx_version, tx_prepared_at, tx_committed_at, before_tx_id, before_tx_state, before_tx_version, before_tx_prepared_at, before_tx_committed_at, before_name, before_email) VALUES
(1, 'sample111n', 'test@11111.com', 'adc7139e-c86b-4dc1-bab8-6cedd1ef053e', 3, 2, 1732686695522, 1732686695696, '183fa126-bb16-4b0e-8470-94bcd034917f', 3, 1, 1732622694074, 1732622694143, 'sample111n', 'test@11111.com'),
(10, 'sample333n', 'test@3333.com', 'adc7139e-c86b-4dc1-bab8-6cedd1ef053e', 3, 2, 1732686695522, 1732686695696, '183fa126-bb16-4b0e-8470-94bcd034917f', 3, 1, 1732622694074, 1732622694143, 'sample333n', 'test@3333.com');
