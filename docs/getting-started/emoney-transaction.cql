DROP KEYSPACE IF EXISTS emoney;
DROP KEYSPACE IF EXISTS coordinator;

CREATE KEYSPACE emoney WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE KEYSPACE coordinator WITH replication = {'class': 'SimpleStrategy','replication_factor': 1 };

CREATE TABLE emoney.account (
  id TEXT,
  balance INT,
  tx_id TEXT,
  tx_prepared_at BIGINT,
  tx_committed_at BIGINT,
  tx_state INT,
  tx_version INT,
  before_balance INT,
  before_tx_id TEXT,
  before_tx_prepared_at BIGINT,
  before_tx_committed_at BIGINT,
  before_tx_state INT,
  before_tx_version INT,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS coordinator.state (
  tx_id text,
  tx_state int,
  tx_created_at bigint,
  PRIMARY KEY (tx_id)
);
