CREATE KEYSPACE IF NOT EXISTS emoney WITH REPLICATION ={'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE emoney.account (
	id TEXT,
	balance INT,

	before_balance INT,

	tx_id text,
	tx_prepared_at bigint,
	tx_committed_at bigint,
	tx_state int,
	tx_version int,

	before_tx_id text,
	before_tx_prepared_at bigint,
	before_tx_committed_at bigint,
	before_tx_state int,
	before_tx_version int,

	PRIMARY KEY (id),
);


DROP KEYSPACE IF EXISTS coordinator;
CREATE KEYSPACE coordinator WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE coordinator.state (
	tx_id text PRIMARY KEY,
	tx_created_at bigint,
	tx_state int
);