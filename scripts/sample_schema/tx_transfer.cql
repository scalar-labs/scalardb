DROP KEYSPACE IF EXISTS transfer;
CREATE KEYSPACE IF NOT EXISTS transfer WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
CREATE KEYSPACE IF NOT EXISTS coordinator WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };

CREATE TABLE IF NOT EXISTS transfer.tx_transfer (
    account_id int,
    account_type int,
    balance int,
    before_balance int,
    before_tx_committed_at bigint,
    before_tx_id text,
    before_tx_prepared_at bigint,
    before_tx_state int,
    before_tx_version int,
    tx_committed_at bigint,
    tx_id text,
    tx_prepared_at bigint,
    tx_state int,
    tx_version int,
    PRIMARY KEY (account_id, account_type)
) WITH compaction = { 'class' : 'LeveledCompactionStrategy' };

CREATE TABLE IF NOT EXISTS coordinator.state (
    tx_id text,
    tx_state int,
    tx_created_at bigint,
    PRIMARY KEY (tx_id)
);
