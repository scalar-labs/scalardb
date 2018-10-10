DROP KEYSPACE IF EXISTS emoney;
DROP KEYSPACE IF EXISTS coordinator;

CREATE KEYSPACE emoney WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE emoney.account (
    id TEXT,
    balance INT,
    PRIMARY KEY (id)
);
