## Databaes schema in Scalar DB

Scalar DB has its own data model (and schema), and it's mapped to implementation-specific data model and schema.
Also, it stores internal metadata for managing transaction logs and statuses.
This document briefly explains how data model and schema is mapped in between Scalar DB and other implementations, and what are the internal metadata.

## Scalar DB and Cassandra

Data model in Scalar DB is pretty similar to the data model in Cassandra except that Scalar DB is more like a simple key-value data model and it does not support secondary indexes.
The primary key in Scalar DB is composed of one or more partition keys and zero or more clustering keys. Similary, the primary key in Cassandra is composed of one or more partition keys and zero or more clustering columns.


Data types supported in Scalar DB is a little restricted than Cassandra.
Here is the supported data types and the mapping to Cassandra data types.

|Scalar DB  |Cassandra  |
|---|---|
|BOOLEAN  |boolean  |
|INT  |int  |
|BIGINT  |bigint  |
|FLOAT  |float  |
|DOUBLE  |double  |
|TEXT  |text  |
|BLOB  |blob  |

## Internal metadata in Scalar DB

Scalar DB executes transactions in a client-coordinated manner by storing and retrieving metadata stored along with the actual records.
Thus, additional values for the metadata need to be defined in the schema in addition to required values by applications.

Here is an example schmea in Cassadra when it's used with Scalar DB transactions.
```sql
CREATE TABLE example.table1 (
; keys and values required by an application
  k1 TEXT,
  k2 INT,
  v1 INT

; metadata for transaction management
  tx_id TEXT,
  tx_prepared_at BIGINT,
  tx_committed_at BIGINT,
  tx_state INT,
  tx_version INT,
  before_v1 INT,
  before_tx_committed_at BIGINT,
  before_tx_id TEXT,
  before_tx_prepared_at BIGINT,
  before_tx_state INT,
  before_tx_version INT,

  PRIMARY KEY (k1, k2)
);
```

Lets' assume that k1 is a partition key, k2 is a clustering key and v1 is a value, and those are the values required by an application.
In addition to those, Scalar DB requires metadata for managing transactions.
The rule behind it is as follows.
* add `tx_id`, `tx_prepared_at`, `tx_committed_at`, `tx_state`, `tx_version` for metadata for the current record
* add `before_` prefixed values for each existing value except for primary keys (partition keys and clustering keys) for managing before image

Also, we need state table for managing transaction states as follows.
```sql
CREATE TABLE IF NOT EXISTS coordinator.state (
  tx_id text,
  tx_state int,
  tx_created_at bigint,
  PRIMARY KEY (tx_id)
);
```

## Schema generator and loader

It's a little hard for application developers to care for the schema mapping and metadata for transactions,
we are preparing tools to generate implementation-specific schema and load the schema without caring too much about those.

coming soon
