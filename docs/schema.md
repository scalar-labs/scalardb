## Database schema in Scalar DB

Scalar DB has its own data model and schema and is mapped to a implementation-specific data model and schema.
Also, it stores internal metadata for managing transaction logs and statuses.
This document briefly explains how the data model and schema is mapped between Scalar DB and other implementations, and what are the internal metadata.

## Scalar DB and Cassandra

The data model in Scalar DB is quite similar to the data model in Cassandra, except that in Scalar DB it is more like a simple key-value data model and it does not support secondary indexes.
The primary key in Scalar DB is composed of one or more partition keys and zero or more clustering keys. Similarly, the primary key in Cassandra is composed of one or more partition keys and zero or more clustering columns.


Data types supported in Scalar DB are a little more restricted than in Cassandra.
Here are the supported data types and their mapping to Cassandra data types.

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
Thus, along with any required values by the application, additional values for the metadata need to be defined in the schema.

Here is an example schema in Cassandra when it is used with Scalar DB transactions.

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

Let's assume that k1 is a partition key, k2 is a clustering key and v1 is a value, and those are the values required by an application.
In addition to those, Scalar DB requires metadata for managing transactions.
The rule behind it is as follows.
* add `tx_id`, `tx_prepared_at`, `tx_committed_at`, `tx_state`, `tx_version` as metadata for the current record
* add `before_` prefixed values for each existing value except for primary keys (partition keys and clustering keys) for managing before image

Additionally, we need a state table for managing transaction states as follows.

```sql
CREATE TABLE IF NOT EXISTS coordinator.state (
  tx_id text,
  tx_state int,
  tx_created_at bigint,
  PRIMARY KEY (tx_id)
);
```

## Schema generator and loader

It is a little hard for application developers to care for the schema mapping and metadata for transactions,
so we offer tools for generating and loading schema without much knowledge about imlementation schema and Scalar DB internal metadata.
Please see [this](/tools/schema/README.md) for more details.
