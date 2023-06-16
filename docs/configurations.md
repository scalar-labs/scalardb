# ScalarDB Configurations

This document describes the configurations for ScalarDB.

## Transaction manager configurations

ScalarDB has several transaction manager implementations, such as Consensus Commit, gRPC, and JDBC, and you can specify one of the implementations with the `scalar.db.transaction_manager` property.
The following sections describes the configurations for each transaction manager.

### Consensus Commit

Consensus Commit is the default transaction manager type in ScalarDB.
If you don't specify the `scalar.db.transaction_manager` property, or you specify `consensus-commit` for the property, Consensus Commit is used.

The following sections describes the configurations for Consensus Commit.

#### Basic configurations

The basic configurations for Consensus Commit are as follows:

| name                                             | description                                                                                                                                                                                                       | default     |
|--------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| scalar.db.transaction_manager                    | `consensus-commit` should be specified.                                                                                                                                                                           | -           |
| scalar.db.consensus_commit.isolation_level       | Isolation level used for Consensus Commit. Either `SNAPSHOT` or `SERIALIZABLE` can be specified.                                                                                                                  | SNAPSHOT    |
| scalar.db.consensus_commit.serializable_strategy | Serializable strategy used for Consensus Commit. Either `EXTRA_READ` or `EXTRA_WRITE` can be specified. If `SNAPSHOT` is specified in the property `scalar.db.consensus_commit.isolation_level`, this is ignored. | EXTRA_READ  |
| scalar.db.consensus_commit.coordinator.namespace | Namespace name of coordinator tables.                                                                                                                                                                             | coordinator |

#### Performance related configurations

The Performance related configurations for Consensus Commit are as follows:

| name                                                    | description                                                                    | default                                                           |
|---------------------------------------------------------|--------------------------------------------------------------------------------|-------------------------------------------------------------------|
| scalar.db.consensus_commit.parallel_executor_count      | The number of the executors (threads) for the parallel execution.              | 128                                                               |
| scalar.db.consensus_commit.parallel_preparation.enabled | Whether or not the preparation phase is executed in parallel.                  | true                                                              |
| scalar.db.consensus_commit.parallel_validation.enabled  | Whether or not the validation phase (in `EXTRA_READ`) is executed in parallel. | The value of `scalar.db.consensus_commit.parallel_commit.enabled` |
| scalar.db.consensus_commit.parallel_commit.enabled      | Whether or not the commit phase is executed in parallel.                       | true                                                              |
| scalar.db.consensus_commit.parallel_rollback.enabled    | Whether or not the rollback phase is executed in parallel.                     | The value of `scalar.db.consensus_commit.parallel_commit.enabled` |
| scalar.db.consensus_commit.async_commit.enabled         | Whether or not the commit phase is executed asynchronously.                    | false                                                             |
| scalar.db.consensus_commit.async_rollback.enabled       | Whether or not the rollback phase is executed asynchronously.                  | The value of `scalar.db.consensus_commit.async_commit.enabled`    |

#### Underlying Storage/Database Configurations

Consensus Commit has the storage abstraction layer and supports multiple underlying storages.
You can specify the storage implementation by the `scalar.db.storage` property.

The following describes the configurations available for each storage.

- For Cassandra, the following configurations are available:

| name                     | description                              | default |
|--------------------------|------------------------------------------|---------|
| scalar.db.storage        | `cassandra` should be specified.         | -       |
| scalar.db.contact_points | Comma separated contact points.          |         |
| scalar.db.contact_port   | Port number for all the contact points . |         |
| scalar.db.username       | Username to access the database.         |         |
| scalar.db.password       | Password to access the database.         |         |

- For Cosmos DB for NoSQL, the following configurations are available:

| name                                     | description                                                                                   | default  |
|------------------------------------------|-----------------------------------------------------------------------------------------------|----------|
| scalar.db.storage                        | `cosmos` should be specified.                                                                 | -        |
| scalar.db.contact_points                 | Azure Cosmos DB endpoint the SDK will connect to.                                             |          |
| scalar.db.password                       | Either a master or readonly key used to perform authentication for accessing Azure Cosmos DB. |          |
| scalar.db.cosmos.table_metadata.database | Database name for the table metadata used for ScalarDB.                                       | scalardb |

- For DynamoDB, the following configurations are available:

| name                                      | description                                                                                                                                                                                                                                                  | default  |
|-------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| scalar.db.storage                         | `dynamo` should be specified.                                                                                                                                                                                                                                | -        |
| scalar.db.contact_points                  | Region with which the SDK should communicate (e.g., `us-east-1`).                                                                                                                                                                                            |          |
| scalar.db.username                        | AWS access key, used to identify the user interacting with AWS.                                                                                                                                                                                              |          |
| scalar.db.password                        | AWS secret access key, used to authenticate the user interacting with AWS.                                                                                                                                                                                   |          |
| scalar.db.dynamo.endpoint_override        | Endpoint with which the SDK should communicate. It's primarily used for testing to use a local instance instead of an AWS service.                                                                                                                           |          |
| scalar.db.dynamo.table_metadata.namespace | Namespace name for the table metadata used for ScalarDB.                                                                                                                                                                                                     | scalardb |
| scalar.db.dynamo.namespace.prefix         | Prefix for the user namespaces and metadata namespace names. Since AWS requires to have unique tables names in a single AWS region, this is useful if you want to use multiple ScalarDB environments (development, production, etc.) in a single AWS region. |          |

- For JDBC databases, the following configurations are available:

| name                                                    | description                                                                                                                                                                  | default                      |
|---------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------|
| scalar.db.storage                                       | `jdbc` should be specified.                                                                                                                                                  | -                            |
| scalar.db.contact_points                                | JDBC connection URL.                                                                                                                                                         |                              |
| scalar.db.username                                      | Username to access the database.                                                                                                                                             |                              |
| scalar.db.password                                      | Password to access the database.                                                                                                                                             |                              |
| scalar.db.jdbc.connection_pool.min_idle                 | Minimum number of idle connections in the connection pool.                                                                                                                   | 20                           |
| scalar.db.jdbc.connection_pool.max_idle                 | Maximum number of connections that can remain idle in the connection pool.                                                                                                   | 50                           |
| scalar.db.jdbc.connection_pool.max_total                | Maximum total number of idle and borrowed connections that can be active at the same time for the connection pool. Use a negative value for no limit.                        | 100                          |
| scalar.db.jdbc.prepared_statements_pool.enabled         | Setting true to this property enables prepared statement pooling.                                                                                                            | false                        |
| scalar.db.jdbc.prepared_statements_pool.max_open        | Maximum number of open statements that can be allocated from the statement pool at the same time, or negative for no limit.                                                  | -1                           |
| scalar.db.jdbc.isolation_level                          | Isolation level for JDBC. Either `READ_UNCOMMITTED` or `READ_COMMITTED` or `REPEATABLE_READ` or `SERIALIZABLE` can be specified.                                             | Underlying database specific |
| scalar.db.jdbc.table_metadata.schema                    | Schema name for the table metadata used for ScalarDB.                                                                                                                        | scalardb                     |
| scalar.db.jdbc.table_metadata.connection_pool.min_idle  | Minimum number of idle connections in the connection pool for the table metadata.                                                                                            | 5                            |
| scalar.db.jdbc.table_metadata.connection_pool.max_idle  | Maximum number of connections that can remain idle in the connection pool for the table metadata.                                                                            | 10                           |
| scalar.db.jdbc.table_metadata.connection_pool.max_total | Maximum total number of idle and borrowed connections that can be active at the same time for the connection pool for the table metadata. Use a negative value for no limit. | 25                           |
| scalar.db.jdbc.admin.connection_pool.min_idle           | Minimum number of idle connections in the connection pool for admin.                                                                                                         | 5                            |
| scalar.db.jdbc.admin.connection_pool.max_idle           | Maximum number of connections that can remain idle in the connection pool for admin.                                                                                         | 10                           |
| scalar.db.jdbc.admin.connection_pool.max_total          | Maximum total number of idle and borrowed connections that can be active at the same time for the connection pool for admin. Use a negative value for no limit.              | 25                           |

When you use SQLite3 as a JDBC database, you need to set `scalar.db.contact_points` as follows.

```properties
scalar.db.contact_points=jdbc:sqlite:your-db.sqlite3?busy_timeout=10000
```

Unlike other JDBC databases, [SQLite3 does not fully support concurrent access](https://www.sqlite.org/lang_transaction.html).
In order to avoid frequent errors caused internally by the [`SQLITE_BUSY`](https://www.sqlite.org/rescode.html#busy),
it is recommended to set a [`busy_timeout`](https://www.sqlite.org/c3ref/busy_timeout.html) parameter.

##### Multi-storage

ScalarDB supports a Multi-storage where you can use multiple storage implementations at the same time.
You can use Multi-storage by specifying `multi-storage` for the `scalar.db.storage` property.

For the details of the Multi-storage, see [Multi-storage Transactions](multi-storage-transactions.md).

### ScalarDB Server (gRPC)

[ScalarDB Server](scalardb-server.md) is a standalone server that provides a gRPC interface to ScalarDB.
To interact with ScalarDB Server, you need to specify `grpc` for the `scalar.db.transaction_manager` property.

The following configurations are available for ScalarDB Server:

| name                                                    | description                                               | default                |
|---------------------------------------------------------|-----------------------------------------------------------|------------------------|
| scalar.db.transaction_manager                           | `grpc` should be specified.                               | -                      |
| scalar.db.contact_points                                | ScalarDB Server host.                                     |                        |
| scalar.db.contact_port                                  | Port number for ScalarDB Server.                          | 60051                  |
| scalar.db.grpc.deadline_duration_millis                 | The deadline duration for gRPC connections.               | 60000 (60 seconds)     |
| scalar.db.grpc.max_inbound_message_size                 | The maximum message size allowed for a single gRPC frame. | The gRPC default value |
| scalar.db.grpc.max_inbound_metadata_size                | The maximum size of metadata allowed to be received.      | The gRPC default value |

For the details of ScalarDB Server, see [ScalarDB Server](scalardb-server.md).

### JDBC transactions

You can also use native JDBC transactions through ScalarDB when you only interact with one JDBC database. However, you cannot use most of ScalarDB features when you use JDBC transactions, which might defeat the purpose of using ScalarDB. So, please carefully consider your use case.
To use JDBC transactions, you need to specify `jdbc` for the `scalar.db.transaction_manager` property.

The following configurations are available for JDBC transactions:

| name                                                    | description                                                                                                                                                                   | default                      |
|---------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------|
| scalar.db.transaction_manager                           | `jdbc` should be specified.                                                                                                                                                   | -                            |
| scalar.db.contact_points                                | JDBC connection URL.                                                                                                                                                          |                              |
| scalar.db.username                                      | Username to access the database.                                                                                                                                              |                              |
| scalar.db.password                                      | Password to access the database.                                                                                                                                              |                              |
| scalar.db.jdbc.connection_pool.min_idle                 | Minimum number of idle connections in the connection pool.                                                                                                                    | 20                           |
| scalar.db.jdbc.connection_pool.max_idle                 | Maximum number of connections that can remain idle in the connection pool.                                                                                                    | 50                           |
| scalar.db.jdbc.connection_pool.max_total                | Maximum total number of idle and borrowed connections that can be active at the same time for the connection pool. Use a negative value for no limit.                         | 100                          |
| scalar.db.jdbc.prepared_statements_pool.enabled         | Setting true to this property enables prepared statement pooling.                                                                                                             | false                        |
| scalar.db.jdbc.prepared_statements_pool.max_open        | Maximum number of open statements that can be allocated from the statement pool at the same time, or negative for no limit.                                                   | -1                           |
| scalar.db.jdbc.isolation_level                          | Isolation level for JDBC. Either `READ_UNCOMMITTED` or `READ_COMMITTED` or `REPEATABLE_READ` or `SERIALIZABLE` can be specified.                                              | Underlying database specific |
| scalar.db.jdbc.table_metadata.schema                    | Schema name for the table metadata used for ScalarDB.                                                                                                                         | scalardb                     |
| scalar.db.jdbc.table_metadata.connection_pool.min_idle  | Minimum number of idle connections in the connection pool for the table metadata.                                                                                             | 5                            |
| scalar.db.jdbc.table_metadata.connection_pool.max_idle  | Maximum number of connections that can remain idle in the connection pool for the table metadata.                                                                             | 10                           |
| scalar.db.jdbc.table_metadata.connection_pool.max_total | Mmaximum total number of idle and borrowed connections that can be active at the same time for the connection pool for the table metadata. Use a negative value for no limit. | 25                           |
| scalar.db.jdbc.admin.connection_pool.min_idle           | Minimum number of idle connections in the connection pool for admin.                                                                                                          | 5                            |
| scalar.db.jdbc.admin.connection_pool.max_idle           | Maximum number of connections that can remain idle in the connection pool for admin.                                                                                          | 10                           |
| scalar.db.jdbc.admin.connection_pool.max_total          | Maximum total number of idle and borrowed connections that can be active at the same time for the connection pool for admin. Use a negative value for no limit.               | 25                           |

When you use SQLite3 as a JDBC database, you need to set `scalar.db.contact_points` as follows.

```properties
scalar.db.contact_points=jdbc:sqlite:your-db.sqlite3?busy_timeout=10000
```

Unlike other JDBC databases, [SQLite3 does not fully support concurrent access](https://www.sqlite.org/lang_transaction.html).
In order to avoid frequent errors caused internally by the [`SQLITE_BUSY`](https://www.sqlite.org/rescode.html#busy),
it is recommended to set a [`busy_timeout`](https://www.sqlite.org/c3ref/busy_timeout.html) parameter.

## ScalarDB Server configurations

[ScalarDB Server](scalardb-server.md) is a standalone server that provides a gRPC interface to ScalarDB.
This section explains the ScalarDB Server configurations.

In addition to [Transaction manager configurations](#transaction-manager-configurations) and [Other configurations](#other-configurations), the following configurations are available for ScalarDB Server:

| name                                            | description                                                                                      | default                |
|-------------------------------------------------|--------------------------------------------------------------------------------------------------|------------------------|
| scalar.db.server.port                           | Port number for ScalarDB Server.                                                                 | 60051                  |
| scalar.db.server.prometheus_exporter_port       | Prometheus exporter port. Prometheus exporter will not be started if a negative number is given. | 8080                   |
| scalar.db.server.grpc.max_inbound_message_size  | The maximum message size allowed to be received.                                                 | The gRPC default value |
| scalar.db.server.grpc.max_inbound_metadata_size | The maximum size of metadata allowed to be received.                                             | The gRPC default value |
| scalar.db.server.decommissioning_duration_secs  | The decommissioning duration in seconds.                                                         | 30                     |

For the details of ScalarDB Server, see [ScalarDB Server](scalardb-server.md).

## Other configurations

This section explains other configurations.

Other configurations are available for ScalarDB:

| name                                                           | description                                                                                                                                                                                                   | default            |
|----------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------|
| scalar.db.metadata.cache_expiration_time_secs                  | ScalarDB has a metadata cache to reduce the number of requests to the database. This setting specifies the expiration time of the cache in seconds.                                                           | -1 (no expiration) |
| scalar.db.active_transaction_management.expiration_time_millis | ScalarDB maintains ongoing transactions, which can be resumed using a transaction ID. This setting specifies the expiration time of this transaction management feature in milliseconds.                      | -1 (no expiration) |
| scalar.db.default_namespace_name                               | The given namespace name will be used by operations that do not already specify a namespace. If you would like to use this setting with ScalarDB server, it needs to be set on the client-side configuration. |                    |

## Configuration examples

This section shows several configuration examples.

### Example 1

```
[App (ScalarDB Library with Consensus Commit)] ---> [Underlying storage/database]
```

In this setting, App (ScalarDB Library with Consensus Commit) connects to the underlying storage/database (in this case Cassandra) directly.
Note that this setting exists only for development purposes and is not recommended for production use.
This is because the App needs to implement [scalar-admin](https://github.com/scalar-labs/scalar-admin) interface to take transactionally-consistent backups for ScalarDB, which requires an extra burden for users.

In this case, an example of configurations in App are as follows:
```properties
# Transaction manager implementation
scalar.db.transaction_manager=consensus-commit

# Storage implementation
scalar.db.storage=cassandra

# Comma separated contact points
scalar.db.contact_points=<cassandra host>

# Credential information to access the database
scalar.db.username=<username>
scalar.db.password=<password>
```

### Example 2

```
[App (ScalarDB Library with gRPC)] ---> [ScalarDB Server (ScalarDB Library with Consensus Commit)] ---> [Underlying storage/database]
```

In this setting, App (ScalarDB Library with gRPC) connects to an underlying storage/database through ScalarDB Server.
This setting is recommended for production use.
This is because ScalarDB Server implements [scalar-admin](https://github.com/scalar-labs/scalar-admin) interface, which enables you to take transactionally-consistent backups for ScalarDB by pausing the ScalarDB Server.

In this case, an example of configurations for App is as follows:
```properties
# Transaction manager implementation
scalar.db.transaction_manager=grpc

# ScalarDB Server host
scalar.db.contact_points=<ScalarDB Server host>

# ScalarDB Server port
scalar.db.contact_port=<ScalarDB Server port>
```

And an example of configurations for ScalarDB Server is as follows:
```properties
# Storage implementation
scalar.db.storage=cassandra

# Comma separated contact points
scalar.db.contact_points=<cassandra host>

# Credential information to access the database
scalar.db.username=<username>
scalar.db.password=<password>
```
