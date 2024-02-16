# ScalarDB Configurations

This page describes the available configurations for ScalarDB.

## ScalarDB client configurations

ScalarDB provides its own transaction protocol called Consensus Commit. You can use the Consensus Commit protocol directly through the ScalarDB client library or through [ScalarDB Cluster (redirects to the Enterprise docs site)](https://scalardb.scalar-labs.com/docs/latest/scalardb-cluster/), which is a component that is available only in the ScalarDB Enterprise edition.

### Use Consensus Commit directly

Consensus Commit is the default transaction manager type in ScalarDB. To use the Consensus Commit transaction manager, add the following to the ScalarDB properties file:

```properties
scalar.db.transaction_manager=consensus-commit
```

{% capture notice--info %}
**Note**

If you don't specify the `scalar.db.transaction_manager` property, `consensus-commit` will be the default value.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

#### Basic configurations

The following basic configurations are available for the Consensus Commit transaction manager:

| Name                                                  | Description                                                                                                                                                                                                                                                                                                                                                                                                  | Default       |
|-------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `scalar.db.transaction_manager`                       | `consensus-commit` should be specified.                                                                                                                                                                                                                                                                                                                                                                      | -             |
| `scalar.db.consensus_commit.isolation_level`          | Isolation level used for Consensus Commit. Either `SNAPSHOT` or `SERIALIZABLE` can be specified.                                                                                                                                                                                                                                                                                                             | `SNAPSHOT`    |
| `scalar.db.consensus_commit.serializable_strategy`    | Serializable strategy used for Consensus Commit. Either `EXTRA_READ` or `EXTRA_WRITE` can be specified. If `SNAPSHOT` is specified in the property `scalar.db.consensus_commit.isolation_level`, this configuration will be ignored.                                                                                                                                                                         | `EXTRA_READ`  |
| `scalar.db.consensus_commit.coordinator.namespace`    | Namespace name of Coordinator tables.                                                                                                                                                                                                                                                                                                                                                                        | `coordinator` |
| `scalar.db.consensus_commit.include_metadata.enabled` | If set to `true`, `Get` and `Scan` operations results will contain transaction metadata. To see the transaction metadata columns details for a given table, you can use the `DistributedTransactionAdmin.getTableMetadata()` method, which will return the table metadata augmented with the transaction metadata columns. Using this configuration can be useful to investigate transaction-related issues. | `false`       |

#### Performance-related configurations

The following performance-related configurations are available for the Consensus Commit transaction manager:

| Name                                                            | Description                                                                    | Default                                                           |
|-----------------------------------------------------------------|--------------------------------------------------------------------------------|-------------------------------------------------------------------|
| `scalar.db.consensus_commit.parallel_executor_count`            | Number of executors (threads) for parallel execution.                          | `128`                                                             |
| `scalar.db.consensus_commit.parallel_preparation.enabled`       | Whether or not the preparation phase is executed in parallel.                  | `true`                                                            |
| `scalar.db.consensus_commit.parallel_validation.enabled`        | Whether or not the validation phase (in `EXTRA_READ`) is executed in parallel. | The value of `scalar.db.consensus_commit.parallel_commit.enabled` |
| `scalar.db.consensus_commit.parallel_commit.enabled`            | Whether or not the commit phase is executed in parallel.                       | `true`                                                            |
| `scalar.db.consensus_commit.parallel_rollback.enabled`          | Whether or not the rollback phase is executed in parallel.                     | The value of `scalar.db.consensus_commit.parallel_commit.enabled` |
| `scalar.db.consensus_commit.async_commit.enabled`               | Whether or not the commit phase is executed asynchronously.                    | `false`                                                           |
| `scalar.db.consensus_commit.async_rollback.enabled`             | Whether or not the rollback phase is executed asynchronously.                  | The value of `scalar.db.consensus_commit.async_commit.enabled`    |
| `scalar.db.consensus_commit.parallel_implicit_pre_read.enabled` | Whether or not implicit pre-read is executed in parallel.                      | `true`                                                            |

#### Underlying storage or database configurations

Consensus Commit has a storage abstraction layer and supports multiple underlying storages. You can specify the storage implementation by using the `scalar.db.storage` property.

Select a database to see the configurations available for each storage.

<div id="tabset-1">
<div class="tab">
  <button class="tablinks" onclick="openTab(event, 'Cassandra', 'tabset-1')" id="defaultOpen-1">Cassandra</button>
  <button class="tablinks" onclick="openTab(event, 'CosmosDB_for_NoSQL', 'tabset-1')">CosmosDB for NoSQL</button>
  <button class="tablinks" onclick="openTab(event, 'DynamoDB', 'tabset-1')">DynamoDB</button>
  <button class="tablinks" onclick="openTab(event, 'JDBC_databases', 'tabset-1')">JDBC databases</button>
</div>

<div id="Cassandra" class="tabcontent" markdown="1">

The following configurations are available for Cassandra:

| Name                                    | Description                                                           | Default    |
|-----------------------------------------|-----------------------------------------------------------------------|------------|
| `scalar.db.storage`                     | `cassandra` must be specified.                                        | -          |
| `scalar.db.contact_points`              | Comma-separated contact points.                                       |            |
| `scalar.db.contact_port`                | Port number for all the contact points.                               |            |
| `scalar.db.username`                    | Username to access the database.                                      |            |
| `scalar.db.password`                    | Password to access the database.                                      |            |

</div>
<div id="CosmosDB_for_NoSQL" class="tabcontent" markdown="1">

The following configurations are available for CosmosDB for NoSQL:

| Name                                 | Description                                                                                              | Default  |
|--------------------------------------|----------------------------------------------------------------------------------------------------------|----------|
| `scalar.db.storage`                  | `cosmos` must be specified.                                                                              | -        |
| `scalar.db.contact_points`           | Azure Cosmos DB for NoSQL endpoint with which ScalarDB should communicate.                               |          |
| `scalar.db.password`                 | Either a master or read-only key used to perform authentication for accessing Azure Cosmos DB for NoSQL. |          |
| `scalar.db.cosmos.consistency_level` | Consistency level used for Cosmos DB operations. `STRONG` or `BOUNDED_STALENESS` can be specified.       | `STRONG` |

</div>
<div id="DynamoDB" class="tabcontent" markdown="1">

The following configurations are available for DynamoDB:

| Name                                  | Description                                                                                                                                                                                                                                                 | Default    |
|---------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------|
| `scalar.db.storage`                   | `dynamo` must be specified.                                                                                                                                                                                                                                 | -          |
| `scalar.db.contact_points`            | AWS region with which ScalarDB should communicate (e.g., `us-east-1`).                                                                                                                                                                                      |            |
| `scalar.db.username`                  | AWS access key used to identify the user interacting with AWS.                                                                                                                                                                                              |            |
| `scalar.db.password`                  | AWS secret access key used to authenticate the user interacting with AWS.                                                                                                                                                                                   |            |
| `scalar.db.dynamo.endpoint_override`  | Amazon DynamoDB endpoint with which ScalarDB should communicate. This is primarily used for testing with a local instance instead of an AWS service.                                                                                                        |            |
| `scalar.db.dynamo.namespace.prefix`   | Prefix for the user namespaces and metadata namespace names. Since AWS requires having unique tables names in a single AWS region, this is useful if you want to use multiple ScalarDB environments (development, production, etc.) in a single AWS region. |            |

</div>
<div id="JDBC_databases" class="tabcontent" markdown="1">

The following configurations are available for JDBC databases:

| Name                                                      | Description                                                                                                                                                                  | Default                      |
|-----------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------|
| `scalar.db.storage`                                       | `jdbc` must be specified.                                                                                                                                                    | -                            |
| `scalar.db.contact_points`                                | JDBC connection URL.                                                                                                                                                         |                              |
| `scalar.db.username`                                      | Username to access the database.                                                                                                                                             |                              |
| `scalar.db.password`                                      | Password to access the database.                                                                                                                                             |                              |
| `scalar.db.jdbc.connection_pool.min_idle`                 | Minimum number of idle connections in the connection pool.                                                                                                                   | `20`                         |
| `scalar.db.jdbc.connection_pool.max_idle`                 | Maximum number of connections that can remain idle in the connection pool.                                                                                                   | `50`                         |
| `scalar.db.jdbc.connection_pool.max_total`                | Maximum total number of idle and borrowed connections that can be active at the same time for the connection pool. Use a negative value for no limit.                        | `100`                        |
| `scalar.db.jdbc.prepared_statements_pool.enabled`         | Setting this property to `true` enables prepared-statement pooling.                                                                                                          | `false`                      |
| `scalar.db.jdbc.prepared_statements_pool.max_open`        | Maximum number of open statements that can be allocated from the statement pool at the same time. Use a negative value for no limit.                                         | `-1`                         |
| `scalar.db.jdbc.isolation_level`                          | Isolation level for JDBC. `READ_UNCOMMITTED`, `READ_COMMITTED`, `REPEATABLE_READ`, or `SERIALIZABLE` can be specified.                                                       | Underlying-database specific |
| `scalar.db.jdbc.table_metadata.connection_pool.min_idle`  | Minimum number of idle connections in the connection pool for the table metadata.                                                                                            | `5`                          |
| `scalar.db.jdbc.table_metadata.connection_pool.max_idle`  | Maximum number of connections that can remain idle in the connection pool for the table metadata.                                                                            | `10`                         |
| `scalar.db.jdbc.table_metadata.connection_pool.max_total` | Maximum total number of idle and borrowed connections that can be active at the same time for the connection pool for the table metadata. Use a negative value for no limit. | `25`                         |
| `scalar.db.jdbc.admin.connection_pool.min_idle`           | Minimum number of idle connections in the connection pool for admin.                                                                                                         | `5`                          |
| `scalar.db.jdbc.admin.connection_pool.max_idle`           | Maximum number of connections that can remain idle in the connection pool for admin.                                                                                         | `10`                         |
| `scalar.db.jdbc.admin.connection_pool.max_total`          | Maximum total number of idle and borrowed connections that can be active at the same time for the connection pool for admin. Use a negative value for no limit.              | `25`                         |

{% capture notice--info %}
**Note**

If you're using SQLite3 as a JDBC database, you must set `scalar.db.contact_points` as follows:

```properties
scalar.db.contact_points=jdbc:sqlite:<YOUR_DB>.sqlite3?busy_timeout=10000
```

Unlike other JDBC databases, [SQLite3 doesn't fully support concurrent access](https://www.sqlite.org/lang_transaction.html).
To avoid frequent errors caused internally by [`SQLITE_BUSY`](https://www.sqlite.org/rescode.html#busy), we recommend setting a [`busy_timeout`](https://www.sqlite.org/c3ref/busy_timeout.html) parameter.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

</div>
</div>

##### Multi-storage support

ScalarDB supports using multiple storage implementations simultaneously. You can use multiple storages by specifying `multi-storage` as the value for the `scalar.db.storage` property.

For details about using multiple storages, see [Multi-Storage Transactions](multi-storage-transactions.md).

### Use Consensus Commit through ScalarDB Cluster

[ScalarDB Cluster (redirects to the Enterprise docs site)](https://scalardb.scalar-labs.com/docs/latest/scalardb-cluster/) is a component that provides a gRPC interface to ScalarDB.

For details about client configurations, see the ScalarDB Cluster [client configurations (redirects to the Enterprise docs site)](https://scalardb.scalar-labs.com/docs/latest/scalardb-cluster/developer-guide-for-scalardb-cluster-with-java-api/#client-configurations).

## Cross-partition scan configurations

By enabling the cross-partition scan option below, the `Scan` operation can retrieve all records across partitions. In addition, you can specify arbitrary conditions and orderings in the cross-partition `Scan` operation by enabling `cross_partition_scan.filtering` and `cross_partition_scan.ordering`, respectively. Currently, the cross-partition scan with filtering and ordering is available only for JDBC databases. To enable filtering and ordering, `scalar.db.cross_partition_scan.enabled` must be set to `true`.

For details on how to use cross-partition scan, see [Scan operation](./api-guide.md#scan-operation).

{% capture notice--warning %}
**Attention**

For non-JDBC databases, we do not recommend enabling cross-partition scan with the `SERIALIAZABLE` isolation level because transactions could be executed at a lower isolation level (that is, `SNAPSHOT`). When using non-JDBC databases, use cross-partition scan at your own risk only if consistency does not matter for your transactions.
{% endcapture %}

<div class="notice--warning">{{ notice--warning | markdownify }}</div>

| Name                                               | Description                                   | Default |
|----------------------------------------------------|-----------------------------------------------|---------|
| `scalar.db.cross_partition_scan.enabled`           | Enable cross-partition scan.                  | `false` |
| `scalar.db.cross_partition_scan.filtering.enabled` | Enable filtering in cross-partition scan.     | `false` |
| `scalar.db.cross_partition_scan.ordering.enabled`  | Enable ordering in cross-partition scan.      | `false` |

## Other ScalarDB configurations

The following are additional configurations available for ScalarDB:

| Name                                                             | Description                                                                                                                                                                                 | Default              |
|------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------|
| `scalar.db.metadata.cache_expiration_time_secs`                  | ScalarDB has a metadata cache to reduce the number of requests to the database. This setting specifies the expiration time of the cache in seconds.                                         | `-1` (no expiration) |
| `scalar.db.active_transaction_management.expiration_time_millis` | ScalarDB maintains ongoing transactions, which can be resumed by using a transaction ID. This setting specifies the expiration time of this transaction management feature in milliseconds. | `-1` (no expiration) |
| `scalar.db.default_namespace_name`                               | The given namespace name will be used by operations that do not already specify a namespace.                                                                                                |                      |
| `scalar.db.system_namespace_name`                                | The given namespace name will be used by ScalarDB internally.                                                                                                                               | `scalardb`           |

## Placeholder usage

You can use placeholders in the values, and they are replaced with environment variables (`${env:<ENVIRONMENT_VARIABLE_NAME>}`) or system properties (`${sys:<SYSTEM_PROPERTY_NAME>}`). You can also specify default values in placeholders like `${sys:<SYSTEM_PROPERTY_NAME>:-<DEFAULT_VALUE>}`.

The following is an example of a configuration that uses placeholders:

```properties
scalar.db.username=${env:<SCALAR_DB_USERNAME>:-admin}
scalar.db.password=${env:<SCALAR_DB_PASSWORD>}
```

In this example configuration, ScalarDB reads the username and password from environment variables. If the environment variable `SCALAR_DB_USERNAME` does not exist, ScalarDB uses the default value `admin`.

## Configuration examples

This section provides some configuration examples.

### Configuration example #1 - App and database

```mermaid
flowchart LR
    app["<b>App</b><br />(ScalarDB library with<br />Consensus Commit)"]
    db[(Underlying storage or database)]
    app --> db
```

In this example configuration, the app (ScalarDB library with Consensus Commit) connects to an underlying storage or database (in this case, Cassandra) directly.

{% capture notice--warning %}
**Attention**

This configuration exists only for development purposes and isn’t suitable for a production environment. This is because the app needs to implement the [Scalar Admin](https://github.com/scalar-labs/scalar-admin) interface to take transactionally consistent backups for ScalarDB, which requires additional configurations.
{% endcapture %}

<div class="notice--warning">{{ notice--warning | markdownify }}</div>

The following is an example of the configuration for connecting the app to the underlying database through ScalarDB:

```properties
# Transaction manager implementation.
scalar.db.transaction_manager=consensus-commit

# Storage implementation.
scalar.db.storage=cassandra

# Comma-separated contact points.
scalar.db.contact_points=<CASSANDRA_HOST>

# Credential information to access the database.
scalar.db.username=<USERNAME>
scalar.db.password=<PASSWORD>
```

### Configuration example #2 - App, ScalarDB Cluster, and database

```mermaid
flowchart LR
    app["App -<br />ScalarDB library with gRPC"]
    cluster["ScalarDB Cluster -<br />(ScalarDB library with<br />Consensus Commit)"]
    db[(Underlying storage or database)]
    app --> cluster --> db
```

In this example configuration, the app (ScalarDB library with gRPC) connects to an underlying storage or database (in this case, Cassandra) through ScalarDB Cluster, which is a component that is available only in the ScalarDB Enterprise edition.

{% capture notice--info %}
**Note**

This configuration is acceptable for production use because ScalarDB Cluster implements the [Scalar Admin](https://github.com/scalar-labs/scalar-admin) interface, which enables you to take transactionally consistent backups for ScalarDB by pausing ScalarDB Cluster.

{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

The following is an example of the configuration for connecting the app to the underlying database through ScalarDB Cluster:

```properties
# Transaction manager implementation.
scalar.db.transaction_manager=cluster

# Contact point of the cluster.
scalar.db.contact_points=indirect:<SCALARDB_CLUSTER_CONTACT_POINT>
```

For details about client configurations, see the ScalarDB Cluster [client configurations (redirects to the Enterprise docs site)](https://scalardb.scalar-labs.com/docs/latest/scalardb-cluster/developer-guide-for-scalardb-cluster-with-java-api/#client-configurations).
