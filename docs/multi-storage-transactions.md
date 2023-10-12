# Multi-Storage Transactions

ScalarDB transactions can span multiple storages or databases while maintaining ACID compliance by using a feature called *multi-storage transactions*.

This page explains how multi-storage transactions work and how to configure the feature in ScalarDB.

## How multi-storage transactions work in ScalarDB

In ScalarDB, the `multi-storage` implementation holds multiple storage instances and has mapping from a table name or a namespace name to a proper storage instance. When an operation is executed, the multi-storage transactions feature chooses a proper storage instance from the specified table or namespace by using the table-storage or namespace-storage mapping and uses that storage instance.

## How to configure ScalarDB to support multi-storage transactions

To enable multi-storage transactions, you need to specify `consensus-commit` as the value for `scalar.db.transaction_manager`, `multi-storage` as the value for `scalar.db.storage`, and configure your databases in the ScalarDB properties file.

The following is an example of configurations for multi-storage transactions:

```properties
# Consensus Commit is required to support multi-storage transactions.
scalar.db.transaction_manager=consensus-commit

# Multi-storage implementation is used for Consensus Commit.
scalar.db.storage=multi-storage

# Define storage names by using a comma-separated format. 
# In this case, "cassandra" and "mysql" are used.
scalar.db.multi_storage.storages=cassandra,mysql

# Define the "cassandra" storage.
# When setting storage properties, such as `storage`, `contact_points`, `username`, and `password`, for multi-storage transactions, the format is `scalar.db.multi_storage.storages.<STORAGE_NAME>.<PROPERTY_NAME>`.
# For example, to configure the `scalar.db.contact_points` property for Cassandra, specify `scalar.db.multi_storage.storages.cassandra.contact_point`.
scalar.db.multi_storage.storages.cassandra.storage=cassandra
scalar.db.multi_storage.storages.cassandra.contact_points=localhost
scalar.db.multi_storage.storages.cassandra.username=cassandra
scalar.db.multi_storage.storages.cassandra.password=cassandra

# Define the "mysql" storage.
# When defining JDBC-specific configurations for multi-storage transactions, you can follow a similar format of `scalar.db.multi_storage.storages.<STORAGE_NAME>.<PROPERTY_NAME>`.
# For example, to configure the `scalar.db.jdbc.connection_pool.min_idle` property for MySQL, specify `scalar.db.multi_storage.storages.mysql.jdbc.connection_pool.min_idle`.
scalar.db.multi_storage.storages.mysql.storage=jdbc
scalar.db.multi_storage.storages.mysql.contact_points=jdbc:mysql://localhost:3306/
scalar.db.multi_storage.storages.mysql.username=root
scalar.db.multi_storage.storages.mysql.password=mysql
# Define the JDBC-specific configurations for the "mysql" storage.
scalar.db.multi_storage.storages.mysql.jdbc.connection_pool.min_idle=5
scalar.db.multi_storage.storages.mysql.jdbc.connection_pool.max_idle=10
scalar.db.multi_storage.storages.mysql.jdbc.connection_pool.max_total=25

# Define table mappings from a table name to a storage.
# The format is "<TABLE_NAME>:<STORAGE_NAME>,...".
scalar.db.multi_storage.table_mapping=user.ORDER:cassandra,user.CUSTOMER:mysql,coordinator.state:cassandra

# Define the default storage that's used if a specified table doesn't have any mapping.
scalar.db.multi_storage.default_storage=cassandra
```

For additional configurations, see [ScalarDB Configurations](configurations.md).

## Hands-on tutorial

For a hands-on tutorial, see [Create a Sample Application That Supports Multi-Storage Transactions](https://github.com/scalar-labs/scalardb-samples/tree/main/multi-storage-transaction-sample).
