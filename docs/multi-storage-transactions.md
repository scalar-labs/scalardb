# Multi-storage Transactions

Scalar DB transactions can span multiple storages/databases while preserving ACID property with a
feature called *Multi-storage Transactions*. This documentation explains the feature briefly.

## How Multi-storage Transactions works

Internally, the `multi-storage` implementation holds multiple storage instances and has mappings
from a table name/a namespace name to a proper storage instance. When an operation is executed, it
chooses a proper storage instance from the specified table/namespace by using the
table-storage/namespace-storage mappings and uses it.

## Configuration

You can use Multi-storage transactions in the same way as the other storages/databases at the code
level as long as the configuration is properly set for `multi-storage`. An example of the
configuration is shown as follows:

```properties
# The storage is "multi-storage"
scalar.db.storage=multi-storage

# Define storage names, comma-separated format. In this case, "cassandra" and "mysql"
scalar.db.multi_storage.storages=cassandra,mysql

# Define the "cassandra" storage. You can set the storage properties (storage, contact_points, username, etc.) with the property name "scalar.db.multi_storage.storages.<storage name>.<property name without the prefix 'scalar.db.'>". For example, if you want to specify the "scalar.db.contact_points" property for the "cassandra" storage, you can specify "scalar.db.multi_storage.storages.cassandra.contact_points"
scalar.db.multi_storage.storages.cassandra.storage=cassandra
scalar.db.multi_storage.storages.cassandra.contact_points=localhost
scalar.db.multi_storage.storages.cassandra.username=cassandra
scalar.db.multi_storage.storages.cassandra.password=cassandra

# Define the "mysql" storage 
scalar.db.multi_storage.storages.mysql.storage=jdbc
scalar.db.multi_storage.storages.mysql.contact_points=jdbc:mysql://localhost:3306/
scalar.db.multi_storage.storages.mysql.username=root
scalar.db.multi_storage.storages.mysql.password=mysql
# JDBC specific configurations for the "mysql" storage. As mentioned before, the format is "scalar.db.multi_storage.storages.<storage name>.<property name without the prefix 'scalar.db.'>". So for example, if you want to specify the "scalar.db.jdbc.connection_pool.min_idle" property for the "mysql" storage, you can specify "scalar.db.multi_storage.storages.mysql.jdbc.connection_pool.min_idle"
scalar.db.multi_storage.storages.mysql.jdbc.connection_pool.min_idle=5
scalar.db.multi_storage.storages.mysql.jdbc.connection_pool.max_idle=10
scalar.db.multi_storage.storages.mysql.jdbc.connection_pool.max_total=25

# Define table mappings from a table name to a storage. The format is "<table name>:<storage name>,..."
scalar.db.multi_storage.table_mapping=user.ORDER:cassandra,user.CUSTOMER:mysql,coordinator.state:cassandra

# Define namespace mappings from a namespace name to a storage. The format is "<namespace name>:<storage name>,..."
scalar.db.multi_storage.namespace_mapping=user:cassandra,coordinator:mysql

# Define the default storage that’s used if a specified table doesn’t have any table mapping
scalar.db.multi_storage.default_storage=cassandra
```

## Further reading

Please see the following sample to learn Multi-storage Transactions further:

- [Multi-storage Transaction Sample](https://github.com/scalar-labs/scalardb-samples/tree/main/multi-storage-transaction-sample)
