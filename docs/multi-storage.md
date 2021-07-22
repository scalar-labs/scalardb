# Multi-storage in Scalar DB

Scalar DB transactions can span multiple storages/databases while preserving ACID property with a feature called `multi-storage`.
This documentation explains the feature briefly.

## How Multi-storage works

Internally, the `multi-storage` implementation holds multiple storage instances and has mappings from a table name/a namespace name to a proper storage instance.
When an operation is executed, it chooses a proper storage instance from the specified table/namespace by using the table-storage/namespace-storage mappings and uses it.

## Configuration

You can use `multi-storage` in the same way as the other storages/databases at the code level as long as the configuration is properly set for `multi-storage`.
On the other hand, the configuration is different.
An example of the configuration is as follows:

```
# The storage is "multi-storage"
scalar.db.storage=multi-storage

# Define storage names, comma-separated format. In this case, "cassandra" and "mysql"
scalar.db.multi_storage.storages=cassandra,mysql

# Define the "cassandra" storage. You can set the storage properties (storage, contact_points, username, etc.) with the property name "scalar.db.multi_storage.storages.<storage name>.<property name>"
scalar.db.multi_storage.storages.cassandra.storage=cassandra
scalar.db.multi_storage.storages.cassandra.contact_points=localhost
scalar.db.multi_storage.storages.cassandra.username=cassandra
scalar.db.multi_storage.storages.cassandra.password=cassandra
scalar.db.multi_storage.storages.cassandra.namespace_prefix=prefix

# Define the "mysql" storage 
scalar.db.multi_storage.storages.mysql.storage=jdbc
scalar.db.multi_storage.storages.mysql.contact_points=jdbc:mysql://localhost:3306/
scalar.db.multi_storage.storages.mysql.username=root
scalar.db.multi_storage.storages.mysql.password=mysql
scalar.db.multi_storage.storages.mysql.jdbc.connection.pool.min_idle=5
scalar.db.multi_storage.storages.mysql.jdbc.connection.pool.max_idle=10
scalar.db.multi_storage.storages.mysql.jdbc.connection.pool.max_total=25

# Define table mappings from a table name to a storage. The format is "<table name>:<storage name>,..."
scalar.db.multi_storage.table_mapping=user.ORDER:cassandra,user.CUSTOMER:mysql,coordinator.state:cassandra

# Define namespace mappings from a namespace name to a storage. The format is "<namespace name>:<storage name>,..."
scalar.db.multi_storage.namespace_mapping=user:cassandra,coordinator:mysql

# Define the default storage that’s used if a specified table doesn’t have any table mapping
scalar.db.multi_storage.default_storage=cassandra
```
