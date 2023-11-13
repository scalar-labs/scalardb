# ScalarDB Java API Guide

The ScalarDB Java API is mainly composed of the Administrative API and Transactional API. This guide briefly explains what kinds of APIs exist, how to use them, and related topics like how to handle exceptions.

## Administrative API

This section explains how to execute administrative operations programmatically by using the Administrative API in ScalarDB.

{% capture notice--info %}
**Note**

Another method for executing administrative operations is to use [Schema Loader](schema-loader.md).
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

### Get a `DistributedTransactionAdmin` instance

You first need to get a `DistributedTransactionAdmin` instance to execute administrative operations.

To get a `DistributedTransactionAdmin` instance, you can use `TransactionFactory` as follows:

```java
TransactionFactory transactionFactory = TransactionFactory.create("<CONFIGURATION_FILE_PATH>");
DistributedTransactionAdmin admin = transactionFactory.getTransactionAdmin();
```

For details about configurations, see [ScalarDB Configurations](configurations.md).

After you have executed all administrative operations, you should close the `DistributedTransactionAdmin` instance as follows:

```java
admin.close();
```

### Create a namespace

Before creating tables, namespaces must be created since a table belongs to one namespace.

You can create a namespace as follows:

```java
// Create the namespace "ns". If the namespace already exists, an exception will be thrown.
admin.createNamespace("ns");

// Create the namespace only if it does not already exist.
boolean ifNotExists = true;
admin.createNamespace("ns", ifNotExists);

// Create the namespace with options.
Map<String, String> options = ...;
admin.createNamespace("ns", options);
```

#### Creation options

In the creation operations, like creating a namespace and creating a table, you can specify options that are maps of option names and values (`Map<String, String>`). By using the options, you can set storage adapter–specific configurations.

Select your database to see the options available:

<div id="tabset-1">
<div class="tab">
  <button class="tablinks" onclick="openTab(event, 'Cassandra', 'tabset-1')" id="defaultOpen-1">Cassandra</button>
  <button class="tablinks" onclick="openTab(event, 'Cosmos_DB_for_NoSQL', 'tabset-1')">Cosmos DB for NoSQL</button>
  <button class="tablinks" onclick="openTab(event, 'DynamoDB', 'tabset-1')">DynamoDB</button>
  <button class="tablinks" onclick="openTab(event, 'JDBC_databases', 'tabset-1')">JDBC databases</button>
</div>

<div id="Cassandra" class="tabcontent" markdown="1">

| Name                 | Description                                                                            | Default          |
|----------------------|----------------------------------------------------------------------------------------|------------------|
| replication-strategy | Cassandra replication strategy. Must be `SimpleStrategy` or `NetworkTopologyStrategy`. | `SimpleStrategy` |
| compaction-strategy  | Cassandra compaction strategy, Must be `LCS`, `STCS` or `TWCS`.                        | `STCS`           |
| replication-factor   | Cassandra replication factor.                                                          | 3                |

</div>
<div id="Cosmos_DB_for_NoSQL" class="tabcontent" markdown="1">

| Name       | Description                                         | Default |
|------------|-----------------------------------------------------|---------|
| ru         | Base resource unit.                                 | 400     |
| no-scaling | Disable auto-scaling for Cosmos DB for NoSQL.       | false   |

</div>
<div id="DynamoDB" class="tabcontent" markdown="1">

| Name       | Description                             | Default |
|------------|-----------------------------------------|---------|
| no-scaling | Disable auto-scaling for DynamoDB.      | false   |
| no-backup  | Disable continuous backup for DynamoDB. | false   |
| ru         | Base resource unit.                     | 10      |

</div>
<div id="JDBC_databases" class="tabcontent" markdown="1">

No options are available for JDBC databases.

</div>
</div>

### Create a table

When creating a table, you should define the table metadata and then create the table.

To define the table metadata, you can use `TableMetadata`. The following shows how to define the columns, partition key, clustering key including clustering orders, and secondary indexes of a table:

```java
// Define the table metadata.
TableMetadata tableMetadata =
    TableMetadata.newBuilder()
        .addColumn("c1", DataType.INT)
        .addColumn("c2", DataType.TEXT)
        .addColumn("c3", DataType.BIGINT)
        .addColumn("c4", DataType.FLOAT)
        .addColumn("c5", DataType.DOUBLE)
        .addPartitionKey("c1")
        .addClusteringKey("c2", Scan.Ordering.Order.DESC)
        .addClusteringKey("c3", Scan.Ordering.Order.ASC)
        .addSecondaryIndex("c4")
        .build();
```

For details about the data model of ScalarDB, see [Data Model](design.md#data-model).

Then, create a table as follows:

```java
// Create the table "ns.tbl". If the table already exists, an exception will be thrown.
admin.createTable("ns", "tbl", tableMetadata);

// Create the table only if it does not already exist.
boolean ifNotExists = true;
admin.createTable("ns", "tbl", tableMetadata, ifNotExists);

// Create the table with options.
Map<String, String> options = ...;
admin.createTable("ns", "tbl", tableMetadata, options);
```

### Create a secondary index

You can create a secondary index as follows:

```java
// Create a secondary index on column "c5" for table "ns.tbl". If a secondary index already exists, an exception will be thrown.
admin.createIndex("ns", "tbl", "c5");

// Create the secondary index only if it does not already exist.
boolean ifNotExists = true;
admin.createIndex("ns", "tbl", "c5", ifNotExists);

// Create the secondary index with options.
Map<String, String> options = ...;
admin.createIndex("ns", "tbl", "c5", options);
```

### Add a new column to a table

You can add a new, non-partition key column to a table as follows:

```java
// Add a new column "c6" with the INT data type to the table "ns.tbl".
admin.addNewColumnToTable("ns", "tbl", "c6", DataType.INT)
```

{% capture notice--warning %}
**Attention**

You should carefully consider adding a new column to a table because the execution time may vary greatly depending on the underlying storage. Please plan accordingly and consider the following, especially if the database runs in production:

- **For Cosmos DB for NoSQL and DynamoDB:** Adding a column is almost instantaneous as the table schema is not modified. Only the table metadata stored in a separate table is updated.
- **For Cassandra:** Adding a column will only update the schema metadata and will not modify the existing schema records. The cluster topology is the main factor for the execution time. Changes to the schema metadata are shared to each cluster node via a gossip protocol. Because of this, the larger the cluster, the longer it will take for all nodes to be updated.
- **For relational databases (MySQL, Oracle, etc.):** Adding a column shouldn't take a long time to execute.
{% endcapture %}

<div class="notice--warning">{{ notice--warning | markdownify }}</div>

### Truncate a table

You can truncate a table as follows:

```java
// Truncate the table "ns.tbl".
admin.truncateTable("ns", "tbl");
```

### Drop a secondary index

You can drop a secondary index as follows:

```java
// Drop the secondary index on column "c5" from table "ns.tbl". If the secondary index does not exist, an exception will be thrown.
admin.dropIndex("ns", "tbl", "c5");

// Drop the secondary index only if it exists.
boolean ifExists = true;
admin.dropIndex("ns", "tbl", "c5", ifExists);
```

### Drop a table

You can drop a table as follows:

```java
// Drop the table "ns.tbl". If the table does not exist, an exception will be thrown.
admin.dropTable("ns", "tbl");

// Drop the table only if it exists.
boolean ifExists = true;
admin.dropTable("ns", "tbl", ifExists);
```

### Drop a namespace

You can drop a namespace as follows:

```java
// Drop the namespace "ns". If the namespace does not exist, an exception will be thrown.
admin.dropNamespace("ns");

// Drop the namespace only if it exists.
boolean ifExists = true;
admin.dropNamespace("ns", ifExists);
```

### Get existing namespaces

You can get the existing namespaces as follows:

```java
Set<String> namespaces = admin.getNamespaceNames();
```

### Get the tables of a namespace

You can get the tables of a namespace as follows:

```java
// Get the tables of the namespace "ns".
Set<String> tables = admin.getNamespaceTableNames("ns");
```

### Get table metadata

You can get table metadata as follows:

```java
// Get the table metadata for "ns.tbl".
TableMetadata tableMetadata = admin.getTableMetadata("ns", "tbl");
```

### Specify operations for the Coordinator table

The Coordinator table is used by the [Transactional API](#transactional-api) to track the statuses of transactions.

When using a transaction manager, you must create the Coordinator table to execute transactions. In addition to creating the table, you can truncate and drop the Coordinator table.

#### Create the Coordinator table

You can create the Coordinator table as follows:

```java
// Create the Coordinator table.
admin.createCoordinatorTables();

// Create the Coordinator table only if one does not already exist.
boolean ifNotExist = true;
admin.createCoordinatorTables(ifNotExist);

// Create the Coordinator table with options.
Map<String, String> options = ...;
admin.createCoordinatorTables(options);
```

#### Truncate the Coordinator table

You can truncate the Coordinator table as follows:

```java
// Truncate the Coordinator table.
admin.truncateCoordinatorTables();
```

#### Drop the Coordinator table

You can drop the Coordinator table as follows:

```java
// Drop the Coordinator table.
admin.dropCoordinatorTables();

// Drop the Coordinator table if one exist.
boolean ifExist = true;
admin.dropCoordinatorTables(ifExist);
```

## Transactional API

This section explains how to execute transactional operations by using the Transactional API in ScalarDB.

### Get a `DistributedTransactionManager` instance

You first need to get a `DistributedTransactionManager` instance to execute transactional operations.

To get a `DistributedTransactionManager` instance, you can use `TransactionFactory` as follows:

```java
TransactionFactory transactionFactory = TransactionFactory.create("<CONFIGURATION_FILE_PATH>");
DistributedTransactionManager transactionManager = transactionFactory.getTransactionManager();
```

After you have executed all transactional operations, you should close the `DistributedTransactionManager` instance as follows:

```java
transactionManager.close();
```

### Begin or start a transaction

Before executing transactional CRUD operations, you need to begin or start a transaction.

You can begin a transaction as follows:

```java
// Begin a transaction.
DistributedTransaction transaction = transactionManager.begin();
```

Or, you can start a transaction as follows:

```java
// Start a transaction.
DistributedTransaction transaction = transactionManager.start();
```

Alternatively, you can use the `begin` method for a transaction by specifying a transaction ID as follows:

```java
// Begin a transaction with specifying a transaction ID.
DistributedTransaction transaction = transactionManager.begin("<TRANSACTION_ID>");
```

Or, you can use the `start` method for a transaction by specifying a transaction ID as follows:

```java
// Start a transaction with specifying a transaction ID.
DistributedTransaction transaction = transactionManager.start("<TRANSACTION_ID>");
```

{% capture notice--info %}
**Note**

Specifying a transaction ID is useful when you want to link external systems to ScalarDB. Otherwise, you should use the `begin()` method or the `start()` method.

When you specify a transaction ID, make sure you specify a unique ID (for example, UUID v4) throughout the system since ScalarDB depends on the uniqueness of transaction IDs for correctness.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

### Join a transaction

Joining a transaction is particularly useful in a stateful application where a transaction spans multiple client requests. In such a scenario, the application can start a transaction during the first client request. Then, in subsequent client requests, the application can join the ongoing transaction by using the `join()` method.

You can join an ongoing transaction that has already begun by specifying the transaction ID as follows:

```java
// Join a transaction.
DistributedTransaction transaction = transactionManager.join("<TRANSACTION_ID>");
```

{% capture notice--info %}
**Note**

To get the transaction ID with `getId()`, you can specify the following:

```java
tx.getId();
```
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

### Resume a transaction

Resuming a transaction is particularly useful in a stateful application where a transaction spans multiple client requests. In such a scenario, the application can start a transaction during the first client request. Then, in subsequent client requests, the application can resume the ongoing transaction by using the `resume()` method.

You can resume an ongoing transaction that you have already begun by specifying a transaction ID as follows:

```java
// Resume a transaction.
DistributedTransaction transaction = transactionManager.resume("<TRANSACTION_ID>");
```

{% capture notice--info %}
**Note**

To get the transaction ID with `getId()`, you can specify the following:

```java
tx.getId();
```
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

### Implement CRUD operations

The following sections describe key construction and CRUD operations.

{% capture notice--info %}
**Note**

Although all the builders of the CRUD operations can specify consistency by using the `consistency()` methods, those methods are ignored. Instead, the `LINEARIZABLE` consistency level is always used in transactions.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

#### Key construction

Most CRUD operations need to specify `Key` objects (partition-key, clustering-key, etc.). So, before moving on to CRUD operations, the following explains how to construct a `Key` object.

For a single column key, you can use `Key.of<TYPE_NAME>()` methods to construct the key as follows:

```java
// For a key that consists of a single column of INT.
Key key1 = Key.ofInt("col1", 1);

// For a key that consists of a single column of BIGINT.
Key key2 = Key.ofBigInt("col1", 100L);

// For a key that consists of a single column of DOUBLE.
Key key3 = Key.ofDouble("col1", 1.3d);

// For a key that consists of a single column of TEXT.
Key key4 = Key.ofText("col1", "value");
```

For a key that consists of two to five columns, you can use the `Key.of()` method to construct the key as follows. Similar to `ImmutableMap.of()` in Guava, you need to specify column names and values in turns:

```java
// For a key that consists of two to five columns.
Key key1 = Key.of("col1", 1, "col2", 100L);
Key key2 = Key.of("col1", 1, "col2", 100L, "col3", 1.3d);
Key key3 = Key.of("col1", 1, "col2", 100L, "col3", 1.3d, "col4", "value");
Key key4 = Key.of("col1", 1, "col2", 100L, "col3", 1.3d, "col4", "value", "col5", false);
```

For a key that consists of more than five columns, we can use the builder to construct the key as follows:

```java
// For a key that consists of more than five columns.
Key key = Key.newBuilder()
    .addInt("col1", 1)
    .addBigInt("col2", 100L)
    .addDouble("col3", 1.3d)
    .addText("col4", "value")
    .addBoolean("col5", false)
    .addInt("col6", 100)
    .build();
```

#### `Get` operation

`Get` is an operation to retrieve a single record specified by a primary key.

You need to create a `Get` object first, and then you can execute the object by using the `transaction.get()` method as follows:

```java
// Create a `Get` operation.
Key partitionKey = Key.ofInt("c1", 10);
Key clusteringKey = Key.of("c2", "aaa", "c3", 100L);

Get get =
    Get.newBuilder()
        .namespace("ns")
        .table("tbl")
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .projections("c1", "c2", "c3", "c4")
        .build();

// Execute the `Get` operation.
Optional<Result> result = transaction.get(get);
```

You can also specify projections to choose which columns are returned.

##### Handle `Result` objects

The `Get` operation and `Scan` operation return `Result` objects. The following shows how to handle `Result` objects.

You can get a column value of a result by using `get<TYPE_NAME>("<COLUMN_NAME>")` methods as follows:

```java
// Get the BOOLEAN value of a column.
boolean booleanValue = result.getBoolean("<COLUMN_NAME>");

// Get the INT value of a column.
int intValue = result.getInt("<COLUMN_NAME>");

// Get the BIGINT value of a column.
long bigIntValue = result.getBigInt("<COLUMN_NAME>");

// Get the FLOAT value of a column.
float floatValue = result.getFloat("<COLUMN_NAME>");

// Get the DOUBLE value of a column.
double doubleValue = result.getDouble("<COLUMN_NAME>");

// Get the TEXT value of a column.
String textValue = result.getText("<COLUMN_NAME>");

// Get the BLOB value of a column as a `ByteBuffer`.
ByteBuffer blobValue = result.getBlob("<COLUMN_NAME>");

// Get the BLOB value of a column as a `byte` array.
byte[] blobValueAsBytes = result.getBlobAsBytes("<COLUMN_NAME>");
```

And if you need to check if a value of a column is null, you can use the `isNull("<COLUMN_NAME>")` method.

``` java
// Check if a value of a column is null.
boolean isNull = result.isNull("<COLUMN_NAME>");
```

For more details, see the `Result` page in the [Javadoc](https://javadoc.io/doc/com.scalar-labs/scalardb/latest/index.html) of the version of ScalarDB that you're using.

##### Execute `Get` by using a secondary index

You can execute a `Get` operation by using a secondary index.

Instead of specifying a partition key, you can specify an index key (indexed column) to use a secondary index as follows:

```java
// Create a `Get` operation by using a secondary index.
Key indexKey = Key.ofFloat("c4", 1.23F);

Get get =
    Get.newBuilder()
        .namespace("ns")
        .table("tbl")
        .indexKey(indexKey)
        .projections("c1", "c2", "c3", "c4")
        .build();

// Execute the `Get` operation.
Optional<Result> result = transaction.get(get);
```

{% capture notice--info %}
**Note**

If the result has more than one record, `transaction.get()` will throw an exception. If you want to handle multiple results, see [Execute `Scan` by using a secondary index](#execute-scan-by-using-a-secondary-index).

{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

#### `Scan` operation

`Scan` is an operation to retrieve multiple records within a partition. You can specify clustering-key boundaries and orderings for clustering-key columns in `Scan` operations.

You need to create a `Scan` object first, and then you can execute the object by using the `transaction.scan()` method as follows:

```java
// Create a `Scan` operation.
Key partitionKey = Key.ofInt("c1", 10);
Key startClusteringKey = Key.of("c2", "aaa", "c3", 100L);
Key endClusteringKey = Key.of("c2", "aaa", "c3", 300L);

Scan scan =
    Scan.newBuilder()
        .namespace("ns")
        .table("tbl")
        .partitionKey(partitionKey)
        .start(startClusteringKey)
        .end(endClusteringKey)
        .projections("c1", "c2", "c3", "c4")
        .orderings(Scan.Ordering.desc("c2"), Scan.Ordering.asc("c3"))
        .limit(10)
        .build();

// Execute the `Scan` operation.
List<Result> results = transaction.scan(scan);
```

You can omit the clustering-key boundaries or specify either a `start` boundary or an `end` boundary. If you don't specify `orderings`, you will get results ordered by the clustering order that you defined when creating the table.

In addition, you can specify `projections` to choose which columns are returned and use `limit` to specify the number of records to return in `Scan` operations.

##### Execute `Scan` by using a secondary index

You can execute a `Scan` operation by using a secondary index.

Instead of specifying a partition key, you can specify an index key (indexed column) to use a secondary index as follows:

```java
// Create a `Scan` operation by using a secondary index.
Key indexKey = Key.ofFloat("c4", 1.23F);

Scan scan =
    Scan.newBuilder()
        .namespace("ns")
        .table("tbl")
        .indexKey(indexKey)
        .projections("c1", "c2", "c3", "c4")
        .limit(10)
        .build();

// Execute the `Scan` operation.
List<Result> results = transaction.scan(scan);
```

{% capture notice--info %}
**Note**

You can't specify clustering-key boundaries and orderings in `Scan` by using a secondary index.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

##### Execute `Scan` without specifying a partition key to retrieve all the records of a table

You can execute a `Scan` operation without specifying a partition key.

Instead of calling the `partitionKey()` method in the builder, you can call the `all()` method to scan a table without specifying a partition key as follows:

```java
// Create a `Scan` operation without specifying a partition key.
Scan scan =
    Scan.newBuilder()
        .namespace("ns")
        .table("tbl")
        .all()
        .projections("c1", "c2", "c3", "c4")
        .limit(10)
        .build();

// Execute the `Scan` operation.
List<Result> results = transaction.scan(scan);
```

{% capture notice--info %}
**Note**

You can't specify clustering-key boundaries and orderings in `Scan` without specifying a partition key.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

#### `Put` operation

`Put` is an operation to put a record specified by a primary key. The operation behaves as an upsert operation for a record, in which the operation updates the record if the record exists or inserts the record if the record does not exist.

You need to create a `Put` object first, and then you can execute the object by using the `transaction.put()` method as follows:

```java
// Create a `Put` operation.
Key partitionKey = Key.ofInt("c1", 10);
Key clusteringKey = Key.of("c2", "aaa", "c3", 100L);

Put put =
    Put.newBuilder()
        .namespace("ns")
        .table("tbl")
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .floatValue("c4", 1.23F)
        .doubleValue("c5", 4.56)
        .build();

// Execute the `Put` operation.
transaction.put(put);
```

You can also put a record with `null` values as follows:

```java
Put put =
    Put.newBuilder()
        .namespace("ns")
        .table("tbl")
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .floatValue("c4", null)
        .doubleValue("c5", null)
        .build();
```

##### Disable implicit pre-read for `Put` operations

In Consensus Commit, an application must read a record before mutating the record with `Put` and `Delete` operations to obtain the latest states of the record if the record exists. If an application does not read the record explicitly in a transaction, ScalarDB will read the record on behalf of the application before committing the transaction. We call this feature *implicit pre-read*.

If you are certain that a record you are trying to mutate does not exist, you can disable implicit pre-read for the `Put` operation for better performance. For example, if you load initial data, you can and should disable implicit pre-read. A `Put` operation without implicit pre-read is faster than a regular `Put` operation because the operation skips an unnecessary read. However, if the record exists, the operation will fail due to a conflict. This occurs because ScalarDB assumes that another transaction wrote the record.

You can disable implicit pre-read for a `Put` operation by specifying `disableImplicitPreRead()` in the `Put` operation builder as follows:

```java
Put put =
    Put.newBuilder()
        .namespace("ns")
        .table("tbl")
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .floatValue("c4", 1.23F)
        .doubleValue("c5", 4.56)
        .disableImplicitPreRead()
        .build();
```

#### `Delete` operation

`Delete` is an operation to delete a record specified by a primary key.

You need to create a `Delete` object first, and then you can execute the object by using the `transaction.delete()` method as follows:

```java
// Create a `Delete` operation.
Key partitionKey = Key.ofInt("c1", 10);
Key clusteringKey = Key.of("c2", "aaa", "c3", 100L);

Delete delete =
    Delete.newBuilder()
        .namespace("ns")
        .table("tbl")
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .build();

// Execute the `Delete` operation.
transaction.delete(delete);
```

#### `Put` and `Delete` with a condition

You can write arbitrary conditions (for example, a bank account balance must be equal to or more than zero) that you require a transaction to meet before being committed by implementing logic that checks the conditions in the transaction. Alternatively, you can write simple conditions in a mutation operation, such as `Put` and `Delete`.

When a `Put` or `Delete` operation includes a condition, the operation is executed only if the specified condition is met. If the condition is not met when the operation is executed, an exception called `UnsatisfiedConditionException` will be thrown.

##### Conditions for `Put`

You can specify a condition in a `Put` operation as follows:

```java
// Build a condition.
MutationCondition condition =
    ConditionBuilder.putIf(ConditionBuilder.column("c4").isEqualToFloat(0.0F))
        .and(ConditionBuilder.column("c5").isEqualToDouble(0.0))
        .build();

Put put =
    Put.newBuilder()
        .namespace("ns")
        .table("tbl")
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .floatValue("c4", 1.23F)
        .doubleValue("c5", 4.56)
        .condition(condition) // condition
        .build();

// Execute the `Put` operation.
transaction.put(put);
```

In addition to using the `putIf` condition, you can specify the `putIfExists` and `putIfNotExists` conditions as follows:

```java
// Build a `putIfExists` condition.
MutationCondition putIfExistsCondition = ConditionBuilder.putIfExists();

// Build a `putIfNotExists` condition.
MutationCondition putIfNotExistsCondition = ConditionBuilder.putIfNotExists();
```

##### Conditions for `Delete`

You can specify a condition in a `Delete` operation as follows:

```java
// Build a condition.
MutationCondition condition =
    ConditionBuilder.deleteIf(ConditionBuilder.column("c4").isEqualToFloat(0.0F))
        .and(ConditionBuilder.column("c5").isEqualToDouble(0.0))
        .build();

Delete delete =
    Delete.newBuilder()
        .namespace("ns")
        .table("tbl")
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .condition(condition) // condition
        .build();

// Execute the `Delete` operation.
transaction.delete(delete);
```

In addition to using the `deleteIf` condition, you can specify the `deleteIfExists` condition as follows:

```java
// Build a `deleteIfExists` condition.
MutationCondition deleteIfExistsCondition = ConditionBuilder.deleteIfExists();
```

#### Mutate operation

Mutate is an operation to execute multiple mutations (`Put` and `Delete` operations).

You need to create mutation objects first, and then you can execute the objects by using the `transaction.mutate()` method as follows:

```java
// Create `Put` and `Delete` operations.
Key partitionKey = Key.ofInt("c1", 10);

Key clusteringKeyForPut = Key.of("c2", "aaa", "c3", 100L);

Put put =
    Put.newBuilder()
        .namespace("ns")
        .table("tbl")
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKeyForPut)
        .floatValue("c4", 1.23F)
        .doubleValue("c5", 4.56)
        .build();

Key clusteringKeyForDelete = Key.of("c2", "bbb", "c3", 200L);

Delete delete =
    Delete.newBuilder()
        .namespace("ns")
        .table("tbl")
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKeyForDelete)
        .build();

// Execute the operations.
transaction.mutate(Arrays.asList(put, delete));
```

#### Default namespace for CRUD operations

A default namespace for all CRUD operations can be set by using a property in the ScalarDB configuration.

```properties
scalar.db.default_namespace_name=<NAMESPACE_NAME>
```

Any operation that does not specify a namespace will use the default namespace set in the configuration.

```java
// This operation will target the default namespace.
Scan scanUsingDefaultNamespace =
    Scan.newBuilder()
        .table("tbl")
        .all()
        .build();
// This operation will target the "ns" namespace.
Scan scanUsingSpecifiedNamespace =
    Scan.newBuilder()
        .namespace("ns")
        .table("tbl")
        .all()
        .build();
```

### Commit a transaction

After executing CRUD operations, you need to commit a transaction to finish it.

You can commit a transaction as follows:

```java
// Commit a transaction.
transaction.commit();
```

### Roll back or abort a transaction

If an error occurs when executing a transaction, you can roll back or abort the transaction.

You can roll back a transaction as follows:

```java
// Roll back a transaction.
transaction.rollback();
```

Or, you can abort a transaction as follows:

```java
// Abort a transaction.
transaction.abort();
```

For details about how to handle exceptions in ScalarDB, see [How to handle exceptions](#how-to-handle-exceptions).

## How to handle exceptions

When executing a transaction, you will also need to handle exceptions properly.

{% capture notice--warning %}
**Attention**

If you don't handle exceptions properly, you may face anomalies or data inconsistency.
{% endcapture %}

<div class="notice--warning">{{ notice--warning | markdownify }}</div>

The following sample code shows how to handle exceptions:

```java
public class Sample {
  public static void main(String[] args) throws Exception {
    TransactionFactory factory = TransactionFactory.create("<CONFIGURATION_FILE_PATH>");
    DistributedTransactionManager transactionManager = factory.getTransactionManager();

    int retryCount = 0;
    TransactionException lastException = null;

    while (true) {
      if (retryCount++ > 0) {
        // Retry the transaction three times maximum.
        if (retryCount >= 3) {
          // Throw the last exception if the number of retries exceeds the maximum.
          throw lastException;
        }

        // Sleep 100 milliseconds before retrying the transaction.
        TimeUnit.MILLISECONDS.sleep(100);
      }

      DistributedTransaction transaction = null;
      try {
        // Begin a transaction.
        transaction = transactionManager.begin();

        // Execute CRUD operations in the transaction.
        Optional<Result> result = transaction.get(...);
        List<Result> results = transaction.scan(...);
        transaction.put(...);
        transaction.delete(...);

        // Commit the transaction.
        transaction.commit();
      } catch (UnsatisfiedConditionException e) {
        // You need to handle `UnsatisfiedConditionException` only if a mutation operation specifies a condition.
        // This exception indicates the condition for the mutation operation is not met.

        try {
          transaction.rollback();
        } catch (RollbackException ex) {
          // Rolling back the transaction failed. Since the transaction should eventually recover, 
          // you don't need to do anything further. You can simply log the occurrence here.
        }

        // You can handle the exception here, according to your application requirements.

        return;
      } catch (UnknownTransactionStatusException e) {
        // If you catch `UnknownTransactionStatusException` when committing the transaction, 
        // it indicates that the status of the transaction, whether it was successful or not, is unknown.
        // In such a case, you need to check if the transaction is committed successfully or not and 
        // retry the transaction if it failed. How to identify a transaction status is delegated to users.
        return;
      } catch (TransactionException e) {
        // For other exceptions, you can try retrying the transaction.

        // For `CrudConflictException`, `CommitConflictException`, and `TransactionNotFoundException`,
        // you can basically retry the transaction. However, for the other exceptions, the transaction
        // will still fail if the cause of the exception is non-transient. In such a case, you will 
        // exhaust the number of retries and throw the last exception.

        if (transaction != null) {
          try {
            transaction.rollback();
          } catch (RollbackException ex) {
            // Rolling back the transaction failed. The transaction should eventually recover,
            // so you don't need to do anything further. You can simply log the occurrence here.
          }
        }

        lastException = e;
      }
    }
  }
}
```

### `TransactionException` and `TransactionNotFoundException`

The `begin()` API could throw `TransactionException` or `TransactionNotFoundException`:

- If you catch `TransactionException`, this exception indicates that the transaction has failed to begin due to transient or non-transient faults. You can try retrying the transaction, but you may not be able to begin the transaction due to non-transient faults.
- If you catch `TransactionNotFoundException`, this exception indicates that the transaction has failed to begin due to transient faults. In this case, you can retry the transaction.

The `join()` API could also throw `TransactionNotFoundException`. You can handle this exception in the same way that you handle the exceptions for the `begin()` API.

### `CrudException` and `CrudConflictException`

The APIs for CRUD operations (`get()`, `scan()`, `put()`, `delete()`, and `mutate()`) could throw `CrudException` or `CrudConflictException`:

- If you catch `CrudException`, this exception indicates that the transaction CRUD operation has failed due to transient or non-transient faults. You can try retrying the transaction from the beginning, but the transaction may still fail if the cause is non-transient.
- If you catch `CrudConflictException`, this exception indicates that the transaction CRUD operation has failed due to transient faults (for example, a conflict error). In this case, you can retry the transaction from the beginning.

### `UnsatisfiedConditionException`

The APIs for mutation operations (`put()`, `delete()`, and `mutate()`) could also throw `UnsatisfiedConditionException`.

If you catch `UnsatisfiedConditionException`, this exception indicates that the condition for the mutation operation is not met. You can handle this exception according to your application requirements.

### `CommitException`, `CommitConflictException`, and `UnknownTransactionStatusException`

The `commit()` API could throw `CommitException`, `CommitConflictException`, or `UnknownTransactionStatusException`:

- If you catch `CommitException`, this exception indicates that committing the transaction fails due to transient or non-transient faults. You can try retrying the transaction from the beginning, but the transaction may still fail if the cause is non-transient.
- If you catch `CommitConflictException`, this exception indicates that committing the transaction has failed due to transient faults (for example, a conflict error). In this case, you can retry the transaction from the beginning.
- If you catch `UnknownTransactionStatusException`, this exception indicates that the status of the transaction, whether it was successful or not, is unknown. In this case, you need to check if the transaction is committed successfully and retry the transaction if it has failed.

How to identify a transaction status is delegated to users. You may want to create a transaction status table and update it transactionally with other application data so that you can get the status of a transaction from the status table.

### Notes about some exceptions

Although not illustrated in the sample code, the `resume()` API could also throw `TransactionNotFoundException`. This exception indicates that the transaction associated with the specified ID was not found and/or the transaction might have expired. In either case, you can retry the transaction from the beginning since the cause of this exception is basically transient.

In the sample code, for `UnknownTransactionStatusException`, the transaction is not retried because the application must check if the transaction was successful to avoid potential duplicate operations. For other exceptions, the transaction is retried because the cause of the exception is transient or non-transient. If the cause of the exception is transient, the transaction may succeed if you retry it. However, if the cause of the exception is non-transient, the transaction will still fail even if you retry it. In such a case, you will exhaust the number of retries.

{% capture notice--info %}
**Note**

In the sample code, the transaction is retried three times maximum and sleeps for 100 milliseconds before it is retried. But you can choose a retry policy, such as exponential backoff, according to your application requirements.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

## Investigating Consensus Commit transaction manager errors

To investigate errors when using the Consensus Commit transaction manager, you can enable a configuration that will return table metadata augmented with transaction metadata columns, which can be helpful when investigating transaction-related issues. This configuration, which is only available when troubleshooting the Consensus Commit transaction manager, enables you to see transaction metadata column details for a given table by using the `DistributedTransactionAdmin.getTableMetadata()` method.

By adding the following configuration, `Get` and `Scan` operations results will contain [transaction metadata](schema-loader.md#internal-metadata-for-consensus-commit):

```properties
# By default, this configuration is set to `false`.
scalar.db.consensus_commit.include_metadata.enabled=true
```
