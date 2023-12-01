# Storage Abstraction and API Guide

This page explains how to use the Storage API for users who are experts in ScalarDB.

One of the keys to achieving storage-agnostic or database-agnostic ACID transactions on top of existing storage and database systems is the storage abstraction capabilities that ScalarDB provides. Storage abstraction defines a [data model](design.md#data-model) and the APIs (Storage API) that issue operations on the basis of the data model.

Although you will likely use the [Transactional API](api-guide.md#transactional-api) in most cases, another option is to use the Storage API.

The benefits of using the Storage API include the following:

- As with the Transactional API, you can write your application code without worrying too much about the underlying storage implementation.
- If you don't need transactions for some of the data in your application, you can use the Storage API to partially avoid transactions, which results in faster execution.

{% capture notice--warning %}
**Attention**

Directly using the Storage API or mixing the Transactional API and the Storage API could cause unexpected behavior. For example, since the Storage API cannot provide transaction capability, the API could cause anomalies or data inconsistency if failures occur when executing operations.

Therefore, you should be *very* careful about using the Storage API and use it only if you know exactly what you are doing.
{% endcapture %}

<div class="notice--warning">{{ notice--warning | markdownify }}</div>

## Storage API Example

This section explains how the Storage API can be used in a basic electronic money application.

{% capture notice--warning %}
**Attention**

The electronic money application is simplified for this example and isn’t suitable for a production environment.
{% endcapture %}

<div class="notice--warning">{{ notice--warning | markdownify }}</div>

### ScalarDB configuration

Before you begin, you should configure ScalarDB in the same way mentioned in [Getting Started with ScalarDB](getting-started-with-scalardb.md).

With that in mind, this Storage API example assumes that the configuration file `scalardb.properties` exists.

### Set up the database schema

You need to define the database schema (the method in which the data will be organized) in the application. For details about the supported data types, see [Data type mapping between ScalarDB and other databases](https://scalardb.scalar-labs.com/docs/latest/schema-loader/#data-type-mapping-between-scalardb-and-the-other-databases).

For this example, create a file named `emoney-storage.json` in the `scalardb/docs/getting-started` directory. Then, add the following JSON code to define the schema.

{% capture notice--info %}
**Note**

In the following JSON, the `transaction` field is set to `false`, which indicates that you should use this table with the Storage API.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

```json
{
  "emoney.account": {
    "transaction": false,
    "partition-key": [
      "id"
    ],
    "clustering-key": [],
    "columns": {
      "id": "TEXT",
      "balance": "INT"
    }
  }
}
```

To apply the schema, go to the [ScalarDB Releases](https://github.com/scalar-labs/scalardb/releases) page and download the ScalarDB Schema Loader that matches the version of ScalarDB that you are using to the `getting-started` folder.

Then, run the following command, replacing `<VERSION>` with the version of the ScalarDB Schema Loader that you downloaded:

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --config scalardb.properties -f emoney-storage.json
```

### Example code

The following is example source code for the electronic money application that uses the Storage API.

{% capture notice--warning %}
**Attention**

As previously mentioned, since the Storage API cannot provide transaction capability, the API could cause anomalies or data inconsistency if failures occur when executing operations. Therefore, you should be *very* careful about using the Storage API and use it only if you know exactly what you are doing.
{% endcapture %}

<div class="notice--warning">{{ notice--warning | markdownify }}</div>

```java
public class ElectronicMoney {

  private static final String SCALARDB_PROPERTIES =
      System.getProperty("user.dir") + File.separator + "scalardb.properties";
  private static final String NAMESPACE = "emoney";
  private static final String TABLENAME = "account";
  private static final String ID = "id";
  private static final String BALANCE = "balance";

  private final DistributedStorage storage;

  public ElectronicMoney() throws IOException {
    StorageFactory factory = StorageFactory.create(SCALARDB_PROPERTIES);
    storage = factory.getStorage();
  }

  public void charge(String id, int amount) throws ExecutionException {
    // Retrieve the current balance for id
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLENAME)
            .partitionKey(Key.ofText(ID, id))
            .build();
    Optional<Result> result = storage.get(get);

    // Calculate the balance
    int balance = amount;
    if (result.isPresent()) {
      int current = result.get().getInt(BALANCE);
      balance += current;
    }

    // Update the balance
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLENAME)
            .partitionKey(Key.ofText(ID, id))
            .intValue(BALANCE, balance)
            .build();
    storage.put(put);
  }

  public void pay(String fromId, String toId, int amount) throws ExecutionException {
    // Retrieve the current balances for ids
    Get fromGet =
        Get.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLENAME)
            .partitionKey(Key.ofText(ID, fromId))
            .build();
    Get toGet =
        Get.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLENAME)
            .partitionKey(Key.ofText(ID, toId))
            .build();
    Optional<Result> fromResult = storage.get(fromGet);
    Optional<Result> toResult = storage.get(toGet);

    // Calculate the balances (it assumes that both accounts exist)
    int newFromBalance = fromResult.get().getInt(BALANCE) - amount;
    int newToBalance = toResult.get().getInt(BALANCE) + amount;
    if (newFromBalance < 0) {
      throw new RuntimeException(fromId + " doesn't have enough balance.");
    }

    // Update the balances
    Put fromPut =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLENAME)
            .partitionKey(Key.ofText(ID, fromId))
            .intValue(BALANCE, newFromBalance)
            .build();
    Put toPut =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLENAME)
            .partitionKey(Key.ofText(ID, toId))
            .intValue(BALANCE, newToBalance)
            .build();
    storage.put(fromPut);
    storage.put(toPut);
  }

  public int getBalance(String id) throws ExecutionException {
    // Retrieve the current balances for id
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLENAME)
            .partitionKey(Key.ofText(ID, id))
            .build();
    Optional<Result> result = storage.get(get);

    int balance = -1;
    if (result.isPresent()) {
      balance = result.get().getInt(BALANCE);
    }
    return balance;
  }

  public void close() {
    storage.close();
  }
}
```

## Storage API guide

The Storage API is composed of the Administrative API and CRUD API.

### Administrative API

You can execute administrative operations programmatically as described in this section.

{% capture notice--info %}
**Note**

Another method that you could use to execute administrative operations is by using [Schema Loader](schema-loader.md).
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

#### Get a `DistributedStorageAdmin` instance

To execute administrative operations, you first need to get a `DistributedStorageAdmin` instance. You can obtain the `DistributedStorageAdmin` instance from `StorageFactory` as follows:

```java
StorageFactory storageFactory = StorageFactory.create("<CONFIGURATION_FILE_PATH>");
DistributedStorageAdmin admin = storageFactory.getStorageAdmin();
```

For details about configurations, see [ScalarDB Configurations](configurations.md).

After you have executed all administrative operations, you should close the `DistributedStorageAdmin` instance as follows:

```java
admin.close();
```

#### Create a namespace

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

For details about creation options, see [Creation options](api-guide.md#creation-options).

#### Create a table

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

#### Create a secondary index

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

#### Add a new column to a table

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

#### Truncate a table

You can truncate a table as follows:

```java
// Truncate the table "ns.tbl".
admin.truncateTable("ns", "tbl");
```

#### Drop a secondary index

You can drop a secondary index as follows:

```java
// Drop the secondary index on column "c5" from table "ns.tbl". If the secondary index does not exist, an exception will be thrown.
admin.dropIndex("ns", "tbl", "c5");

// Drop the secondary index only if it exists.
boolean ifExists = true;
admin.dropIndex("ns", "tbl", "c5", ifExists);
```

#### Drop a table

You can drop a table as follows:

```java
// Drop the table "ns.tbl". If the table does not exist, an exception will be thrown.
admin.dropTable("ns", "tbl");

// Drop the table only if it exists.
boolean ifExists = true;
admin.dropTable("ns", "tbl", ifExists);
```

#### Drop a namespace

You can drop a namespace as follows:

```java
// Drop the namespace "ns". If the namespace does not exist, an exception will be thrown.
admin.dropNamespace("ns");

// Drop the namespace only if it exists.
boolean ifExists = true;
admin.dropNamespace("ns", ifExists);
```

#### Get table metadata

You can get table metadata as follows:

```java
// Get the table metadata for "ns.tbl".
TableMetadata tableMetadata = admin.getTableMetadata("ns", "tbl");
```

### Repair a table

You can repair the table metadata of an existing table as follows:

```java
// Repair the table "ns.tbl" with options.
TableMetadata tableMetadata =
    TableMetadata.newBuilder()
        ...
        .build();
Map<String, String> options = ...;
admin.repairTable("ns", "tbl", tableMetadata, options);
```

### Implement CRUD operations

The following sections describe CRUD operations.

#### Get a `DistributedStorage` instance

To execute CRUD operations in the Storage API, you need to get a `DistributedStorage` instance.

You can get an instance as follows:

```java
StorageFactory storageFactory = StorageFactory.create("<CONFIGURATION_FILE_PATH>");
DistributedStorage storage = storageFactory.getStorage();
```

After you have executed all CRUD operations, you should close the `DistributedStorage` instance as follows:

```java
storage.close();
```

#### `Get` operation

`Get` is an operation to retrieve a single record specified by a primary key.

You need to create a `Get` object first, and then you can execute the object by using the `storage.get()` method as follows:

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
Optional<Result> result = storage.get(get);
```

You can also specify projections to choose which columns are returned.

For details about how to construct `Key` objects, see [Key construction](api-guide.md#key-construction). And, for details about how to handle `Result` objects, see [Handle Result objects](api-guide.md#handle-result-objects).

##### Specify a consistency level

You can specify a consistency level in each operation (`Get`, `Scan`, `Put`, and `Delete`) in the Storage API as follows:

```java
Get get =
    Get.newBuilder()
        .namespace("ns")
        .table("tbl")
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .consistency(Consistency.LINEARIZABLE) // Consistency level
        .build();
```

The following table describes the three consistency levels:

| Consistency level   | Description |
| ------------------- | ----------- |
| `SEQUENTIAL`        | Sequential consistency assumes that the underlying storage implementation makes all operations appear to take effect in some sequential order and the operations of each individual process appear in this sequence. |
| `EVENTUAL`          | Eventual consistency assumes that the underlying storage implementation makes all operations take effect eventually. |
| `LINEARIZABLE`      | Linearizable consistency assumes that the underlying storage implementation makes each operation appear to take effect atomically at some point between its invocation and completion. |

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
Optional<Result> result = storage.get(get);
```

{% capture notice--info %}
**Note**

If the result has more than one record, `storage.get()` will throw an exception.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

#### `Scan` operation

`Scan` is an operation to retrieve multiple records within a partition. You can specify clustering-key boundaries and orderings for clustering-key columns in `Scan` operations.

You need to create a `Scan` object first, and then you can execute the object by using the `storage.scan()` method as follows:

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
Scanner scanner = storage.scan(scan);
```

You can omit the clustering-key boundaries or specify either a `start` boundary or an `end` boundary. If you don't specify `orderings`, you will get results ordered by the clustering order that you defined when creating the table.

In addition, you can specify `projections` to choose which columns are returned and use `limit` to specify the number of records to return in `Scan` operations.

##### Handle `Scanner` objects

A `Scan` operation in the Storage API returns a `Scanner` object.

If you want to get results one by one from the `Scanner` object, you can use the `one()` method as follows:

```java
Optional<Result> result = scanner.one();
```

Or, if you want to get a list of all results, you can use the `all()` method as follows:

```java
List<Result> results = scanner.all();
```

In addition, since `Scanner` implements `Iterable`, you can use `Scanner` in a for-each loop as follows:

```java
for (Result result : scanner) {
    ...
}
```

Remember to close the `Scanner` object after getting the results:

```java
scanner.close();
```

Or you can use `try`-with-resources as follows:

```java
try (Scanner scanner = storage.scan(scan)) {
  ...
}
```

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
Scanner scanner = storage.scan(scan);
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
Key partitionKey = Key.ofInt("c1", 10);
Key startClusteringKey = Key.of("c2", "aaa", "c3", 100L);
Key endClusteringKey = Key.of("c2", "aaa", "c3", 300L);

Scan scan =
    Scan.newBuilder()
        .namespace("ns")
        .table("tbl")
        .all()
        .projections("c1", "c2", "c3", "c4")
        .limit(10)
        .build();

// Execute the `Scan` operation.
Scanner scanner = storage.scan(scan);
```

{% capture notice--info %}
**Note**

You can't specify clustering-key boundaries and orderings in `Scan` without specifying a partition key.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

#### `Put` operation

`Put` is an operation to put a record specified by a primary key. The operation behaves as an upsert operation for a record, in which the operation updates the record if the record exists or inserts the record if the record does not exist.

You need to create a `Put` object first, and then you can execute the object by using the `storage.put()` method as follows:

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
storage.put(put);
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

{% capture notice--info %}
**Note**

If you specify `enableImplicitPreRead()`, `disableImplicitPreRead()`, or `implicitPreReadEnabled()` in the `Put` operation builder, they will be ignored.

{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

#### `Delete` operation

`Delete` is an operation to delete a record specified by a primary key.

You need to create a `Delete` object first, and then you can execute the object by using the `storage.delete()` method as follows:

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
storage.delete(delete);
```

#### `Put` and `Delete` with a condition

You can write arbitrary conditions (for example, a bank account balance must be equal to or more than zero) that you require an operation to meet before being executed by implementing logic that checks the conditions. Alternatively, you can write simple conditions in a mutation operation, such as `Put` and `Delete`.

When a `Put` or `Delete` operation includes a condition, the operation is executed only if the specified condition is met. If the condition is not met when the operation is executed, an exception called `NoMutationException` will be thrown.

##### Conditions for `Put`

In a `Put` operation in the Storage API, you can specify a condition that causes the `Put` operation to be executed only when the specified condition matches. This operation is like a compare-and-swap operation where the condition is compared and the update is performed atomically.

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
```

Other than the `putIf` condition, you can specify the `putIfExists` and `putIfNotExists` conditions as follows:

```java
// Build a `putIfExists` condition.
MutationCondition putIfExistsCondition = ConditionBuilder.putIfExists();

// Build a `putIfNotExists` condition.
MutationCondition putIfNotExistsCondition = ConditionBuilder.putIfNotExists();
```

##### Conditions for `Delete`

Similar to a `Put` operation, you can specify a condition in a `Delete` operation in the Storage API.

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
        .condition(condition)  // condition
        .build();
```

In addition to using the `deleteIf` condition, you can specify the `deleteIfExists` condition as follows:

```java
// Build a `deleteIfExists` condition.
MutationCondition deleteIfExistsCondition = ConditionBuilder.deleteIfExists();
```

#### Mutate operation

Mutate is an operation to execute multiple mutations (`Put` and `Delete` operations) in a single partition.

You need to create mutation objects first, and then you can execute the objects by using the `storage.mutate()` method as follows:

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
storage.mutate(Arrays.asList(put, delete));
```

{% capture notice--info %}
**Note**

A Mutate operation only accepts mutations for a single partition; otherwise, an exception will be thrown.

In addition, if you specify multiple conditions in a Mutate operation, the operation will be executed only when all the conditions match.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

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
