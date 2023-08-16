# Storage abstraction

ScalarDB is middleware on top of existing storage/database systems and provides storage/database-agnostic ACID transactions on top of the systems.
One of the keys to achieving the storage/database-agnostic ACID transactions is a storage abstraction that ScalarDB provides.
The storage abstraction defines [a data model](design.md#data-model) and APIs (Storage API) that issue operations on the basis of the data model.
Although you use the [Transaction API](api-guide.md) in most cases, you can also directly use the Storage API.
There are a few benefits of using the Storage API.
First, as with Transaction API, you can write your application code without caring about the underlying storage implementation too much.
Second, when you don't need transactions for some data in your application, you can use the Storage API to (partially) avoid transactions for achieving faster execution.

However, directly using the Storage API or mixing the Transaction API and the Storage API could cause unexpected behaviors.
For example, since the Storage API cannot provide transaction capability, it could cause data inconsistencies/anomalies when failures occur during the execution of the operations.
Therefore, you should be very careful about using the Storage API, and please use it only if you know exactly what you are doing.

In this document, we explain how to use the Storage API for users who are experts in ScalarDB.

## Storage API Example

This section explains how the Storage API can be used in a simple electronic money example application.

### ScalarDB configuration

The configuration is the same as when you use the ACID transaction manager.
Please see [Getting Started](getting-started-with-scalardb.md) for the details of the configuration.

From here, we assume that the configuration file **scalardb.properties** exists.

### Set up database schema

First, you need to define how the data will be organized (a.k.a database schema) in the application with ScalarDB database schema.
Here is a database schema for the sample application.
You can create a JSON file emoney-storage.json with the JSON below.

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

Note that the `transaction` field is set to `false`, which indicates you use this table with the Storage API.

To apply the schema, download the Schema Loader that matches the version you use from [scalardb releases](https://github.com/scalar-labs/scalardb/releases), and run the following command to load the schema.

```
$ java -jar scalardb-schema-loader-<version>.jar --config scalardb.properties -f emoney-storage.json
```

### Example code

This is a simple electronic money application with the Storage API.
(Be careful: it is simplified for ease of reading and far from practical and is certainly not production-ready.)

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

Again, note that the Storage API doesn't provide transaction capability and could cause data inconsistencies when failures occur during the execution of the operations.

## Storage API Guide

The Storage API is mainly composed of Administrative API and CRUD API.
This section explains how to use them.

* [Administrative API](#administrative-api)
* [CRUD API](#crud-api)

### Administrative API

You can execute administrative operations programmatically as follows, but you can also execute those operations through [Schema Loader](schema-loader.md).

#### Get a DistributedStorageAdmin instance

To execute administrative operations, you first need to get a `DistributedStorageAdmin` instance.
The `DistributedStorageAdmin` instance can be obtained from `StorageFactory` as follows:

```java
StorageFactory storageFactory = StorageFactory.create("<configuration file path>");
DistributedStorageAdmin admin = storageFactory.getStorageAdmin();
```

For details about configurations, see [ScalarDB Configurations](configurations.md).

Once you have executed all administrative operations, you should close the `DistributedStorageAdmin` instance as follows:

```java
admin.close();
```

#### Create a namespace

Before creating tables, namespaces must be created since a table belongs to one namespace.
You can create a namespace as follows:

```java
// Create a namespace "ns". It will throw an exception if the namespace already exists
admin.createNamespace("ns");

// Create a namespace only if it does not already exist
boolean ifNotExists = true;
admin.createNamespace("ns", ifNotExists);

// Create a namespace with options
Map<String, String> options = ...;
admin.createNamespace("ns", options);
```

Please see [Creation options](api-guide.md#creation-options) for the details of the creation options.

#### Create a table

Next, we will discuss table creation.

You firstly need to create the TaleMetadata as follows:

```java
// Define a table metadata
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

Here you define columns, a partition key, a clustering key including clustering orders, and secondary indexes of a table.

Please see [ScalarDB design document - Data Model](design.md#data-model) for the details of the ScalarDB Data Model.

And then, you can create a table as follows:

```java
// Create a table "ns.tbl". It will throw an exception if the table already exists
admin.createTable("ns", "tbl", tableMetadata);

// Create a table only if it does not already exist
boolean ifNotExists = true;
admin.createTable("ns", "tbl", tableMetadata, ifNotExists);

// Create a table with options
Map<String, String> options = ...;
admin.createTable("ns", "tbl", tableMetadata, options);
```

#### Create a secondary index

You can create a secondary index as follows:

```java
// Create a secondary index on a column "c5" of a table "ns.tbl". It will throw an exception if the secondary index already exists
admin.createIndex("ns", "tbl", "c5");

// Create a secondary index only if it does not already exist
boolean ifNotExists = true;
admin.createIndex("ns", "tbl", "c5", ifNotExists);

// Create a secondary index with options
Map<String, String> options = ...;
admin.createIndex("ns", "tbl", "c5", options);
```

### Add a new column to a table

You can add a new non-partition key column to a table as follows:
```java
// Add the new column "c6" of type INT to the table "ns.tbl"  
admin.addNewColumnToTable("ns", "tbl", "c6", DataType.INT)
```

This should be executed with significant consideration as the execution time may vary greatly
depending on the underlying storage. Please plan accordingly especially if the database runs in production:
- For Cosmos DB for NoSQL and DynamoDB: this operation is almost instantaneous as the table
  schema is not modified. Only the table metadata stored in a separated table are updated.
- For Cassandra: adding a column will only update the schema metadata and do not modify existing
  schema records. The cluster topology is the main factor for the execution time. Since the schema
  metadata change propagates to each cluster node via a gossip protocol, the larger the cluster, the
  longer it will take for all nodes to be updated.
- For relational databases (MySQL, Oracle, etc.): it may take a very long time to execute and a
  table-lock may be performed. 

#### Truncate a table

You can truncate a table as follows:

```java
// Truncate a table "ns.tbl"
admin.truncateTable("ns", "tbl");
```

#### Drop a secondary index

You can drop a secondary index as follows:

```java
// Drop a secondary index on a column "c5" of a table "ns.tbl". It will throw an exception if the secondary index does not exists
admin.dropIndex("ns", "tbl", "c5");

// Drop a secondary index only if it exists
boolean ifExists = true;
admin.dropIndex("ns", "tbl", "c5", ifExists);
```

#### Drop a table

You can drop a table as follows:

```java
// Drop a table "ns.tbl". It will throw an exception if the table does not exists
admin.dropTable("ns", "tbl");

// Drop a table only if it exists
boolean ifExists = true;
admin.dropTable("ns", "tbl", ifExists);
```

#### Drop a namespace

You can drop a namespace as follows:

```java
// Drop a namespace "ns". It will throw an exception if the namespace does not exists
admin.dropNamespace("ns");

// Drop a namespace only if it exists
boolean ifExists = true;
admin.dropNamespace("ns", ifExists);
```

#### Get a table metadata

You can get a table metadata as follows:

```java
// Get a table metadata of "ns.tbl"
TableMetadata tableMetadata = admin.getTableMetadata("ns", "tbl");
```

### CRUD API

#### Get a DistributedStorage instance

You need to get a `DistributedStorage` instance to execute CRUD operations in the Storage API.
You can get it in the following way:

```java
StorageFactory storageFactory = StorageFactory.create("<configuration file path>");
DistributedStorage storage = storageFactory.getStorage();
```

Once you have executed all CRUD operations, you should close the `DistributedStorage` instance as follows:

```java
storage.close();
```

#### Get operation

`Get` is an operation to retrieve a single record specified by a primary key.

You need to create a Get object first, and then you can execute it with the `storage.get()` method as follows:

```java
// Create a Get operation
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

// Execute the Get operation
Optional<Result> result = storage.get(get);
```

You can also specify projections to choose which columns are returned.

Please see [Key construction](api-guide.md#key-construction) for the details of how to construct `Key` objects.

Also, please see [Handle Result objects](api-guide.md#handle-result-objects) for the details of how to handle `Result` objects.

##### Consistency level

You can specify a consistency level in each operation (Get, Scan, Put, and Delete) in the Storage API as follows:

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

We have three consistency levels as described in the following:

| consistency level | description |
| ----------------- | ----------- |
| SEQUENTIAL        | Sequential consistency. With this consistency, it assumes that the underlying storage implementation makes all operations appear to take effect in some sequential order, and the operations of each individual process appear in this sequence. |
| EVENTUAL          | Eventual consistency. With this consistency, it assumes that the underlying storage implementation makes all operations take effect eventually. |
| LINEARIZABLE      | Linearizable consistency. With this consistency, it assumes that the underlying storage implementation makes each operation appears to take effect atomically at some point between its invocation and completion. |

##### Get with a secondary index

You can also execute a Get operation with a secondary index.

Instead of specifying a partition key, you can specify an index key (specifying an indexed column) to use a secondary index as follows:

```java
// Create a Get operation with a secondary index
Key indexKey = Key.ofFloat("c4", 1.23F);

Get get =
    Get.newBuilder()
        .namespace("ns")
        .table("tbl")
        .indexKey(indexKey)
        .projections("c1", "c2", "c3", "c4")
        .build();

// Execute the Get operation
Optional<Result> result = storage.get(get);
```

Note that if the result has more than one record, the `storage.get()` throws an exception.

#### Scan operation

`Scan` is an operation to retrieve multiple records within a partition.
You can specify clustering key boundaries and orderings for clustering key columns in Scan operations.

You need to create a Scan object first, and then you can execute it with the `storage.scan()` method as follows:

```java
// Create a Scan operation
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

// Execute the Scan operation
Scanner scanner = storage.scan(scan);
```

You can omit the clustering key boundaries, or you can specify either a start boundary or an end boundary.
If you don't specify orderings, you get results ordered by clustering order you defined when creating the table.

Also, you can specify projections to choose which columns are returned, and limit to specify the number of records to return in Scan operations.

##### Handle Scanner objects

A Scan operation in the Storage API returns a `Scanner` object.

If you want to get results one by one from the Scanner object, you can use the `one()` method as follows:
```java
Optional<Result> result = scanner.one();
```

Or, if you want to get results all at once as a `List`, you can use the `all()` method as follows:
```java
List<Result> results = scanner.all();
```

Also, as Scanner implements `Iterable`, you can use it in a for-each loop as follows:

```java
for (Result result : scanner) {
    ...
}
```

Please don't forget to close the Scanner object after getting results from it:

```java
scanner.close();
```

Or you can use `try-with-resources` as follows:
```java
try (Scanner scanner = storage.scan(scan)) {
  ...
}
```

##### Scan with a secondary index

You can also execute a Scan operation with a secondary index.

Instead of specifying a partition key, you can specify an index key (specifying an indexed column) to use a secondary index as follows:

```java
// Create a Scan operation with a secondary index
Key indexKey = Key.ofFloat("c4", 1.23F);

Scan scan =
    Scan.newBuilder()
        .namespace("ns")
        .table("tbl")
        .indexKey(indexKey)
        .projections("c1", "c2", "c3", "c4")
        .limit(10)
        .build();

// Execute the Scan operation
Scanner scanner = storage.scan(scan);
```

Note that you can't specify clustering key boundaries and orderings in Scan with a secondary index.

##### Scan without a partition key to retrieve all the records of a table

You can also execute a Scan operation without specifying a partition key.

Instead of calling the `partitionKey()` method in the builder, you can call the `all()` method to scan a table without specifying a partition key as follows:

```java
// Create a Scan operation without a partition key
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

// Execute the Scan operation
Scanner scanner = storage.scan(scan);
```

Note that you can't specify clustering key boundaries and orderings in Scan without a partition key.

#### Put operation

`Put` is an operation to put a record specified by a primary key.
It behaves as an upsert operation: If it exists, it updates it; otherwise, it inserts it.

You need to create a Put object first, and then you can execute it with the `storage.put()` method as follows:

```java
// Create a Put operation
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

// Execute the Put operation
storage.put(put);
```

You can also put a record with null values as follows:

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

##### Put with a condition

In a Put operation in the Storage API, you can specify a condition, and only when the specified condition matches, the Put operation is executed.
It's like a well-known CAS (compare and swap) operation, and the condition comparison and the update are performed atomically.

You can specify a condition in a Put operation as follows:

```java
// Build a condition
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
// Build a putIfExists condition
MutationCondition putIfExistsCondition = ConditionBuilder.putIfExists();

// Build a putIfNotExists condition
MutationCondition putIfNotExistsCondition = ConditionBuilder.putIfNotExists();
```

#### Delete operation

`Delete` is an operation to delete a record specified by a primary key.

You need to create a Delete object first, and then you can execute it with the `storage.delete()` method as follows:

```java
// Create a Delete operation
Key partitionKey = Key.ofInt("c1", 10);
Key clusteringKey = Key.of("c2", "aaa", "c3", 100L);

Delete delete =
    Delete.newBuilder()
        .namespace("ns")
        .table("tbl")
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .build();

// Execute the Delete operation
storage.delete(delete);
```

##### Delete with a condition

Similar to a Put operation, you can specify a condition in a Delete operation in the Storage API.

You can specify a condition in a Delete operation as follows:

```java
// Build a condition
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

Other than the `deleteIf` condition, you can specify the `deleteIfExists` condition as follows:

```java
// Build a deleteIfExists condition
MutationCondition deleteIfExistsCondition = ConditionBuilder.deleteIfExists();
```

#### Mutate operation

Mutate is an operation to execute multiple mutations (Put and Delete operations) in a single partition.

You need to create mutation objects first, and then you can execute them with the `storage.mutate()` method as follows:

```java
// Create Put and Delete operations
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

// Execute the operations
storage.mutate(Arrays.asList(put, delete));
```

Note that a Mutate operation only accepts mutations for a single partition; otherwise, it throws an exception.

And if you specify multiple conditions in a Mutate operation, the operation is executed only when all the conditions match.

#### Use a default namespace for CRUD operations

A default namespace for all the CRUD operations can be set with a property of the ScalarDB configuration.
If you would like to use this setting with ScalarDB server, it needs to be set on the client-side configuration.

```properties
scalar.db.default_namespace_name=<a_namespace_name>
```

Any operation that does not specify a namespace will use the default namespace set in the configuration.

```java
//This operation will target the default namespace
Scan scanUsingDefaultNamespace =
    Scan.newBuilder()
        .table("tbl")
        .all()
        .build();
//This operation will target the "ns" namespace
Scan scanUsingSpecifiedNamespace =
    Scan.newBuilder()
        .namespace("ns")
        .table("tbl")
        .all()
        .build();
```

## References

* [Java API Guide](api-guide.md)
* [Design document](design.md)
* [Getting started](getting-started-with-scalardb.md)
* [ScalarDB Server](scalardb-server.md)
* [Schema Loader](schema-loader.md)
