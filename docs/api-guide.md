# Java API Guide

ScalarDB Java API is mainly composed of Administrative API and Transactional API.
This guide briefly explains what kind of APIs exist and how to use them.

* [Administrative API](#administrative-api)
* [Transactional API](#transactional-api)

## Administrative API

This section explains how to execute administrative operations with Administrative API in ScalarDB.
You can execute administrative operations programmatically as follows, but you can also execute those operations through [Schema Loader](schema-loader.md).

### Get a DistributedTransactionAdmin instance

To execute administrative operations, you first need to get a `DistributedTransactionAdmin` instance.
The `DistributedTransactionAdmin` instance can be obtained from `TransactionFactory` as follows:

```java
TransactionFactory transactionFactory = TransactionFactory.create("<configuration file path>");
DistributedTransactionAdmin admin = transactionFactory.getTransactionAdmin();
```

For details about configurations, see [ScalarDB Configurations](configurations.md).

Once you have executed all administrative operations, you should close the `DistributedTransactionAdmin` instance as follows:

```java
admin.close();
```

### Create a namespace

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

#### Creation Options

In the creation operations (creating a namespace, creating a table, etc.), you can specify options that are maps of option names and values (`Map<String, String>`).
With the options, we can set storage adapter specific configurations.

Currently, we can set the following options for the storage adapters:

For Cosmos DB for NoSQL:

| name       | value                                        | default |
|------------|----------------------------------------------|---------|
| ru         | Base resource unit                           | 400     |
| no-scaling | Disable auto-scaling for Cosmos DB for NoSQL | false   |

For DynamoDB:

| name       | value                                  | default |
|------------|----------------------------------------|---------|
| no-scaling | Disable auto-scaling for DynamoDB      | false   |
| no-backup  | Disable continuous backup for DynamoDB | false   |
| ru         | Base resource unit                     | 10      |

For Cassandra:

| name                 | value                                                                                 | default          |
|----------------------|---------------------------------------------------------------------------------------|------------------|
| replication-strategy | Cassandra replication strategy, must be `SimpleStrategy` or `NetworkTopologyStrategy` | `SimpleStrategy` |
| compaction-strategy  | Cassandra compaction strategy, must be `LCS`, `STCS` or `TWCS`                        | `STCS`           |
| replication-factor   | Cassandra replication factor                                                          | 1                |


### Create a table

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

### Create a secondary index

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

### Truncate a table

You can truncate a table as follows:

```java
// Truncate a table "ns.tbl"
admin.truncateTable("ns", "tbl");
```

### Drop a secondary index

You can drop a secondary index as follows:

```java
// Drop a secondary index on a column "c5" of a table "ns.tbl". It will throw an exception if the secondary index does not exist
admin.dropIndex("ns", "tbl", "c5");

// Drop a secondary index only if it exists
boolean ifExists = true;
admin.dropIndex("ns", "tbl", "c5", ifExists);
```

### Drop a table

You can drop a table as follows:

```java
// Drop a table "ns.tbl". It will throw an exception if the table does not exist
admin.dropTable("ns", "tbl");

// Drop a table only if it exists
boolean ifExists = true;
admin.dropTable("ns", "tbl", ifExists);
```

### Drop a namespace

You can drop a namespace as follows:

```java
// Drop a namespace "ns". It will throw an exception if the namespace does not exist
admin.dropNamespace("ns");

// Drop a namespace only if it exists
boolean ifExists = true;
admin.dropNamespace("ns", ifExists);
```

### Get a table metadata

You can get a table metadata as follows:

```java
// Get a table metadata of "ns.tbl"
TableMetadata tableMetadata = admin.getTableMetadata("ns", "tbl");
```

### Operations for Coordinator tables

Depending on the transaction manager type, you need to create coordinator tables to execute transactions. 
The following items describe the operations for the coordinator table.

#### Create Coordinator tables

You can create coordinator tables as follows:

```java
// Create coordinator tables
admin.createCoordinatorTables();

// Create coordinator tables only if they do not already exist
boolean ifNotExist = true;
admin.createCoordinatorTables(ifNotExist);

// Create coordinator tables with options
Map<String, String> options = ...;
admin.createCoordinatorTables(options);
```

#### Truncate Coordinator tables

You can truncate coordinator tables as follows:

```java
// Truncate coordinator tables
admin.truncateCoordinatorTables();
```

#### Drop Coordinator tables

You can drop coordinator tables as follows:

```java
// Drop coordinator tables
admin.dropCoordinatorTables();

// Drop coordinator tables if they exist
boolean ifExist = true;
admin.dropCoordinatorTables(ifExist);
```

## Transactional API

This section explains how to execute transactional operations with Transactional API in ScalarDB.

### Get a DistributedTransactionManager instance

You need to get a `DistributedTransactionManager` instance to execute transactional operations.
You can get it in the following way:

```java
TransactionFactory transactionFactory = TransactionFactory.create("<configuration file path>");
DistributedTransactionManager transactionManager = transactionFactory.getTransactionManager();
```

Once you have executed all transactional operations, you should close the `DistributedTransactionManager` instance as follows:

```java
transactionManager.close();
```

### Begin/Start a transaction

You need to begin/start a transaction before executing transactional CRUD operations.
You can begin/start a transaction as follows:

```java
// Begin a transaction
DistributedTransaction transaction = transactionManager.begin();

Or

// Start a transaction
DistributedTransaction transaction = transactionManager.start();
```

You can also begin/start a transaction with specifying a transaction ID as follows:

```java
// Begin a transaction with specifying a transaction ID
DistributedTransaction transaction = transactionManager.begin("<transaction ID>");

Or

// Start a transaction with specifying a transaction ID
DistributedTransaction transaction = transactionManager.start("<transaction ID>");
```

Note that you must guarantee uniqueness of the transaction ID in this case.

### Resume a transaction

You can resume a transaction you have already begun with specifying a transaction ID as follows:

```java
// Resume a transaction
DistributedTransaction transaction = transactionManager.resume("<transaction ID>");
```

It is helpful in a stateful application where a transaction spans multiple client requests.
In that case, the application can begin a transaction in the first client request.
And in the following client requests, it can resume the transaction with the `resume()` method.

### CRUD operations

#### Key construction

Most CRUD operations need to specify `Key` objects (partition-key, clustering-key, etc.).
So, before moving on to CRUD operations, the following explains how to construct a `Key` object.

For a single column key, you can use the `Key.ofXXX()` methods (XXX is a type name) to construct it as follows:

```java
// for a key that consists of a single column of Int
Key key1 = Key.ofInt("col1", 1);

// for a key that consists of a single column of BigInt
Key key2 = Key.ofBigInt("col1", 100L);

// for a key that consists of a single column of Double
Key key3 = Key.ofDouble("col1", 1.3d);

// for a key that consists of a single column of Text
Key key4 = Key.ofText("col1", "value");
```

For a key that consists of 2 - 5 columns, you can use the `Key.of()` methods to construct it as follows:

```java
// for a key that consists of 2 - 5 columns
Key key1 = Key.of("col1", 1, "col2", 100L);
Key key2 = Key.of("col1", 1, "col2", 100L, "col3", 1.3d);
Key key3 = Key.of("col1", 1, "col2", 100L, "col3", 1.3d, "col4", "value");
Key key4 = Key.of("col1", 1, "col2", 100L, "col3", 1.3d, "col4", "value", "col5", false);
```

Similar to `ImmutableMap.of()` in Guava, you need to specify column names and values in turns.

For a key that consists of more than 5 columns, we can use the builder to construct it as follows:

```java
// for a key that consists of more than 5 columns
Key key = Key.newBuilder()
    .addInt("col1", 1)
    .addBigInt("col2", 100L)
    .addDouble("col3", 1.3d)
    .addText("col4", "value")
    .addBoolean("col5", false)
    .addInt("col6", 100)
    .build();
```

#### Get operation

`Get` is an operation to retrieve a single record specified by a primary key.

You need to create a Get object first, and then you can execute it with the `transaction.get()` method as follows:

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
Optional<Result> result = transaction.get(get);
```

You can also specify projections to choose which columns are returned.

##### Handle Result objects

The Get operation and Scan operation return `Result` objects.
So the following shows how to handle `Result` objects.

You can get a column value of a result with `getXXX("<column name>")` methods (XXX is a type name) as follows:

```java
// Get a Boolean value of a column
boolean booleanValue = result.getBoolean("<column name>");

// Get an Int value of a column
int intValue = result.getInt("<column name>");

// Get a BigInt value of a column
long bigIntValue = result.getBigInt("<column name>");

// Get a Float value of a column
float floatValue = result.getFloat("<column name>");

// Get a Double value of a column
double doubleValue = result.getDouble("<column name>");

// Get a Text value of a column
String textValue = result.getText("<column name>");

// Get a Blob value of a column (as a ByteBuffer)
ByteBuffer blobValue = result.getBlob("<column name>");

// Get a Blob value of a column as a byte array
byte[] blobValueAsBytes = result.getBlobAsBytes("<column name>");
```

And if you need to check if a value of a column is null, you can use the `isNull("<column name>")` method.

``` java
// Check if a value of a column is null
boolean isNull = result.isNull("<column name>");
```

Please see also [Javadoc of `Result`](https://javadoc.io/static/com.scalar-labs/scalardb/3.6.0/com/scalar/db/api/Result.html) for more details.

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
Optional<Result> result = transaction.get(get);
```

Note that if the result has more than one record, the `transaction.get()` throws an exception.
If you want to handle multiple results, use [Scan with a secondary index](#scan-with-a-secondary-index).

#### Scan operation

`Scan` is an operation to retrieve multiple records within a partition.
You can specify clustering key boundaries and orderings for clustering key columns in Scan operations.

You need to create a Scan object first, and then you can execute it with the `transaction.scan()` method as follows:

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
List<Result> results = transaction.scan(scan);
```

You can omit the clustering key boundaries, or you can specify either a start boundary or an end boundary.
If you don't specify orderings, you get results ordered by clustering order you defined when creating the table.

Also, you can specify projections to choose which columns are returned, and limit to specify the number of records to return in Scan operations.

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
List<Result> results = transaction.scan(scan);
```

Note that you can't specify clustering key boundaries and orderings in Scan with a secondary index.

##### Scan without a partition key to retrieve all the records of a table

You can also execute a Scan operation without specifying a partition key.

Instead of calling the `partitionKey()` method in the builder, you can call the `all()` method to scan a table without specifying a partition key as follows:

```java
// Create a Scan operation without a partition key
Scan scan =
    Scan.newBuilder()
        .namespace("ns")
        .table("tbl")
        .all()
        .projections("c1", "c2", "c3", "c4")
        .limit(10)
        .build();

// Execute the Scan operation
List<Result> results = transaction.scan(scan);
```

Note that you can't specify clustering key boundaries and orderings in Scan without a partition key.

#### Put operation

`Put` is an operation to put a record specified by a primary key.
It behaves as an upsert operation for a record, i.e., updating the record if the record exists; otherwise, inserting the record.
Note that when you update an existing record, you need to read it using a `Get` or a `Scan` before a `Put` operation.

You need to create a Put object first, and then you can execute it with the `transaction.put()` method as follows:

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
transaction.put(put);
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

#### Delete operation

`Delete` is an operation to delete a record specified by a primary key.
Note that when you delete a record, you need to read it using a `Get` or a `Scan` before a `Delete` operation.

You need to create a Delete object first, and then you can execute it with the `transaction.delete()` method as follows:

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
transaction.delete(delete);
```

#### Mutate operation

Mutate is an operation to execute multiple mutations (Put and Delete operations).

You need to create mutation objects first, and then you can execute them with the `transaction.mutate()` method as follows:

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
transaction.mutate(Arrays.asList(put, delete));
```

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

#### Notes

- All the builders of the CRUD operations can specify consistency with the `consistency()` methods, but it's ignored, and the `LINEARIZABLE` consistency level is always used in transactions.
- Also, the builders of the mutation operations (Put and Delete operations) can specify a condition with the `condition()` methods, but it's ignored, too. 
Please program such conditions in a transaction if you want to implement conditional mutation.

### Commit a transaction

After executing CRUD operations, you need to commit a transaction to finish it.

You can commit a transaction as follows;

```java
// Commit a transaction
transaction.commit();
```

### Rollback/Abort a transaction

If you want to rollback/abort a transaction or an error happens during the execution, you can rollback/abort a transaction.

You can rollback/abort a transaction as follows;

```java
// Rollback a transaction
transaction.rollback();

Or

// Abort a transaction
transaction.abort();
```

Please see [Handle exceptions](#handle-exceptions) for the details of how to handle exceptions in ScalarDB.

## Handle exceptions

Handling exceptions correctly in ScalarDB is very important.
If you mishandle exceptions, your data could become inconsistent.
This document explains how to handle exceptions properly in ScalarDB.

Let's look at the following example code to see how to handle exceptions in ScalarDB.

```java
public class Sample {
  public static void main(String[] args) throws Exception {
    TransactionFactory factory = TransactionFactory.create("<configuration file path>");
    DistributedTransactionManager transactionManager = factory.getTransactionManager();

    int retryCount = 0;
    TransactionException lastException = null;

    while (true) {
      if (retryCount++ > 0) {
        // Retry the transaction three times maximum in this sample code
        if (retryCount >= 3) {
          // Throw the last exception if the number of retries exceeds the maximum
          throw lastException;
        }

        // Sleep 100 milliseconds before retrying the transaction in this sample code
        TimeUnit.MILLISECONDS.sleep(100);
      }

      DistributedTransaction transaction = null;
      try {
        // Begin a transaction
        transaction = transactionManager.begin();

        // Execute CRUD operations in the transaction
        Optional<Result> result = transaction.get(...);
        List<Result> results = transaction.scan(...);
        transaction.put(...);
        transaction.delete(...);

        // Commit the transaction
        transaction.commit();
      } catch (UnknownTransactionStatusException e) {
        // If you catch `UnknownTransactionStatusException` when committing the transaction, it
        // indicates that the status of the transaction, whether it has succeeded or not, is
        // unknown. In such a case, you need to check if the transaction is committed successfully
        // or not and retry it if it failed. How to identify a transaction status is delegated to 
        // users
        return;
      } catch (TransactionException e) {
        // For other exceptions, you can try retrying the transaction.

        // For `CrudConflictException` and `CommitConflictException` and
        // `TransactionNotFoundException`, you can basically retry the transaction. However, for the
        // other exceptions, the transaction may still fail if the cause of the exception is
        // nontransient. In such a case, you will exhaust the number of retries and throw the last
        // exception

        if (transaction != null) {
          try {
            transaction.rollback();
          } catch (RollbackException ex) {
            // Rolling back the transaction failed. As the transaction should eventually recover,
            // you don't need to do anything further. You can simply log the occurrence here
          }
        }

        lastException = e;
      }
    }
  }
}
```

The `begin()` API could throw `TransactionException` or `TransactionNotFoundException`.
If you catch `TransactionException`, it indicates that the transaction has failed to begin due to transient or nontransient faults. You can try retrying the transaction, but you may not be able to begin the transaction due to nontransient faults.
If you catch `TransactionNotFoundException`, it indicates that the transaction has failed to begin due to transient faults. You can retry the transaction.

The APIs for CRUD operations (`get()`, `scan()`, `put()`, `delete()`, and `mutate()`) could throw `CrudException` or `CrudConflictException`.
If you catch `CrudException`, it indicates that the transaction CRUD operation has failed due to transient or nontransient faults. You can try retrying the transaction from the beginning, but the transaction may still fail if the cause is nontransient.
If you catch `CrudConflictException`, it indicates that the transaction CRUD operation has failed due to transient faults (e.g., a conflict error). You can retry the transaction from the beginning.

Also, the `commit()` API could throw `CommitException`, `CommitConflictException`, or `UnknownTransactionStatusException`.
If you catch `CommitException`, it indicates that committing the transaction fails due to transient or nontransient faults. You can try retrying the transaction from the beginning, but the transaction may still fail if the cause is nontransient.
If you catch `CommitConflictException`, it indicates that committing the transaction has failed due to transient faults (e.g., a conflict error). You can retry the transaction from the beginning.
If you catch `UnknownTransactionStatusException`, it indicates that the status of the transaction, whether it has succeeded or not, is unknown.
In such a case, you need to check if the transaction is committed successfully and retry the transaction if it has failed.
How to identify a transaction status is delegated to users.
You may want to create a transaction status table and update it transactionally with other application data so that you can get the status of a transaction from the status table.

Although not illustrated in the sample code, the `resume()` API could also throw `TransactionNotFoundException`.
This exception indicates that the transaction associated with the specified ID was not found and/or the transaction might have expired.
In either case, you can retry the transaction from the beginning since the cause of this exception is basically transient.

In the sample code, for `UnknownTransactionStatusException`, the transaction is not retried because the cause of the exception is nontransient.
For other exceptions, the transaction is retried because the cause of the exception is transient or nontransient.
If the cause of the exception is transient, the transaction may succeed if you retry it.
However, if the cause of the exception is nontransient, the transaction may still fail even if you retry it.
In such a case, you will exhaust the number of retries.

Please note that if you begin a transaction by specifying a transaction ID, you must use a different ID when you retry the transaction.
And, in the sample code, the transaction is retried three times maximum and sleeps for 100 milliseconds before it is retried.
But you can choose a retry policy, such as exponential backoff, according to your application requirements.

## Transactional operations for Two-phase Commit Transaction

Please see [Two-phase Commit Transactions](two-phase-commit-transactions.md).

## Investigate Consensus Commit transactions errors

This configuration is only available to troubleshoot Consensus Commit transactions. By adding the following configuration, `Get` and `Scan` operations results will contain [transaction metadata](schema-loader.md#internal-metadata-for-consensus-commit).
To see the transaction metadata columns details for a given table, you can use the `DistributedTransactionAdmin.getTableMetadata()` method which will return the table metadata augmented with the transaction metadata columns. 
All in all, using this configuration can be useful to investigate transaction related issues.

```properties
# By default, it is set to "false".
scalar.db.consensus_commit.include_metadata.enabled=true
```

## References

* [Design document](design.md)
* [Getting started](getting-started-with-scalardb.md)
* [Multi-storage Transactions](multi-storage-transactions.md)
* [Two-phase Commit Transactions](two-phase-commit-transactions.md)
* [ScalarDB Server](scalardb-server.md)
