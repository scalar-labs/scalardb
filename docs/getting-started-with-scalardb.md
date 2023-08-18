{% include end-of-support.html %}

# Getting Started with Scalar DB

Here we assume Oracle JDK 8 and the underlying storage/database such as Cassandra are properly configured.
If you haven't done it, please configure them first by following [this](getting-started.md).

## Build

For building Scalar DB, what you will need to do is as follows.
```
$ SCALARDB_HOME=/path/to/scalardb
$ cd $SCALARDB_HOME
$ ./gradlew installDist
$ sudo mkdir /var/log/scalar
$ sudo chmod 777 /var/log/scalar
```
Or you can download from [maven central repository](https://mvnrepository.com/artifact/com.scalar-labs/scalardb).
For example in Gradle, you can add the following dependency to your build.gradle. Please replace the `<version>` with the version you want to use.
```
dependencies {
    implementation group: 'com.scalar-labs', name: 'scalardb', version: '<version>'
}
```

Let's move to the `getting-started` directory so that we can avoid too much copy-and-paste.
```
$ cd docs/getting-started
```

## Set up database schema

First of all, you need to define how the data will be organized (a.k.a database schema) in the application with Scalar DB database schema.
Here is a database schema for the sample application. For the supported data types, please see [this doc](schema.md) for more details.
You can create a JSON file `emoney-storage.json` with the JSON below.

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

To apply the schema, download the Schema Loader that matches with the version you use from [scalardb releases](https://github.com/scalar-labs/scalardb/releases), and run the following command to load the schema.

```
$ java -jar scalardb-schema-loader-<version>.jar --config /path/to/database.properties -f emoney-storage.json
```

## Store & retrieve data with storage API

[`ElectronicMoneyWithStorage.java`](./getting-started/src/main/java/sample/ElectronicMoneyWithStorage.java)
is a simple electronic money application with storage API.
(Be careful: it is simplified for ease of reading and far from practical and is certainly not production-ready.)

```java
public class ElectronicMoneyWithStorage extends ElectronicMoney {

  private final DistributedStorage storage;

  public ElectronicMoneyWithStorage() throws IOException {
    StorageFactory factory = new StorageFactory(dbConfig);
    storage = factory.getStorage();
    storage.with(NAMESPACE, TABLENAME);
  }

  @Override
  public void charge(String id, int amount) throws ExecutionException {
    // Retrieve the current balance for id
    Get get = new Get(new Key(ID, id));
    Optional<Result> result = storage.get(get);

    // Calculate the balance
    int balance = amount;
    if (result.isPresent()) {
      int current = result.get().getValue(BALANCE).get().getAsInt();
      balance += current;
    }

    // Update the balance
    Put put = new Put(new Key(ID, id)).withValue(BALANCE, balance);
    storage.put(put);
  }

  @Override
  public void pay(String fromId, String toId, int amount) throws ExecutionException {
    // Retrieve the current balances for ids
    Get fromGet = new Get(new Key(ID, fromId));
    Get toGet = new Get(new Key(ID, toId));
    Optional<Result> fromResult = storage.get(fromGet);
    Optional<Result> toResult = storage.get(toGet);

    // Calculate the balances (it assumes that both accounts exist)
    int newFromBalance = fromResult.get().getValue(BALANCE).get().getAsInt() - amount;
    int newToBalance = toResult.get().getValue(BALANCE).get().getAsInt() + amount;
    if (newFromBalance < 0) {
      throw new RuntimeException(fromId + " doesn't have enough balance.");
    }

    // Update the balances
    Put fromPut = new Put(new Key(ID, fromId)).withValue(BALANCE, newFromBalance);
    Put toPut = new Put(new Key(ID, toId)).withValue(BALANCE, newToBalance);
    storage.put(fromPut);
    storage.put(toPut);
  }

  @Override
  public void close() {
    storage.close();
  }
}
```

Now we can run the application.
```
$ ../../gradlew run --args="-mode storage -action charge -amount 1000 -to user1"
$ ../../gradlew run --args="-mode storage -action charge -amount 0 -to merchant1"
$ ../../gradlew run --args="-mode storage -action pay -amount 100 -to merchant1 -from user1"
```

## Set up database schema for transaction

To use transaction, we can just add a key `transaction` and value as `true` in the Scalar DB schema we used.
You can create a JSON file `emoney-transaction.json` with the JSON bellow.

```json
{
  "emoney.account": {
    "transaction": true,
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

Before reapplying the schema, please drop the existing namespace first by issuing the following. 

```
$ java -jar scalardb-schema-loader-<version>.jar --config /path/to/database.properties -f emoney-storage.json -D
$ java -jar scalardb-schema-loader-<version>.jar --config /path/to/database.properties --coordinator -f emoney-transaction.json
```
- The `--coordinator` is specified because we have a table with transaction enabled in the schema.

## Store & retrieve data with transaction API

The previous application seems fine under ideal conditions, but it is problematic when some failure happens during its operation or when multiple operations occur at the same time because it is not transactional.
For example, money transfer (pay) from `A's balance` to `B's balance` is not done atomically in the application, and there might be a case where only `A's balance` is decreased (and `B's balance` is not increased) if a failure happens right after the first `put` and some money will be lost.

With the transaction capability of Scalar DB, we can make such operations to be executed with ACID properties.

Now we can update the code as follows to make it transactional.
```java
public class ElectronicMoneyWithTransaction extends ElectronicMoney {

  private final DistributedTransactionManager manager;

  public ElectronicMoneyWithTransaction() throws IOException {
    TransactionFactory factory = new TransactionFactory(dbConfig);
    manager = factory.getTransactionManager();
    manager.with(NAMESPACE, TABLENAME);
  }

  @Override
  public void charge(String id, int amount) throws TransactionException {
    // Start a transaction
    DistributedTransaction tx = manager.start();

    // Retrieve the current balance for id
    Get get = new Get(new Key(ID, id));
    Optional<Result> result = tx.get(get);

    // Calculate the balance
    int balance = amount;
    if (result.isPresent()) {
      int current = result.get().getValue(BALANCE).get().getAsInt();
      balance += current;
    }

    // Update the balance
    Put put = new Put(new Key(ID, id)).withValue(BALANCE, balance);
    tx.put(put);

    // Commit the transaction (records are automatically recovered in case of failure)
    tx.commit();
  }

  @Override
  public void pay(String fromId, String toId, int amount) throws TransactionException {
    // Start a transaction
    DistributedTransaction tx = manager.start();

    // Retrieve the current balances for ids
    Get fromGet = new Get(new Key(ID, fromId));
    Get toGet = new Get(new Key(ID, toId));
    Optional<Result> fromResult = tx.get(fromGet);
    Optional<Result> toResult = tx.get(toGet);

    // Calculate the balances (it assumes that both accounts exist)
    int newFromBalance = fromResult.get().getValue(BALANCE).get().getAsInt() - amount;
    int newToBalance = toResult.get().getValue(BALANCE).get().getAsInt() + amount;
    if (newFromBalance < 0) {
      throw new RuntimeException(fromId + " doesn't have enough balance.");
    }

    // Update the balances
    Put fromPut = new Put(new Key(ID, fromId)).withValue(BALANCE, newFromBalance);
    Put toPut = new Put(new Key(ID, toId)).withValue(BALANCE, newToBalance);
    tx.put(fromPut);
    tx.put(toPut);

    // Commit the transaction (records are automatically recovered in case of failure)
    tx.commit();
  }

  @Override
  public void close() {
    manager.close();
  }
}
```

As you can see, it's not very different from the code with `DistributedStorage`.
This code instead uses `DistributedTransactionManager` and all the CRUD operations are done through the `DistributedTransaction` object returned from `DistributedTransactionManager.start()`.

Now let's run the application with transaction mode.
```
$ ../../gradlew run --args="-mode transaction -action charge -amount 1000 -to user1"
$ ../../gradlew run --args="-mode transaction -action charge -amount 0 -to merchant1"
$ ../../gradlew run --args="-mode transaction -action pay -amount 100 -to merchant1 -from user1"
```

## Use JDBC transaction

When you use a JDBC database as a backend database, you can optionally use the native transaction manager of a JDBC database instead of the default `ConsensusCommit` transaction manager.

To use the native transaction manager, you need to set `jdbc` to a transaction manager type in **scalardb.properties** as follows.

```
scalar.db.transaction_manager=jdbc
```

You don't need to set a key `transaction` to `true` in Scalar DB schema for the native transaction manager.
So you can use the same schema file as **emoney-storage.json**.

## Further documentation

These are just simple examples of how Scalar DB is used. For more information, please take a look at the following documents.

* [Design Document](design.md)
* Javadoc
    * [scalardb](https://javadoc.io/doc/com.scalar-labs/scalardb/latest/index.html) - A library that makes non-ACID distributed databases/storages ACID-compliant
    * [scalardb-rpc](https://javadoc.io/doc/com.scalar-labs/scalardb-rpc/latest/index.html) - Scalar DB RPC libraries
    * [scalardb-server](https://javadoc.io/doc/com.scalar-labs/scalardb-server/latest/index.html) - Scalar DB Server that is the gRPC interfarce of Scalar DB
* [Requirements in the underlining databases](requirements.md)
* [Database schema in Scalar DB](schema.md)
* [Schema Loader](https://github.com/scalar-labs/scalardb/tree/master/schema-loader/README.md)
* [How to Back up and Restore](backup-restore.md)
* [Multi-storage Transactions](multi-storage-transactions.md)
* [Two-phase Commit Transactions](two-phase-commit-transactions.md)
* [Scalar DB server](scalardb-server.md)
