# Getting Started with ScalarDB

Here we assume Oracle JDK 8 and the underlying storage/database such as Cassandra are properly configured.
If you haven't done it, please configure them first by following [this](getting-started.md).

Let's move to the `getting-started` directory so that we can avoid too much copy-and-paste.
```shell
$ cd docs/getting-started
```

## Set up database schema

First of all, you need to define how the data will be organized (a.k.a database schema) in the application with ScalarDB database schema.
Here is a database schema for the sample application.
For the supported data types, please see [this doc](schema-loader.md#data-type-mapping-between-scalardb-and-the-other-databases) for more details.
You can create a JSON file `emoney.json` with the JSON below.

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

To apply the schema, download the Schema Loader that matches with the version you use from [scalardb releases](https://github.com/scalar-labs/scalardb/releases), and run the following command to load the schema.

```shell
$ java -jar scalardb-schema-loader-<version>.jar --config scalardb.properties --schema-file emoney.json --coordinator
```

The `--coordinator` option is specified because we have a table with transaction enabled in the schema.
Please see [here](schema-loader.md) for more details of the Schema Loader

## Store & retrieve data

[`ElectronicMoney.java`](./getting-started/src/main/java/sample/ElectronicMoney.java) is a simple electronic money application.
(Be careful: it is simplified for ease of reading and far from practical and is certainly not production-ready.)

```java
public class ElectronicMoney {

  private static final String SCALARDB_PROPERTIES =
          System.getProperty("user.dir") + File.separator + "scalardb.properties";
  private static final String NAMESPACE = "emoney";
  private static final String TABLENAME = "account";
  private static final String ID = "id";
  private static final String BALANCE = "balance";

  private final DistributedTransactionManager manager;

  public ElectronicMoney() throws IOException {
    TransactionFactory factory = TransactionFactory.create(SCALARDB_PROPERTIES);
    manager = factory.getTransactionManager();
  }

  public void charge(String id, int amount) throws TransactionException {
    // Start a transaction
    DistributedTransaction tx = manager.start();

    try {
      // Retrieve the current balance for id
      Get get =
              Get.newBuilder()
                      .namespace(NAMESPACE)
                      .table(TABLENAME)
                      .partitionKey(Key.ofText(ID, id))
                      .build();
      Optional<Result> result = tx.get(get);

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
      tx.put(put);

      // Commit the transaction (records are automatically recovered in case of failure)
      tx.commit();
    } catch (Exception e) {
      tx.abort();
      throw e;
    }
  }

  public void pay(String fromId, String toId, int amount) throws TransactionException {
    // Start a transaction
    DistributedTransaction tx = manager.start();

    try {
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
      Optional<Result> fromResult = tx.get(fromGet);
      Optional<Result> toResult = tx.get(toGet);

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
      tx.put(fromPut);
      tx.put(toPut);

      // Commit the transaction (records are automatically recovered in case of failure)
      tx.commit();
    } catch (Exception e) {
      tx.abort();
      throw e;
    }
  }

  public int getBalance(String id) throws TransactionException {
    // Start a transaction
    DistributedTransaction tx = manager.start();

    try {
      // Retrieve the current balances for id
      Get get =
              Get.newBuilder()
                      .namespace(NAMESPACE)
                      .table(TABLENAME)
                      .partitionKey(Key.ofText(ID, id))
                      .build();
      Optional<Result> result = tx.get(get);

      int balance = -1;
      if (result.isPresent()) {
        balance = result.get().getInt(BALANCE);
      }

      // Commit the transaction
      tx.commit();

      return balance;
    } catch (Exception e) {
      tx.abort();
      throw e;
    }
  }

  public void close() {
    manager.close();
  }
}
```

Now we can run the application.

- Charge `1000` to `user1`:
```shell
$ ./gradlew run --args="-action charge -amount 1000 -to user1"
```

- Charge `0` to `merchant1` (Just create an account for `merchant1`):
```shell
$ ./gradlew run --args="-action charge -amount 0 -to merchant1"
```

- Pay `100` from `user1` to `merchant1`:
```shell
$ ./gradlew run --args="-action pay -amount 100 -from user1 -to merchant1"
```

- Get the balance of `user1`:
```shell
$ ./gradlew run --args="-action getBalance -id user1"
```

- Get the balance of `merchant1`:
```shell
$ ./gradlew run --args="-action getBalance -id merchant1"
```

## Further reading

These are just simple examples of how ScalarDB is used. For more information, please take a look at the following documents.

* [Design Document](design.md)
* [ScalarDB Sample](https://github.com/scalar-labs/scalardb-samples/tree/main/scalardb-sample)
* [Java API Guide](api-guide.md)
* Javadoc
    * [scalardb](https://javadoc.io/doc/com.scalar-labs/scalardb/latest/index.html) - A library that makes non-ACID distributed databases/storages ACID-compliant
    * [scalardb-rpc](https://javadoc.io/doc/com.scalar-labs/scalardb-rpc/latest/index.html) - ScalarDB RPC libraries
    * [scalardb-server](https://javadoc.io/doc/com.scalar-labs/scalardb-server/latest/index.html) - ScalarDB Server that is the gRPC interface of ScalarDB
* [Requirements in the underlying databases](requirements.md)
* [Schema Loader](schema-loader.md)
* [How to Back up and Restore](backup-restore.md)
* [Multi-storage Transactions](multi-storage-transactions.md)
* [Two-phase Commit Transactions](two-phase-commit-transactions.md)
* [ScalarDB Server](scalardb-server.md)
