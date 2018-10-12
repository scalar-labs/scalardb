## Getting Started with Scalar DB v1

## Overview
Scalar DB v1 is a library that provides a distributed storage abstraction and client-coordinated distributed transaction on the storage.
This document briefly explains how you can get started with Scalar DB with a simple electronic money application.

## Install prerequisites

Scalar DB v1 is written in Java and uses Cassandra as an underlining storage implementation, so the following software is required to run it.
* [Oracle JDK 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (OpenJDK 8) or higher
* [Casssandra](http://cassandra.apache.org/) 3.11.x (the current stable version as of writing)
    * Take a look at [this document](http://cassandra.apache.org/download/) for how to set up Cassandra.
* Other libraries used from the above are automatically installed through gradle

In addition to the above, the following software is needed to use scheme tools.
* make
* [Golang](https://golang.org/)

From here, we assume Oracle JDK 8, Cassandra 3.11.x, make and Golang are properly installed in your local environment.

## Build

For building Scalar DB, what you will need to do is as follows.
```
$ SCALARDB_HOME=/path/to/scalardb
$ cd $SCALARDB_HOME
$ ./gradlew installDist
$ sudo mkdir /var/log/scalar; sudo chmod 777 /var/log/scalar
$ cd tools/schema
$ make
$ cd -
```

Let's move to the `getting-started` directory so that we can avoid too much copy-and-paste.
```
$ cd docs/getting-started
```

## Set up database schema

First of all, you need to define how the data will be organized (a.k.a database schema) in the application with Scalar DB database schema.
Here is a database schema for the sample application. For the supported data types, please see [this doc](schema.md) for more details.

```sql:emoney-storage.sdbql
REPLICATION 1

CREATE NAMESPACE emoney

CREATE TABLE emoney.account (
  id TEXT PARTITIONKEY
  balance INT
);
```

To load the schema file, please run the following command.
```
$ $SCALARDB_HOME/tools/schema/loader emoney-storage.sdbql
```

## Store & retrieve data with storage service

[`ElectronicMoneyWithStorage.java`](./getting-started/src/main/java/sample/ElectronicMoneyWithStorage.java)
is a simple electronic money application with storage service.
(Be careful: it is simplified for ease of reading and far from practical and is certainly not production-ready.)

```java:ElectronicMoneyWithStorage.java
public class ElectronicMoneyWithStorage extends ElectronicMoney {
  private final StorageService service;

  public ElectronicMoneyWithStorage() {
    Injector injector = Guice.createInjector(new StorageModule(new DatabaseConfig(props)));
    service = injector.getInstance(StorageService.class);
    service.with(NAMESPACE, TABLENAME);
  }

  @Override
  public void charge(String id, int amount) throws ExecutionException {
    // Retrieve the current balance for id
    Get get = new Get(new Key(new TextValue(ID, id)));
    Optional<Result> result = service.get(get);

    // Calculate the balance
    int balance = amount;
    if (result.isPresent()) {
      int current = ((IntValue) result.get().getValue(BALANCE).get()).get();
      balance += current;
    }

    // Update the balance
    Put put = new Put(new Key(new TextValue(ID, id))).withValue(new IntValue(BALANCE, balance));
    service.put(put);
  }

  @Override
  public void pay(String fromId, String toId, int amount) throws ExecutionException {
    // Retrieve the current balances for ids
    Get fromGet = new Get(new Key(new TextValue(ID, fromId)));
    Get toGet = new Get(new Key(new TextValue(ID, toId)));
    Optional<Result> fromResult = service.get(fromGet);
    Optional<Result> toResult = service.get(toGet);

    // Calculate the balances (it assumes that both accounts exist)
    int newFromBalance = ((IntValue) (fromResult.get().getValue(BALANCE).get())).get() - amount;
    int newToBalance = ((IntValue) (toResult.get().getValue(BALANCE).get())).get() + amount;
    if (newFromBalance < 0) {
      throw new RuntimeException(fromId + " doesn't have enough balance.");
    }

    // Update the balances
    Put fromPut =
        new Put(new Key(new TextValue(ID, fromId)))
            .withValue(new IntValue(BALANCE, newFromBalance));
    Put toPut =
        new Put(new Key(new TextValue(ID, toId))).withValue(new IntValue(BALANCE, newToBalance));
    service.put(fromPut);
    service.put(toPut);
  }

  @Override
  public void close() {
    service.close();
  }
}
```

Now we can run the application.
```
$ ../../gradlew run --args="-mode storage -action charge -amount 1000 -to user1"
$ ../../gradlew run --args="-mode storage -action charge -amount 0 -to merchant1"
$ ../../gradlew run --args="-mode storage -action pay -amount 100 -to merchant1 -from user1"
```

## Store & retrieve data with transaction service

The previous application seems fine under ideal conditions, but it is problematic when some failure happens during its operation or when multiple operations occur at the same time because it is not transactional.
For example, money transfer (pay) from `A's balance` to `B's balance` is not done atomically in the application, and there might be a case where only `A's balance` is decreased (and `B's balance` is not increased) if a failure happens right after the first `put` and some money will be lost.

With the transaction capability of Scalar DB, we can make such operations to be executed with ACID properties.
Before updating the code, we need to update the schema to make it transaction capable by adding `TRANSACTION` keyword in `CREATE TABLE`.

```sql:emoney-transaction.sdbql
REPLICATION 1

CREATE NAMESPACE emoney

CREATE TRANSACTION TABLE emoney.account (
  id TEXT PARTITIONKEY
  balance INT
);
```

Before reapplying the schema, please drop the existing namespace first by issuing the following.
(Sorry you need to issue implmentation specific commands to do this.)
```
$ cqlsh -e "drop keyspace emoney"
$ $SCALARDB_HOME/tools/schema/loader emoney-transaction.sdbql
```

Now we can update the code as follows to make it transactional.
```java:ElectronicMoneyWithTransaction.java
public class ElectronicMoneyWithTransaction extends ElectronicMoney {
  private final TransactionService service;

  public ElectronicMoneyWithTransaction() {
    Injector injector = Guice.createInjector(new TransactionModule(new DatabaseConfig(props)));
    service = injector.getInstance(TransactionService.class);
    service.with(NAMESPACE, TABLENAME);
  }

  @Override
  public void charge(String id, int amount)
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Start a transaction
    DistributedTransaction tx = service.start();

    // Retrieve the current balance for id
    Get get = new Get(new Key(new TextValue(ID, id)));
    Optional<Result> result = tx.get(get);

    // Calculate the balance
    int balance = amount;
    if (result.isPresent()) {
      int current = ((IntValue) result.get().getValue(BALANCE).get()).get();
      balance += current;
    }

    // Update the balance
    Put put = new Put(new Key(new TextValue(ID, id))).withValue(new IntValue(BALANCE, balance));
    tx.put(put);

    // Commit the transaction (records are automatically recovered in case of failure)
    tx.commit();
  }

  @Override
  public void pay(String fromId, String toId, int amount)
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Start a transaction
    DistributedTransaction tx = service.start();

    // Retrieve the current balances for ids
    Get fromGet = new Get(new Key(new TextValue(ID, fromId)));
    Get toGet = new Get(new Key(new TextValue(ID, toId)));
    Optional<Result> fromResult = tx.get(fromGet);
    Optional<Result> toResult = tx.get(toGet);

    // Calculate the balances (it assumes that both accounts exist)
    int newFromBalance = ((IntValue) (fromResult.get().getValue(BALANCE).get())).get() - amount;
    int newToBalance = ((IntValue) (toResult.get().getValue(BALANCE).get())).get() + amount;
    if (newFromBalance < 0) {
      throw new RuntimeException(fromId + " doesn't have enough balance.");
    }

    // Update the balances
    Put fromPut =
        new Put(new Key(new TextValue(ID, fromId)))
            .withValue(new IntValue(BALANCE, newFromBalance));
    Put toPut =
        new Put(new Key(new TextValue(ID, toId))).withValue(new IntValue(BALANCE, newToBalance));
    tx.put(fromPut);
    tx.put(toPut);

    // Commit the transaction (records are automatically recovered in case of failure)
    tx.commit();
  }

  @Override
  public void close() {
    service.close();
  }
}
```

As you can see, it's not very different from the code with `StorageService`.
This code instead uses `TransactionService` and all the CRUD operations are done through the `DistributedTransaction` object returned from `TransactionService.start()`.

Now let's run the application with transaction mode.
```
$ ../../gradlew run --args="-mode transaction -action charge -amount 1000 -to user1"
$ ../../gradlew run --args="-mode transaction -action charge -amount 0 -to merchant1"
$ ../../gradlew run --args="-mode transaction -action pay -amount 100 -to merchant1 -from user1"
```

## Further documentation

These are just simple examples of how Scalar DB is used. For more information, please take a look at the following documents.
* [Design Document](/docs/design.md)
* [Javadoc](https://scalar-labs.github.io/scalardb/javadoc/)
* [Database schema in Scalar DB](/tools/schema/README.md)
