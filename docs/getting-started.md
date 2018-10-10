# Getting Started with Scalar DB v1

## Overview
Scalar DB v1 is a library that provides an distributed storage abstraction and client-coordinated distributed transaction on the storage.
This document briefly explains how you can get started with Scalar DB with a simple electronic money application.

## Install prerequisites

Scalar DB v1 is written in Java and uses Cassandra as an underlining storage implementation, so the following software is required to run it.
* [Oracle JDK 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (OpenJDK 8) or higher
* [Casssandra](http://cassandra.apache.org/) 3.11.x (the current stable version as of writing)
    * Take a look at [the document](http://cassandra.apache.org/download/) for how to set up Cassandra.
* Other libraries are automatically installed through gradle

From here, we assume Oracle JDK 8 and Cassandra 3.11.x are properly installed in your local environment.

# Build

For building Scalar DB, what you need to do to is pretty simple as follows.
```
$ cd /path/to/scalardb
$ ./gradlew installDist
```

Let's move to the getting started directory if you don't want to copy-and-paste the following codes.
```
$ cd docs/getting-started
```

## Set up database schema

First of all, you need to define how a data is organized (a.k.a database schema) in an application.
Currently you need to define it with storage implementation specific schema.
For the mapping between Cassandra schema and Scalar DB schema, please take a look at [this document](schema.md).
NOTICE: We are planning to have Scalar DB specific schema definition and schema loader.

In this document, let's use the following Cassandra schema.

```sql:emoney-storage.cql
DROP KEYSPACE IF EXISTS emoney;
DROP KEYSPACE IF EXISTS coordinator;

CREATE KEYSPACE emoney WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE emoney.account (
  id TEXT,
  balance INT,
  PRIMARY KEY (id)
);
```

Now, you can load the schema to Cassandra with the following command.
```
$ cqlsh -f emoney-storage.cql
```

## Store & retrieve data with storage service

Here is a simple electronic money application with storage service.
(Be careful that it's simplified for ease of reading and far from practical and production-ready.)
You can find the full source code at [here](./getting-started).

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

LET's run the application.
```
$ ../../gradlew run --args="-mode storage -action charge -amount 1000 -to user1"
$ ../../gradlew run --args="-mode storage -action pay -amount 100 -to merchant1 -from user1" 
```

## Store & retrieve data with transaction service

The previous application seems fine in normal cases, but it's problematic when some failure happens during the operation or when multiple operations occur at the same time because it is not transactional. 
In other words, money transfer (pay) from `from account's balance` to `to account's balance` is not done atomically in the application, and there might be case where only `from accont's balance` is decreased if a failure happens right after the first `put` or some money will be lost.

With transaction capability of Scalar DB, we can make such operations to be executed with ACID properties.
Before updating the code, we need to update the schema to make it transaction capable.

```sql:emoney-transaction.cql
DROP KEYSPACE IF EXISTS emoney;
DROP KEYSPACE IF EXISTS coordinator;

CREATE KEYSPACE emoney WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE KEYSPACE coordinator WITH replication = {'class': 'SimpleStrategy','replication_factor': 1 };

CREATE TABLE emoney.account (
  id TEXT,
  balance INT,
  tx_id TEXT,
  tx_prepared_at BIGINT,
  tx_committed_at BIGINT,
  tx_state INT,
  tx_version INT,
  before_balance INT,
  before_tx_id TEXT,
  before_tx_prepared_at BIGINT,
  before_tx_committed_at BIGINT,
  before_tx_state INT,
  before_tx_version INT,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS coordinator.state (
  tx_id text,
  tx_state int,
  tx_created_at bigint,
  PRIMARY KEY (tx_id)
);
```
We don't go deeper here to explan what are those, but added definitions are metadata used by client-coordinated transaction of Scalar DB.
For more detail, please take a look at [this document](schema.md).

After re-applying the schema, we can update the code like the following to make it transactional.
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

## Further documentation

These are just simple examples of how Scalar DB is used. For more information, please take a look at the following documents.
* [Design Document](/docs/design.md)
* [Javadoc](https://scalar-labs.github.io/scalardb/javadoc/)
