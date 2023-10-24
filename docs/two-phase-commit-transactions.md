# Transactions with a Two-Phase Commit Interface

ScalarDB supports executing transactions with a two-phase commit interface. With the two-phase commit interface, you can execute a transaction that spans multiple processes or applications, like in a microservice architecture.

This page explains how transactions with a two-phase commit interface work in ScalarDB and how to configure and execute them in ScalarDB.

## How transactions with a two-phase commit interface work in ScalarDB

ScalarDB normally executes transactions in a single transaction manager instance with a one-phase commit interface. In transactions with a one-phase commit interface, you start a transaction, execute CRUD operations, and commit the transaction in the same transaction manager instance.

In ScalarDB, you can execute transactions with a two-phase commit interface that span multiple transaction manager instances. The transaction manager instances can be in the same process or application, or the instances can be in different processes or applications. For example, if you have transaction manager instances in multiple microservices, you can execute a transaction that spans multiple microservices.

In transactions with a two-phase commit interface, there are two roles—Coordinator and a participant—that collaboratively execute a single transaction.

The Coordinator process and the participant processes all have different transaction manager instances. The Coordinator process first starts a transaction, and the participant processes join the transaction. After executing CRUD operations, the Coordinator process and the participant processes commit the transaction by using the two-phase interface.

## How to execute transactions with a two-phase commit interface

To execute a two-phase commit transaction, you must get the transaction manager instance. Then, the Coordinator process can start the transaction, and the participant can process the transaction.

### Get a `TwoPhaseCommitTransactionManager` instance

You first need to get a `TwoPhaseCommitTransactionManager` instance to execute transactions with a two-phase commit interface.

To get a `TwoPhaseCommitTransactionManager` instance, you can use `TransactionFactory` as follows:

```java
TransactionFactory factory = new TransactionFactory(new DatabaseConfig(new File("<CONFIGURATION_FILE_PATH>")));
TwoPhaseCommitTransactionManager manager = factory.getTwoPhaseCommitTransactionManager();
```

### Start a transaction (for Coordinator)

For the process or application that starts the transaction to act as Coordinator, you should use the following `start` method:

```java
// Start a transaction.
TwoPhaseCommitTransaction tx = manager.start();
```

Alternatively, you can use the `start` method for a transaction by specifying a transaction ID as follows:

```java
// Start a transaction by specifying a transaction ID.
TwoPhaseCommitTransaction tx = manager.start("<TRANSACTION_ID>");
```

### Join a transaction (for participants)

For participants, you can join a transaction by specifying the transaction ID associated with the transaction that Coordinator has started or begun as follows:

```java
TwoPhaseCommitTransaction tx = manager.join("<TRANSACTION_ID>")
```

{% capture notice--info %}
**Note**

To get the transaction ID with `getId()`, you can specify the following:

```java
tx.getId();
```
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

### CRUD operations for the transaction

The CRUD operations for `TwoPhaseCommitTransacton` are the same as the transaction API. `TwoPhaseCommitTransacton` uses `get()`, `put()`, `delete()`, and `mutate()` to execute CRUD operations.

The following is example code for CRUD operations in transactions with a two-phase commit interface:

```java
TwoPhaseCommitTransaction tx = ...

// Retrieve the current balances by ID.
Get fromGet = new Get(new Key(ID, fromId));
Get toGet = new Get(new Key(ID, toId));
Optional<Result> fromResult = tx.get(fromGet);
Optional<Result> toResult = tx.get(toGet);

// Calculate the balances (assuming that both accounts exist).
int newFromBalance = fromResult.get().getValue(BALANCE).get().getAsInt() - amount;
int newToBalance = toResult.get().getValue(BALANCE).get().getAsInt() + amount;

// Update the balances.
Put fromPut = new Put(new Key(ID, fromId)).withValue(BALANCE, newFromBalance);
Put toPut = new Put(new Key(ID, toId)).withValue(BALANCE, newToBalance);
tx.put(fromPut);
tx.put(toPut);
```

### Prepare, commit, or roll back a transaction

After finishing CRUD operations, you need to commit the transaction. As with the standard two-phase commit protocol, there are two phases: prepare and commit.

In all the Coordinator and participant processes, you need to prepare and then commit the transaction as follows:

```java
TwoPhaseCommitTransaction tx = ...

try {
  // Execute CRUD operations in the Coordinator and participant processes.
  ...

  // Prepare phase: Prepare the transaction in all the Coordinator and participant processes.
  tx.prepare();
  ...

  // Commit phase: Commit the transaction in all the Coordinator and participant processes.
  tx.commit()
  ...
} catch (TransactionException e) {
  // If an error happens, you will need to roll back the transaction in all the Coordinator and participant processes.
  tx.rollback();
  ...
}
```

For `prepare()`, if any of the Coordinator or participant processes fail to prepare the transaction, you will need to call `rollback()` (or `abort()`) in all the Coordinator and participant processes.

For `commit()`, if any of the Coordinator or participant processes successfully commit the transaction, you can consider the transaction as committed. When a transaction has been committed, you can ignore any errors in the other Coordinator and participant processes. If all the Coordinator and participant processes fail to commit the transaction, you will need to call `rollback()` (or `abort()`) in all the Coordinator and participant processes.

#### Validate the transaction

Depending on the concurrency control protocol, you need to call `validate()` in all the Coordinator and participant processes after `prepare()` and before `commit()`, as shown below:

```java
// Prepare phase 1: Prepare the transaction in all the Coordinator and participant processes.
tx.prepare();
...

// Prepare phase 2: Validate the transaction in all the Coordinator and participant processes.
tx.validate()
...

// Commit phase: Commit the transaction in all the Coordinator and participant processes.
tx.commit()
...
```

Similar to `prepare()`, if any of the Coordinator or participant processes fail to validate the transaction, you will need to call `rollback()` (or `abort()`) in all the Coordinator and participant processes. In addition, you can call `validate()` in the Coordinator and participant processes in parallel for better performance.

{% capture notice--info %}
**Note**

When using the [Consensus Commit](configurations.md#use-consensus-commit-directly) transaction manager with `EXTRA_READ` set as the value for `scalar.db.consensus_commit.serializable_strategy` and `SERIALIZABLE` set as the value for `scalar.db.consensus_commit.isolation_level`, you need to call `validate()`. However, if you are not using Consensus Commit, specifying `validate()` will not have any effect.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

### Resume the transaction (for participants)

You can get the transaction object (an instance of `TwoPhaseCommitTransaction`) that you have previously joined. `TwoPhaseCommitTransactionManager` manages the transaction objects that you have joined, and you can get it by using the transaction ID.

The following shows how `resume()` works:

```java
TwoPhaseCommitTransaction tx = manager.resume("<TRANSACTION_ID>")
```

## Hands-on tutorial

One of the use cases for transactions with a two-phase commit interface is microservice transactions. For a hands-on tutorial, see [Create a Sample Application That Supports Microservice Transactions](https://github.com/scalar-labs/scalardb-samples/tree/main/microservice-transaction-sample).
