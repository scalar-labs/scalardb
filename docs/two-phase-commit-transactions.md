{% include end-of-support.html %}

# Two-phase Commit Transactions

Scalar DB also supports two-phase commit style transactions called *Two-phase Commit Transactions*.
With Two-phase Commit Transactions, you can execute a transaction that spans multiple processes/applications (e.g., Microservices).

This document briefly explains how to execute Two-phase Commit Transactions in Scalar DB.

## Configuration

The configuration for Two-phase Commit Transactions is the same as the one for the transaction API.

For example, you can set the following configuration when you use Cassandra:
```
# Comma separated contact points
scalar.db.contact_points=cassandra

# Port number for all the contact points. Default port number for each database is used if empty.
scalar.db.contact_port=9042

# Credential information to access the database
scalar.db.username=cassandra
scalar.db.password=cassandra

# Storage implementation. Either cassandra or cosmos or dynamo or jdbc can be set. Default storage is cassandra.
scalar.db.storage=cassandra
```

Please see [Getting Started](getting-started.md) for configurations of other databases/storages.

### Scalar DB server

You can also execute Two-phase Commit Transactions through the Scalar DB server.
You don't need a special configuration for Two-phase Commit Transactions, so you can follow [the Scalar DB server document](scalardb-server.md) to use it.

## How to execute Two-phase Commit Transactions

This section explains how to execute Two-phase Commit Transactions.

Like a well-known two-phase commit protocol, there are two roles, a coordinator and a participant, that collaboratively execute a single transaction.
The coordinator process first starts a transaction, and the participant processes join the transaction after that.

### Get a TwoPhaseCommitTransactionManager instance

First, you need to get a `TwoPhaseCommitTransactionManager` instance to execute Two-phase Commit Transactions.
You can use `TransactionFactory` to get a `TwoPhaseCommitTransactionManager` instance as follows:
```Java
TransactionFactory factory = new TransactionFactory(new DatabaseConfig(new File("<configuration file path>")));
TwoPhaseCommitTransactionManager manager = factory.getTwoPhaseCommitTransactionManager();
```

### Start a transaction (coordinator only)

You can start a transaction as follows:
```Java
TwoPhaseCommitTransaction tx = manager.start();
```

The process/application that starts the transaction acts as a coordinator, as mentioned.

You can also start a transaction by specifying a transaction ID as follows:
```Java
TwoPhaseCommitTransaction tx = manager.start("<transaction ID>");
```

And, you can get the transaction ID with `getId()` as follows:
```Java
tx.getId();
```

### Join the transaction (participant only)

If you are a participant, you can join the transaction that has been started by the coordinator as follows:
```Java
TwoPhaseCommitTransaction tx = manager.join("<transaction ID>")
```

You need to specify the transaction ID associated with the transaction that the coordinator has started.

#### Resume the transaction (participant only)

You can get the transaction object (the `TwoPhaseCommitTransaction` instance) that you have already joined with `TwoPhaseCommitTransactionManager.resume()`:
```Java
TwoPhaseCommitTransaction tx = manager.resume("<transaction ID>")
```

`TwoPhaseCommitTransactionManager` manages the transaction objects that you have joined, and you can get it with the transaction ID.

### CRUD operations for the transaction

The way to execute CRUD operations in Two-phase Commit Transactions is the same as the transaction API.
`TwoPhaseCommitTransacton` has `get()`/`put()`/`delete()`/`mutate()` to execute CRUD operations.

This is an example code for CRUD operations in Two-phase Commit Transactions:
```java
TwoPhaseCommitTransaction tx = ...

// Retrieve the current balances for ids
Get fromGet = new Get(new Key(ID, fromId));
Get toGet = new Get(new Key(ID, toId));
Optional<Result> fromResult = tx.get(fromGet);
Optional<Result> toResult = tx.get(toGet);

// Calculate the balances (it assumes that both accounts exist)
int newFromBalance = fromResult.get().getValue(BALANCE).get().getAsInt() - amount;
int newToBalance = toResult.get().getValue(BALANCE).get().getAsInt() + amount;

// Update the balances
Put fromPut = new Put(new Key(ID, fromId)).withValue(BALANCE, newFromBalance);
Put toPut = new Put(new Key(ID, toId)).withValue(BALANCE, newToBalance);
tx.put(fromPut);
tx.put(toPut);
```

### Prepare/Commit/Rollback the transaction

After finishing CRUD operations, you need to commit the transaction.
Like a well-known two-phase commit protocol, there are two phases: prepare and commit phases.
You first need to prepare the transaction in all the coordinator/participant processes, then you need to call in the order of coordinator's `commit()` and the participants' `commit()` as follows:
```Java
TwoPhaseCommitTransaction tx = ...

try {
  // Execute CRUD operations in the coordinator/participant processes
  ...

  // Prepare phase: Prepare the transaction in all the coordinator/participant processes
  tx.prepare();
  ...

  // Commit phase: Commit the transaction in all the coordinator/participant processes
  tx.commit()
  ...
} catch (TransactionException e) {
  // When an error happans, you need to rollback the transaction in all the coordinator/participant processes
  tx.rollback();
  ...
}
```

If an error happens, you need to call `rollback()` in all the coordinator/participant processes.
Note that you need to call it in the coordinator process first, and then call it in the participant processes in parallel.

You can call `prepare()` in the coordinator/participant processes in parallel.
Similarly, you can also call `commit()` in the participant processes in parallel.

#### Validate the transaction

Depending on the concurrency control protocol, you need to call `validate()` in all the coordinator/participant processes after `prepare()` and before `commit()`:
```java
// Prepare phase 1: Prepare the transaction in all the coordinator/participant processes
tx.prepare();
...

// Prepare phase 2: Validate the transaction in all the coordinator/participant processes
tx.validate()
...

// Commit phase: Commit the transaction in all the coordinator/participant processes
tx.commit()
...
```

Similar to `prepare()`, you can call `validate()` in the coordinator/participant processes in parallel.

Currently, you need to call `validate()` when you use the `Consensus Commit` transaction manager with `EXTRA_READ` serializable strategy in `SERIALIZABLE` isolation level.
In other cases, `validate()` does nothing.

## Further documentation

One of the use cases for Two-phase Commit Transactions is Microservice Transaction.
Please see the following sample to learn Two-phase Commit Transactions further:
- [Microservice Transaction Sample](https://github.com/scalar-labs/scalardb-samples/tree/main/microservice-transaction-sample)
