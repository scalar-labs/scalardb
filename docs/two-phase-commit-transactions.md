# Two-phase Commit Transactions

ScalarDB also supports two-phase commit style transactions called *Two-phase Commit Transactions*.
With Two-phase Commit Transactions, you can execute a transaction that spans multiple processes/applications (e.g., Microservices).

This document briefly explains how to execute Two-phase Commit Transactions in ScalarDB.

## Overview

ScalarDB normally executes transactions in a single transaction manager instance with a one-phase commit interface, which we call normal transactions.
In that case, you begin a transaction, execute CRUD operations, and commit the transaction in the same transaction manager instance.

In addition to normal transactions, ScalarDB also supports *Two-phase Commit Transactions*, which execute transactions with a two-phase interface.
Two-phase Commit Transactions execute a transaction that spans multiple transaction manager instances.
The transaction manager instances can be in the same process/application or in different processes/applications.
For example, if you have transaction manager instances in multiple microservices, you can execute a transaction that spans multiple microservices.

In Two-phase Commit Transactions, there are two roles, a coordinator and a participant, that collaboratively execute a single transaction.
A coordinator process and participant processes all have different transaction manager instances.
The coordinator process first begins a transaction, and the participant processes join the transaction.
After executing CRUD operations, the coordinator process and the participant processes commit the transaction by using the two-phase interface.

## Configuration

The configuration for Two-phase Commit Transactions is the same as the one for the normal transaction.

For example, you can set the following configuration when you use Cassandra:

```properties
# Consensus commit is required to use Two-phase Commit Transactions.
scalar.db.transaction_manager=consensus-commit

# Storage implementation.
scalar.db.storage=cassandra

# Comma-separated contact points.
scalar.db.contact_points=cassandra

# Port number for all the contact points.
scalar.db.contact_port=9042

# Credential information to access the database.
scalar.db.username=cassandra
scalar.db.password=cassandra
```

For details about configurations, see [ScalarDB Configurations](configurations.md).

## How to execute Two-phase Commit Transactions

This section explains how to execute Two-phase Commit Transactions.

Like a well-known two-phase commit protocol, there are two roles, a coordinator and a participant, that collaboratively execute a single transaction.
The coordinator process first begins a transaction, and the participant processes join the transaction.

### Get a `TwoPhaseCommitTransactionManager` instance

First, you need to get a `TwoPhaseCommitTransactionManager` instance to execute Two-phase Commit Transactions.

You can use `TransactionFactory` to get a `TwoPhaseCommitTransactionManager` instance as follows:

```java
TransactionFactory factory = TransactionFactory.create("<configuration file path>");
TwoPhaseCommitTransactionManager transactionManager = factory.getTwoPhaseCommitTransactionManager();
```

### Begin/Start a transaction (for coordinator)

You can begin/start a transaction as follows:

```java
// Begin a transaction
TwoPhaseCommitTransaction tx = transactionManager.begin();

Or

// Start a transaction
TwoPhaseCommitTransaction tx = transactionManager.start();
```

The process/application that begins the transaction acts as a coordinator, as mentioned.

You can also begin/start a transaction by specifying a transaction ID as follows:

```java
// Begin a transaction with specifying a transaction ID
TwoPhaseCommitTransaction tx = transactionManager.begin("<transaction ID>");

Or

// Start a transaction with specifying a transaction ID
TwoPhaseCommitTransaction tx = transactionManager.start("<transaction ID>");
```

Note that you must guarantee uniqueness of the transaction ID in this case.

And, you can get the transaction ID with `getId()` as follows:
```java
tx.getId();
```

### Join the transaction (for participants)

If you are a participant, you can join the transaction that has been begun by the coordinator as follows:

```java
TwoPhaseCommitTransaction tx = transactionManager.join("<transaction ID>")
```

You need to specify the transaction ID associated with the transaction that the coordinator has begun.

### CRUD operations for the transaction

The CRUD operations of `TwoPhaseCommitTransacton` are the same as the ones of `DistributedTransaction`.
So please see also [Java API Guide - CRUD operations](api-guide.md#crud-operations) for the details.

This is an example code for CRUD operations in Two-phase Commit Transactions:

```java
TwoPhaseCommitTransaction tx = ...

// Retrieve the current balances for ids
Get fromGet =
    Get.newBuilder()
        .namespace(NAMESPACE)
        .table(TABLE)
        .partitionKey(new Key(ID, fromId))
        .build();

Get toGet =
    Get.newBuilder()
        .namespace(NAMESPACE)
        .table(TABLE)
        .partitionKey(new Key(ID, toId))
        .build();

Optional<Result> fromResult = tx.get(fromGet);
Optional<Result> toResult = tx.get(toGet);

// Calculate the balances (it assumes that both accounts exist)
int newFromBalance = fromResult.get().getInt(BALANCE) - amount;
int newToBalance = toResult.get().getInt(BALANCE) + amount;

// Update the balances
Put fromPut =
    Put.newBuilder()
        .namespace(NAMESPACE)
        .table(TABLE)
        .partitionKey(new Key(ID, fromId))
        .intValue(BALANCE, newFromBalance)
        .build();

Put toPut =
    Put.newBuilder()
        .namespace(NAMESPACE)
        .table(TABLE)
        .partitionKey(new Key(ID, toId))
        .intValue(BALANCE, newToBalance)
        .build();

tx.put(fromPut);
tx.put(toPut);
```

### Prepare/Commit/Rollback the transaction

After finishing CRUD operations, you need to commit the transaction.
Like a well-known two-phase commit protocol, there are two phases: prepare and commit phases.
You first need to prepare the transaction in all the coordinator/participant processes, and then you need to commit the transaction in all the coordinator/participant processes as follows:

```java
TwoPhaseCommitTransaction tx = ...

try {
  // Execute CRUD operations in the coordinator/participant processes
  ...

  // Prepare phase: Prepare the transaction in all the coordinator/participant processes
  tx.prepare();
  ...

  // Commit phase: Commit the transaction in all the coordinator/participant processes
  tx.commit();
  ...
} catch (TransactionException e) {
  // When an error happans, you need to rollback the transaction in all the coordinator/participant processes
  tx.rollback();
  ...
}
```

For `prepare()`, if any of the coordinator or participant processes fails to prepare the transaction, you will need to call `rollback()` (or `abort()`) in all the coordinator/participant processes.

For `commit()`, if any of the coordinator or participant processes succeed in committing the transaction, you can consider the transaction as committed.
In other words, in that situation, you can ignore the errors in the other coordinator/participant processes.
If all the coordinator/participant processes fail to commit the transaction, you need to call `rollback()` (or `abort()`) in all the coordinator/participant processes.

For better performance, you can call `prepare()`, `commit()`, `rollback()` in the coordinator/participant processes in parallel, respectively. 

#### Validate the transaction

Depending on the concurrency control protocol, you need to call `validate()` in all the coordinator/participant processes after `prepare()` and before `commit()`:

```java
// Prepare phase 1: Prepare the transaction in all the coordinator/participant processes
tx.prepare();
...

// Prepare phase 2: Validate the transaction in all the coordinator/participant processes
tx.validate();
...

// Commit phase: Commit the transaction in all the coordinator/participant processes
tx.commit();
...
```

Similar to `prepare()`, if any of the coordinator or participant processes fails to validate the transaction, you will need to call `rollback()` (or `abort()`) in all the coordinator/participant processes. 
Also, you can call `validate()` in the coordinator/participant processes in parallel for better performance.

Currently, you need to call `validate()` when you use the `Consensus Commit` transaction manager with `EXTRA_READ` serializable strategy in `SERIALIZABLE` isolation level.
In other cases, `validate()` does nothing.

### Execute a transaction with multiple transaction manager instances

By using the APIs described above, you can execute a transaction with multiple transaction manager instances as follows:

```java
TransactionFactory factory1 =
    TransactionFactory.create("<PATH_TO_CONFIGURATION_FILE_FOR_TRANSACTION_MANAGER_1>");
TwoPhaseCommitTransactionManager transactionManager1 =
    factory1.getTwoPhaseCommitTransactionManager();

TransactionFactory factory2 =
    TransactionFactory.create("<PATH_TO_CONFIGURATION_FILE_FOR_TRANSACTION_MANAGER_2>");
TwoPhaseCommitTransactionManager transactionManager2 =
    factory2.getTwoPhaseCommitTransactionManager();

TwoPhaseCommitTransaction transaction1 = null;
TwoPhaseCommitTransaction transaction2 = null;
try {
  // Begin a transaction
  transaction1 = transactionManager1.begin();

  // Join the transaction begun by transactionManager1 with the transaction ID
  transaction2 = transactionManager2.join(transaction1.getId());

  // Execute CRUD operations in the transaction
  Optional<Result> result = transaction1.get(...);
  List<Result> results = transaction2.scan(...);
  transaction1.put(...);
  transaction2.delete(...);

  // Prepare the transaction
  transaction1.prepare();
  transaction2.prepare();

  // Validate the transaction
  transaction1.validate();
  transaction2.validate();

  // Commit the transaction. If any of the transactions succeeds to commit, you can regard the
  // transaction as committed
  AtomicReference<TransactionException> exception = new AtomicReference<>();
  boolean anyMatch =
      Stream.of(transaction1, transaction2)
          .anyMatch(
              t -> {
                try {
                  t.commit();
                  return true;
                } catch (TransactionException e) {
                  exception.set(e);
                  return false;
                }
              });

  // If all the transactions fail to commit, throw the exception and rollback the transaction
  if (!anyMatch) {
    throw exception.get();
  }
} catch (TransactionException e) {
  // Rollback the transaction
  if (transaction1 != null) {
    try {
      transaction1.rollback();
    } catch (RollbackException e1) {
      // Handle the exception
    }
  }
  if (transaction2 != null) {
    try {
    transaction2.rollback();
    } catch (RollbackException e1) {
      // Handle the exception
    }
  }
}
```

For simplicity, the above example code doesn't handle the exceptions that can be thrown by the APIs.
For more details, see [Handle exceptions](#handle-exceptions).

As previously mentioned, for `commit()`, if any of the coordinator or participant processes succeed in committing the transaction, you can regard the transaction as committed.
Also, for better performance, you can execute `prepare()`, `validate()`, and `commit()` in parallel, respectively.

### Resume a transaction

Given that processes or applications using Two-phase Commit Transactions usually involve multiple request/response exchanges, you might need to execute a transaction across various endpoints or APIs.
For such scenarios, you can use `resume()` to resume a transaction object (an instance of `TwoPhaseCommitTransaction`) that you previously began or joined. The following shows how `resume()` works:

```java
// Join (or begin) the transaction
TwoPhaseCommitTransaction tx = transactionManager.join("<transaction ID>");

...

// Resume the transaction by the trnasaction ID
TwoPhaseCommitTransaction tx1 = transactionManager.resume("<transaction ID>")
```

For example, let's say you have two services that have the following endpoints:

```java
interface ServiceA {
  void facadeEndpoint() throws Exception;
}

interface ServiceB {
  void endpoint1(String txId) throws Exception;

  void endpoint2(String txId) throws Exception;

  void prepare(String txId) throws Exception;

  void commit(String txId) throws Exception;

  void rollback(String txId) throws Exception;
}
```

And, let's say a client calls `ServiceA.facadeEndpoint()` that begins a transaction that spans the two services (`ServiceA` and `ServiceB`) as follows:

```java
public class ServiceAImpl implements ServiceA {

  private TwoPhaseCommitTransactionManager transactionManager = ...;
  private ServiceB serviceB = ...;

  ...

  @Override
  public void facadeEndpoint() throws Exception {
    TwoPhaseCommitTransaction tx = transactionManager.begin();

    try {
      ...

      // Call ServiceB endpoint1
      serviceB.endpoint1(tx.getId());

      ...

      // Call ServiceB endpoint2
      serviceB.endpoint2(tx.getId());

      ...

      // Prepare
      tx.prepare();
      serviceB.prepare(tx.getId());

      // Commit
      tx.commit();
      serviceB.commit(tx.getId());
    } catch (Exception e) {
      // Rollback
      tx.rollback();
      serviceB.rollback(tx.getId());
    }
  }
}
```

This facade endpoint in `ServiceA` calls multiple endpoints (`endpoint1()`, `endpoint2()`, `prepare()`, `commit()`, and `rollback()`) of `ServiceB`.
And in Two-phase Commit Transactions, you need to use the same transaction object across the endpoints.
For this situation, you can resume the transaction.
The implementation of `ServiceB` is as follows:

```java
public class ServiceBImpl implements ServiceB {

  private TwoPhaseCommitTransactionManager transactionManager = ...;

  ...

  @Override
  public void endpoint1(String txId) throws Exception {
    // First, you need to join the transaction
    TwoPhaseCommitTransaction tx = transactionManager.join(txId);
  }

  @Override
  public void endpoint2(String txId) throws Exception {
    // You can resume the transaction that you joined in endpoint1()
    TwoPhaseCommitTransaction tx = transactionManager.resume(txId);
  }

  @Override
  public void prepare(String txId) throws Exception {
    // You can resume the transaction
    TwoPhaseCommitTransaction tx = transactionManager.resume(txId);

    ...

    // Prepare
    tx.prepare();
  }

  @Override
  public void commit(String txId) throws Exception {
    // You can resume the transaction
    TwoPhaseCommitTransaction tx = transactionManager.resume(txId);

    ...

    // Commit
    tx.commit();
  }

  @Override
  public void rollback(String txId) throws Exception {
    // You can resume the transaction
    TwoPhaseCommitTransaction tx = transactionManager.resume(txId);

    ...

    // Rollback
    tx.rollback();
  }
}
```

As you can see, by resuming the transaction, you can share the same transaction object across multiple endpoints in `ServiceB`.

### Handle exceptions

In the previous section, you saw [how to execute a transaction with multiple transaction manager instances](#execute-a-transaction-with-multiple-transaction-manager-instances).
However, you may also need to handle exceptions properly.
If you don't handle exceptions properly, you may face anomalies or data inconsistency.
This section describes how to handle exceptions in Two-phase Commit Transactions.

Two-phase Commit Transactions are basically executed by multiple processes/applications (a coordinator and participants).
However, in this example code, we use multiple transaction managers (`transactionManager1` and `transactionManager2`) in a single process for ease of explanation.

The following example code shows how to handle exceptions in Two-phase Commit Transactions:

```java
public class Sample {
  public static void main(String[] args) throws Exception {
    TransactionFactory factory1 =
        TransactionFactory.create("<PATH_TO_CONFIGURATION_FILE_FOR_TRANSACTION_MANAGER_1>");
    TwoPhaseCommitTransactionManager transactionManager1 =
        factory1.getTwoPhaseCommitTransactionManager();

    TransactionFactory factory2 =
        TransactionFactory.create("<PATH_TO_CONFIGURATION_FILE_FOR_TRANSACTION_MANAGER_2>");
    TwoPhaseCommitTransactionManager transactionManager2 =
        factory2.getTwoPhaseCommitTransactionManager();

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

      TwoPhaseCommitTransaction transaction1 = null;
      TwoPhaseCommitTransaction transaction2 = null;
      try {
        // Begin a transaction
        transaction1 = transactionManager1.begin();

        // Join the transaction begun by transactionManager1 with the transaction ID
        transaction2 = transactionManager2.join(transaction1.getId());

        // Execute CRUD operations in the transaction
        Optional<Result> result = transaction1.get(...);
        List<Result> results = transaction2.scan(...);
        transaction1.put(...);
        transaction2.delete(...);

        // Prepare the transaction
        prepare(transaction1, transaction2);

        // Validate the transaction
        validate(transaction1, transaction2);

        // Commit the transaction
        commit(transaction1, transaction2);
      } catch (UnknownTransactionStatusException e) {
        // If you catch `UnknownTransactionStatusException` when committing the transaction, it
        // indicates that the status of the transaction, whether it has succeeded or not, is
        // unknown. In such a case, you need to check if the transaction is committed successfully
        // or not and retry it if it failed. How to identify a transaction status is delegated to
        // users
        return;
      } catch (TransactionException e) {
        // For other exceptions, you can try retrying the transaction.

        // For `CrudConflictException`, `PreparationConflictException`,
        // `ValidationConflictException`, `CommitConflictException` and
        // `TransactionNotFoundException`, you can basically retry the transaction. However, for the
        // other exceptions, the transaction may still fail if the cause of the exception is
        // nontransient. In such a case, you will exhaust the number of retries and throw the last
        // exception

        rollback(transaction1, transaction2);

        lastException = e;
      }
    }
  }

  private static void prepare(TwoPhaseCommitTransaction... transactions)
      throws TransactionException {
    // You can execute `prepare()` in parallel
    List<TransactionException> exceptions =
        Stream.of(transactions)
            .parallel()
            .map(
                t -> {
                  try {
                    t.prepare();
                    return null;
                  } catch (TransactionException e) {
                    return e;
                  }
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    // If any of the transactions failed to prepare, throw the exception
    if (!exceptions.isEmpty()) {
      throw exceptions.get(0);
    }
  }

  private static void validate(TwoPhaseCommitTransaction... transactions)
      throws TransactionException {
    // You can execute `validate()` in parallel
    List<TransactionException> exceptions =
        Stream.of(transactions)
            .parallel()
            .map(
                t -> {
                  try {
                    t.validate();
                    return null;
                  } catch (TransactionException e) {
                    return e;
                  }
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    // If any of the transactions failed to validate, throw the exception
    if (!exceptions.isEmpty()) {
      throw exceptions.get(0);
    }
  }

  private static void commit(TwoPhaseCommitTransaction... transactions)
      throws TransactionException {
    // You can execute `commit()` in parallel
    List<TransactionException> exceptions =
        Stream.of(transactions)
            .parallel()
            .map(
                t -> {
                  try {
                    t.commit();
                    return null;
                  } catch (TransactionException e) {
                    return e;
                  }
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    // If any of the transactions succeeded to commit, you can regard the transaction as committed
    if (exceptions.size() < transactions.length) {
      if (!exceptions.isEmpty()) {
        // You can log the exceptions here if you want
      }

      return; // Succeeded to commit
    }

    //
    // If all the transactions failed to commit:
    //

    // If any of the transactions failed to commit due to `UnknownTransactionStatusException`, throw
    // it because you should not retry the transaction in such a case
    Optional<TransactionException> unknownTransactionStatusException =
        exceptions.stream().filter(e -> e instanceof UnknownTransactionStatusException).findFirst();
    if (unknownTransactionStatusException.isPresent()) {
      throw unknownTransactionStatusException.get();
    }

    // Otherwise, throw the first exception
    throw exceptions.get(0);
  }

  private static void rollback(TwoPhaseCommitTransaction... transactions) {
    Stream.of(transactions)
        .parallel()
        .filter(Objects::nonNull)
        .forEach(
            t -> {
              try {
                t.rollback();
              } catch (RollbackException e) {
                // Rolling back the transaction failed. As the transaction should eventually
                // recover, you don't need to do anything further. You can simply log the occurrence
                // here
              }
            });
  }
}
```

The `begin()` API could throw `TransactionException` or `TransactionNotFoundException`.
If you catch `TransactionException`, it indicates that the transaction has failed to begin due to transient or nontransient faults. You can try retrying the transaction, but you may not be able to begin the transaction due to nontransient faults.
If you catch `TransactionNotFoundException`, it indicates that the transaction has failed to begin due to transient faults. You can retry the transaction.

The `join()` API could also throw `TransactionException` or `TransactionNotFoundException`.
You can handle these exceptions in the same way that you handle the exceptions for the `begin()` API.

The APIs for CRUD operations (`get()`, `scan()`, `put()`, `delete()`, and `mutate()`) could throw `CrudException` or `CrudConflictException`.
If you catch `CrudException`, it indicates that the transaction CRUD operation has failed due to transient or nontransient faults. You can try retrying the transaction from the beginning, but the transaction may still fail if the cause is nontransient.
If you catch `CrudConflictException`, it indicates that the transaction CRUD operation has failed due to transient faults (e.g., a conflict error). You can retry the transaction from the beginning.

The `prepare()` API could throw `PreparationException` or `PreparationConflictException`.
If you catch `PreparationException`, it indicates that preparing the transaction fails due to transient or nontransient faults. You can try retrying the transaction from the beginning, but the transaction may still fail if the cause is nontransient.
If you catch `PreparationConflictException`, it indicates that preparing the transaction has failed due to transient faults (e.g., a conflict error). You can retry the transaction from the beginning.

The `validate()` API could throw `ValidationException` or `ValidationConflictException`.
If you catch `ValidationException`, it indicates that validating the transaction fails due to transient or nontransient faults. You can try retrying the transaction from the beginning, but the transaction may still fail if the cause is nontransient.
If you catch `ValidationConflictException`, it indicates that validating the transaction has failed due to transient faults (e.g., a conflict error). You can retry the transaction from the beginning.

Also, the `commit()` API could throw `CommitException`, `CommitConflictException`, or `UnknownTransactionStatusException`.
If you catch `CommitException`, it indicates that committing the transaction fails due to transient or nontransient faults. You can try retrying the transaction from the beginning, but the transaction may still fail if the cause is nontransient.
If you catch `CommitConflictException`, it indicates that committing the transaction has failed due to transient faults (e.g., a conflict error). You can retry the transaction from the beginning.
If you catch `UnknownTransactionStatusException`, it indicates that the status of the transaction, whether it has succeeded or not, is unknown.
In such a case, you need to check if the transaction is committed successfully or retry the transaction if it has failed.
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

## Request routing in Two-phase Commit Transactions

Services using Two-phase Commit Transactions usually execute a transaction by exchanging multiple requests and responses as follows:

![](images/two_phase_commit_sequence_diagram.png)

Also, each service typically has multiple servers (or hosts) for scalability and availability and uses server-side (proxy) or client-side load balancing to distribute requests to the servers.
In such a case, since a transaction processing in Two-phase Commit Transactions is stateful, requests in a transaction must be routed to the same servers while different transactions need to be distributed to balance the load.

![](images/two_phase_commit_load_balancing.png)

There are several approaches to achieve it depending on the protocol between the services. The next section introduces some approaches for gRPC and HTTP/1.1.

### gPRC

For details about load balancing in gRPC, see [gRPC Load Balancing](https://grpc.io/blog/grpc-load-balancing/).

When you use a client-side load balancer, you can use the same gRPC connection to send requests in a transaction, which guarantees that the requests go to the same servers.

When you use a server-side (proxy) load balancer, solutions are different between an L3/L4 (transport level) load balancer and an L7 (application level) load balancer.
When using an L3/L4 load balancer, you can use the same gRPC connection to send requests in a transaction, similar to when you use a client-side load balancer.
Requests in the same gRPC connection always go to the same server in L3/L4 load balancing.
When using an L7 load balancer, since requests in the same gRPC connection don't necessarily go to the same server, you need to use cookies or similar method to route requests to the correct server.
For example, if you use [Envoy](https://www.envoyproxy.io/), you can use session affinity (sticky session) for gRPC.
Alternatively, you can use [bidirectional streaming RPC in gRPC](https://grpc.io/docs/what-is-grpc/core-concepts/#bidirectional-streaming-rpc) since the L7 load balancer distributes requests in the same stream to the same server.

### HTTP/1.1

Typically, you use a server-side (proxy) load balancer with HTTP/1.1.
When using an L3/L4 load balancer, you can use the same HTTP connection to send requests in a transaction, which guarantees the requests go to the same server.
When using an L7 load balancer, since requests in the same HTTP connection don't necessarily go to the same server, you need to use cookies or similar method to route requests to the correct server.
You can use session affinity (sticky session) in that case.

## Further reading

One of the use cases for Two-phase Commit Transactions is Microservice Transaction.
Please see the following sample to learn Two-phase Commit Transactions further:

- [Microservice Transaction Sample](https://github.com/scalar-labs/scalardb-samples/tree/main/microservice-transaction-sample)
