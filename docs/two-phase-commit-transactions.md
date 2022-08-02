# Two-phase Commit Transactions

Scalar DB also supports two-phase commit style transactions called *Two-phase Commit Transactions*.
With Two-phase Commit Transactions, you can execute a transaction that spans multiple processes/applications (e.g., Microservices).

This document briefly explains how to execute Two-phase Commit Transactions in Scalar DB.

## Configuration

The configuration for Two-phase Commit Transactions is the same as the one for the normal transaction.

For example, you can set the following configuration when you use Cassandra:

```properties
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

### Scalar DB Server

You can also execute Two-phase Commit Transactions through the Scalar DB Server.
You don't need a special configuration for Two-phase Commit Transactions, so you can follow [the Scalar DB Server document](scalardb-server.md) to use it.

## How to execute Two-phase Commit Transactions

This section explains how to execute Two-phase Commit Transactions.

Like a well-known two-phase commit protocol, there are two roles, a coordinator and a participant, that collaboratively execute a single transaction.
The coordinator process first starts a transaction, and the participant processes join the transaction after that.

### Get a TwoPhaseCommitTransactionManager instance

First, you need to get a `TwoPhaseCommitTransactionManager` instance to execute Two-phase Commit Transactions.
You can use `TransactionFactory` to get a `TwoPhaseCommitTransactionManager` instance as follows:
```java
TransactionFactory factory = TransactionFactory.create("<configuration file path>");
TwoPhaseCommitTransactionManager manager = factory.getTwoPhaseCommitTransactionManager();
```

### Start a transaction (coordinator only)

You can start a transaction as follows:
```java
TwoPhaseCommitTransaction tx = manager.start();
```

The process/application that starts the transaction acts as a coordinator, as mentioned.

You can also start a transaction by specifying a transaction ID as follows:
```java
TwoPhaseCommitTransaction tx = manager.start("<transaction ID>");
```

And, you can get the transaction ID with `getId()` as follows:
```java
tx.getId();
```

### Join the transaction (participant only)

If you are a participant, you can join the transaction that has been started by the coordinator as follows:
```java
TwoPhaseCommitTransaction tx = manager.join("<transaction ID>")
```

You need to specify the transaction ID associated with the transaction that the coordinator has started.

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
You first need to prepare the transaction in all the coordinator/participant processes, then you need to call in the order of coordinator's `commit()` and the participants' `commit()` as follows:
```java
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

### Request Routing in Two-phase Commit Transactions

Services using Two-phase Commit Transactions usually execute a transaction by exchanging multiple requests and responses as follows:

<p align="center">
<img src="images/two_phase_commit_sequence_diagram.png" width="400" />
</p>

Also, each service typically has multiple servers (or hosts) for scalability and availability and uses server-side (proxy) or client-side load balancing to distribute requests to the servers.
In such a case, since a transaction processing in Two-phase Commit Transactions is stateful, requests in a transaction must be routed to the same servers while different transactions need to be distributed to balance the load.

<p align="center">
<img src="images/two_phase_commit_load_balancing.png" width="500" />
</p>

There are several approaches to achieve it depending on the protocol between the services. Here, we introduce some approaches for gRPC and HTTP/1.1.

#### gPRC

Please see [this document](https://grpc.io/blog/grpc-load-balancing/) for the details of gRPC Load Balancing.

When you use a client-side load balancer, you can use the same gRPC connection to send requests in a transaction, which guarantees that the requests go to the same servers.

When you use a server-side (proxy) load balancer, solutions are different between when using L3/L4 (transport level) and L7 (application level) load balancer.
When using an L3/L4 load balancer, you can use the same gRPC connection to send requests in a transaction, similar to when you use a client-side load balancer.
Requests in the same gRPC connection always go to the same server in L3/L4 load balancing.
When using an L7 load balancer, since requests in the same gRPC connection do not necessarily go to the same server, you need to use cookies or similar for routing requests to correct server.
For example, when you use [Envoy](https://www.envoyproxy.io/), you can use session affinity (sticky session) for gRPC.
Or you can also use [Bidirectional streaming RPC in gRPC](https://grpc.io/docs/what-is-grpc/core-concepts/#bidirectional-streaming-rpc) since the L7 load balancer distributes requests in the same stream to the same server.

#### HTTP/1.1

Typically, you use a server-side (proxy) load balancer with HTTP/1.1.
When using an L3/L4 load balancer, you can use the same HTTP connection to send requests in a transaction, which guarantees the requests go to the same server.
When using an L7 load balancer, since requests in the same HTTP connection do not necessarily go to the same server, you need to use cookies or similar for routing requests to correct server.
You can use session affinity (sticky session) in that case.

#### Suspend and resume a transaction

Since services using Two-phase Commit Transactions exchange multiple requests/responses, you may need to execute a transaction across multiple endpoints/APIs.
For such cases, you can suspend and resume a transaction object (a `TwoPhaseCommitTransaction` instance) as follows:

```java
// Join the transaction
TwoPhaseCommitTransaction tx = manager.join("<transaction ID>");

....

// Suspend the transaction
manager.suspend(tx);

...

// Resume the suspended transaction by the trnasaction ID
TwoPhaseCommitTransaction tx1 = manager.resume("<transaction ID>")
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

And, let's say a client calls `ServiceA.facadeEndpoint()` that starts a transaction that spans the two services (`ServiceA` and `ServiceB`) as follows:

```java
public class ServiceAImpl implements ServiceA {

  private TwoPhaseCommitTransactionManager manager = ...;
  private ServiceB serviceB = ...;

  ...

  @Override
  public void facadeEndpoint() throws Exception {
    TwoPhaseCommitTransaction tx = manager.start();

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
For this situation, you can suspend and resume the transaction.
The implementation of `ServiceB` is as follows:

```java
public class ServiceBImpl implements ServiceB {

  private TwoPhaseCommitTransactionManager manager = ...;

  ...

  @Override
  public void endpoint1(String txId) throws Exception {
    // First, you need to join the transaction
    TwoPhaseCommitTransaction tx = manager.join(txId);

    ...

    // Suspend the transaction object
    manager.suspend(tx);
  }

  @Override
  public void endpoint2(String txId) throws Exception {
    // You can resume the transaction suspended in endpoint1()
    TwoPhaseCommitTransaction tx = manager.resume(txId);

    ...

    // Suspend the transaction object
    manager.suspend(tx);
  }

  @Override
  public void prepare(String txId) throws Exception {
    // You can resume the suspended transaction
    TwoPhaseCommitTransaction tx = manager.resume(txId);

    ...

    // Prepare
    tx.prepare();

    ...

    // Suspend the transaction object
    manager.suspend(tx);
  }

  @Override
  public void commit(String txId) throws Exception {
    // You can resume the suspended transaction
    TwoPhaseCommitTransaction tx = manager.resume(txId);
    try {
      ...

      // Commit
      tx.commit();
    } catch (Exception e) {
      // Suspend the transaction object (you need to suspend the transaction when commit fails
      // because you need to rollback the transaction after that)
      manager.suspend(tx);
    }
  }

  @Override
  public void rollback(String txId) throws Exception {
    // You can resume the suspended transaction
    TwoPhaseCommitTransaction tx = manager.resume(txId);

    ...

    // Rollback
    tx.rollback();
  }
}
```

As you can see, by suspending and resuming the transaction, you can share the same transaction object across multiple endpoints in `ServiceB`.

## Further reading

One of the use cases for Two-phase Commit Transactions is Microservice Transaction.
Please see the following sample to learn Two-phase Commit Transactions further:

- [Microservice Transaction Sample](https://github.com/scalar-labs/scalardb-samples/tree/main/microservice-transaction-sample)
