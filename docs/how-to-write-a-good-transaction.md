# A Guide on How to write a good transaction

This document sets out some guidelines for writing transactions for Scalar DB.

## Exception handling

Handling exceptions correctly in Scalar DB is very important.
If you mishandle exceptions, your data could become inconsistent.
This section explains how to handle exceptions properly in Scalar DB.

The example code of transactions in Scalar DB is as follows:

```java
public class Sample {
  public static void main(String[] args) throws IOException, InterruptedException {
    TransactionFactory factory = new TransactionFactory(new DatabaseConfig(...));
    DistributedTransactionManager manager = factory.getTransactionManager();

    int retryCount = 0;

    while (true) {
      if (retryCount > 0) {
        if (retryCount == 3) {
          // Retry the transaction three times maximum in this sample code
          return;
        }
        // Sleep 100 milliseconds before retrying the transaction in this sample code
        TimeUnit.MILLISECONDS.sleep(100);
      }

      // Start a transaction
      DistributedTransaction tx;
      try {
        tx = manager.start();
      } catch (TransactionException e) {
        // If starting a transaction fails, that indicates something terrible happens, so you
        // should cancel the transaction
        return;
      }

      try {
        // Execute CRUD operations in the transaction
        Optional<Result> result = tx.get(...);
        List<Result> results = tx.scan(...);
        tx.put(...);
        tx.delete(...);

        // Commit the transaction
        tx.commit();
      } catch (CrudConflictException | CommitConflictException e) {
        // If you catch CrudConflictException or CommitConflictException, that indicates
        // transaction conflict happens, so you can retry the transaction
        try {
          tx.abort();
        } catch (AbortException ex) {
          // Aborting the transaction fails. You can log it here
        }
        retryCount++;
        continue;
      } catch (CrudException | CommitException e) {
        // If you catch CrudException or CommitException, that indicates something terrible
        // happens, so you should cancel the transaction
        try {
          tx.abort();
        } catch (AbortException ex) {
          // Aborting the transaction fails. You can log it here
        }
        return;
      } catch (UnknownTransactionStatusException e) {
        // If you catch UnknownTransactionStatusException, you are not sure the transaction succeeds
        // or not. In this case, you need to check if the transaction is committed or not, and then
        // if the transaction fails, you can retry it if needed. The way how to check it is
        // dependent on the application
        return;
      }
    }
  }
}
```

The APIs for CRUD operations (`get()`/`scan()`/`put()`/`delete()`/`mutate()`) could throw `CrudException` and `CrudConflictException`.
If you catch `CrudException`, that indicates something terrible happens (e.g., database failure, network error, and so on), so you should cancel the transaction (or you should retry the transaction after the failure/error is fixed).
If you catch `CrudConflictException`, that indicates transaction conflict happens so that you can retry the transaction after a little while.
The sample code retries three times maximum and sleeps 100 milliseconds before retrying the transaction, which can be adjusted for your application.

Also, the `commit()` API could throw `CommitException`, `CommitConflictException`, and `UnknownTransactionException`.
If you catch `CommitException`, similar to `CrudException`, you should cancel the transaction at that time.
If you catch `CommitConflictException`, similar to `CrudConflictException`, you can retry the transaction after a little while.
If you catch `UnknownTransactionException`, you are not sure the transaction succeeds or not.
In this case, you need to check if the transaction is committed or not, and then if the transaction fails, you can retry it if required.
The way how to check it is dependent on the application.

### For Two-phase Commit Transactions

In addition to the above, you need to handle more exceptions in [Two-phase Commit Transactions](two-phase-commit-transactions.md).

In Two-phase Commit Transactions, you additionally need to call the `prepare()` API (and the `validate()` API when required).

The `prepare()` API could throw `PreparationException` and `PreparationConflictException`.
If you catch `PreparationException`, that indicates something terrible happens (e.g., database failure, network error, and so on), so you should cancel the transaction (or you should retry the transaction after the failure/error is fixed).
If you catch `PreparationConflictException`, that indicates transaction conflict happens so that you can retry the transaction after a little while.

Also, the `validate()` API could throw `ValidationException` and `ValidationConflictException`.
If you catch `ValidationException`, similar to `PreparationException`, so you should cancel the transaction at that time.
If you catch `ValidationConflictException`, similar to `PreparationConflictException`, you can retry the transaction after a little while.

## Guidelines for the `Consensus Commit` transaction manager

- Blind writes are not allowed. So you must read a record before writing it in a transaction
- Currently, reading already written records is not allowed
- Currently, scan operations are not allowed for now in the `EXTRA_WRITE` serializable strategy in the `SERIALIZABLE` isolation level

### For Two-phase Commit Transactions

- You can execute `prepare()` and `validate()` in parallel in coordinator and participants
- `commit()` and `rollback()` must be called in coordinator first and then in participants
- Don't forget to call `validate()` when you use the `EXTRA_READ` serializable strategy in the `SERIALIZABLE` isolation level

Please see [Two-phase Commit Transactions](two-phase-commit-transactions.md) for the details.

## References

- [Getting Started with Scalar DB](getting-started.md)
- [Two-phase Commit Transactions](two-phase-commit-transactions.md)
- [Microservice Transaction Sample](https://github.com/scalar-labs/scalardb-samples/tree/main/microservice-transaction-sample)
