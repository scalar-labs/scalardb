# A Guide on How to Handle Exceptions

Handling exceptions correctly in Scalar DB is very important.
If you mishandle exceptions, your data could become inconsistent.
This document explains how to handle exceptions properly in Scalar DB.

Let's look at the following example code to see how to handle exceptions in Scalar DB.

```java
public class Sample {
  public static void main(String[] args) throws IOException, InterruptedException {
    TransactionFactory factory = new TransactionFactory(new DatabaseConfig(...));
    DistributedTransactionManager manager = factory.getTransactionManager();

    int retryCount = 0;

    while (true) {
      if (retryCount > 0) {
        // Retry the transaction three times maximum in this sample code
        if (retryCount == 3) {
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
        // If starting a transaction fails, it indicates some failure happens during a transaction,
        // so you should cancel the transaction or retry the transaction after the failure/error is
        // fixed
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
        // If you catch CrudConflictException or CommitConflictException, it indicates conflicts 
        // happen during a transaction so that you can retry the transaction
        try {
          tx.abort();
        } catch (AbortException ex) {
          // Aborting the transaction fails. You can log it here
        }
        retryCount++;
      } catch (CrudException | CommitException e) {
        // If you catch CrudException or CommitException, it indicates some failure happens, so you
        // should cancel the transaction or retry the transaction after the failure/error is fixed
        try {
          tx.abort();
        } catch (AbortException ex) {
          // Aborting the transaction fails. You can log it here
        }
        return;
      } catch (UnknownTransactionStatusException e) {
        // If you catch `UnknownTransactionException`, you are not sure if the transaction succeeds
        // or not. In such a case, you need to check if the transaction is committed successfully
        // or not and retry it if it failed. How to identify a transaction status is delegated to
        // users
        return;
      }
    }
  }
}
```

The APIs for CRUD operations (`get()`/`scan()`/`put()`/`delete()`/`mutate()`) could throw `CrudException` and `CrudConflictException`.
If you catch `CrudException`, it indicates some failure (e.g., database failure and network error) happens during a transaction, so you should cancel the transaction or retry the transaction after the failure/error is fixed.
If you catch `CrudConflictException`, it indicates conflicts happen during a transaction so that you can retry the transaction, preferably with well-adjusted exponential backoff based on your application and environment.
The sample code retries three times maximum and sleeps 100 milliseconds before retrying the transaction.

Also, the `commit()` API could throw `CommitException`, `CommitConflictException`, and `UnknownTransactionException`.
If you catch `CommitException`, like the `CrudException` case, you should cancel the transaction or retry the transaction after the failure/error is fixed.
If you catch `CommitConflictException`, like the `CrudConflictException` case, you can retry the transaction.
If you catch `UnknownTransactionException`, you are not sure if the transaction succeeds or not.
In such a case, you need to check if the transaction is committed successfully or not and retry it if it fails.
How to identify a transaction status is delegated to users.
You may want to create a transaction status table and update it transactionally with other application data so that you can get the status of a transaction from the status table.

## For Two-phase Commit Transactions

You need to handle more exceptions when you use [Two-phase Commit Transactions](two-phase-commit-transactions.md) because you additionally need to call the `prepare()` API (and the `validate()` API when required).

The `prepare()` API could throw `PreparationException` and `PreparationConflictException`.
If you catch `PreparationException`, like the `CrudException` case, you should cancel the transaction or retry the transaction after the failure/error is fixed.
If you catch `PreparationConflictException`, like the `CrudConflictException` case, you can retry the transaction.

Also, the `validate()` API could throw `ValidationException` and `ValidationConflictException`.
If you catch `ValidationException`, like the `CrudException` case, you should cancel the transaction or retry the transaction after the failure/error is fixed.
If you catch `ValidationConflictException`, like the `CrudConflictException` case, you can retry the transaction.

## References

- [Getting Started with Scalar DB](getting-started.md)
- [Two-phase Commit Transactions](two-phase-commit-transactions.md)
- [Microservice Transaction Sample](https://github.com/scalar-labs/scalardb-samples/tree/main/microservice-transaction-sample)
