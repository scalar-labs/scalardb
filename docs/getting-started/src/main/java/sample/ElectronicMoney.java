package sample;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import java.io.File;
import java.io.IOException;
import java.util.Optional;

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
