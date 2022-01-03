package sample;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import java.io.IOException;
import java.util.Optional;

public class ElectronicMoneyWithTransaction extends ElectronicMoney {

  private final DistributedTransactionManager manager;

  public ElectronicMoneyWithTransaction() throws IOException {
    TransactionFactory factory = new TransactionFactory(dbConfig);
    manager = factory.getTransactionManager();
    manager.with(NAMESPACE, TABLENAME);
  }

  @Override
  public void charge(String id, int amount) throws TransactionException {
    // Start a transaction
    DistributedTransaction tx = manager.start();

    try {
      // Retrieve the current balance for id
      Get get = new Get(new Key(ID, id));
      Optional<Result> result = tx.get(get);

      // Calculate the balance
      int balance = amount;
      if (result.isPresent()) {
        int current = result.get().getValue(BALANCE).get().getAsInt();
        balance += current;
      }

      // Update the balance
      Put put = new Put(new Key(ID, id)).withValue(BALANCE, balance);
      tx.put(put);

      // Commit the transaction (records are automatically recovered in case of failure)
      tx.commit();
    } catch (Exception e) {
      tx.abort();
      throw e;
    }
  }

  @Override
  public void pay(String fromId, String toId, int amount) throws TransactionException {
    // Start a transaction
    DistributedTransaction tx = manager.start();

    try {
      // Retrieve the current balances for ids
      Get fromGet = new Get(new Key(ID, fromId));
      Get toGet = new Get(new Key(ID, toId));
      Optional<Result> fromResult = tx.get(fromGet);
      Optional<Result> toResult = tx.get(toGet);

      // Calculate the balances (it assumes that both accounts exist)
      int newFromBalance = fromResult.get().getValue(BALANCE).get().getAsInt() - amount;
      int newToBalance = toResult.get().getValue(BALANCE).get().getAsInt() + amount;
      if (newFromBalance < 0) {
        throw new RuntimeException(fromId + " doesn't have enough balance.");
      }

      // Update the balances
      Put fromPut = new Put(new Key(ID, fromId)).withValue(BALANCE, newFromBalance);
      Put toPut = new Put(new Key(ID, toId)).withValue(BALANCE, newToBalance);
      tx.put(fromPut);
      tx.put(toPut);

      // Commit the transaction (records are automatically recovered in case of failure)
      tx.commit();
    } catch (Exception e) {
      tx.abort();
      throw e;
    }
  }

  @Override
  public void close() {
    manager.close();
  }
}
