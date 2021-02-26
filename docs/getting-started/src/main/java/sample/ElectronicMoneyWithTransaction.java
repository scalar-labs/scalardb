package sample;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.service.TransactionModule;
import com.scalar.db.service.TransactionService;

import java.io.IOException;
import java.util.Optional;

public class ElectronicMoneyWithTransaction extends ElectronicMoney {
  private final TransactionService service;

  public ElectronicMoneyWithTransaction() throws IOException {
    Injector injector = Guice.createInjector(new TransactionModule(dbConfig));
    service = injector.getInstance(TransactionService.class);
    service.with(NAMESPACE, TABLENAME);
  }

  @Override
  public void charge(String id, int amount) throws TransactionException {
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
  public void pay(String fromId, String toId, int amount) throws TransactionException {
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
