package sample;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.service.StorageModule;
import com.scalar.db.service.StorageService;

import java.util.Optional;

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
