package sample;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.io.IOException;
import java.util.Optional;

public class ElectronicMoneyWithStorage extends ElectronicMoney {

  private final DistributedStorage storage;

  public ElectronicMoneyWithStorage() throws IOException {
    StorageFactory factory = new StorageFactory(dbConfig);
    storage = factory.getStorage();
    storage.with(NAMESPACE, TABLENAME);
  }

  @Override
  public void charge(String id, int amount) throws ExecutionException {
    // Retrieve the current balance for id
    Get get = new Get(new Key(ID, id));
    Optional<Result> result = storage.get(get);

    // Calculate the balance
    int balance = amount;
    if (result.isPresent()) {
      int current = result.get().getValue(BALANCE).get().getAsInt();
      balance += current;
    }

    // Update the balance
    Put put = new Put(new Key(ID, id)).withValue(BALANCE, balance);
    storage.put(put);
  }

  @Override
  public void pay(String fromId, String toId, int amount) throws ExecutionException {
    // Retrieve the current balances for ids
    Get fromGet = new Get(new Key(ID, fromId));
    Get toGet = new Get(new Key(ID, toId));
    Optional<Result> fromResult = storage.get(fromGet);
    Optional<Result> toResult = storage.get(toGet);

    // Calculate the balances (it assumes that both accounts exist)
    int newFromBalance = fromResult.get().getValue(BALANCE).get().getAsInt() - amount;
    int newToBalance = toResult.get().getValue(BALANCE).get().getAsInt() + amount;
    if (newFromBalance < 0) {
      throw new RuntimeException(fromId + " doesn't have enough balance.");
    }

    // Update the balances
    Put fromPut = new Put(new Key(ID, fromId)).withValue(BALANCE, newFromBalance);
    Put toPut = new Put(new Key(ID, toId)).withValue(BALANCE, newToBalance);
    storage.put(fromPut);
    storage.put(toPut);
  }

  @Override
  public void close() {
    storage.close();
  }
}
