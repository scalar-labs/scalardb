package com.scalar.db.transaction.autocommit;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.common.AbstractDistributedTransaction;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class AutoCommitTransaction extends AbstractDistributedTransaction {

  private final String txId;
  private final DistributedStorage storage;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public AutoCommitTransaction(String txId, DistributedStorage storage) {
    this.txId = txId;
    this.storage = storage;
  }

  @Override
  public String getId() {
    return txId;
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException {
    try {
      return storage.get(copyAndSetTargetToIfNot(get).withConsistency(Consistency.LINEARIZABLE));
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, txId);
    }
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    try (Scanner scanner =
        storage.scan(copyAndSetTargetToIfNot(scan).withConsistency(Consistency.LINEARIZABLE))) {
      return scanner.all();
    } catch (ExecutionException | IOException e) {
      throw new CrudException(e.getMessage(), e, txId);
    }
  }

  @Override
  public void put(Put put) throws CrudException {
    try {
      storage.put(copyAndSetTargetToIfNot(put).withConsistency(Consistency.LINEARIZABLE));
    } catch (NoMutationException e) {
      throw new UnsatisfiedConditionException(e.getMessage(), e, txId);
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, txId);
    }
  }

  @Override
  public void put(List<Put> puts) throws CrudException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    try {
      storage.delete(copyAndSetTargetToIfNot(delete).withConsistency(Consistency.LINEARIZABLE));
    } catch (NoMutationException e) {
      throw new UnsatisfiedConditionException(e.getMessage(), e, txId);
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, txId);
    }
  }

  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    mutate(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    checkArgument(!mutations.isEmpty(), CoreError.EMPTY_MUTATIONS_SPECIFIED.buildMessage());
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        put((Put) mutation);
      } else if (mutation instanceof Delete) {
        delete((Delete) mutation);
      }
    }
  }

  @Override
  public void commit() {
    // Do nothing
  }

  @Override
  public void rollback() {
    // Do nothing
  }

  @VisibleForTesting
  DistributedStorage getStorage() {
    return storage;
  }
}
