package com.scalar.db.common;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.transaction.CrudException;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class ReadOnlyDistributedTransaction extends DecoratedDistributedTransaction {

  public ReadOnlyDistributedTransaction(DistributedTransaction transaction) {
    super(transaction);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException {
    throw new IllegalStateException(
        CoreError.MUTATION_NOT_ALLOWED_IN_READ_ONLY_TRANSACTION.buildMessage(getId()));
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException {
    throw new IllegalStateException(
        CoreError.MUTATION_NOT_ALLOWED_IN_READ_ONLY_TRANSACTION.buildMessage(getId()));
  }

  @Override
  public void insert(Insert insert) throws CrudException {
    throw new IllegalStateException(
        CoreError.MUTATION_NOT_ALLOWED_IN_READ_ONLY_TRANSACTION.buildMessage(getId()));
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException {
    throw new IllegalStateException(
        CoreError.MUTATION_NOT_ALLOWED_IN_READ_ONLY_TRANSACTION.buildMessage(getId()));
  }

  @Override
  public void update(Update update) throws CrudException {
    throw new IllegalStateException(
        CoreError.MUTATION_NOT_ALLOWED_IN_READ_ONLY_TRANSACTION.buildMessage(getId()));
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    throw new IllegalStateException(
        CoreError.MUTATION_NOT_ALLOWED_IN_READ_ONLY_TRANSACTION.buildMessage(getId()));
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    throw new IllegalStateException(
        CoreError.MUTATION_NOT_ALLOWED_IN_READ_ONLY_TRANSACTION.buildMessage(getId()));
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    throw new IllegalStateException(
        CoreError.MUTATION_NOT_ALLOWED_IN_READ_ONLY_TRANSACTION.buildMessage(getId()));
  }
}
