package com.scalar.db.transaction.rpc;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.AbstractTwoPhaseCommitTransaction;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationException;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class GrpcTwoPhaseCommitTransaction extends AbstractTwoPhaseCommitTransaction {

  private final String txId;
  private final GrpcTwoPhaseCommitTransactionOnBidirectionalStream stream;

  public GrpcTwoPhaseCommitTransaction(
      String txId, GrpcTwoPhaseCommitTransactionOnBidirectionalStream stream) {
    this.txId = txId;
    this.stream = stream;
  }

  @Override
  public String getId() {
    return txId;
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException {
    get = copyAndSetTargetToIfNot(get);
    return stream.get(get);
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    scan = copyAndSetTargetToIfNot(scan);
    return stream.scan(scan);
  }

  @Override
  public void put(Put put) throws CrudException {
    put = copyAndSetTargetToIfNot(put);
    stream.mutate(put);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    delete = copyAndSetTargetToIfNot(delete);
    stream.mutate(delete);
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    mutate(deletes);
  }

  @Override
  public void insert(Insert insert) throws CrudException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void update(Update update) throws CrudException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    mutations = copyAndSetTargetToIfNot(mutations);
    stream.mutate(mutations);
  }

  @Override
  public void prepare() throws PreparationException {
    stream.prepare();
  }

  @Override
  public void validate() throws ValidationException {
    stream.validate();
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    stream.commit();
  }

  @Override
  public void rollback() throws RollbackException {
    stream.rollback();
  }

  @Override
  public void abort() throws AbortException {
    stream.abort();
  }
}
