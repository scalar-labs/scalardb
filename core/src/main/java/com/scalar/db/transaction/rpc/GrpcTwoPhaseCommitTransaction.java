package com.scalar.db.transaction.rpc;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.transaction.common.AbstractTwoPhaseCommitTransaction;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class GrpcTwoPhaseCommitTransaction extends AbstractTwoPhaseCommitTransaction {

  private final String txId;
  private final GrpcTwoPhaseCommitTransactionOnBidirectionalStream stream;
  private final boolean isCoordinator;
  private final GrpcTwoPhaseCommitTransactionManager manager;

  public GrpcTwoPhaseCommitTransaction(
      String txId,
      GrpcTwoPhaseCommitTransactionOnBidirectionalStream stream,
      boolean isCoordinator,
      GrpcTwoPhaseCommitTransactionManager manager) {
    this.txId = txId;
    this.stream = stream;
    this.isCoordinator = isCoordinator;
    this.manager = manager;
  }

  @Override
  public String getId() {
    return txId;
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException {
    updateTransactionExpirationTime();
    get = copyAndSetTargetToIfNot(get);
    return stream.get(get);
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    updateTransactionExpirationTime();
    scan = copyAndSetTargetToIfNot(scan);
    return stream.scan(scan);
  }

  @Override
  public void put(Put put) throws CrudException {
    updateTransactionExpirationTime();
    put = copyAndSetTargetToIfNot(put);
    stream.mutate(put);
  }

  @Override
  public void put(List<Put> puts) throws CrudException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    updateTransactionExpirationTime();
    delete = copyAndSetTargetToIfNot(delete);
    stream.mutate(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    mutate(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    updateTransactionExpirationTime();
    mutations = copyAndSetTargetToIfNot(mutations);
    stream.mutate(mutations);
  }

  @Override
  public void prepare() throws PreparationException {
    updateTransactionExpirationTime();
    stream.prepare();
  }

  @Override
  public void validate() throws ValidationException {
    updateTransactionExpirationTime();
    stream.validate();
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    try {
      stream.commit();
    } finally {
      if (!isCoordinator) {
        manager.removeTransaction(getId());
      }
    }
  }

  @Override
  public void rollback() throws RollbackException {
    try {
      stream.rollback();
    } finally {
      if (!isCoordinator) {
        manager.removeTransaction(getId());
      }
    }
  }

  private void updateTransactionExpirationTime() {
    if (!isCoordinator) {
      manager.updateTransactionExpirationTime(txId);
    }
  }
}
