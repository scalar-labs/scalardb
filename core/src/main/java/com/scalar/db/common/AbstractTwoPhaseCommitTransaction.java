package com.scalar.db.common;

import static com.google.common.base.Preconditions.checkArgument;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.util.ScalarDbUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public abstract class AbstractTwoPhaseCommitTransaction implements TwoPhaseCommitTransaction {

  private Optional<String> namespace;
  private Optional<String> tableName;

  public AbstractTwoPhaseCommitTransaction() {
    namespace = Optional.empty();
    tableName = Optional.empty();
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void with(String namespace, String tableName) {
    this.namespace = Optional.ofNullable(namespace);
    this.tableName = Optional.ofNullable(tableName);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void withNamespace(String namespace) {
    this.namespace = Optional.ofNullable(namespace);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<String> getNamespace() {
    return namespace;
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void withTable(String tableName) {
    this.tableName = Optional.ofNullable(tableName);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<String> getTable() {
    return tableName;
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    checkArgument(!mutations.isEmpty(), CoreError.EMPTY_MUTATIONS_SPECIFIED.buildMessage());
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        put((Put) mutation);
      } else if (mutation instanceof Delete) {
        delete((Delete) mutation);
      } else if (mutation instanceof Insert) {
        insert((Insert) mutation);
      } else if (mutation instanceof Upsert) {
        upsert((Upsert) mutation);
      } else {
        assert mutation instanceof Update;
        update((Update) mutation);
      }
    }
  }

  @Override
  public List<BatchResult> batch(List<? extends Operation> operations) throws CrudException {
    checkArgument(!operations.isEmpty(), CoreError.EMPTY_OPERATIONS_SPECIFIED.buildMessage());
    List<BatchResult> ret = new ArrayList<>();
    for (Operation operation : operations) {
      if (operation instanceof Get) {
        Optional<Result> result = get((Get) operation);
        ret.add(new BatchResultImpl(result));
      } else if (operation instanceof Scan) {
        List<Result> results = scan((Scan) operation);
        ret.add(new BatchResultImpl(results));
      } else if (operation instanceof Put) {
        put((Put) operation);
        ret.add(BatchResultImpl.PUT_BATCH_RESULT);
      } else if (operation instanceof Insert) {
        insert((Insert) operation);
        ret.add(BatchResultImpl.INSERT_BATCH_RESULT);
      } else if (operation instanceof Upsert) {
        upsert((Upsert) operation);
        ret.add(BatchResultImpl.UPSERT_BATCH_RESULT);
      } else if (operation instanceof Update) {
        update((Update) operation);
        ret.add(BatchResultImpl.UPDATE_BATCH_RESULT);
      } else if (operation instanceof Delete) {
        delete((Delete) operation);
        ret.add(BatchResultImpl.DELETE_BATCH_RESULT);
      } else {
        throw new AssertionError("Unknown operation: " + operation);
      }
    }
    return ret;
  }

  protected <T extends Operation> List<T> copyAndSetTargetToIfNot(List<T> operations) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(operations, namespace, tableName);
  }

  protected Get copyAndSetTargetToIfNot(Get get) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(get, namespace, tableName);
  }

  protected Scan copyAndSetTargetToIfNot(Scan scan) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(scan, namespace, tableName);
  }

  protected Put copyAndSetTargetToIfNot(Put put) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(put, namespace, tableName);
  }

  protected Delete copyAndSetTargetToIfNot(Delete delete) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(delete, namespace, tableName);
  }

  protected Insert copyAndSetTargetToIfNot(Insert insert) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(insert, namespace, tableName);
  }

  protected Upsert copyAndSetTargetToIfNot(Upsert upsert) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(upsert, namespace, tableName);
  }

  protected Update copyAndSetTargetToIfNot(Update update) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(update, namespace, tableName);
  }
}
